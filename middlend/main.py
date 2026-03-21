"""
Main entry point for the refactored Trading Bot.
"""
import asyncio
import logging
import sys
import os
import time
import pandas as pd

# --- Path Setup ---
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

# --- Module Imports ---
from middlend.utils.loggerConfig import setupLoggingMiddlend as setupLogging
from middlend.core.bot import TradingBot
from middlend.core.SMA20_200 import SMABot
from middlend.core.SCLPNG1h_1min import SCLPNGBot
from middlend.ml import model as mlModel
from middlend.analysis.technical import calculateFeatures
from middleware.config import constants as config
from middleware.database import dbManager

# --- External Project Imports ---
from middleware.scheduler.autoScheduler import getTiempoEspera, isRestTime
from middlend.data.dataLoader import getParametros
from middleware.config import settings
from middleware.api import twelvedata as tdApi

INTERVAL = settings.INTERVAL
INTERVALmax = settings.INTERVALmax

async def preload_time_series_data(symbolsToScan, apiKey, interval, nVelas):
    """
    Obtiene los datos de time series una sola vez para todos los símbolos.
    Retorna un diccionario: {symbol: dataframe}
    """
    preloaded_data = {}
    for symbolInfo in symbolsToScan:
        symbol = symbolInfo['symbol']
        logger.info(f"Obteniendo datos de 12Data para {symbol}...")
        df = await tdApi.getTimeSeries(symbol, interval, apiKey, nVelas)
        if df is not None and len(df) >= 100:
            preloaded_data[symbol] = df
        else:
            logger.warning(f"[{symbol}] Datos insuficientes ({len(df) if df is not None else 0} velas).")
    return preloaded_data


async def run_sequential_analysis(bot, sma_bot, sclpng_bot, symbolsToScan, apiKey, interval, nVelas):
    """
    Ejecuta el análisis de forma secuencial:
    1. Obtener API key para este símbolo (rota entre cuentas)
    2. Descargar símbolo
    3. Ejecutar Sniper
    4. Ejecutar SMA20-200
    5. Ejecutar SCLPNG1h_1min
    6. Esperar 9 segundos mínimos entre descargas (para no exceder 8 llamadas/min)
    """
    MIN_WAIT_SECONDS = 3
    
    for idx, symbolInfo in enumerate(symbolsToScan):
        symbol = symbolInfo['symbol']
        start_time = time.time()
        
        # Obtener API key para este símbolo (rota entre cuentas)
        symbolApiKey, _, nombreKey, _, _ = getParametros()
        logger.info(f"Procesando {symbol} ({idx+1}/{len(symbolsToScan)}) con cuenta {nombreKey}...")
        
        # Guardar nombreKey en symbolInfo para usar en logs de descarga
        symbolInfo['cuenta'] = nombreKey
        
        # 1. Descargar datos para este símbolo
        logger.info(f"Descargando datos de 12Data para {symbol} [{nombreKey}]...")
        #df = await tdApi.getTimeSeriesSymbolWithDB ( symbol, interval, symbolApiKey, nVelas, nombreKey)
        df = await tdApi.getTimeSeries(symbol, interval, symbolApiKey, nVelas, nombreKey)
        
        if df is None or len(df) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes. Saltando...")
            if idx < len(symbolsToScan) - 1:
                await asyncio.sleep(MIN_WAIT_SECONDS)
            continue
        
        # Crear diccionario con los datos para este símbolo
        preloaded_data = {symbol: df}
        symbolInfo['intervalo'] = interval
        
        # 2. Ejecutar Sniper
        logger.info(f"Ejecutando estrategia Sniper para {symbol}...")
        await bot.runAnalysisCycle_for_symbol(symbolInfo, preloaded_data, symbolApiKey)
        
        # 3. Ejecutar SMA20-200
        logger.info(f"Ejecutando estrategia SMA20-200 para {symbol}...")
        try:
            logger.info(f"[MAIN] >>> Entrando SMA BOT para {symbol}")
            
            await sma_bot.runAnalysisCycle_for_symbol(
                symbolInfo, 
                preloaded_data, 
                symbolApiKey
            )
            
            logger.info(f"[MAIN] <<< SMA BOT terminó para {symbol}")

        except Exception as e:
            logger.error(f"[MAIN] ERROR en SMA BOT para {symbol}: {e}", exc_info=True)
        
        # 4. Ejecutar SCLPNG1h_1min
        logger.info(f"Ejecutando estrategia SCLPNG1h_1min para {symbol}...")
        await sclpng_bot.runAnalysisCycle_for_symbol(symbolInfo, preloaded_data, symbolApiKey)
        
        # 5. Calcular tiempo total y esperar lo necesario para cumplir 3s mínimo entre descargas
        elapsed = time.time() - start_time
        wait_time = max(0, MIN_WAIT_SECONDS - elapsed)
        
        if wait_time > 0:
            logger.info(f"Esperando {wait_time:.1f}s para cumplir límite de 12Data.com (8 llamadas/min)...")
            await asyncio.sleep(wait_time)
        else:
            logger.info(f"Ciclo completado en {elapsed:.1f}s (sin espera adicional)")

# 1. Set up logging at the very beginning
setupLogging()
logger = logging.getLogger(__name__)

async def main():
    """
    Main execution function.
    """
    logger.info("=============================================")
    logger.info("====== Inicializando Bot de Trading Middlend ======")
    logger.info("=============================================")

    # --- Model Loading/Training ---
    # Attempt to load the pre-trained model
    model = mlModel.loadModel(config.MODEL_FILE_PATH)

    if model is None:
        logger.warning("No se encontró modelo pre-entrenado. Intentando entrenar uno nuevo.")
        logger.info("Obteniendo gran dataset para entrenamiento inicial del modelo...")
        
        # We need data to train. We'll use the old API module temporarily
        # to get a large chunk of data. This should be a separate, offline script in a real scenario.
        apiKey, interval, _, nVelas, _ = getParametros()
        
        # Fetch a large number of candles just for training
        trainingDf = await tdApi.getTimeSeries("EUR/USD", interval, apiKey, nVelas=5000)

        if trainingDf is not None and not trainingDf.empty:
            logger.info("Calculando features (incluyendo ATR) para datos de entrenamiento...")
            trainingDf = calculateFeatures(trainingDf)
            # This is a synchronous call, which is fine for a one-off training task
            mlModel.trainAndSaveModel(trainingDf, config.MODEL_FILE_PATH)
            # Try loading again
            model = mlModel.loadModel(config.MODEL_FILE_PATH)
        
        if model is None:
            logger.critical("Error al entrenar o cargar el modelo. El bot no puede continuar sin un modelo.")
            return

    # --- Bot Initialization ---
    bot = TradingBot(mlModelInstance=model)
    sma_bot = SMABot()
    sclpng_bot = SCLPNGBot()
    
    logger.info("Bot inicializado correctamente. Iniciando bucle principal...")

    # --- Main Loop ---
    wasOperating = True  # Assume we're operating initially
    
    while True:
        try:
            isOperating = not isRestTime()
            
            # Detectar cambio de estado
            if wasOperating and not isOperating:
                # Se acaba de detener (viernes 17:00)
                logger.info("\n\n====== MERCADO CERRADO - Bot detenido ======\n\n")
            elif not wasOperating and isOperating:
                # Se acaba de iniciar (domingo 17:00)
                logger.info("\n\n====== MERCADO ABIERTO - Bot iniciado ======\n\n")
            
            wasOperating = isOperating
            
            if isOperating:
                logger.info("Iniciando ciclo de análisis...")
                await asyncio.sleep(5)
                
                # Obtener parámetros ANTES del análisis
                apiKey, intervaloActual, nombreKey, nVelas, _ = getParametros()
                
                # Obtener símbolos a analizar
                symbolsToScan = dbManager.getSymbols()
                
                # Ejecutar análisis de forma SECUENCIAL (descarga -> Sniper -> SMA -> SCLPNG -> espera 9s)
                logger.info("Iniciando análisis secuencial con límite de 12Data.com...")
                #await run_sequential_analysis(bot, sma_bot, sclpng_bot, symbolsToScan, apiKey, intervaloActual, nVelas)
                await run_sequential_analysis(bot, sma_bot, sclpng_bot, symbolsToScan, apiKey, INTERVAL, nVelas)
                # Calcular espera para el PRÓXIMO ciclo
                # Usamos el intervalo del ciclo actual: si fue 1h, el próximo será 15min (y viceversa)
                proximoIntervalo = INTERVAL if intervaloActual == INTERVALmax else INTERVAL
                
                if "min" in proximoIntervalo:
                    proximaEspera = int(proximoIntervalo.replace("min", ""))
                else:
                    proximaEspera = int(proximoIntervalo.replace("h", "")) * 60
                
                logger.info(f"Ciclo completado ({intervaloActual}). Próximo análisis en {proximaEspera}min ({proximoIntervalo})\n\n")
                await getTiempoEspera(proximaEspera)
            
            else:
                logger.info("Mercado cerrado o en horario de descanso. Durmiendo.")
                _, _, _, _, esperaMin = getParametros()
                await getTiempoEspera(esperaMin)

        except Exception as e:
            logger.critical(f"Ocurrió un error inesperado en el bucle principal: {e}", exc_info=True)
            logger.info("Reiniciando bucle después de 60 segundos de espera...")
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido manualmente. ¡Adiós!")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Ocurrió un error fatal fuera del bucle principal: {e}", exc_info=True)
        sys.exit(1)
