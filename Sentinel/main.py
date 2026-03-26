"""
Main entry point for the refactored Trading Bot.
"""
import asyncio
import logging
import sys
import os
import time
import pandas as pd
import pytz
from datetime import datetime
from zoneinfo import ZoneInfo

# --- Path Setup ---
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

# --- Module Imports ---
from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging
from Sentinel.core.bot import TradingBot
from Sentinel.core.SMA20_200 import SMABot
from Sentinel.core.SclpngNY import SCLPNGBot
from Sentinel.ml import model as mlModel
from Sentinel.analysis.technical import calculateFeatures
from middleware.utils.momentum import momentum as momentumAnalyzer
from middleware.config import constants as config
from middleware.database import dbManager
from middleware.database.dbManager import get_min_wait_time

# --- External Project Imports ---
from middleware.scheduler.autoScheduler import getTiempoEspera, isRestTime
from Sentinel.data.dataLoader import getParametros
from middleware.config import settings
from middleware.config.constants import API_KEYS, FESTIVOS, TIMEZONE

TIMEZONE_LOCAL = ZoneInfo(TIMEZONE)
MAX_CANDLES_PER_CALL = 5000

def resampleData(df: pd.DataFrame, targetInterval: str) -> pd.DataFrame:
    if targetInterval == "5min":
        return df
    intervalMap = {
        "15min": "15T",
        "30min": "30T",
        "1h": "1H",
        "2h": "2H",
        "4h": "4H"
    }
   
    rule = intervalMap.get(targetInterval, targetInterval)
    dfResampled = df.resample(rule).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    return dfResampled
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
        logger.info(f"Obteniendo datos de 12Data para {symbol} (intervalo base 5min)...")
        df = await tdApi.getTimeSeries({"symbol": symbol, "interval": "5min", "apikey": apiKey, "outputSize": nVelas})
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
    5. Ejecutar SclpngNY
    6. Esperar 9 segundos mínimos entre descargas (para no exceder 8 llamadas/min)
    """
    MIN_WAIT_SECONDS = get_min_wait_time()
    
    for idx, symbolInfo in enumerate(symbolsToScan):
        symbol = symbolInfo['symbol']
        start_time = time.time()
        
        # Obtener API key para este símbolo (rota entre cuentas)
        symbolApiKey, _, nombreKey, _, _ = getParametros()
        logger.info(f"Procesando {symbol} ({idx+1}/{len(symbolsToScan)}) con cuenta {nombreKey}...")
        
        # Guardar nombreKey en symbolInfo para usar en logs de descarga
        symbolInfo['cuenta'] = nombreKey
        
        # 1. Descargar datos para este símbolo (siempre 5min - intervalo base)
        logger.info(f"Descargando datos de {dbManager.DATA_SOURCE} para {symbol} [{nombreKey}] (5min)...")
        
        params = {
                "symbol": symbol,
                "interval": "5min",
                "apikey": symbolApiKey,
                "outputSize": MAX_CANDLES_PER_CALL,
                "timezone":TIMEZONE_LOCAL
            }
        df = await tdApi.getTimeSeries(params)
        
        if df is None or len(df) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes. Saltando...")
            if idx < len(symbolsToScan) - 1:
                await asyncio.sleep(MIN_WAIT_SECONDS)
            continue
        
        # Limpiar NaN básicos de los datos descargados
        df = df.dropna(subset=['close', 'high', 'low', 'open'])
        logger.info(f"[{symbol}] Tras limpieza inicial: {len(df)} velas válidas")
        
        if len(df) < 200:
            logger.warning(f"[{symbol}] Datos insuficientes tras limpieza. Saltando...")
            if idx < len(symbolsToScan) - 1:
                await asyncio.sleep(MIN_WAIT_SECONDS)
            continue
        
        # Calcular momentum para este símbolo
        logger.info(f"Calculando momentum para {symbol}...")
        try:
            estadosMomentum = await momentumAnalyzer(symbol, df)
            symbolInfo['momentum'] = estadosMomentum
        except Exception as e:
            logger.error(f"[{symbol}] Error calculando momentum: {e}")
            symbolInfo['momentum'] = None
        
        # Resamplear datos para cada estrategia
        df15m = resampleData(df, "15min") if interval != "15min" else df
        
        # 2. Ejecutar Sniper (usa intervalo configurado)
        logger.info(f"Ejecutando estrategia Sniper para {symbol}...")
        preloadedDataSniper = {symbol: df15m if interval != "15min" else df}
        symbolInfo['intervalo'] = interval
        await bot.runAnalysisCycle_for_symbol(symbolInfo, preloadedDataSniper, symbolApiKey)
        
        # 3. Ejecutar SMA20-200 (usa 15min)
        logger.info(f"Ejecutando estrategia SMA20-200 para {symbol}...")
        preloadedDataSMA = {symbol: df15m}
        symbolInfo['intervalo'] = "15min"
        try:
            logger.info(f"[MAIN] >>> Entrando SMA BOT para {symbol}")
            
            await sma_bot.runAnalysisCycle_for_symbol(
                symbolInfo, 
                preloadedDataSMA, 
                symbolApiKey
            )
            
            logger.info(f"[MAIN] <<< SMA BOT terminó para {symbol}")

        except Exception as e:
            logger.error(f"[MAIN] ERROR en SMA BOT para {symbol}: {e}", exc_info=True)
        
        # 4. Ejecutar SclpngNY (solo después de 9:00 NY)
        ahoraMX = datetime.now(pytz.timezone('America/Mexico_City'))
        
        esDST = SCLPNGBot.isNyDST(ahoraMX)
        if esDST:
            inicioAperturaNY = ahoraMX.replace(hour=6, minute=0, second=0, microsecond=0)
            finAperturaNY = ahoraMX.replace(hour=7, minute=0, second=0, microsecond=0)
            cierreNY = ahoraMX.replace(hour=12, minute=0, second=0, microsecond=0)
            horaNY = 8
            horaFinNY = 9
            horaCierreNY = 14
        else:
            inicioAperturaNY = ahoraMX.replace(hour=7, minute=0, second=0, microsecond=0)
            finAperturaNY = ahoraMX.replace(hour=8, minute=0, second=0, microsecond=0)
            cierreNY = ahoraMX.replace(hour=13, minute=0, second=0, microsecond=0)
            horaNY = 8
            horaFinNY = 9
            horaCierreNY = 14
        
        logger.info(f"[SCLPNG] Hora MX: {ahoraMX.hour}, Inicio NY: {horaNY}:00 NY ({inicioAperturaNY.hour}:00 MX), Fin: {horaFinNY}:00 NY ({finAperturaNY.hour}:00 MX), Cierre: {horaCierreNY}:00 NY ({cierreNY.hour}:00 MX)")
        
        horasDesdeFinApertura = (ahoraMX - finAperturaNY).total_seconds() / 3600
        
        if ahoraMX <= finAperturaNY:
            logger.info(f"[SCLPNG] Aún no abre sesión NY (hora {ahoraMX.hour}), saltando...")
        elif horasDesdeFinApertura > 2:
            logger.info(f"[SCLPNG] Han pasado más de 2 horas ({horasDesdeFinApertura:.1f}h) desde fin apertura NY, saltando...")
        elif sclpng_bot.signalGenerada:
            logger.info(f"[SCLPNG] Señales ya generadas, saltando...")
        else:
            logger.info(f"Ejecutando estrategia SclpngNY para {symbol}...")
            
            dfIndex = df.index
            if dfIndex.tz is None:
                dfIndex = dfIndex.tz_localize('America/Mexico_City')
            
            maskApertura = (dfIndex >= inicioAperturaNY) & (dfIndex < finAperturaNY)
            dfApertura = df.loc[maskApertura]
            
            if len(dfApertura) > 0:
                precioMaximo = dfApertura['high'].max()
                precioMinimo = dfApertura['low'].min()
                
                logger.info(f"[SCLPNG] Nivel sesión NY: Max={precioMaximo}, Min={precioMinimo}")
                
                from middleware.utils.communications import alertaInmediata
                textNivel = (
                    f"<b>NIVELES APERTURA NY - {symbol}</b>\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"<center><b>Sesión: {inicioAperturaNY.strftime('%H:%M')} - {finAperturaNY.strftime('%H:%M')}</b></center>\n\n"
                    f"  🔴 MAX: {precioMaximo:,.4f}\n"
                    f"  🟢 MIN: {precioMinimo:,.4f}\n"
                    f"━━━━━━━━━━━━━━━\n"
                )
                if horasDesdeFinApertura <= 0.5:  # 30 minutos
                    await alertaInmediata(1, textNivel)
                
                maskPostApertura = dfIndex >= finAperturaNY
                dfPostApertura = df.loc[maskPostApertura]
                
                logger.info(f"[SCLPNG] Velas post-apertura: {len(dfPostApertura)}")
                
                preloadedDataSCLPNG = {symbol: dfPostApertura}
                symbolInfo['intervalo'] = "5min"
                symbolInfo['precioMaximo'] = precioMaximo
                symbolInfo['precioMinimo'] = precioMinimo
                symbolInfo['finAperturaNY'] = finAperturaNY
                symbolInfo['cierreNY'] = cierreNY
                
                await sclpng_bot.runAnalysisCycleForSymbol(symbolInfo, preloadedDataSCLPNG, symbolApiKey)
            else:
                logger.warning(f"[SCLPNG] No se encontraron velas en período de apertura NY")
        
        # 5. Calcular tiempo total y esperar lo necesario para cumplir 3s mínimo entre descargas
        elapsed = time.time() - start_time
        wait_time = max(0, MIN_WAIT_SECONDS - elapsed)
        
        if wait_time > 0:
            logger.info(f"Esperando {wait_time:.1f}s para cumplir límite de 12Data.com (8 llamadas/min)...\n\n")
            await asyncio.sleep(wait_time)
        else:
            logger.info(f"Ciclo completado en {elapsed:.1f}s (sin espera adicional)\n\n")

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
        trainingDf = await tdApi.getTimeSeries({"symbol": "EUR/USD", "interval": interval, "apikey": apiKey, "outputSize": 5000})

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
