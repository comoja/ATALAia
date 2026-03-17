"""
Main entry point for the refactored Trading Bot.
"""
import asyncio
import logging
import sys
import os

# --- Path Setup ---
# This allows the script to find both `backend` and `middlend` modules.
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

# --- Module Imports ---
from middlend.utils.loggerConfig import setupLogging
from middlend.core.bot import TradingBot
from middlend.ml import model as mlModel
from middlend.analysis.technical import calculateFeatures
from middlend import configConstants as config

# --- External Project Imports ---
from middlend.scheduler.autoScheduler import getTiempoEspera, isRestTime
from middlend.data.dataLoader import getParametros
from middlend.config.settings import INTERVAL, INTERVALmax
from middlend.api import twelvedata as oldTwelvedataApi # For training data

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
        trainingDf = await oldTwelvedataApi.getTimeSeries("EUR/USD", interval, apiKey, nVelas=5000)

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
    
    logger.info("Bot inicializado correctamente. Iniciando bucle principal...")

    # --- Main Loop ---
    wasOperating = True  # Assume we're operating initially
    
    while True:
        try:
            isOperating = not isRestTime()
            
            # Detectar cambio de estado
            if wasOperating and not isOperating:
                # Se acaba de detener (viernes 17:00)
                logger.info("====== MERCADO CERRADO - Bot detenido hasta el domingo 17:00 ======")
            elif not wasOperating and isOperating:
                # Se acaba de iniciar (domingo 17:00)
                logger.info("====== MERCADO ABIERTO - Bot iniciado ======")
            
            wasOperating = isOperating
            
            if isOperating:
                logger.info("Iniciando ciclo de análisis...")
                # Obtener parámetros ANTES del análisis
                apiKey, intervaloActual, nombreKey, nVelas, _ = getParametros()
                await bot.runAnalysisCycle()
                
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
