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
from middlend.api import twelvedata as oldTwelvedataApi # For training data

# 1. Set up logging at the very beginning
setupLogging()
logger = logging.getLogger(__name__)

async def main():
    """
    Main execution function.
    """
    logger.info("=============================================")
    logger.info("====== Initializing Middlend Trading Bot ======")
    logger.info("=============================================")

    # --- Model Loading/Training ---
    # Attempt to load the pre-trained model
    model = mlModel.loadModel(config.MODEL_FILE_PATH)

    if model is None:
        logger.warning("No pre-trained model found. Attempting to train a new one.")
        logger.info("Fetching a large dataset for initial model training...")
        
        # We need data to train. We'll use the old API module temporarily
        # to get a large chunk of data. This should be a separate, offline script in a real scenario.
        apiKey, interval, _, nVelas, _ = getParametros()
        
        # Fetch a large number of candles just for training
        trainingDf = await oldTwelvedataApi.getTimeSeries("EUR/USD", interval, apiKey, nVelas=5000)

        if trainingDf is not None and not trainingDf.empty:
            logger.info("Calculating features (including ATR) for training data...")
            trainingDf = calculateFeatures(trainingDf)
            # This is a synchronous call, which is fine for a one-off training task
            mlModel.trainAndSaveModel(trainingDf, config.MODEL_FILE_PATH)
            # Try loading again
            model = mlModel.loadModel(config.MODEL_FILE_PATH)
        
        if model is None:
            logger.critical("Failed to train or load the model. The bot cannot continue without a model.")
            return

    # --- Bot Initialization ---
    bot = TradingBot(mlModelInstance=model)
    
    logger.info("Bot initialized successfully. Starting main loop...")

    # --- Main Loop ---
    while True:
        try:
            if not isRestTime():
                logger.info("Not rest time. Starting analysis cycle.")
                await bot.runAnalysisCycle()
            else:
                logger.info("Market is closed or in rest time. Sleeping.")

            # Get the wait time for the next candle
            # Note: This logic is inherited from the original project.
            _, _, _, _, esperaMin = getParametros()
            await getTiempoEspera(esperaMin)

        except Exception as e:
            logger.critical(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
            logger.info("Restarting loop after a 60-second delay...")
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped manually. Goodbye!")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"A fatal error occurred outside the main loop: {e}", exc_info=True)
        sys.exit(1)
