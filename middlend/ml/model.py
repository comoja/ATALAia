"""
Module for training, saving, loading, and using the ML model.
"""
import logging
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
import joblib
import os
from typing import Tuple

# Assuming new structure allows these imports
from middlend.configConstants import (
    MODEL_PARAMS, MODEL_FEATURES, MODEL_FILE_PATH,
    ML_TARGET_HORIZON_LOW_VOL, ML_TARGET_HORIZON_HIGH_VOL, ML_TARGET_HORIZON_NORMAL_VOL
)

logger = logging.getLogger(__name__)

def calculateAtr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculates the Average True Range (ATR) indicator.
    """
    high = df["high"]
    low = df["low"]
    close = df["close"]
    
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr = tr.rolling(window=period).mean()
    
    return atr

def defineMlTarget(df: pd.DataFrame) -> pd.DataFrame:
    """
    Defines the target variable for the machine learning model based on future price movement.
    """
    dfTarget = df.copy()
    
    # Calculate ATR if not present
    if "atr" not in dfTarget.columns:
        dfTarget["atr"] = calculateAtr(dfTarget)
    
    # Determine prediction horizon based on relative volatility
    atrAvg = dfTarget["atr"].rolling(60).mean()
    volRelativeVal = (dfTarget["atr"] / atrAvg).iloc[-1]
    
    if volRelativeVal < 0.8:
        horizon = ML_TARGET_HORIZON_LOW_VOL
    elif volRelativeVal > 1.2:
        horizon = ML_TARGET_HORIZON_HIGH_VOL
    else:
        horizon = ML_TARGET_HORIZON_NORMAL_VOL
        
    # The target is 1 if the price in `horizon` periods is higher than current price + 0.5 * ATR
    futurePrice = dfTarget["close"].shift(-horizon)
    targetPrice = dfTarget["close"] + (dfTarget["atr"] * 0.5)
    
    dfTarget["target"] = (futurePrice > targetPrice).astype(int)
    
    return dfTarget

def cleanDataForModel(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series]:
    """
    Cleans the DataFrame by removing NaNs and infinities and separates features (X) and target (y).
    """
    dfClean = df.replace([np.inf, -np.inf], np.nan).dropna(subset=MODEL_FEATURES + ["target"])
    
    X = dfClean[MODEL_FEATURES]
    y = dfClean["target"]
    
    return X, y

def trainAndSaveModel(df: pd.DataFrame, modelPath: str = MODEL_FILE_PATH):
    """
    Trains a new RandomForestClassifier and saves it to a file.
    """
    logger.info("Iniciando entrenamiento del modelo de ML...")
    
    dfWithTarget = defineMlTarget(df)
    X, y = cleanDataForModel(dfWithTarget)
    
    if len(X) < 100:
        logger.warning(f"No hay suficientes datos limpios para entrenar el modelo (solo {len(X)} filas). Abortando entrenamiento.")
        return

    # Exclude the most recent 12 candles from training to prevent lookahead bias with the target
    X_train = X.iloc[:-12]
    yTrain = y.iloc[:-12]

    try:
        model = RandomForestClassifier(**MODEL_PARAMS)
        model.fit(X_train, yTrain)
        
        # Ensure the directory exists
        os.makedirs(os.path.dirname(modelPath), exist_ok=True)
        joblib.dump(model, modelPath)
        
        logger.info(f"✅ Modelo entrenado y guardado exitosamente en: {modelPath}")

    except Exception as e:
        logger.critical(f"❌ Error crítico durante el entrenamiento o guardado del modelo: {e}", exc_info=True)

def loadModel(modelPath: str = MODEL_FILE_PATH) -> RandomForestClassifier | None:
    """
    Loads a pre-trained model from a file.
    """
    try:
        if not os.path.exists(modelPath):
            logger.warning(f"No se encontró un modelo entrenado en {modelPath}. Se necesita entrenar un modelo primero.")
            return None
            
        model = joblib.load(modelPath)
        logger.info(f"Modelo cargado exitosamente desde: {modelPath}")
        return model
    except Exception as e:
        logger.error(f"Error al cargar el modelo desde {modelPath}: {e}", exc_info=True)
        return None

def predictProba(model: RandomForestClassifier, X: pd.DataFrame) -> float | None:
    """
    Makes a probability prediction on the latest data point.
    """
    if model is None or X.empty:
        logger.warning("No se puede predecir: el modelo o los datos de entrada están vacíos.")
        return None
        
    try:
        # Predict probability for the last row (latest data)
        # The result is an array of probabilities for [class_0, class_1]
        probaForClass1 = model.predict_proba(X.iloc[-1:])[0][1]
        return float(probaForClass1)
    except Exception as e:
        logger.error(f"Error durante la predicción de probabilidad: {e}", exc_info=True)
        return None
