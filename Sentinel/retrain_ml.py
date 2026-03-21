"""
Script para reentrenar el modelo de ML.
Ejecutar: python -m Sentinel.retrain_ml
"""
import asyncio
import sys
import os

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging
from middleware.config import constants as config
from Sentinel.ml import model as mlModel
from Sentinel.analysis.technical import calculateFeatures
from Sentinel.data.dataLoader import getParametros
from middleware.api import twelvedata

setupLogging()

async def retrain():
    print("=" * 50)
    print("REENTRENAMIENTO DEL MODELO ML")
    print("=" * 50)
    
    # Obtener parámetros
    apiKey, interval, _, nVelas, _ = getParametros()
    symbol = "EUR/USD"  # Símbolo para entrenamiento
    
    print(f"Descargando datos de {symbol}...")
    df = await twelvedata.getTimeSeries(symbol, interval, apiKey, nVelas=5000)
    
    if df is None or df.empty:
        print("❌ Error: No se pudieron obtener datos")
        return
    
    print(f"Datos obtenidos: {len(df)} velas")
    print("Calculando features...")
    
    df = calculateFeatures(df)
    
    print("Entrenando modelo...")
    mlModel.trainAndSaveModel(df, config.MODEL_FILE_PATH)
    
    print("Cargando modelo...")
    model = mlModel.loadModel(config.MODEL_FILE_PATH)
    
    if model:
        print("✅ Modelo reentrenado y cargado correctamente")
    else:
        print("❌ Error al cargar el modelo")

if __name__ == "__main__":
    asyncio.run(retrain())
