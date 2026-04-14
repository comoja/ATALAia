"""
Script para entrenar el modelo de regresión ML.
Ejecutar: python -m Sentinel.train_reg_model
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

async def train_reg():
    print("=" * 50)
    print("ENTRENAMIENTO DEL MODELO DE REGRESIÓN ML")
    print("=" * 50)
    
    apiKey, interval, _, nVelas, _ = getParametros()
    symbol = "EUR/USD"
    
    print(f"Descargando datos de {symbol}...")
    df = await twelvedata.getTimeSeries({"symbol": symbol, "interval": interval, "apikey": apiKey, "outputSize": 5000})
    
    if df is None or df.empty:
        print("Error: No se pudieron obtener datos")
        return
    
    print(f"Datos obtenidos: {len(df)} velas")
    print("Calculando features...")
    
    df = calculateFeatures(df)
    
    print("Entrenando modelo de regresión...")
    mlModel.trainAndSaveRegModel(df, config.MODEL_REG_FILE_PATH)
    
    print("Cargando modelo de regresión...")
    model = mlModel.loadRegModel(config.MODEL_REG_FILE_PATH)
    
    if model:
        print("Modelo de regresión entrenado y cargado correctamente")
    else:
        print("Error al cargar el modelo de regresión")

if __name__ == "__main__":
    asyncio.run(train_reg())
