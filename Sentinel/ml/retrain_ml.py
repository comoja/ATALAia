"""
Script para reentrenar el modelo de ML usando datos de la BD.
Ejecutar: python -m Sentinel.retrain_ml
"""
import asyncio
import sys
import os
import pandas as pd
from sqlalchemy import text

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging
from middleware.config import constants as config
from Sentinel.ml import model as mlModel
from Sentinel.analysis.technical import calculateFeatures
from middleware.database import dbManager

setupLogging()

async def retrain():
    """Función de reentrenamiento para usar como módulo o standalone."""
    print("=" * 50)
    print("REENTRENAMIENTO DEL MODELO ML (desde BD)")
    print("=" * 50)
    
    try:
        symbols_data = dbManager.getSentinelSymbols()
        symbols = [s['symbol'] for s in symbols_data if isinstance(s, dict) and 'symbol' in s]
        print(f"Símbolos activos encontrados en ConnectionPool: {symbols}")
        
        all_data = []
        
        for symbol in symbols:
            print(f"Descargando datos de {symbol}...")
            
            # Obtener velas via ConnectionPool (limit alto para entrenamiento)
            rows = dbManager._call_connection_pool(
                "GET",
                "/candles/symbol-query",
                params={"symbol": symbol, "timeframe": "5min", "limit": 10000}
            )
            
            if not rows:
                # Fallback: intentar con 15min si no hay 5min
                rows = dbManager._call_connection_pool(
                    "GET",
                    "/candles/symbol-query",
                    params={"symbol": symbol, "timeframe": "15min", "limit": 5000}
                )
            
            if rows and isinstance(rows, list):
                df = pd.DataFrame(rows)
                if 'timestamp' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df = df.sort_values('timestamp').reset_index(drop=True)
                    df['symbol'] = symbol
                    
                    # Convertir columnas numéricas a float ANTES del resample
                    for col in ['open', 'high', 'low', 'close', 'volume']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    
                    # Resample a 15min
                    df.set_index('timestamp', inplace=True)
                    df_resampled = df[['open', 'high', 'low', 'close', 'volume']].resample('15min').agg({
                        'open': 'first',
                        'high': 'max',
                        'low': 'min',
                        'close': 'last',
                        'volume': 'sum'
                    }).dropna()
                    df_resampled.reset_index(inplace=True)
                    df_resampled['symbol'] = symbol
                    
                    if len(df_resampled) > 100:
                        all_data.append(df_resampled)
                        print(f"  -> {len(df_resampled)} velas (15min)")
                    else:
                        print(f"  -> No suficientes datos para 15min")
        
        if not all_data:
            print("❌ Error: No se pudieron obtener datos de la BD")
            return

        
        # Combinar todos los datos
        df_combined = pd.concat(all_data, ignore_index=True)
        print(f"\nTotal de velas: {len(df_combined)}")
        
        # Convertir a tipos correctos para talib
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df_combined[col] = df_combined[col].astype('float64')
        
        # Calcular features
        print("Calculando features...")
        df_combined = calculateFeatures(df_combined)
        
        # Eliminar NaN
        df_combined = df_combined.dropna()
        print(f"Después de dropna: {len(df_combined)} velas")
        
        # Entrenar modelo
        print("Entrenando modelo...")
        mlModel.trainAndSaveModel(df_combined, config.MODEL_FILE_PATH)
        
        print("Cargando modelo...")
        model = mlModel.loadModel(config.MODEL_FILE_PATH)
        
        if model:
            print("✅ Modelo reentrenado y cargado correctamente")
        else:
            print("❌ Error al cargar el modelo")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(retrain())