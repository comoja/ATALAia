"""
Script para entrenar el modelo de regresión ML usando datos de la BD.
Ejecutar: python Sentinel/ml/train_reg_model.py
"""
import asyncio
import sys
import os
import pandas as pd
from sqlalchemy import text

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging
from middleware.config import constants as config
from Sentinel.ml import model as mlModel
from Sentinel.ml.model import MODEL_REG_FILE_PATH
from Sentinel.analysis.technical import calculateFeatures

setupLogging()

async def train_reg():
    print("=" * 50)
    print("ENTRENAMIENTO DEL MODELO DE REGRESIÓN ML (desde BD)")
    print("=" * 50)
    
    try:
        from middleware.config.constants import dbConfig
        from sqlalchemy import create_engine
        
        engine = create_engine(
            f"mysql+mysqlconnector://{dbConfig['user']}:{dbConfig['password']}@{dbConfig['host']}/{dbConfig['database']}"
        )
        
        # Obtener únicamente los símbolos activos de Sentinel desde la tabla máster `symbols`
        with engine.connect() as conn:
            result = conn.execute(text("SELECT symbol FROM symbols WHERE activoSentinel = 1 ORDER BY symbol"))
            symbols = [row[0] for row in result.fetchall()]
        
        print(f"Símbolos encontrados en BD: {symbols}")
        
        all_data = []
        
        for symbol in symbols:
            print(f"Descargando datos de {symbol}...")
            
            with engine.connect() as conn:
                result = conn.execute(
                    text("""
                        SELECT timestamp, open, high, low, close, volume 
                        FROM candles 
                        WHERE symbol = :symbol AND timeframe = '5min'
                        ORDER BY timestamp ASC
                        LIMIT 10000
                    """),
                    {"symbol": symbol}
                )
                rows = result.fetchall()
            
            if rows:
                df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.sort_values('timestamp').reset_index(drop=True)
                
                # Resample a 15min
                df.set_index('timestamp', inplace=True)
                df_resampled = df.resample('15min').agg({
                    'open': 'first',
                    'high': 'max',
                    'low': 'min',
                    'close': 'last',
                    'volume': 'sum'
                }).dropna()
                df_resampled.reset_index(inplace=True)
                
                if len(df_resampled) > 100:
                    all_data.append(df_resampled)
                    print(f"  -> {len(df_resampled)} velas (15min)")
        
        if not all_data:
            print("❌ Error: No se pudieron obtener datos de la BD")
            return
        
        # Combinar todos los datos
        df_combined = pd.concat(all_data, ignore_index=True)
        print(f"\nTotal de velas: {len(df_combined)}")
        
        # Convertir a tipos correctos para talib
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df_combined[col] = df_combined[col].astype('float64')
        
        print("Calculando features...")
        df_combined = calculateFeatures(df_combined)
        df_combined = df_combined.dropna()
        print(f"Después de dropna: {len(df_combined)} velas")
        
        print("Entrenando modelo de regresión...")
        mlModel.trainAndSaveRegModel(df_combined, MODEL_REG_FILE_PATH)
        
        print("✅ Modelo de regresión entrenado correctamente")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(train_reg())