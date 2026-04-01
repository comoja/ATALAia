"""
Script para regenerar datos de 1h desde los datos de 15min existentes
considerando el timezone America/Mexico_City
"""
import asyncio
import sys
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
import pytz

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.config.constants import dbConfig

def regenerate_1h():
    engine = create_engine(
        f"mysql+mysqlconnector://{dbConfig['user']}:{dbConfig['password']}@{dbConfig['host']}/{dbConfig['database']}"
    )
    
    symbol = "XAU/USD"
    TIMEZONE = "America/Mexico_City"
    cdmx_tz = pytz.timezone(TIMEZONE)
    
    print(f"=== Regenerando datos de 1h para {symbol} (timezone: {TIMEZONE}) ===")
    
    # Get 15min data
    print("Obteniendo datos de 15min...")
    with engine.connect() as conn:
        df = pd.read_sql(
            text("SELECT timestamp, open, high, low, close, volume FROM candles WHERE symbol=:symbol AND timeframe=:timeframe ORDER BY timestamp ASC"),
            conn,
            params={"symbol": symbol, "timeframe": "15min"}
        )
    
    print(f"  Velas de 15min: {len(df)}")
    
    if df.empty:
        print("No hay datos de 15min!")
        return
    
    # Convert to timezone-aware datetime
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.tz_localize(cdmx_tz)
    print(f"  Timestamp con timezone: {df['timestamp'].iloc[0]}")
    
    df = df.set_index('timestamp')
    
    # Resample to 1h - las velas serán de 00:00 a 01:00, 01:00 a 02:00, etc. en timezone México
    print("Resampleando 15min -> 1h...")
    dfResampled = df.resample(rule='1h', closed='right', label='right').agg({
        'open': 'first', 'high': 'max', 'low': 'min',
        'close': 'last', 'volume': 'sum'
    }).dropna()
    
    print(f"  Velas de 1h generadas: {len(dfResampled)}")
    print(f"  Primera vela 1h: {dfResampled.index[0]}")
    print(f"  Ultima vela 1h: {dfResampled.index[-1]}")
    
    # Save to DB - convert to naive datetime for storage
    print("Guardando en DB...")
    dfResampled = dfResampled.reset_index()
    dfResampled.rename(columns={'index': 'timestamp'}, inplace=True)
    
    # Convert to timezone-naive for storage (store as Mexico time string)
    dfResampled['timestamp'] = dfResampled['timestamp'].dt.tz_localize(None)
    dfResampled['timestamp'] = dfResampled['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    dfResampled['symbol'] = symbol
    dfResampled['timeframe'] = '1h'
    
    # Delete existing 1h data first
    with engine.connect() as conn:
        conn.execute(text("DELETE FROM candles WHERE symbol=:symbol AND timeframe='1h'"), {"symbol": symbol})
        conn.commit()
    
    print(f"  Datos 1h antiguos eliminados")
    
    # Use INSERT IGNORE to skip duplicates
    with engine.begin() as conn:
        for _, row in dfResampled.iterrows():
            conn.execute(
                text("INSERT IGNORE INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume) VALUES (:symbol, :timeframe, :timestamp, :open, :high, :low, :close, :volume)"),
                {"symbol": row['symbol'], "timeframe": row['timeframe'], "timestamp": row['timestamp'], "open": row['open'], "high": row['high'], "low": row['low'], "close": row['close'], "volume": row['volume']}
            )
    
    print(f"  Velas de 1h guardadas: {len(dfResampled)}")
    print("=== Proceso completado ===")

if __name__ == "__main__":
    regenerate_1h()
