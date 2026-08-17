import sys
import os
import pandas as pd
import numpy as np
import talib as ta
from datetime import datetime, timedelta

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def analyze_xau_may14():
    print("Analizando XAU/USD del 14 de mayo en timeframe de 1h...")
    
    # Rango amplio alrededor del 14 de mayo
    startDate = "2026-05-01 00:00:00"
    endDate = "2026-05-16 00:00:00"
    
    conn = dbConnection.getConnection()
    if conn is None:
        print("Error al conectar a MySQL")
        return
        
    symbol = "XAU/USD"
    timeframe = "5min"
    
    query = """
        SELECT timestamp as datetime, open, high, low, close, volume
        FROM candles
        WHERE symbol = %s AND timeframe = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp ASC
    """
    df5m = pd.read_sql(query, conn, params=(symbol, timeframe, startDate, endDate))
    conn.close()
    
    if df5m.empty or len(df5m) < 100:
        print("Datos 5m insuficientes para XAU/USD")
        return
        
    df5m['datetime'] = pd.to_datetime(df5m['datetime'])
    df5m.set_index('datetime', inplace=True)
    
    # Resample a 1h
    df = df5m.resample('1h').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    df["ema20"] = ta.EMA(df['close'].values, timeperiod=20)
    df["ema200"] = ta.EMA(df['close'].values, timeperiod=200)
    df["diff"] = df["ema20"] - df["ema200"]
    df["atr"] = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
    
    print("\nHistorial de velas de 1h del 14 de mayo de 2026:")
    print(f"{'Timestamp RAW':<20} | {'CDMX (RAW-6h)':<13} | {'Close':<8} | {'EMA20':<8} | {'EMA200':<8} | {'Diff':<8} | {'ATR':<6}")
    print("-" * 90)
    
    for idx in range(len(df)):
        t = df.index[idx]
        t_raw = t.strftime('%Y-%m-%d %H:%M')
        cdmx_time = t - timedelta(hours=6)
        cdmx_str = cdmx_time.strftime('%Y-%m-%d %H:%M')
        
        # Filtrar solo el 14 de mayo en hora CDMX
        if cdmx_time.strftime("%Y-%m-%d") == "2026-05-14":
            row = df.iloc[idx]
            print(f"{t_raw:<20} | {cdmx_str:<13} | {row['close']:<8.2f} | {row['ema20']:<8.2f} | {row['ema200']:<8.2f} | {row['diff']:<+8.3f} | {row['atr']:<6.2f}")
            
    print("\nEvaluación del Cruce:")
    for idx in range(2, len(df)):
        t = df.index[idx]
        cdmx_time = t - timedelta(hours=6)
        
        if cdmx_time.strftime("%Y-%m-%d") == "2026-05-14":
            diff_current = df["diff"].iloc[idx]
            diff_prev1 = df["diff"].iloc[idx-1]
            diff_prev2 = df["diff"].iloc[idx-2]
            
            t_str = t.strftime('%Y-%m-%d %H:%M')
            cdmx_str = cdmx_time.strftime('%Y-%m-%d %H:%M')
            
            # detectCross actual (1 vela atrás)
            cross_1 = None
            if (diff_current > 0) and (diff_prev1 <= 0):
                cross_1 = "LARGO"
            elif (diff_current < 0) and (diff_prev1 >= 0):
                cross_1 = "CORTO"
                
            if cross_1:
                print(f"-> Cruce en vela {t_str} (CDMX {cdmx_str}): {cross_1}")
                print(f"   diff: {diff_current:.3f}, diff_prev1: {diff_prev1:.3f}, diff_prev2: {diff_prev2:.3f}")

if __name__ == '__main__':
    analyze_xau_may14()
