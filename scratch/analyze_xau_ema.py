import sys
import os
import pandas as pd
import numpy as np
import talib as ta
from datetime import datetime, timedelta

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from Sentinel.analysis import technical

def analyze_xau_crossover():
    print("Analizando cruce EMA20/200 en XAU/USD (Cargando historial desde 2026-05-01)...")
    
    # Cargar historial amplio para inicializar la EMA 200
    startDate = "2026-05-01 00:00:00"
    endDate = "2026-05-27 12:00:00"
    
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
        print(f"Datos 5m insuficientes para XAU/USD")
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
    
    print("\nHistorial de velas de 1h resampleadas del 25 de mayo (alrededor de las 22:00 CDMX):")
    print(f"{'Timestamp RAW':<20} | {'CDMX (RAW-6h)':<13} | {'Close':<8} | {'EMA20':<8} | {'EMA200':<8} | {'Diff':<8}")
    print("-" * 80)
    
    # Las 22:00 CDMX del 25 de mayo es a las 04:00 UTC del 26 de mayo (si la DB es UTC)
    # Vamos a mostrar las velas de 1h del 25 y 26 de mayo
    for idx in range(len(df)):
        t = df.index[idx]
        t_raw = t.strftime('%Y-%m-%d %H:%M')
        cdmx_time = t - timedelta(hours=6)
        cdmx_str = cdmx_time.strftime('%Y-%m-%d %H:%M')
        
        # Filtrar solo el día 25 de mayo en hora CDMX
        if cdmx_time.strftime("%Y-%m-%d") == "2026-05-25":
            row = df.iloc[idx]
            print(f"{t_raw:<20} | {cdmx_str:<13} | {row['close']:<8.2f} | {row['ema20']:<8.2f} | {row['ema200']:<8.2f} | {row['diff']:<+8.3f}")
            
    print("\nEvaluación del Cruce:")
    for idx in range(2, len(df)):
        t = df.index[idx]
        cdmx_time = t - timedelta(hours=6)
        
        if cdmx_time.strftime("%Y-%m-%d") == "2026-05-25":
            diff_current = df["diff"].iloc[idx]
            diff_prev1 = df["diff"].iloc[idx-1]
            diff_prev2 = df["diff"].iloc[idx-2]
            
            t_str = t.strftime('%Y-%m-%d %H:%M')
            cdmx_str = cdmx_time.strftime('%Y-%m-%d %H:%M')
            
            # detectCross actual (1 vela atrás)
            cross_1 = None
            if (diff_current > 0) and (diff_prev1 <= 0):
                cross_1 = "LARGO (Alcista)"
            elif (diff_current < 0) and (diff_prev1 >= 0):
                cross_1 = "CORTO (Bajista)"
                
            # detectCross con 2 velas atrás de tolerancia
            cross_2 = None
            if (diff_current > 0) and (diff_prev1 <= 0 or diff_prev2 <= 0) and not (diff_prev1 > 0 and diff_prev2 > 0):
                cross_2 = "LARGO (Tolerancia 2 velas)"
            elif (diff_current < 0) and (diff_prev1 >= 0 or diff_prev2 >= 0) and not (diff_prev1 < 0 and diff_prev2 < 0):
                cross_2 = "CORTO (Tolerancia 2 velas)"
                
            if cross_1:
                print(f"-> Cruce 1 VELA en vela {t_str} (CDMX {cdmx_str}): {cross_1}")
                print(f"   diff: {diff_current:.3f}, diff_prev1: {diff_prev1:.3f}, diff_prev2: {diff_prev2:.3f}")
            elif cross_2:
                print(f"-> Cruce 2 VELAS en vela {t_str} (CDMX {cdmx_str}): {cross_2}")
                print(f"   diff: {diff_current:.3f}, diff_prev1: {diff_prev1:.3f}, diff_prev2: {diff_prev2:.3f}")

if __name__ == '__main__':
    analyze_xau_crossover()
