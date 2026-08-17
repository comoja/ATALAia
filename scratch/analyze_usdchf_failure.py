import sys
import os
import pandas as pd
import numpy as np
import talib as ta
from datetime import datetime, timedelta

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection, dbManager
from Sentinel.analysis import technical

def analyze_usdchf_failure():
    print("Analizando la falla de señal USD/CHF del 2026-05-27...")
    
    startDate = "2026-05-27 00:00:00"
    endDate = "2026-05-27 12:00:00"
    
    conn = dbConnection.getConnection()
    if conn is None:
        print("Error de conexión a MySQL")
        return
        
    symbol = "USD/CHF"
    timeframe = "5min"
    
    query = """
        SELECT timestamp as datetime, open, high, low, close, volume
        FROM candles
        WHERE symbol = %s AND timeframe = %s AND timestamp >= %s AND timestamp <= %s
        ORDER BY timestamp ASC
    """
    df = pd.read_sql(query, conn, params=(symbol, timeframe, startDate, endDate))
    conn.close()
    
    if df.empty:
        print("No hay velas de hoy cargadas para USD/CHF")
        return
        
    df['datetime'] = pd.to_datetime(df['datetime'])
    df.set_index('datetime', inplace=True)
    
    # Calcular features
    df_feat = technical.calculateFeatures(df)
    
    # Buscar el FVG de las 08:15:00
    fvgs = technical.detect_fvgs(df_feat, apply_high_prob_filters=False)
    fvg_815 = next((f for f in fvgs if pd.to_datetime(f['timestamp']).strftime("%H:%M:%S") == "08:15:00"), None)
    
    if fvg_815:
        print(f"\nFVG de las 08:15:00 encontrado:")
        print(f"  Tipo: {fvg_815['type']}, Size: {fvg_815['size']:.5f}, Clasificacion: {fvg_815['classification']}")
        print(f"  Top: {fvg_815['top']:.5f}, Bottom: {fvg_815['bottom']:.5f}")
        
        # Validar si pasó el filtro de alta probabilidad (apply_high_prob_filters=True)
        fvgs_hp = technical.detect_fvgs(df_feat, apply_high_prob_filters=True)
        f_hp = next((f for f in fvgs_hp if pd.to_datetime(f['timestamp']).strftime("%H:%M:%S") == "08:15:00"), None)
        if f_hp:
            print(f"  -> HP Filter: PASSED (Clasificacion: {f_hp['classification']})")
        else:
            print("  -> HP Filter: FAILED")
    else:
        print("No se encontró el FVG de las 08:15:00 en el detector")
        
    # Obtener tendencia del símbolo desde la DB
    sym_info = dbManager.getSymbol(symbol)
    weekly_trend = sym_info.get('weekly_trend', 'NEUTRAL') if sym_info else 'NEUTRAL'
    print(f"\nTendencia Macro en DB para USD/CHF (weekly_trend): {weekly_trend}")
    
    # Trayectoria del precio a partir de las 08:15:00 CDMX
    # Verifiquemos las velas desde las 08:15:00 en adelante
    print("\nHistorial de velas desde las 08:15 en adelante:")
    print(f"{'Time CDMX':<10} | {'Open':<8} | {'High':<8} | {'Low':<8} | {'Close':<8} | {'EMA200':<8}")
    print("-" * 65)
    
    entry_time = None
    sl_touched_time = None
    entry_price = 0.78512
    sl_price = 0.78583
    
    for idx in range(len(df_feat)):
        t = df_feat.index[idx]
        t_str = t.strftime('%H:%M:%S')
        if t_str >= "08:10:00":
            row = df_feat.iloc[idx]
            print(f"{t_str:<10} | {row['open']:<8.5f} | {row['high']:<8.5f} | {row['low']:<8.5f} | {row['close']:<8.5f} | {row['ema200']:<8.5f}")
            
            # Evaluar cuándo cruzó la entrada y el Stop Loss
            if t_str >= "08:37:00": # Hora de emisión de la señal
                if sl_touched_time is None and row['high'] >= sl_price:
                    sl_touched_time = t_str

    if sl_touched_time:
        print(f"\n-> Stop Loss ({sl_price}) TOCADO a las: {sl_touched_time} CDMX")
    else:
        print(f"\n-> Stop Loss ({sl_price}) NO tocado aún en el registro de velas")

if __name__ == '__main__':
    analyze_usdchf_failure();
