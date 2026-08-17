import sys
import os
import pandas as pd
from datetime import datetime

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection, dbManager
from Sentinel.analysis import technical

def find_fvgs_yesterday():
    print("Buscando FVGs de ayer a las 19:30...")
    rawSymbols = dbManager.getSymbols()
    activeSymbols = [s['symbol'] for s in rawSymbols if s.get('Activo') == 1]
    print(f"Simbolos activos a evaluar: {activeSymbols}")
    
    # Ayer fue 2026-05-26
    startDate = "2026-05-25 00:00:00"
    endDate = "2026-05-27 00:00:00"
    
    conn = dbConnection.getConnection()
    if conn is None:
        print("Error al conectar a la base de datos MySQL")
        return
        
    for symbol in activeSymbols:
        for timeframe in ['5min', '15min', '1h']:
            try:
                query = """
                    SELECT timestamp as datetime, open, high, low, close, volume
                    FROM candles
                    WHERE symbol = %s AND timeframe = %s AND timestamp >= %s AND timestamp <= %s
                    ORDER BY timestamp ASC
                """
                df = pd.read_sql(query, conn, params=(symbol, timeframe, startDate, endDate))
                if df.empty or len(df) < 5:
                    continue
                
                df['datetime'] = pd.to_datetime(df['datetime'])
                df.set_index('datetime', inplace=True)
                
                # Ejecutar detector de FVG
                # Vamos a calcular features para que tenga ATR y EMA200
                df_feat = technical.calculateFeatures(df)
                fvgs = technical.detect_fvgs(df_feat, apply_high_prob_filters=False)
                
                for f in fvgs:
                    fvg_time = pd.to_datetime(f['timestamp'])
                    # Filtrar por ayer a las 19:30 (o cercano)
                    if fvg_time.strftime("%Y-%m-%d") == "2026-05-26" and fvg_time.hour == 19:
                        print(f"★ ENCONTRADO FVG en {symbol} ({timeframe}) a las {fvg_time.strftime('%H:%M')} ★")
                        print(f"  Tipo: {f['type']}, Size: {f['size']:.5f}, Clasificacion: {f['classification']}")
                        print(f"  Top: {f['top']:.5f}, Bottom: {f['bottom']:.5f}, Mitigated: {f['mitigated']}")
                        
                        # Evaluar si era viable con filtros de alta probabilidad
                        # Corremos con filtros de alta probabilidad activos
                        fvgs_hp = technical.detect_fvgs(df_feat, apply_high_prob_filters=True)
                        f_hp = next((x for x in fvgs_hp if x['timestamp'] == f['timestamp']), None)
                        if f_hp:
                            print(f"  -> HP filter PASSED! Clasificacion HP: {f_hp['classification']}")
                        else:
                            print("  -> HP filter FAILED!")
            except Exception as e:
                print(f"Error procesando {symbol} {timeframe}: {e}")
                
    conn.close()

if __name__ == '__main__':
    find_fvgs_yesterday()
