import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from Sentinel.analysis import technical

def find_xau_fvgs():
    print("Buscando FVGs exhaustivamente para XAU/USD...")
    
    # Rango de búsqueda amplio de ayer a hoy
    startDate = "2026-05-25 00:00:00"
    endDate = "2026-05-27 12:00:00"
    
    conn = dbConnection.getConnection()
    if conn is None:
        print("Error al conectar a la base de datos MySQL")
        return
        
    symbol = "XAU/USD"
    # Analizaremos timeframes de 5min, 15min, 1h
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
                print(f"No hay velas cargadas para XAU/USD en {timeframe}")
                continue
            
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            
            # Calcular features para ATR e indicadores
            df_feat = technical.calculateFeatures(df)
            fvgs = technical.detect_fvgs(df_feat, apply_high_prob_filters=False)
            
            for f in fvgs:
                fvg_time = pd.to_datetime(f['timestamp'])
                # Vamos a mostrar todos los FVGs del 26 de mayo por la tarde/noche y 27 por la mañana
                # Queremos los de ayer 26 de mayo en la tarde o noche, que en CDMX o UTC sean cercanos
                # Mostraremos todos los FVGs del 2026-05-26 en adelante para que podamos auditarlos
                if fvg_time.strftime("%Y-%m-%d") in ["2026-05-26", "2026-05-27"]:
                    # Calcular hora equivalente en CDMX (suponiendo que la DB guarda en UTC)
                    # Si el index tiene timezone, lo convertimos. Si no, mostramos el raw y calculamos
                    time_raw = fvg_time.strftime('%Y-%m-%d %H:%M')
                    # Asumimos que la DB puede estar en UTC. CDMX es UTC-6.
                    cdmx_est = (fvg_time - timedelta(hours=6)).strftime('%H:%M')
                    
                    print(f"★ FVG {timeframe} a las {time_raw} (Est. CDMX: {cdmx_est}) ★")
                    print(f"  Tipo: {f['type']}, Size: {f['size']:.3f}, Clasificacion: {f['classification']}")
                    print(f"  Top: {f['top']:.2f}, Bottom: {f['bottom']:.2f}, Mitigated: {f['mitigated']}")
                    
                    # Evaluar viabilidad de alta probabilidad
                    fvgs_hp = technical.detect_fvgs(df_feat, apply_high_prob_filters=True)
                    f_hp = next((x for x in fvgs_hp if x['timestamp'] == f['timestamp']), None)
                    if f_hp:
                        print(f"  -> HP Filter PASSED! Clasificacion HP: {f_hp['classification']}")
                        # Verificar si pasó el resto de filtros de GenericFVG (age, tp, sl, etc.)
                    else:
                        print("  -> HP Filter FAILED!")
        except Exception as e:
            print(f"Error procesando {symbol} {timeframe}: {e}")
            
    conn.close()

if __name__ == '__main__':
    find_xau_fvgs()
