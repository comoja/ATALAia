import sys
import os
import pandas as pd
from datetime import datetime

projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from middleware.database import dbConnection
from Sentinel.analysis.risk import checkTradeClosure

def main():
    print("=== AUDITORÍA DE TRADES CERRADOS ===")
    conn = dbConnection.getConnection()
    if not conn:
        print("No se pudo conectar a la base de datos.")
        return
    
    try:
        cursor = conn.cursor(dictionary=True)
        # Traer los últimos 150 trades cerrados
        cursor.execute("""
            SELECT idTrade, idCuenta, symbol, direction, strategy, openTime, closeTime, status, stopLoss, takeProfit, exitPrice, pnl 
            FROM trades 
            WHERE status = 'CLOSED' OR closeTime IS NOT NULL
            ORDER BY idTrade DESC
            LIMIT 150
        """)
        trades = cursor.fetchall()
        print(f"Auditando los últimos {len(trades)} trades cerrados...\n")
        
        errors_count = 0
        for t in trades:
            idTrade = t['idTrade']
            symbol = t['symbol']
            openTime = t['openTime']
            closeTime = t['closeTime']
            sl = float(t['stopLoss']) if t['stopLoss'] is not None else None
            tp = float(t['takeProfit']) if t['takeProfit'] is not None else None
            side = t['direction'].upper()
            db_exit_price = float(t['exitPrice']) if t['exitPrice'] is not None else None
            pnl = float(t['pnl']) if t['pnl'] is not None else 0.0
            
            # Si no hay openTime, closeTime, o exitPrice, omitir
            if not openTime or not closeTime or db_exit_price is None:
                continue
                
            # Determinar si el cierre de la DB fue por SL, TP o Manual/Otro
            db_outcome = "MANUAL/OTRO"
            if sl is not None and tp is not None:
                if side == "LARGO":
                    if db_exit_price <= sl + 0.00001:
                        db_outcome = "SL"
                    elif db_exit_price >= tp - 0.00001:
                        db_outcome = "TP"
                elif side == "CORTO":
                    if db_exit_price >= sl - 0.00001:
                        db_outcome = "SL"
                    elif db_exit_price <= tp + 0.00001:
                        db_outcome = "TP"
            
            # Si el cierre en la DB parece manual (el precio no coincide con SL ni TP),
            # no podemos asegurar que fue un error del bot, ya que el usuario pudo cerrarlo manualmente.
            # Solo auditamos toques reales si el db_outcome es SL o TP,
            # o si las velas muestran que debió haber cerrado por SL/TP mucho antes de la hora de cierre.
            
            # Buscar velas en la base de datos entre openTime y closeTime
            cursor.execute("""
                SELECT timestamp, open, high, low, close 
                FROM candles 
                WHERE symbol = %s AND timeframe = '5min' AND timestamp >= %s AND timestamp <= %s
                ORDER BY timestamp ASC
            """, (symbol, openTime, closeTime))
            candles = cursor.fetchall()
            
            if not candles:
                # Intentar con pequeño buffer por diferencias de segundos
                cursor.execute("""
                    SELECT timestamp, open, high, low, close 
                    FROM candles 
                    WHERE symbol = %s AND timeframe = '5min' 
                      AND timestamp >= DATE_SUB(%s, INTERVAL 5 MINUTE) 
                      AND timestamp <= DATE_ADD(%s, INTERVAL 5 MINUTE)
                    ORDER BY timestamp ASC
                """, (symbol, openTime, closeTime))
                candles = cursor.fetchall()
                
            if not candles:
                # Omitir si no hay velas
                continue
                
            # Convertir a DataFrame
            df = pd.DataFrame(candles)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            for col in ['open', 'high', 'low', 'close']:
                df[col] = df[col].astype(float)
                
            # Evaluar si las velas del periodo determinan un toque de SL o TP
            closure = checkTradeClosure(df, t)
            
            if closure:
                expected_reason = closure['reason']
                expected_time = closure['closeTime']
                
                # Comprobar desfase de tiempo (si debió cerrar hace más de 1 hora)
                time_diff = (pd.to_datetime(closeTime) - pd.to_datetime(expected_time)).total_seconds()
                
                mismatch = False
                reasons = []
                
                # Si el bot cerró tarde por más de 10 minutos
                if time_diff > 600:
                    mismatch = True
                    reasons.append(f"Debió cerrar a las {expected_time} por {expected_reason} pero cerró a las {closeTime} ({time_diff/60:.1f} min tarde)")
                
                # Si cerró por un motivo diferente al que tocó primero
                if db_outcome != expected_reason and db_outcome != "MANUAL/OTRO":
                    mismatch = True
                    reasons.append(f"Tocó primero {expected_reason} pero en BD el precio de salida coincide con {db_outcome}")
                
                if mismatch:
                    errors_count += 1
                    print(f"❌ ID {idTrade} ({symbol} {side} - {t['strategy']}): Inconsistencia en el cierre.")
                    for r in reasons:
                        print(f"   -> {r}")
            else:
                # Si las velas dicen que NO tocó ni SL ni TP en todo el periodo, pero en DB se cerró por SL/TP
                if db_outcome in ["SL", "TP"]:
                    errors_count += 1
                    print(f"❌ ID {idTrade} ({symbol} {side} - {t['strategy']}): Cerrado en DB como {db_outcome} a {db_exit_price}, pero las velas muestran que nunca tocó ese nivel.")
                    max_high = df['high'].max()
                    min_low = df['low'].min()
                    print(f"   -> Rango real de velas: High={max_high:.5f}, Low={min_low:.5f} (SL: {sl}, TP: {tp})")
                
        print(f"\nResumen: {errors_count} inconsistencias encontradas.")
    except Exception as e:
        print("Error en auditoría:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
