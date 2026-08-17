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
    print("=== EVALUANDO CIERRE DE TRADES ABIERTOS ===")
    conn = dbConnection.getConnection()
    if not conn:
        print("No se pudo conectar a la base de datos.")
        return
    
    try:
        cursor = conn.cursor(dictionary=True)
        # Traer todos los trades en estado OPEN
        cursor.execute("""
            SELECT idTrade, idCuenta, symbol, direction, strategy, openTime, status, stopLoss, takeProfit 
            FROM trades 
            WHERE status = 'OPEN'
        """)
        trades = cursor.fetchall()
        print(f"Encontrados {len(trades)} trades abiertos.\n")
        
        closed_count = 0
        for t in trades:
            symbol = t['symbol']
            openTime = t['openTime']
            sl = t['stopLoss']
            tp = t['takeProfit']
            side = t['direction']
            
            cursor.execute("""
                SELECT timestamp, open, high, low, close 
                FROM candles 
                WHERE symbol = %s AND timeframe = '5min' AND timestamp >= %s 
                ORDER BY timestamp ASC
            """, (symbol, openTime))
            candles = cursor.fetchall()
            
            if not candles:
                print(f"⚠️ ID {t['idTrade']} ({symbol} {side}): Sin velas en BD desde {openTime}")
                continue
                
            # Convertir a DataFrame
            df = pd.DataFrame(candles)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df.set_index('timestamp', inplace=True)
            
            # Castear a float
            for col in ['open', 'high', 'low', 'close']:
                df[col] = df[col].astype(float)
                
            # Evaluar cierre
            closure = checkTradeClosure(df, t)
            if closure:
                closed_count += 1
                print(f"🔴 ID {t['idTrade']} ({symbol} {side} - {t['strategy']}): DEBIÓ CERRAR por {closure['reason']} a {closure['exitPrice']} en {closure['closeTime']}")
            else:
                last_candle = df.iloc[-1]
                last_close = last_candle['close']
                print(f"🟢 ID {t['idTrade']} ({symbol} {side} - {t['strategy']}): Abierto. Último: {last_close} (SL: {sl}, TP: {tp})")
                
        print(f"\nResumen: {closed_count} trades debieron cerrar de {len(trades)} evaluados.")
    except Exception as e:
        print("Error en evaluación:", e)
    finally:
        conn.close()

if __name__ == "__main__":
    main()
