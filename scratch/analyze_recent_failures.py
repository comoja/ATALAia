import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def analyze():
    print("Conectando a base de datos...")
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        cursor.execute("""
            SELECT 
                idTrade, symbol, strategy, direction, entryPrice, stopLoss, takeProfit, exitPrice, openTime, closeTime, pnl
            FROM trades
            WHERE strategy IN ('Sniper', 'QTrend', 'BreakoutProbability')
            ORDER BY openTime DESC
            LIMIT 25
        """)
        trades = cursor.fetchall()
        print("\n--- DETALLES DE ÚLTIMOS TRADES CERRADOS ---")
        for t in trades:
            sl_pips = abs(t['entryPrice'] - t['stopLoss'])
            tp_pips = abs(t['entryPrice'] - t['takeProfit'])
            rr = tp_pips / sl_pips if sl_pips > 0 else 0
            
            print(f"\nID: {t['idTrade']} | Símbolo: {t['symbol']} | Estrategia: {t['strategy']}")
            print(f"  Dirección: {t['direction']} | Entrada: {t['entryPrice']} | Salida: {t['exitPrice']}")
            print(f"  SL: {t['stopLoss']} | TP: {t['takeProfit']} | R:R: {rr:.2f}")
            print(f"  Apertura: {t['openTime']} | Cierre: {t['closeTime']}")
            print(f"  PNL: {t['pnl']}")
            
    except Exception as e:
        print(f"Error analizando trades: {e}")
        
    conn.close()

if __name__ == "__main__":
    analyze()
