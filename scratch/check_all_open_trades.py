import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_trades():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT idTrade, idCuenta, symbol, strategy, direction, entryPrice, stopLoss, takeProfit, ticketId, openTime
        FROM trades
        WHERE closeTime IS NULL
    """)
    rows = cursor.fetchall()
    print(f"📋 Todos los trades abiertos ({len(rows)}):")
    for r in rows:
        print(f"  - ID: {r['idTrade']}, Cuenta ID: {r['idCuenta']}, Símbolo: {r['symbol']}, Estrategia: {r['strategy']}, Dirección: {r['direction']}, Entrada: {r['entryPrice']}, SL: {r['stopLoss']}, TP: {r['takeProfit']}, Ticket: {r['ticketId']}, Creado: {r['openTime']}")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_trades()
