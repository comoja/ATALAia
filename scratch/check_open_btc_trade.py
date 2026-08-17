import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_trade():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("""
        SELECT idTrade, symbol, strategy, direction, entryPrice, stopLoss, takeProfit, ticketId, openTime
        FROM trades
        WHERE idCuenta = 2 AND symbol = 'BTC/USD' AND strategy = 'CruceEMA' AND closeTime IS NULL
    """)
    row = cursor.fetchone()
    if row:
        print("✅ Trade abierto encontrado:")
        print(row)
    else:
        print("❌ No se encontró ningún trade abierto para BTC/USD y CruceEMA en la cuenta 2.")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_trade()
