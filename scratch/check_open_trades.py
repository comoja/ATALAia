import sys
import os
import asyncio

projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from middleware.database import dbManager

def main():
    print("=== TRADES ABIERTOS EN LA BASE DE DATOS ===")
    try:
        conn = dbManager.dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT idTrade, idCuenta, symbol, direction, strategy, openTime, status, stopLoss, takeProfit FROM trades WHERE status = 'OPEN'")
        rows = cursor.fetchall()
        print(f"Total trades abiertos: {len(rows)}")
        for r in rows:
            print(f"ID: {r['idTrade']} | Cuenta: {r['idCuenta']} | Símbolo: {r['symbol']} | Dir: {r['direction']} | Estrategia: {r['strategy']} | Abierto: {r['openTime']}")
        conn.close()
    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
    main()
