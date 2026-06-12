import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_candles():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT MIN(timestamp) as min_ts, MAX(timestamp) as max_ts, COUNT(*) as cnt, symbol FROM candles WHERE timeframe='5min' GROUP BY symbol")
        rows = cursor.fetchall()
        print("Candles summary in DB:")
        for r in rows:
            print(f"  Symbol: {r['symbol']} | Count: {r['cnt']} | Min: {r['min_ts']} | Max: {r['max_ts']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_candles()
