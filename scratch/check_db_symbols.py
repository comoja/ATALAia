import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_symbols():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("DESCRIBE SentinelSymbol")
        cols = cursor.fetchall()
        print("--- COLUMNAS EN SentinelSymbol ---")
        for c in cols:
            print(f"Columna: {c['Field']} | Tipo: {c['Type']}")
            
        cursor.execute("SELECT * FROM SentinelSymbol LIMIT 10")
        rows = cursor.fetchall()
        print("\n--- REGISTROS EN SentinelSymbol ---")
        for r in rows:
            print({k: v for k, v in r.items() if v is not None})
    except Exception as e:
        print(f"Error: {e}")
    conn.close()

if __name__ == "__main__":
    check_symbols()
