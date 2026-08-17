import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_exclusions():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM symbolNotStrategia")
        rows = cursor.fetchall()
        print(f"Exclusions in symbolNotStrategia ({len(rows)}):")
        for r in rows:
            print(f"  Symbol: {r['symbol']} | Strategy: {r['strategy']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_exclusions()
