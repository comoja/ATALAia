import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_indexes():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SHOW INDEX FROM candles")
        rows = cursor.fetchall()
        print("Indexes on 'candles':")
        for r in rows:
            print(f"  Key_name: {r['Key_name']} | Column_name: {r['Column_name']} | Non_unique: {r['Non_unique']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_indexes()
