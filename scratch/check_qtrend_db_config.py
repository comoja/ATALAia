import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_qtrend():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT * FROM strategyConfig WHERE strategy = 'QTrend'")
        row = cursor.fetchone()
        print("QTrend config in DB:")
        if row:
            for k, v in row.items():
                print(f"  {k}: {v}")
        else:
            print("  No config found for QTrend!")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_qtrend()
