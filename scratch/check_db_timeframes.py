import sys
import os
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, rutaRaiz)
from middleware.database import dbConnection
import pandas as pd

def check():
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT timeframe FROM candles WHERE symbol='EUR/USD'")
    rows = cursor.fetchall()
    print("Timeframes para EUR/USD:", rows)
    conn.close()

if __name__ == '__main__':
    check()
