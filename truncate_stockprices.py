import sys
import os

project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

from middleware.database import dbConnection

def truncate_stockprices():
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE StockPrices")
        conn.commit()
        cursor.close()
        conn.close()
        print("StockPrices table truncated successfully.")
    except Exception as e:
        print(f"Error truncating StockPrices: {e}")

if __name__ == '__main__':
    truncate_stockprices()
