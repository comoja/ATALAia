import sys
import os
import time

# Add the middleware directory to the Python path
sys.path.append('/Volumes/TimeMachine/ATALAia')
sys.path.append('/Volumes/TimeMachine/ATALAia/middleware')

from middleware.database.dbConnection import getConnection

def main():
    conn = getConnection()
    if not conn:
        print("Could not connect to database")
        return
        
    cursor = conn.cursor()
    
    # Check current execution time for the heavy query
    test_symbol = "XAUUSD" # A common one
    
    # Try finding any symbol if XAUUSD doesn't exist
    cursor.execute("SELECT symbol FROM StockPrices LIMIT 1")
    row = cursor.fetchone()
    if row:
        test_symbol = row[0]
    else:
        print("No symbols in StockPrices table!")
        return

    print(f"Testing with symbol: {test_symbol}")
    
    start_time = time.time()
    cursor.execute(
        "SELECT priceDate, closePrice FROM StockPrices WHERE symbol=%s ORDER BY priceDate DESC LIMIT 5000",
        (test_symbol,)
    )
    cursor.fetchall()
    print(f"Query without index took: {time.time() - start_time:.4f} seconds")

    # Check if the index exists
    cursor.execute("SHOW INDEX FROM StockPrices")
    indexes = cursor.fetchall()
    
    # Find indexes on symbol and priceDate
    index_names = [idx[2] for idx in indexes]
    
    if "idx_symbol_date" not in index_names:
        print("Creating index 'idx_symbol_date' on StockPrices(symbol, priceDate)...")
        start_time = time.time()
        cursor.execute("CREATE INDEX idx_symbol_date ON StockPrices(symbol, priceDate DESC)")
        conn.commit()
        print(f"Index created in {time.time() - start_time:.4f} seconds")
    else:
        print("Index 'idx_symbol_date' already exists!")
        
    # Test query time again
    start_time = time.time()
    cursor.execute(
        "SELECT priceDate, closePrice FROM StockPrices WHERE symbol=%s ORDER BY priceDate DESC LIMIT 5000",
        (test_symbol,)
    )
    cursor.fetchall()
    print(f"Query with index took: {time.time() - start_time:.4f} seconds")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
