import sys
import os
import pandas as pd
import warnings

warnings.filterwarnings('ignore')

project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

from middleware.database import dbConnection

def migrate():
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    
    print("Fetching data from candles...")
    cursor.execute("SELECT DISTINCT timeframe FROM candles")
    tfs = [r[0] for r in cursor.fetchall()]
    print("Timeframes in candles:", tfs)
    
    cursor.execute("SELECT symbol FROM RatioSymbol")
    ratio_symbols = [r[0] for r in cursor.fetchall()]
    print("Ratio Symbols:", ratio_symbols)
    
    if not ratio_symbols:
        print("No RatioSymbols found.")
        return

    tf = '1h' if '1h' in tfs else ('TIMEFRAME_H1' if 'TIMEFRAME_H1' in tfs else '5min')
    print(f"Using timeframe: {tf}")
    
    total_inserted = 0
    for sym in ratio_symbols:
        query = f"SELECT timestamp, close FROM candles WHERE symbol = '{sym}' AND timeframe = '{tf}' AND timestamp >= (NOW() - INTERVAL 3 DAY)"
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print(f"No data for {sym} in candles table (timeframe {tf}).")
            continue
            
        if tf == '5min':
            df.set_index('timestamp', inplace=True)
            df = df.resample('1h').last().dropna().reset_index()
            
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((sym, row['timestamp'], row['close']))
            
        if data_to_insert:
            cursor.executemany(
                "INSERT IGNORE INTO StockPrices (symbol, priceDate, closePrice) VALUES (%s, %s, %s)",
                data_to_insert
            )
            conn.commit()
            total_inserted += len(data_to_insert)
            print(f"Inserted {len(data_to_insert)} records for {sym}")
            
    print(f"Total inserted: {total_inserted}")
    cursor.close()
    conn.close()

if __name__ == '__main__':
    migrate()
