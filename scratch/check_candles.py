import sys
sys.path.append('/Volumes/TimeMachine/ATALAia')
from middleware.database import dbConnection

def check_candles():
    pool = dbConnection.DBConnectionPool()
    conn = pool.get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT MAX(timestamp) as max_ts, MIN(timestamp) as min_ts, COUNT(*) as count FROM candles WHERE symbol='XAG/USD' AND timeframe='5min'")
        row = cursor.fetchone()
        print("XAG/USD 5min Stats:")
        print(row)
        
        cursor.execute("SELECT timestamp, open, high, low, close FROM candles WHERE symbol='XAG/USD' AND timeframe='5min' ORDER BY timestamp DESC LIMIT 5")
        rows = cursor.fetchall()
        print("\nLatest 5 candles:")
        for r in rows:
            print(r)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_candles()
