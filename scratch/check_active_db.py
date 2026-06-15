import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def check_candles():
    conn = dbConnection.getConnection()
    if conn is None:
        return
    cur = conn.cursor(dictionary=True)
    cur.execute("SELECT symbol, MIN(timestamp) as min_ts, MAX(timestamp) as max_ts, COUNT(*) as count FROM candles GROUP BY symbol, timeframe ORDER BY count DESC LIMIT 10")
    rows = cur.fetchall()
    for r in rows:
        print(f"Symbol: {r['symbol']}, Min: {r['min_ts']}, Max: {r['max_ts']}, Count: {r['count']}")
    cur.close()
    conn.close()

if __name__ == '__main__':
    check_candles()
