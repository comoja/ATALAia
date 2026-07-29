import sys, os, pandas as pd
project_root = os.path.abspath('/Volumes/TimeMachine/ATALAia')
sys.path.insert(0, project_root)
from middleware.database import dbManager

def test():
    conn = dbManager.dbConnection.getConnection()
    query = "SELECT timestamp as datetime, close FROM candles WHERE symbol = 'AUD/USD' AND timeframe = '1h' ORDER BY timestamp DESC LIMIT 10"
    df = pd.read_sql(query, conn)
    print(df)
test()
