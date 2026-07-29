import sys, os, pandas as pd
project_root = os.path.abspath('/Volumes/TimeMachine/ATALAia')
sys.path.insert(0, project_root)
from middleware.database import dbManager
import pytz
from middleware.config.constants import TIMEZONE

def test():
    conn = dbManager.dbConnection.getConnection()
    # Fetch some 5min data
    query = "SELECT timestamp as datetime, close FROM candles WHERE symbol = 'AUD/USD' AND timeframe = '5min' ORDER BY timestamp DESC LIMIT 1000"
    df = pd.read_sql(query, conn)
    
    if not df.empty:
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        # Resample to 1h using the close of the last 5min candle in that hour
        df_1h = df.resample('1h').last().dropna()
        df_1h = df_1h.reset_index()
        print(df_1h.head())

test()
