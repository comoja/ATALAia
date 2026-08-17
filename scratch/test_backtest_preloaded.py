import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from middleware.database import dbManager
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval
from middleware.config.constants import TIMEZONE

def loadCandlesRange(symbol: str, startDate: str) -> pd.DataFrame:
    try:
        connection = dbConnection.getConnection()
        query = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = '5min' AND timestamp >= %s
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, connection, params=(symbol, startDate))
        connection.close()
        if df.empty:
            return pd.DataFrame()
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        return df
    except Exception as e:
        print(f"❌ Error al cargar velas para {symbol}: {e}")
        return pd.DataFrame()

def run_test():
    rawSymbols = dbManager.getSymbols()
    activeSymbols = [s['symbol'] for s in rawSymbols if s.get('Activo') == 1]
    if not activeSymbols:
        print("No active symbols found")
        return
    
    symbol = activeSymbols[0]
    historyStartDateStr = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d') + ' 00:00:00'
    df5m = loadCandlesRange(symbol, historyStartDateStr)
    print(f"Loaded {len(df5m)} candles for {symbol}")
    
    cdmxTz = pytz.timezone(TIMEZONE)
    if df5m.index.tzinfo is None:
        df5m.index = df5m.index.tz_localize(cdmxTz)
    else:
        df5m.index = df5m.index.tz_convert(cdmxTz)
        
    df5m_feat = calculateFeatures(df5m)
    df15m = calculateFeatures(resample_to_interval(df5m_feat, "15min"))
    df1h = calculateFeatures(resample_to_interval(df5m_feat, "1h"))
    
    print(f"df15m length: {len(df15m)}")
    print(f"df1h length: {len(df1h)}")
    
    compoundingStartDate = datetime.now() - timedelta(days=7)
    compoundingEndDate = datetime.now()
    compoundingStartAware = cdmxTz.localize(compoundingStartDate)
    compoundingEndAware = cdmxTz.localize(compoundingEndDate)
    
    test_timestamps = df15m.index[(df15m.index >= compoundingStartAware) & (df15m.index <= compoundingEndAware)]
    print(f"Found {len(test_timestamps)} test timestamps")
    
    if len(test_timestamps) > 0:
        t = test_timestamps[0]
        slice_1h = df1h.loc[:t]
        print(f"At t={t}, df1h.loc[:t] length is: {len(slice_1h)}")
        print("Indices of df1h head:", df1h.index[:5])
        print("Indices of df1h tail:", df1h.index[-5:])
        print("Is t tz-aware?", t.tzinfo is not None)
        print("Is df1h index tz-aware?", df1h.index.tzinfo is not None)
        print("t:", t)

if __name__ == '__main__':
    run_test()
