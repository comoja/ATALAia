import sys
import os
import pandas as pd
from datetime import datetime, timedelta
import pytz
import asyncio

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from middleware.database import dbManager
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval
from middleware.config.constants import TIMEZONE
from Sentinel.core.ReversionMedia import ReversionMediaBot

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

async def run_debug():
    rawSymbols = dbManager.getSymbols()
    symbolInfo = next((s for s in rawSymbols if s['symbol'] == 'AUD/USD'), None)
    symbol = symbolInfo['symbol']
    historyStartDateStr = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d') + ' 00:00:00'
    df5m = loadCandlesRange(symbol, historyStartDateStr)
    
    cdmxTz = pytz.timezone(TIMEZONE)
    if df5m.index.tzinfo is None:
        df5m.index = df5m.index.tz_localize(cdmxTz)
    else:
        df5m.index = df5m.index.tz_convert(cdmxTz)
        
    df5m_feat = calculateFeatures(df5m)
    df15m = calculateFeatures(resample_to_interval(df5m_feat, "15min"))
    df1h = calculateFeatures(resample_to_interval(df5m_feat, "1h"))
    
    compoundingStartDate = datetime.now() - timedelta(days=7)
    compoundingEndDate = datetime.now()
    compoundingStartAware = cdmxTz.localize(compoundingStartDate)
    compoundingEndAware = cdmxTz.localize(compoundingEndDate)
    
    test_timestamps = df15m.index[(df15m.index >= compoundingStartAware) & (df15m.index <= compoundingEndAware)]
    
    t = test_timestamps[0]
    preloaded_master = {
        '5min': df5m_feat.loc[:t],
        '15min': df15m.loc[:t],
        '1h': df1h.loc[:t]
    }
    
    bot = ReversionMediaBot()
    
    # Vamos a interceptar/ver qué pasa en runAnalysisCycleForSymbol
    print("--- DEBUGGING ReversionMedia ---")
    print("preloadedData keys:", {symbol: preloaded_master}.keys())
    print("symbolInfo:", symbolInfo)
    print("Executing runAnalysisCycleForSymbol...")
    res = await bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master})
    print("Result:", res)

if __name__ == '__main__':
    asyncio.run(run_debug())
