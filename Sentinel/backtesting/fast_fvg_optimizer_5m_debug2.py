import os, sys, logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
from Sentinel.core.GenericFVG import GenericFVGBot
import asyncio

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

def load_data(symbol, timeframe, limit=1000):
    conn = dbConnection.getConnection()
    try:
        q = f"SELECT timestamp as datetime, open, high, low, close, volume FROM candles WHERE symbol = '{symbol}' AND timeframe = '{timeframe}' ORDER BY timestamp DESC LIMIT {limit}"
        df = pd.read_sql(q, conn)
    finally:
        if conn: conn.close()
    if not df.empty:
        df = df.iloc[::-1].reset_index(drop=True)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
    return df

async def run_backtest():
    symbol = "GBP/USD"
    df_5m = load_data(symbol, '5min', 1000)
    
    bot = GenericFVGBot(intervals=['5min'])
    symbolInfo = {"symbol": symbol, "tipo": "MONEDA", "refCapital": 10000, "refRiskPct": 1, "weekly_trend": "NEUTRAL"}
    
    start_idx = len(df_5m) - 200
        
    for i in range(start_idx, len(df_5m) - 50):
        sub_df = df_5m.iloc[:i]
        preloaded = {symbol: {'5min': sub_df}}
        signals = await bot.runAnalysisCycleForSymbol(symbolInfo, preloaded)
        if signals:
            print(f"Index {i} triggered signal!")

asyncio.run(run_backtest())
