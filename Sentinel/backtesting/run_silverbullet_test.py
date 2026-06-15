import os, sys, asyncio
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
from Sentinel.core.SilverBullet import SilverBulletBot
import logging

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

async def main():
    symbol = "EUR/USD"
    df_5m = load_data(symbol, '5min', 1500)
    if df_5m.empty:
        print("No data for EUR/USD 5min")
        return
        
    bot = SilverBulletBot()
    symbolInfo = {
        "symbol": symbol, "tipo": "MONEDA", "refCapital": 10000, "refRiskPct": 1,
        "weekly_trend": "NEUTRAL", "intervalo": "5min", "pip": 0.0001
    }
    
    signals_found = 0
    start_idx = 200
    for i in range(start_idx, len(df_5m)):
        sub_df = df_5m.iloc[:i]
        preloaded = {symbol: {'5min': sub_df}}
        sig = await bot.runAnalysisCycleForSymbol(symbolInfo, preloaded)
        if sig:
            print(f"\n+++ SIGNAL FOUND in {sig.setup} +++\n{sig.to_dict()}\n")
            signals_found += 1
            
    print(f"Total SilverBullet signals found: {signals_found}")

if __name__ == "__main__":
    asyncio.run(main())
