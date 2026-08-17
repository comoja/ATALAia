import os, sys, logging, asyncio
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
from Sentinel.core.Sniper import SniperBot
from Sentinel.ml import model as mlModel

logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
logging.getLogger("sentinel").setLevel(logging.DEBUG)

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
    df_15m = load_data(symbol, '15min', 1500)
    
    # Load ML Model
    model = mlModel.loadModel()
    if not model:
        print("ML Model missing for Sniper")
        return
        
    bot = SniperBot(model)
    symbolInfo = {"symbol": symbol, "tipo": "MONEDA", "refCapital": 10000, "refRiskPct": 1, "weekly_trend": "NEUTRAL", "intervalo": "15min", "pip": 0.0001}
    
    start_idx = 100
    
    print(f"Starting Sniper debug on {symbol} (15m)...")
    for i in range(start_idx, min(start_idx + 10, len(df_15m) - 20)):
        sub_df = df_15m.iloc[:i]
        preloaded = {symbol: {'15min': sub_df}}
        await bot.runAnalysisCycleForSymbol(symbolInfo, preloaded)

asyncio.run(run_backtest())
