import os, sys, logging, asyncio
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
from Sentinel.core.Sniper import SniperBot
from Sentinel.ml import model as mlModel

logging.basicConfig(level=logging.WARNING)

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
    wins = 0
    losses = 0
    
    closes = df_15m['close'].values
    highs = df_15m['high'].values
    lows = df_15m['low'].values
    
    print(f"Starting Sniper backtest on {symbol} (15m)...")
    for i in range(start_idx, len(df_15m) - 20):
        sub_df = df_15m.iloc[:i]
        preloaded = {symbol: {'15min': sub_df}}
        
        signal = await bot.runAnalysisCycleForSymbol(symbolInfo, preloaded)
        if signal:
            entry = signal.entry_price
            tp = signal.take_profit
            sl = signal.stop_loss
            dir = signal.direction
            
            result = "PENDING"
            for j in range(i, len(df_15m)):
                h = highs[j]
                l = lows[j]
                if dir == "LARGO":
                    if l <= sl: result = "LOSS"; break
                    if h >= tp: result = "WIN"; break
                else:
                    if h >= sl: result = "LOSS"; break
                    if l <= tp: result = "WIN"; break
            
            if result == "WIN":
                wins += 1
            elif result == "LOSS":
                losses += 1
                
    total = wins + losses
    wr = (wins / total * 100) if total > 0 else 0
    print(f"Sniper {symbol}: {wins} W / {losses} L | WR: {wr:.2f}%")

asyncio.run(run_backtest())
