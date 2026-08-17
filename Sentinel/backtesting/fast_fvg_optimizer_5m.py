import os, sys, datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
from Sentinel.core.GenericFVG import GenericFVGBot
import asyncio

def load_data(symbol, timeframe, limit=5000):
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
    df_5m = load_data(symbol, '5min', 5000)
    
    if len(df_5m) < 1000:
        print("Not enough 5min data")
        return
        
    bot = GenericFVGBot(intervals=['5min'])
    symbolInfo = {"symbol": symbol, "tipo": "MONEDA", "refCapital": 10000, "refRiskPct": 1, "weekly_trend": "NEUTRAL"}
    
    wins = 0
    losses = 0
    
    start_idx = len(df_5m) - 1000
        
    for i in range(start_idx, len(df_5m) - 50):
        sub_df = df_5m.iloc[:i]
        
        preloaded = {symbol: {'5min': sub_df}}
        
        signals = await bot.runAnalysisCycleForSymbol(symbolInfo, preloaded)
        if not signals:
            continue
            
        sig = signals[0]
        entry = sig.entry_price
        sl = sig.stop_loss
        tp = sig.take_profit
        direction = sig.direction
        
        future_df = df_5m.iloc[i:i+50]
        outcome = "PENDING"
        for _, row in future_df.iterrows():
            low = row['low']
            high = row['high']
            
            if direction == "LARGO":
                if low <= sl:
                    outcome = "LOSS"
                    break
                if high >= tp:
                    outcome = "WIN"
                    break
            else:
                if high >= sl:
                    outcome = "LOSS"
                    break
                if low <= tp:
                    outcome = "WIN"
                    break
                    
        if outcome == "WIN":
            wins += 1
        elif outcome == "LOSS":
            losses += 1
            
    total = wins + losses
    if total > 0:
        wr = wins / total * 100
        print(f"GenericFVG {symbol}: {wins} W / {losses} L | WR: {wr:.2f}%")
    else:
        print("No signals triggered")

asyncio.run(run_backtest())
