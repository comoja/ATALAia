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
    df_15m = load_data(symbol, '15min', 5000)
    
    bot = GenericFVGBot(intervals=['15min'])
    symbolInfo = {"symbol": symbol, "tipo": "MONEDA", "refCapital": 10000, "refRiskPct": 1, "weekly_trend": "NEUTRAL"}
    
    wins = 0
    losses = 0
    
    # We will step through time and see what it triggers
    # This is a bit slow for a full loop, let's just run it over the last 1000 candles of 15min
    start_idx = 4000
    if len(df_15m) < 4000:
        print("Not enough 15min data")
        return
        
    for i in range(start_idx, len(df_15m) - 50):
        # We slice df_15m up to i
        sub_df_15m = df_15m.iloc[:i]
        
        # Build preloaded data
        preloaded = {symbol: {'15min': sub_df_15m}}
        
        signals = await bot.runAnalysisCycleForSymbol(symbolInfo, preloaded)
        if not signals:
            continue
            
        sig = signals[0]
        entry = sig.entry_price
        sl = sig.stop_loss
        tp = sig.take_profit
        direction = sig.direction
        
        # Check future outcome
        future_df = df_15m.iloc[i:i+50]
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
