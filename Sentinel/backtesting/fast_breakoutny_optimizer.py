import os, sys, datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
import numpy as np
import talib as ta
from Sentinel.analysis import technical
import pytz

def load_data(symbol, hist_start, end_dt):
    conn = dbConnection.getConnection()
    try:
        q = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = '5min' AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
        """
        df = pd.read_sql(q, conn, params=(symbol, hist_start, end_dt))
    finally:
        if conn: conn.close()
    if df is not None and not df.empty:
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        df = df[~df.index.duplicated(keep='last')]
    return df

symbols = ["BTC/USD", "EUR/USD", "GBP/USD", "AUD/USD", "USD/JPY", "XAU/USD"]
end_dt = datetime.datetime.now()
start_dt = end_dt - datetime.timedelta(days=60) # 2 months

def test_combination(body_pct_filter, use_ema200, delay_filter_sec):
    total_pnl = 0
    total_trades = 0
    
    for sym in symbols:
        df = load_data(sym, start_dt, end_dt)
        if df is None or len(df) < 200: continue
        
        # Localize df to UTC if naive, then convert to CDMX to simulate live
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        df.index = df.index.tz_convert('America/Mexico_City')
        
        closePrices = df['close'].values
        openPrices = df['open'].values
        highPrices = df['high'].values
        lowPrices = df['low'].values
        
        ema200 = ta.EMA(closePrices, timeperiod=200)
        
        sym_trades = 0
        sym_pnl = 0
        
        # Group by day
        df['date'] = df.index.date
        days = df['date'].unique()
        
        for day in days:
            df_day = df[df['date'] == day]
            if len(df_day) < 12: continue
            
            # Find NY open 8:30 NY time
            ny_tz = pytz.timezone("America/New_York")
            ny_start = ny_tz.localize(datetime.datetime.combine(day, datetime.time(8, 30)))
            ny_start = ny_start.astimezone(pytz.timezone('America/Mexico_City'))
            ny_end = ny_start + datetime.timedelta(minutes=15)
            
            # Extract range
            df_range = df_day[(df_day.index >= ny_start) & (df_day.index < ny_end)]
            if len(df_range) < 3: continue # Need 3 M5 candles
            
            rangeHigh = df_range['high'].max()
            rangeLow = df_range['low'].min()
            
            # Check for breakout within next 150 minutes
            trading_end = ny_end + datetime.timedelta(minutes=150)
            df_trade = df_day[(df_day.index >= ny_end) & (df_day.index <= trading_end)]
            
            for idx, row in df_trade.iterrows():
                close_price = row['close']
                open_price = row['open']
                
                # Check body filter
                body_size = abs(close_price - open_price)
                total_size = row['high'] - row['low']
                body_pct = body_size / total_size if total_size > 0 else 0
                
                if body_pct < body_pct_filter: continue
                
                # EMA filter
                if use_ema200:
                    idx_pos = df.index.get_loc(idx)
                    ema_val = ema200[idx_pos]
                    if close_price > rangeHigh and close_price < ema_val: continue
                    if close_price < rangeLow and close_price > ema_val: continue
                
                direction = None
                if close_price > rangeHigh:
                    direction = "LARGO"
                    entry = close_price
                    sl = rangeLow
                    tp = entry + (entry - sl)
                elif close_price < rangeLow:
                    direction = "CORTO"
                    entry = close_price
                    sl = rangeHigh
                    tp = entry - (sl - entry)
                
                if direction:
                    # We have an entry. Find exit.
                    idx_pos = df.index.get_loc(idx)
                    future_df = df.iloc[idx_pos+1:idx_pos+100] # Check next 100 candles
                    
                    trade_won = False
                    trade_lost = False
                    
                    for f_idx, f_row in future_df.iterrows():
                        if direction == "LARGO":
                            if f_row['low'] <= sl:
                                trade_lost = True; break
                            if f_row['high'] >= tp:
                                trade_won = True; break
                        else:
                            if f_row['high'] >= sl:
                                trade_lost = True; break
                            if f_row['low'] <= tp:
                                trade_won = True; break
                                
                    if trade_won:
                        sym_pnl += 1.0
                    elif trade_lost:
                        sym_pnl -= 1.0
                    
                    sym_trades += 1
                    break # Only one trade per day
                    
        total_trades += sym_trades
        total_pnl += sym_pnl
        
    return total_trades, total_pnl

print("Optimizando BreakoutNY (Ratio 1:1, TF 5min)...")
print("Baseline (sin EMA, sin body filter):")
t, p = test_combination(0.0, False, 90)
print(f"Trades: {t:3d} | PNL: {p:5.2f} R | WR: {(p+t)/(2*t)*100 if t>0 else 0:.1f}%\n")

for body_pct in [0.0, 0.5, 0.7]:
    for use_ema in [False, True]:
        t, p = test_combination(body_pct, use_ema, 90)
        wr = (p+t)/(2*t)*100 if t>0 else 0
        print(f"Body > {body_pct*100:2.0f}% | EMA200: {use_ema!s:5s} | Trades: {t:3d} | PNL: {p:5.2f} R | WR: {wr:.1f}%")

