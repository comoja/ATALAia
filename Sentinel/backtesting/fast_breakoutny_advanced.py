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
start_dt = end_dt - datetime.timedelta(days=60)

print("Optimizando Opciones Avanzadas...")

def run_test(mode="standard", tp_ratio=1.0, max_atr_range=None, body_pct_filter=0.6):
    total_pnl = 0
    total_trades = 0
    
    for sym in symbols:
        df = load_data(sym, start_dt, end_dt)
        if df is None or len(df) < 200: continue
        
        if df.index.tz is None: df.index = df.index.tz_localize('UTC')
        df.index = df.index.tz_convert('America/Mexico_City')
        
        atr14 = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
        
        sym_trades = 0
        sym_pnl = 0
        
        df['date'] = df.index.date
        days = df['date'].unique()
        
        for day in days:
            df_day = df[df['date'] == day]
            if len(df_day) < 12: continue
            
            ny_tz = pytz.timezone("America/New_York")
            ny_start = ny_tz.localize(datetime.datetime.combine(day, datetime.time(8, 30)))
            ny_start = ny_start.astimezone(pytz.timezone('America/Mexico_City'))
            ny_end = ny_start + datetime.timedelta(minutes=15)
            
            df_range = df_day[(df_day.index >= ny_start) & (df_day.index < ny_end)]
            if len(df_range) < 3: continue
            
            rangeHigh = df_range['high'].max()
            rangeLow = df_range['low'].min()
            range_size = rangeHigh - rangeLow
            
            # Extract ATR at ny_end
            idx_end = df.index.get_loc(df_range.index[-1])
            current_atr = atr14[idx_end]
            
            if max_atr_range is not None and current_atr > 0:
                if range_size > (current_atr * max_atr_range):
                    continue # Range too big
                    
            trading_end = ny_end + datetime.timedelta(minutes=150)
            df_trade = df_day[(df_day.index >= ny_end) & (df_day.index <= trading_end)]
            
            for idx, row in df_trade.iterrows():
                close_price = row['close']
                open_price = row['open']
                
                body_size = abs(close_price - open_price)
                total_size = row['high'] - row['low']
                body_pct = body_size / total_size if total_size > 0 else 0
                
                direction = None
                entry = None
                sl = None
                
                if mode == "standard":
                    if body_pct < body_pct_filter: continue
                    if close_price > rangeHigh:
                        direction = "LARGO"
                        entry = close_price
                        sl = rangeLow
                    elif close_price < rangeLow:
                        direction = "CORTO"
                        entry = close_price
                        sl = rangeHigh
                
                elif mode == "fakeout":
                    # For fakeout, we need to look at previous candle breaking out, and current closing inside
                    idx_pos = df.index.get_loc(idx)
                    prev_row = df.iloc[idx_pos - 1]
                    prev_close = prev_row['close']
                    
                    if prev_close > rangeHigh and close_price < rangeHigh:
                        # Fakeout bullish breakout -> Go SHORT
                        direction = "CORTO"
                        entry = close_price
                        sl = max(prev_row['high'], row['high'])
                    elif prev_close < rangeLow and close_price > rangeLow:
                        # Fakeout bearish breakout -> Go LONG
                        direction = "LARGO"
                        entry = close_price
                        sl = min(prev_row['low'], row['low'])
                        
                if direction:
                    riskDist = abs(entry - sl)
                    if riskDist <= 0: continue
                    
                    if direction == "LARGO": tp = entry + (riskDist * tp_ratio)
                    else: tp = entry - (riskDist * tp_ratio)
                    
                    idx_pos = df.index.get_loc(idx)
                    future_df = df.iloc[idx_pos+1:idx_pos+100]
                    
                    trade_won = False
                    trade_lost = False
                    
                    for f_idx, f_row in future_df.iterrows():
                        if direction == "LARGO":
                            if f_row['low'] <= sl: trade_lost = True; break
                            if f_row['high'] >= tp: trade_won = True; break
                        else:
                            if f_row['high'] >= sl: trade_lost = True; break
                            if f_row['low'] <= tp: trade_won = True; break
                                
                    if trade_won: sym_pnl += tp_ratio
                    elif trade_lost: sym_pnl -= 1.0
                    
                    sym_trades += 1
                    break
                    
        total_trades += sym_trades
        total_pnl += sym_pnl
        
    return total_trades, total_pnl

print("\n--- 1. Ajuste del Ratio RR (Take Profit Parcial) ---")
for ratio in [0.8, 0.6, 0.5]:
    t, p = run_test(mode="standard", tp_ratio=ratio, body_pct_filter=0.6)
    wr = (p/ratio + t)/(2*t)*100 if t>0 else 0  # simplified math for WR display, wait it's easier:
    # Actually if won we add tp_ratio, if lost we subtract 1.
    # WR = (trades_won) / total_trades
    # we can calculate it better but let's just use a simple estimation or rewrite the function to return wins.
    pass

def run_test_accurate(mode="standard", tp_ratio=1.0, max_atr_range=None, body_pct_filter=0.6):
    wins, losses = 0, 0
    pnl = 0
    for sym in symbols:
        df = load_data(sym, start_dt, end_dt)
        if df is None or len(df) < 200: continue
        if df.index.tz is None: df.index = df.index.tz_localize('UTC')
        df.index = df.index.tz_convert('America/Mexico_City')
        atr14 = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
        df['date'] = df.index.date
        days = df['date'].unique()
        for day in days:
            df_day = df[df['date'] == day]
            if len(df_day) < 12: continue
            ny_tz = pytz.timezone("America/New_York")
            ny_start = ny_tz.localize(datetime.datetime.combine(day, datetime.time(8, 30)))
            ny_start = ny_start.astimezone(pytz.timezone('America/Mexico_City'))
            ny_end = ny_start + datetime.timedelta(minutes=15)
            df_range = df_day[(df_day.index >= ny_start) & (df_day.index < ny_end)]
            if len(df_range) < 3: continue
            rangeHigh = df_range['high'].max()
            rangeLow = df_range['low'].min()
            range_size = rangeHigh - rangeLow
            idx_end = df.index.get_loc(df_range.index[-1])
            current_atr = atr14[idx_end]
            if max_atr_range is not None and current_atr > 0 and range_size > (current_atr * max_atr_range):
                continue
            trading_end = ny_end + datetime.timedelta(minutes=150)
            df_trade = df_day[(df_day.index >= ny_end) & (df_day.index <= trading_end)]
            for idx, row in df_trade.iterrows():
                close_price = row['close']
                open_price = row['open']
                body_size = abs(close_price - open_price)
                total_size = row['high'] - row['low']
                body_pct = body_size / total_size if total_size > 0 else 0
                direction, entry, sl = None, None, None
                
                if mode == "standard":
                    if body_pct < body_pct_filter: continue
                    if close_price > rangeHigh:
                        direction = "LARGO"
                        entry = close_price
                        sl = rangeLow
                    elif close_price < rangeLow:
                        direction = "CORTO"
                        entry = close_price
                        sl = rangeHigh
                elif mode == "fakeout":
                    idx_pos = df.index.get_loc(idx)
                    prev_row = df.iloc[idx_pos - 1]
                    prev_close = prev_row['close']
                    if prev_close > rangeHigh and close_price < rangeHigh:
                        direction = "CORTO"
                        entry = close_price
                        sl = max(prev_row['high'], row['high'])
                    elif prev_close < rangeLow and close_price > rangeLow:
                        direction = "LARGO"
                        entry = close_price
                        sl = min(prev_row['low'], row['low'])
                        
                if direction:
                    riskDist = abs(entry - sl)
                    if riskDist <= 0: continue
                    tp = entry + (riskDist * tp_ratio) if direction == "LARGO" else entry - (riskDist * tp_ratio)
                    idx_pos = df.index.get_loc(idx)
                    future_df = df.iloc[idx_pos+1:idx_pos+100]
                    trade_won, trade_lost = False, False
                    for f_idx, f_row in future_df.iterrows():
                        if direction == "LARGO":
                            if f_row['low'] <= sl: trade_lost = True; break
                            if f_row['high'] >= tp: trade_won = True; break
                        else:
                            if f_row['high'] >= sl: trade_lost = True; break
                            if f_row['low'] <= tp: trade_won = True; break
                    if trade_won: wins += 1; pnl += tp_ratio
                    elif trade_lost: losses += 1; pnl -= 1.0
                    break
    t = wins + losses
    wr = (wins / t * 100) if t > 0 else 0
    return t, pnl, wr

for ratio in [1.0, 0.8, 0.6, 0.5]:
    t, p, wr = run_test_accurate(mode="standard", tp_ratio=ratio, body_pct_filter=0.6)
    print(f"TP {ratio:0.1f}x SL | Trades: {t:3d} | PNL: {p:5.2f} R | WR: {wr:.1f}%")

print("\n--- 2. Filtro de Tamaño de Rango Estrecho (< 1.5x ATR) ---")
for atr_mult in [None, 2.0, 1.5, 1.0]:
    t, p, wr = run_test_accurate(mode="standard", tp_ratio=1.0, max_atr_range=atr_mult, body_pct_filter=0.6)
    print(f"Rango < {str(atr_mult)+' ATR' if atr_mult else 'Ilimitado'} | Trades: {t:3d} | PNL: {p:5.2f} R | WR: {wr:.1f}%")

print("\n--- 3. Operar el Fallo (Fakeout / Turtle Soup) con TP 1.0x ---")
for atr_mult in [None, 2.0, 1.5]:
    t, p, wr = run_test_accurate(mode="fakeout", tp_ratio=1.0, max_atr_range=atr_mult)
    print(f"Fakeout (Rango < {str(atr_mult)+' ATR' if atr_mult else 'Ilimitado'}) | Trades: {t:3d} | PNL: {p:5.2f} R | WR: {wr:.1f}%")

