import os, sys, datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
import numpy as np
import pytz

def load_historical_data(symbol, hist_start, end_dt):
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

symbols = ["BTC/USD", "EUR/USD", "GBP/USD", "AUD/USD", "USD/JPY", "XAU/USD", "XAG/USD", "US30", "NAS100", "SPX500"]

end_dt = datetime.datetime.now()
start_dt = end_dt - datetime.timedelta(days=30) 

def run_backtest_for_time(sh, sm, name):
    print(f"\\n--- EVALUANDO {name} (Filtros: EMA200 + Cuerpo >50%) ---")
    rd = 15
    rr = 1.0
    tw = 120
    
    total_pnl = 0
    total_trades = 0
    wins = 0
    losses = 0
    
    for sym in symbols:
        df = load_historical_data(sym, start_dt, end_dt)
        if df is None or df.empty: continue
        
        # Precompute EMA200 for the whole dataframe
        df['ema200'] = df['close'].ewm(span=200, adjust=False).mean()
        
        df['date'] = df.index.date
        days = df['date'].unique()
        
        sym_pnl = 0
        sym_trades = 0
        
        for d in days:
            df_day = df[df['date'] == d]
            if df_day.empty: continue
            
            ny_tz = pytz.timezone("America/New_York")
            ny_ts = pd.Timestamp(datetime.datetime.combine(d, datetime.time(sh, sm))).tz_localize(ny_tz)
            if df_day.index.tz is not None:
                start_ts = ny_ts.tz_convert(df_day.index.tz)
            else:
                local_tz = pytz.timezone("America/Mexico_City")
                start_ts = ny_ts.tz_convert(local_tz).tz_localize(None)
                
            end_ts = start_ts + datetime.timedelta(minutes=rd)
            df_range = df_day[(df_day.index >= start_ts) & (df_day.index < end_ts)]
            if df_range.empty: continue
            
            rangeHigh = float(df_range['high'].max())
            rangeLow = float(df_range['low'].min())
            
            limit_ts = end_ts + datetime.timedelta(minutes=tw)
            df_post = df_day[(df_day.index >= end_ts) & (df_day.index <= limit_ts)]
            if df_post.empty: continue
            
            break_idx = -1
            direction = None
            
            for i in range(len(df_post)):
                closePrice = df_post['close'].iloc[i]
                openPrice = df_post['open'].iloc[i]
                highPrice = df_post['high'].iloc[i]
                lowPrice = df_post['low'].iloc[i]
                ema200 = df_post['ema200'].iloc[i]
                
                body_size = abs(closePrice - openPrice)
                total_size = highPrice - lowPrice
                body_pct = body_size / total_size if total_size > 0 else 0
                
                if closePrice > rangeHigh:
                    if i > 0 and df_post['close'].iloc[i-1] > rangeHigh: continue
                    if closePrice < ema200: continue
                    if body_pct < 0.50: continue
                    break_idx = i
                    direction = 'LARGO'
                    break
                elif closePrice < rangeLow:
                    if i > 0 and df_post['close'].iloc[i-1] < rangeLow: continue
                    if closePrice > ema200: continue
                    if body_pct < 0.50: continue
                    break_idx = i
                    direction = 'CORTO'
                    break
                    
            if direction is None: continue
            
            entry = df_post['close'].iloc[break_idx]
            if direction == 'LARGO':
                sl = rangeLow
                riskDist = entry - sl
                tp = entry + (riskDist * rr)
            else:
                sl = rangeHigh
                riskDist = sl - entry
                tp = entry - (riskDist * rr)
                
            if riskDist <= 0: continue
            
            df_future = df_day.iloc[df_day.index.get_loc(df_post.index[break_idx]) + 1:]
            if df_future.empty: continue
            
            highs = df_future['high'].values
            lows = df_future['low'].values
            
            trade_res = None
            if direction == 'LARGO':
                hit_sl = lows <= sl
                hit_tp = highs >= tp
            else:
                hit_sl = highs >= sl
                hit_tp = lows <= tp
                
            sl_idx = np.argmax(hit_sl) if hit_sl.any() else -1
            tp_idx = np.argmax(hit_tp) if hit_tp.any() else -1
            
            if sl_idx != -1 and tp_idx != -1:
                trade_res = -1.0 if sl_idx <= tp_idx else rr
            elif sl_idx != -1:
                trade_res = -1.0
            elif tp_idx != -1:
                trade_res = rr
                
            if trade_res is not None:
                sym_trades += 1
                sym_pnl += trade_res
                if trade_res > 0: wins += 1
                else: losses += 1
                
        if sym_trades > 0:
            print(f"  {sym:8s}: {sym_trades:2d} trades | PNL: {sym_pnl:5.2f} R")
            
        total_pnl += sym_pnl
        total_trades += sym_trades
        
    winrate = (wins/total_trades*100) if total_trades > 0 else 0
    print(f"--- RESUMEN GLOBAL ---")
    print(f"Total Trades: {total_trades}")
    print(f"Total PNL:    {total_pnl:.2f} R")
    print(f"Winrate:      {winrate:.1f}%")

run_backtest_for_time(8, 30, "Apertura Forex (8:00 + 30m)")
run_backtest_for_time(10, 0, "Apertura US Equities (9:30 + 30m)")
