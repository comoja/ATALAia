import os, sys, datetime
from datetime import timedelta
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
from Sentinel.analysis import technical as _tech
import itertools

def load_data(symbol, timeframe, limit=50000):
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
        
        # Calcular ATR para SL/TP
        import talib as ta
        df['atr'] = ta.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    return df

def simulate_trade(df, entry_time, entry_price, sl_price, tp_price, direction):
    # buscar a partir del entry_time
    try:
        start_idx = df.index.get_loc(entry_time)
    except KeyError:
        return 'UNKNOWN', 0
        
    for i in range(start_idx + 1, min(start_idx + 500, len(df))):
        c_high = df['high'].iloc[i]
        c_low = df['low'].iloc[i]
        
        if direction == 'LARGO':
            if c_low <= sl_price: return 'LOSS', i - start_idx
            if c_high >= tp_price: return 'WIN', i - start_idx
        else:
            if c_high >= sl_price: return 'LOSS', i - start_idx
            if c_low <= tp_price: return 'WIN', i - start_idx
            
    return 'UNKNOWN', 0

def run_optimization():
    symbol = "XAU/USD"
    print(f"Loading data for {symbol}...")
    df_5m = load_data(symbol, '5min', 50000)
    
    if len(df_5m) < 1000:
        print("Not enough 5min data")
        return
        
    print(f"Data loaded: {len(df_5m)} candles. Optimizing MACD for IMACD Filter...")
    
    macd_slows = [20, 26, 30, 34, 40, 50]
    macd_signals = [5, 9, 12, 15]
    
    best_wr = 0
    best_params = None
    results = []
    
    rewardRatio = 1.5
    
    for s, sig in itertools.product(macd_slows, macd_signals):
        # detect FVGs with these MACD parameters
        raw_fvgs = _tech.detect_fvgs(df_5m, apply_high_prob_filters=True, use_impulse_macd_filter=True, macd_slow=s, macd_signal=sig)
        
        wins = 0
        losses = 0
        
        for f in raw_fvgs:
            if f.get('classification') == 'Rechazo/Baja Probabilidad': continue
            
            t = pd.to_datetime(f['timestamp']).replace(tzinfo=None)
            direction = 'LARGO' if f['type'] == 'Bullish_FVG' else 'CORTO'
            entryPrice = f['mid']
            
            atrVal = df_5m['atr'].loc[t] if 'atr' in df_5m.columns else 0.0001
            if pd.isna(atrVal): atrVal = 0.0001
            
            stopLoss = entryPrice - (atrVal * 1.5) if direction == 'LARGO' else entryPrice + (atrVal * 1.5)
            takeProfit = entryPrice + (atrVal * 1.5 * rewardRatio) if direction == 'LARGO' else entryPrice - (atrVal * 1.5 * rewardRatio)
            
            res, _ = simulate_trade(df_5m, t, entryPrice, stopLoss, takeProfit, direction)
            if res == 'WIN': wins += 1
            elif res == 'LOSS': losses += 1
            
        total = wins + losses
        wr = (wins / total * 100) if total > 0 else 0
        
        results.append({
            'slow': s, 'signal': sig, 'trades': total, 'wins': wins, 'losses': losses, 'win_rate': wr
        })
        print(f"[{s}, {sig}] -> Trades: {total}, Win Rate: {wr:.2f}%")
        
        if total > 10 and wr > best_wr:
            best_wr = wr
            best_params = (s, sig)
            
    print("\n=== OPTIMIZATION RESULTS ===")
    results.sort(key=lambda x: x['win_rate'], reverse=True)
    for r in results[:5]:
        print(f"Slow: {r['slow']}, Signal: {r['signal']} | WinRate: {r['win_rate']:.2f}% ({r['wins']}W / {r['losses']}L)")
        
if __name__ == '__main__':
    run_optimization()
