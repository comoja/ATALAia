import os, sys, datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection
import pandas as pd
import numpy as np
import talib as ta
from Sentinel.analysis import technical

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
        
        df = df.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
    return df

symbols = ["BTC/USD", "EUR/USD", "GBP/USD", "AUD/USD", "USD/JPY", "XAU/USD"]
end_dt = datetime.datetime.now()
start_dt = end_dt - datetime.timedelta(days=30) 

def calculateSmoothedHeikinAshi(df, period1, period2):
    openSmooth = df['open'].ewm(span=period1, adjust=False).mean()
    highSmooth = df['high'].ewm(span=period1, adjust=False).mean()
    lowSmooth = df['low'].ewm(span=period1, adjust=False).mean()
    closeSmooth = df['close'].ewm(span=period1, adjust=False).mean()
    haClose = (openSmooth + highSmooth + lowSmooth + closeSmooth) / 4.0
    haOpen = np.zeros(len(df))
    haOpen[0] = (openSmooth.iloc[0] + closeSmooth.iloc[0]) / 2.0
    for i in range(1, len(df)):
        haOpen[i] = (haOpen[i - 1] + haClose.iloc[i - 1]) / 2.0
    haOpenSeries = pd.Series(haOpen, index=df.index)
    haCloseSmooth = haClose.ewm(span=period2, adjust=False).mean()
    haOpenSmooth = haOpenSeries.ewm(span=period2, adjust=False).mean()
    return haCloseSmooth.values, haOpenSmooth.values

def test_combination(st_period, st_mult, min_rr):
    total_pnl = 0
    total_trades = 0
    
    for sym in symbols:
        df = load_data(sym, start_dt, end_dt)
        if df is None or len(df) < 200: continue
        
        closePrices = df['close'].values
        highPrices = df['high'].values
        lowPrices = df['low'].values
        
        ema200 = ta.EMA(closePrices, timeperiod=200)
        ema200 = pd.Series(ema200).ffill().bfill().values
        
        stTrend, stTrail = technical.calculateAtrStop(df, st_period, st_mult)
        haClose, haOpen = calculateSmoothedHeikinAshi(df, 10, 10)
        
        df_impulse = df.copy()
        impMacd, impSig = technical.calculateImpulseMacd(df_impulse, 20, 9)
        impMacd = impMacd.values
        impSig = impSig.values
        
        atr = ta.ATR(highPrices, lowPrices, closePrices, timeperiod=14)
        
        sym_trades = 0
        sym_pnl = 0
        in_trade = False
        trade_dir = None
        entry_price = 0
        sl_price = 0
        tp_price = 0
        
        for i in range(200, len(df) - 1):
            if in_trade:
                high = highPrices[i]
                low = lowPrices[i]
                
                if trade_dir == "LARGO":
                    if low <= sl_price:
                        sym_pnl -= 1.0
                        in_trade = False
                    elif high >= tp_price:
                        sym_pnl += min_rr
                        in_trade = False
                else:
                    if high >= sl_price:
                        sym_pnl -= 1.0
                        in_trade = False
                    elif low <= tp_price:
                        sym_pnl += min_rr
                        in_trade = False
                        
                continue

            lookback = 3
            recent_df = df.iloc[i-lookback:i]
            current_close = closePrices[i]
            
            mssBullish = current_close > recent_df['high'].max()
            mssBearish = current_close < recent_df['low'].min()
            
            macroBullish = current_close > ema200[i]
            macroBearish = current_close < ema200[i]
            
            supertrendBullish = stTrend[i] == 1
            supertrendBearish = stTrend[i] == -1
            
            haBullish = haClose[i] > haOpen[i]
            haBearish = haClose[i] < haOpen[i]
            
            macdBullish = impMacd[i] > impSig[i]
            macdBearish = impMacd[i] < impSig[i]
            
            haWasBearish = any(haClose[j] < haOpen[j] for j in range(i-4, i))
            haWasBullish = any(haClose[j] > haOpen[j] for j in range(i-4, i))
            macdWasBearish = any(impMacd[j] < impSig[j] for j in range(i-4, i))
            macdWasBullish = any(impMacd[j] > impSig[j] for j in range(i-4, i))

            direction = None
            if macroBullish and supertrendBullish and haBullish and macdBullish and mssBullish:
                if haWasBearish or macdWasBearish:
                    direction = "LARGO"
            elif macroBearish and supertrendBearish and haBearish and macdBearish and mssBearish:
                if haWasBullish or macdWasBullish:
                    direction = "CORTO"
                
            if direction:
                slDist = abs(current_close - stTrail[i])
                if slDist <= 0 or slDist > 3.0 * atr[i]: continue
                
                candle_range = highPrices[i] - lowPrices[i]
                if candle_range > 3.0 * atr[i]: continue
                
                in_trade = True
                trade_dir = direction
                entry_price = current_close
                sl_price = stTrail[i]
                sym_trades += 1
                
                if direction == "LARGO":
                    tp_price = entry_price + (min_rr * slDist)
                else:
                    tp_price = entry_price - (min_rr * slDist)
                    
        total_trades += sym_trades
        total_pnl += sym_pnl
        
    return total_trades, total_pnl

print("Evaluando combinaciones de PremiumConfluence (PULLBACK LOGIC - 30 dias - 15min tf)...")
for st_mult in [1.0, 1.5, 2.0]:
    for min_rr in [1.0, 1.5, 2.0]:
        t, p = test_combination(10, st_mult, min_rr)
        print(f"ST(10,{st_mult}) RR:{min_rr} | Trades: {t:3d} | PNL: {p:5.2f} R")

