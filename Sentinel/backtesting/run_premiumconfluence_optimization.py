import os
import sys
import pandas as pd
import numpy as np
import talib as ta
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from Sentinel.analysis import technical
from Sentinel.backtesting import opt_db_helper

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def calculateSmoothedHeikinAshi(df, period1=10, period2=10):
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

def test_symbol_combination(df, symbol, st_period, st_mult, min_rr):
    if df is None or len(df) < 200:
        return 0, 0.0, 0.0, 0.0
        
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    
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
    
    trades = []
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
                    trades.append(-100.0)
                    in_trade = False
                elif high >= tp_price:
                    trades.append(100.0 * min_rr)
                    in_trade = False
            else:
                if high >= sl_price:
                    trades.append(-100.0)
                    in_trade = False
                elif low <= tp_price:
                    trades.append(100.0 * min_rr)
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
            
            if direction == "LARGO":
                tp_price = entry_price + (min_rr * slDist)
            else:
                tp_price = entry_price - (min_rr * slDist)
                
    tCount = len(trades)
    if tCount == 0:
        return 0, 0.0, 0.0, 0.0
    wins = len([t for t in trades if t > 0])
    losses = abs(sum([t for t in trades if t <= 0]))
    profit = sum([t for t in trades if t > 0])
    pf = profit / losses if losses > 0 else (99.0 if profit > 0 else 0.0)
    wr = (wins / tCount) * 100.0
    pnl = sum(trades)
    return tCount, wr, pf, pnl

def runPremiumConfluenceGridSearch():
    print("==========================================================")
    print("   INICIANDO GRID SEARCH OPTIMIZER (PREMIUM CONFLUENCE)   ")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    st_period_combos = [10, 14]
    st_mult_combos = [1.0, 1.5, 2.0, 2.5]
    min_rr_combos = [1.0, 1.5, 2.0, 2.5, 3.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            fallbackParams = {"st_period": 10, "st_mult": 1.5, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('PremiumConfluence', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 150:
            fallbackParams = {"st_period": 10, "st_mult": 1.5, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('PremiumConfluence', symbol, False, fallbackParams)
            continue
            
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for st_period in st_period_combos:
            for st_mult in st_mult_combos:
                for min_rr in min_rr_combos:
                    t, wr, pf, pnl = test_symbol_combination(df15m, symbol, st_period, st_mult, min_rr)
                    
                    if t > 0:
                        comboData = {
                            "Símbolo": symbol,
                            "st_period": st_period,
                            "st_mult": st_mult,
                            "min_rr": min_rr,
                            "Trades": t,
                            "Win Rate": f"{wr:.1f}%",
                            "Profit Factor": round(pf, 2),
                            "PnL USD": pnl
                        }
                        allResultsRaw.append(comboData)
                        
                        if pnl > symbolBestProfit and pf >= 1.0:
                            symbolBestProfit = pnl
                            symbolBestCombo = comboData
                            
        if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            params = {
                "st_period": symbolBestCombo["st_period"],
                "st_mult": symbolBestCombo["st_mult"],
                "min_rr": symbolBestCombo["min_rr"]
            }
            opt_db_helper.saveSymbolStrategyConfig('PremiumConfluence', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: ST=({symbolBestCombo['st_period']},{symbolBestCombo['st_mult']}) | RR={symbolBestCombo['min_rr']} | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"st_period": 10, "st_mult": 1.5, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('PremiumConfluence', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")

    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("premiumconfluence_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("premiumconfluence_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runPremiumConfluenceGridSearch()
