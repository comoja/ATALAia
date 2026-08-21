import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from Sentinel.analysis import technical
from Sentinel.backtesting import opt_db_helper

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def calculateLrc(closePrices: np.ndarray, period: int = 100, dev: float = 2.0):
    n = len(closePrices)
    center = np.full(n, np.nan)
    upper = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    slope = np.full(n, np.nan)
    
    if n < period:
        return center, upper, lower, slope
        
    x = np.arange(period)
    
    for i in range(period - 1, n):
        y = closePrices[i - period + 1 : i + 1]
        m, c = np.polyfit(x, y, 1)
        predVal = m * (period - 1) + c
        center[i] = predVal
        slope[i] = m
        
        yFit = m * x + c
        residuals = y - yFit
        stdDev = np.std(residuals)
        
        upper[i] = predVal + (dev * stdDev)
        lower[i] = predVal - (dev * stdDev)
        
    return center, upper, lower, slope

def checkDivergence(df: pd.DataFrame, rsiSeries: pd.Series, lookback: int = 5) -> dict:
    divergences = {"bullish": False, "bearish": False}
    if len(df) < lookback + 1:
        return divergences

    pricesLow = df['low'].tail(lookback)
    pricesHigh = df['high'].tail(lookback)
    rsiVals = rsiSeries.tail(lookback)

    if pricesLow.iloc[-1] <= pricesLow.iloc[:-1].min():
        minPriceIdx = pricesLow.iloc[:-1].idxmin()
        if minPriceIdx in rsiVals.index:
            if rsiVals.iloc[-1] > rsiVals.loc[minPriceIdx]:
                divergences["bullish"] = True

    if pricesHigh.iloc[-1] >= pricesHigh.iloc[:-1].max():
        maxPriceIdx = pricesHigh.iloc[:-1].idxmax()
        if maxPriceIdx in rsiVals.index:
            if rsiVals.iloc[-1] < rsiVals.loc[maxPriceIdx]:
                divergences["bearish"] = True

    return divergences

def runBacktestForCombo(df1h: pd.DataFrame, symbol: str, lrcPeriod: int, lrcDev: float, minRrVal: float) -> dict:
    df = df1h.copy()
    closePrices = df['close'].values.astype(float)
    centerChannel, upperChannel, lowerChannel, slopeChannel = calculateLrc(closePrices, period=lrcPeriod, dev=lrcDev)
    
    df["lrcCenter"] = centerChannel
    df["lrcUpper"] = upperChannel
    df["lrcLower"] = lowerChannel
    df["lrcSlope"] = slopeChannel
    
    df["rsi"] = pd.Series(ta.RSI(closePrices, timeperiod=14), index=df.index)
    df["atr"] = pd.Series(ta.ATR(df['high'].values.astype(float), df['low'].values.astype(float), closePrices, timeperiod=14), index=df.index)
    
    impulseMacd, impulseSignal = technical.calculateImpulseMacd(df)
    df["impulseMacd"] = impulseMacd
    df["impulseSignal"] = impulseSignal
    
    startIdx = max(lrcPeriod, 20) + 5
    if len(df) < startIdx:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    trades = []
    activeTrade = None
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    
    for i in range(startIdx, len(df)):
        currentPrice = float(df['close'].iloc[i])
        currentTime = df.index[i]
        
        if activeTrade:
            velaHigh = float(df['high'].iloc[i])
            velaLow = float(df['low'].iloc[i])
            direction = activeTrade['direction']
            sl = activeTrade['sl']
            tp = activeTrade['tp']
            
            closed = False
            exitPrice = 0.0
            pnlPips = 0.0
            
            if direction == "LARGO":
                if velaLow <= sl:
                    closed = True
                    exitPrice = sl
                    pnlPips = (sl - activeTrade['entry']) * pipMult
                elif velaHigh >= tp:
                    closed = True
                    exitPrice = tp
                    pnlPips = (tp - activeTrade['entry']) * pipMult
            else:
                if velaHigh >= sl:
                    closed = True
                    exitPrice = sl
                    pnlPips = (activeTrade['entry'] - sl) * pipMult
                elif velaLow <= tp:
                    closed = True
                    exitPrice = tp
                    pnlPips = (activeTrade['entry'] - tp) * pipMult
            
            if closed:
                pnlPips -= opt_db_helper.getSpread(symbol)
                trades.append({
                    "direction": direction,
                    "entryTime": activeTrade['entryTime'],
                    "exitTime": currentTime,
                    "pnl": pnlPips,
                    "result": "WIN" if pnlPips > 0 else "LOSS"
                })
                activeTrade = None
            continue
            
        avgVolume = df['volume'].rolling(window=20).mean().iloc[i]
        currentVolume = df['volume'].iloc[i]
        if currentVolume > 1.5 * avgVolume and avgVolume > 0:
            continue
            
        currentClose = float(df['close'].iloc[i])
        currentHigh = float(df['high'].iloc[i])
        currentLow = float(df['low'].iloc[i])
        currentRsi = float(df['rsi'].iloc[i])
        currentAtr = float(df['atr'].iloc[i])
        
        currentLrcUpper = float(df['lrcUpper'].iloc[i])
        currentLrcLower = float(df['lrcLower'].iloc[i])
        currentLrcSlope = float(df['lrcSlope'].iloc[i])
        
        if np.isnan(currentLrcUpper) or np.isnan(currentRsi) or np.isnan(currentAtr) or np.isnan(currentLrcSlope):
            continue
            
        divergences = checkDivergence(df.iloc[:i+1], df["rsi"].iloc[:i+1], lookback=5)
        
        currentImpulse = df["impulseMacd"].iloc[i]
        prevImpulse = df["impulseMacd"].iloc[i-1]
        currentSignal = df["impulseSignal"].iloc[i]
        prevSignal = df["impulseSignal"].iloc[i-1]
        
        impulseGiroLong = (currentImpulse > currentSignal) and (prevImpulse <= prevSignal)
        impulseGiroShort = (currentImpulse < currentSignal) and (prevImpulse >= prevSignal)
        
        isTrendBullish = (currentLrcSlope > 0)
        direction = None
        
        if currentClose < currentLrcLower and isTrendBullish:
            if (currentRsi < 30 or divergences["bullish"]) and impulseGiroLong:
                direction = "LARGO"
        elif currentClose > currentLrcUpper and not isTrendBullish:
            if (currentRsi > 70 or divergences["bearish"]) and impulseGiroShort:
                direction = "CORTO"
                
        if direction:
            swingLow = df['low'].iloc[max(0, i-14):i+1].min()
            swingHigh = df['high'].iloc[max(0, i-14):i+1].max()
            
            if direction == "LARGO":
                slPrice = min(currentLow - (1.5 * currentAtr), swingLow - (0.2 * currentAtr))
            else:
                slPrice = max(currentHigh + (1.5 * currentAtr), swingHigh + (0.2 * currentAtr))
                
            slDist = abs(currentClose - slPrice)
            if slDist <= 0: continue
            
            centerPrice = currentLrcCenter = float(df['lrcCenter'].iloc[i])
            if direction == "LARGO":
                tpCalculated = max(centerPrice, currentClose + (minRrVal * slDist))
            else:
                tpCalculated = min(centerPrice, currentClose - (minRrVal * slDist))
                
            activeTrade = {
                "direction": direction,
                "entry": currentClose,
                "sl": slPrice,
                "tp": tpCalculated,
                "entryTime": currentTime
            }

    if not trades:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    
    totalProfit = sum(t['pnl'] for t in wins)
    totalLoss = abs(sum(t['pnl'] for t in losses))
    
    winRate = (len(wins) / len(trades)) * 100.0
    profitFactor = totalProfit / totalLoss if totalLoss > 0 else 999.0 if totalProfit > 0 else 0.0
    pnlTotal = sum(t['pnl'] for t in trades)
    
    return {
        "trades": trades,
        "winRate": round(winRate, 2),
        "profitFactor": round(profitFactor, 2),
        "pnl": round(pnlTotal, 2)
    }

def runReversionMediaGridSearch():
    logger.info("==========================================================")
    logger.info("  INICIANDO GRID SEARCH OPTIMIZER (REVERSION A LA MEDIA)  ")
    logger.info("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    lrcPeriodCombos = [50, 80, 100, 120, 150]
    lrcDevCombos = [1.5, 1.8, 2.0, 2.2, 2.5]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos de 5m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"lrcPeriod": 100, "lrcDev": 2.0, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('ReversionMedia', symbol, False, fallbackParams)
            continue
            
        df1h = df5m.resample('1h').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df1h) < 150:
            logger.warning(f"  ⚠️ Datos de 1h insuficientes para {symbol}. Saltando.")
            fallbackParams = {"lrcPeriod": 100, "lrcDev": 2.0, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('ReversionMedia', symbol, False, fallbackParams)
            continue
            
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for lrcPeriod in lrcPeriodCombos:
            for lrcDev in lrcDevCombos:
                for minRr in minRrCombos:
                    res = runBacktestForCombo(df1h, symbol, lrcPeriod, lrcDev, minRr)
                    numTrades = len(res['trades'])
                    
                    row = {
                        "symbol": symbol,
                        "lrcPeriod": lrcPeriod,
                        "lrcDev": lrcDev,
                        "minRr": minRr,
                        "totalTrades": numTrades,
                        "winRate": round(res['winRate'], 2),
                        "profitFactor": round(res['profitFactor'], 2),
                        "pnl": round(res['pnl'], 2)
                    }
                    allResultsRaw.append(row)
                    
                    if numTrades >= 1 and res['winRate'] >= 35.0 and res['profitFactor'] >= 1.00:
                        if res['profitFactor'] > bestPf or (res['profitFactor'] == bestPf and res['winRate'] > bestWr):
                            bestPf = res['profitFactor']
                            bestWr = res['winRate']
                            bestCombo = row
                            
        if bestCombo:
            logger.info(f"  ✨ Mejor combo viable para {symbol}: LRC Period={bestCombo['lrcPeriod']}, LRC Dev={bestCombo['lrcDev']}, Min R:R={bestCombo['minRr']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
            params = {"lrcPeriod": bestCombo["lrcPeriod"], "lrcDev": bestCombo["lrcDev"], "minRr": bestCombo["minRr"]}
            opt_db_helper.saveSymbolStrategyConfig('ReversionMedia', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0 y WR >= 35%) para {symbol}.")
            fallbackParams = {"lrcPeriod": 100, "lrcDev": 2.0, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('ReversionMedia', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró combo viable para {symbol}. Guardado en DB (FALSE).")
            
    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: dfAll.to_csv(opt_db_helper.getOutputPath("reversionmedia_grid_results_all.csv"), index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty: dfBest.to_csv(opt_db_helper.getOutputPath("reversionmedia_grid_results_best.csv"), index=False)

if __name__ == "__main__":
    runReversionMediaGridSearch()
