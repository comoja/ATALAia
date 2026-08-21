import os

base_dir = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/Sentinel/backtesting'

def write_script(filename, content):
    path = os.path.join(base_dir, filename)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content.strip() + '\n')
    print(f"✅ Generated {filename}")

# 4. run_qtrend_optimization.py
qtrend_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import asyncio
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.utils.alertBuilder import adjustTPForMinRR
from Sentinel.analysis import technical
from Sentinel.backtesting import opt_db_helper

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def runBacktestForCombo(df15m: pd.DataFrame, symbol: str, superPeriod: int, superMult: float, fastPeriod: int, slowPeriod: int, tpParam: float) -> dict:
    stTrend, stTrail = technical.calculateAtrStop(df15m, superPeriod, superMult)
    closePrices = df15m['close'].values.astype(float)
    highPrices = df15m['high'].values.astype(float)
    lowPrices = df15m['low'].values.astype(float)
    
    emaFast = ta.EMA(closePrices, timeperiod=fastPeriod)
    emaSlow = ta.EMA(closePrices, timeperiod=slowPeriod)
    atrSeries = ta.ATR(highPrices, lowPrices, closePrices, timeperiod=14)
    
    if len(df15m) < max(superPeriod, slowPeriod) + 10:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    trades = []
    activeTrade = None
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    symbolType = 'CRYPTO' if symbol == 'BTC/USD' else 'METAL' if 'XAU' in symbol or 'XAG' in symbol else 'MONEDA'

    for i in range(max(superPeriod, slowPeriod) + 5, len(df15m)):
        currentPrice = float(closePrices[i])
        currentTime = df15m.index[i]
        
        if activeTrade:
            velaHigh = float(highPrices[i])
            velaLow = float(lowPrices[i])
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
            
        stBullish = stTrend[i] == 1
        stBearish = stTrend[i] == -1
        qBullish = emaFast[i] > emaSlow[i]
        qBearish = emaFast[i] < emaSlow[i]
        
        stBullishAge = -1
        stBearishAge = -1
        qBullishAge = -1
        qBearishAge = -1
        
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if stTrend[idx-1] == -1 and stTrend[idx] == 1:
                    stBullishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if stTrend[idx-1] == 1 and stTrend[idx] == -1:
                    stBearishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if emaFast[idx-1] <= emaSlow[idx-1] and emaFast[idx] > emaSlow[idx]:
                    qBullishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if emaFast[idx-1] >= emaSlow[idx-1] and emaFast[idx] < emaSlow[idx]:
                    qBearishAge = age
                    break
                    
        direction = None
        if stBullish and qBullish and 0 <= stBullishAge <= 3 and 0 <= qBullishAge <= 3:
            direction = "LARGO"
        elif stBearish and qBearish and 0 <= stBearishAge <= 3 and 0 <= qBearishAge <= 3:
            direction = "CORTO"
            
        if direction:
            slPrice = float(stTrail[i])
            slDist = abs(currentPrice - slPrice)
            if slDist <= 0:
                continue
                
            atrVal = atrSeries[i] if not np.isnan(atrSeries[i]) else 0.0
            if atrVal > 0 and slDist > 2.5 * atrVal:
                continue
                
            if symbolType == 'MONEDA':
                if direction == "LARGO":
                    calculatedTp = currentPrice + (tpParam * slDist)
                else:
                    calculatedTp = currentPrice - (tpParam * slDist)
            else:
                tpPctDecimal = tpParam / 100.0
                if direction == "LARGO":
                    calculatedTp = currentPrice * (1.0 + tpPctDecimal)
                else:
                    calculatedTp = currentPrice * (1.0 - tpPctDecimal)
                    
            tpPrice = adjustTPForMinRR(currentPrice, slPrice, calculatedTp, direction, minRR=1.5 if symbolType != 'MONEDA' else tpParam)
            
            activeTrade = {
                "direction": direction,
                "entry": currentPrice,
                "sl": slPrice,
                "tp": tpPrice,
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

async def runQTrendGridSearch():
    logger.info("==========================================================")
    logger.info("  INICIANDO GRID SEARCH OPTIMIZER (QTREND - 15MIN DATA)  ")
    logger.info("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    supertrendPeriodCombos = [8, 10, 12, 14, 16]
    supertrendMultiplierCombos = [1.5, 2.0, 2.5, 3.0, 3.5]
    qtrendFastCombos = [7, 9, 11, 12, 14]
    qtrendSlowCombos = [18, 21, 24, 26, 30]
    
    tpParamCombosMoneda = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    tpParamCombosExotic = [1.0, 1.5, 2.0, 2.5, 3.0]
    
    allResultsRaw = []
    bestResults = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos de 5m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"min_rr": 1.5, "supertrendPeriod": 10, "supertrendMultiplier": 3.0, "qtrendFast": 12, "qtrendSlow": 26, "tpParam": 2.0}
            opt_db_helper.saveSymbolStrategyConfig('QTrend', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 100:
            logger.warning(f"  ⚠️ Datos resampleados a 15m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"min_rr": 1.5, "supertrendPeriod": 10, "supertrendMultiplier": 3.0, "qtrendFast": 12, "qtrendSlow": 26, "tpParam": 2.0}
            opt_db_helper.saveSymbolStrategyConfig('QTrend', symbol, False, fallbackParams)
            continue
            
        isMoneda = symbol not in ['XAU/USD', 'BTC/USD', 'XAG/USD', 'NAS100']
        tpCombos = tpParamCombosMoneda if isMoneda else tpParamCombosExotic
        
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for sPeriod in supertrendPeriodCombos:
            for sMult in supertrendMultiplierCombos:
                for qFast in qtrendFastCombos:
                    for qSlow in qtrendSlowCombos:
                        for tpParam in tpCombos:
                            metrics = runBacktestForCombo(df15m, symbol, sPeriod, sMult, qFast, qSlow, tpParam)
                            nTrades = len(metrics['trades'])
                            
                            resRow = {
                                "symbol": symbol,
                                "supertrendPeriod": sPeriod,
                                "supertrendMultiplier": sMult,
                                "qtrendFast": qFast,
                                "qtrendSlow": qSlow,
                                "tpParam": tpParam,
                                "nTrades": nTrades,
                                "winRate": metrics['winRate'],
                                "profitFactor": metrics['profitFactor'],
                                "pnl": metrics['pnl']
                            }
                            allResultsRaw.append(resRow)
                            
                            if nTrades > 0 and metrics['profitFactor'] >= 1.00 and metrics['winRate'] >= 35.0:
                                if metrics['profitFactor'] > bestPf or (metrics['profitFactor'] == bestPf and metrics['winRate'] > bestWr):
                                    bestPf = metrics['profitFactor']
                                    bestWr = metrics['winRate']
                                    bestCombo = resRow
        
        if bestCombo:
            bestResults.append(bestCombo)
            params = {
                "min_rr": bestCombo["tpParam"],
                "supertrendPeriod": bestCombo["supertrendPeriod"],
                "supertrendMultiplier": bestCombo["supertrendMultiplier"],
                "qtrendFast": bestCombo["qtrendFast"],
                "qtrendSlow": bestCombo["qtrendSlow"],
                "tpParam": bestCombo["tpParam"]
            }
            opt_db_helper.saveSymbolStrategyConfig('QTrend', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            logger.info(f"  🏆 Mejor combo viable para {symbol}: ST_P={bestCombo['supertrendPeriod']}, ST_M={bestCombo['supertrendMultiplier']}, QF={bestCombo['qtrendFast']}, QS={bestCombo['qtrendSlow']}, TP={bestCombo['tpParam']} | PF={bestCombo['profitFactor']}, WR={bestCombo['winRate']}%, PnL={bestCombo['pnl']:.2f}")
        else:
            logger.info(f"  ❌ Ningún combo cumplió con el umbral de viabilidad para {symbol}.")
            fallbackParams = {"min_rr": 1.5, "supertrendPeriod": 10, "supertrendMultiplier": 3.0, "qtrendFast": 12, "qtrendSlow": 26, "tpParam": 2.0}
            opt_db_helper.saveSymbolStrategyConfig('QTrend', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró combo viable para {symbol}. Guardado en DB (FALSE).")

    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: dfAll.to_csv(opt_db_helper.getOutputPath("qtrend_grid_results_all.csv"), index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty: dfBest.to_csv(opt_db_helper.getOutputPath("qtrend_grid_results_best.csv"), index=False)

if __name__ == "__main__":
    asyncio.run(runQTrendGridSearch())
'''
write_script('run_qtrend_optimization.py', qtrend_code)

# 5. run_reversionmedia_optimization.py
reversionmedia_code = '''import sys
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
'''
write_script('run_reversionmedia_optimization.py', reversionmedia_code)

# 6. run_sesgobiashtf_optimization.py
sesgobiashtf_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import asyncio
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.utils.alertBuilder import adjustTPForMinRR
from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot
from Sentinel.backtesting import opt_db_helper

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("sentinel").setLevel(logging.WARNING)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

async def runBacktestForCombo(df15m: pd.DataFrame, symbol: str, swingLookback: int, mssLookback: int, minRr: float) -> dict:
    bot = SesgoBiasHTFBot()
    bot.swing_lookback = swingLookback
    bot.swingLookback = swingLookback
    bot.mss_lookback = mssLookback
    bot.mssLookback = mssLookback
    bot.minRr = minRr
    
    df_daily = bot.resample_ohlcv(df15m, '1d')
    if len(df_daily) >= 14:
        df_daily['sma14'] = df_daily['close'].rolling(14).mean()
        last_sma = df_daily['sma14'].iloc[-1]
        last_close = df_daily['close'].iloc[-1]
        weekly_trend = "ALCISTA" if last_close > last_sma else "BAJISTA"
    else:
        weekly_trend = "NEUTRAL"
        
    symbol_info = {
        'symbol': symbol,
        'weekly_trend': weekly_trend
    }
    
    trades = []
    activeTrade = None
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    
    startIdx = max(swingLookback, mssLookback, 50) + 10
    if len(df15m) < startIdx:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    for i in range(startIdx, len(df15m)):
        currentTime = df15m.index[i]
        
        if activeTrade:
            velaHigh = float(df15m['high'].iloc[i])
            velaLow = float(df15m['low'].iloc[i])
            direction = activeTrade['direction']
            sl = activeTrade['sl']
            tp = activeTrade['tp']
            
            closed = False
            pnlPips = 0.0
            
            if direction == "LARGO":
                if velaLow <= sl:
                    closed = True
                    pnlPips = (sl - activeTrade['entry']) * pipMult
                elif velaHigh >= tp:
                    closed = True
                    pnlPips = (tp - activeTrade['entry']) * pipMult
            else:
                if velaHigh >= sl:
                    closed = True
                    pnlPips = (activeTrade['entry'] - sl) * pipMult
                elif velaLow <= tp:
                    closed = True
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
            
        df_slice = df15m.iloc[:i+1]
        try:
            signal = await bot.analyze(df_slice, symbol_info)
            if signal and signal.action in ["COMPRA", "VENTA"]:
                direction = "LARGO" if signal.action == "COMPRA" else "CORTO"
                currentClose = float(df15m['close'].iloc[i])
                slPrice = signal.stopLoss
                tpPrice = signal.takeProfit
                
                slDist = abs(currentClose - slPrice)
                if slDist > 0:
                    activeTrade = {
                        "direction": direction,
                        "entry": currentClose,
                        "sl": slPrice,
                        "tp": tpPrice,
                        "entryTime": currentTime
                    }
        except Exception:
            continue
            
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

async def runSesgoBiasHTFGridSearch():
    logger.info("==========================================================")
    logger.info("  INICIANDO GRID SEARCH OPTIMIZER (SESGO BIAS HTF)       ")
    logger.info("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    swingLookbackCombos = [15, 20, 25, 30]
    mssLookbackCombos = [8, 10, 12, 15]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos de 5m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"swingLookback": 20, "mssLookback": 10, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('SesgoBiasHTF', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 150:
            logger.warning(f"  ⚠️ Datos de 15m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"swingLookback": 20, "mssLookback": 10, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('SesgoBiasHTF', symbol, False, fallbackParams)
            continue
            
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for swingLookback in swingLookbackCombos:
            for mssLookback in mssLookbackCombos:
                for minRr in minRrCombos:
                    res = await runBacktestForCombo(df15m, symbol, swingLookback, mssLookback, minRr)
                    numTrades = len(res['trades'])
                    
                    row = {
                        "symbol": symbol,
                        "swingLookback": swingLookback,
                        "mssLookback": mssLookback,
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
            logger.info(f"  ✨ Mejor combo viable para {symbol}: Swing={bestCombo['swingLookback']}, MSS={bestCombo['mssLookback']}, Min R:R={bestCombo['minRr']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
            params = {
                "swingLookback": int(bestCombo['swingLookback']),
                "mssLookback": int(bestCombo['mssLookback']),
                "minRr": float(bestCombo['minRr'])
            }
            opt_db_helper.saveSymbolStrategyConfig('SesgoBiasHTF', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0 y WR >= 35%) para {symbol}.")
            fallbackParams = {"swingLookback": 20, "mssLookback": 10, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('SesgoBiasHTF', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró combo viable para {symbol}. Guardado en DB (FALSE).")
            
    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: dfAll.to_csv(opt_db_helper.getOutputPath("sesgobiashtf_grid_results_all.csv"), index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty: dfBest.to_csv(opt_db_helper.getOutputPath("sesgobiashtf_grid_results_best.csv"), index=False)

if __name__ == "__main__":
    asyncio.run(runSesgoBiasHTFGridSearch())
'''
write_script('run_sesgobiashtf_optimization.py', sesgobiashtf_code)

# 7. run_ichimoku_optimization.py
ichimoku_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.utils.alertBuilder import adjustTPForMinRR
from Sentinel.backtesting import opt_db_helper

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def calculateIchimokuIndicators(df: pd.DataFrame, tenkan: int, kijun: int, senkou: int, displacement: int) -> pd.DataFrame:
    high_prices = df['high']
    low_prices = df['low']
    close_prices = df['close']

    tenkan_sen = (high_prices.rolling(window=tenkan).max() + low_prices.rolling(window=tenkan).min()) / 2
    kijun_sen = (high_prices.rolling(window=kijun).max() + low_prices.rolling(window=kijun).min()) / 2
    senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(displacement)
    senkou_span_b = ((high_prices.rolling(window=senkou).max() + low_prices.rolling(window=senkou).min()) / 2).shift(displacement)
    chikou_span = close_prices.shift(-displacement)

    df_res = pd.DataFrame(index=df.index)
    df_res['open'] = df['open']
    df_res['high'] = df['high']
    df_res['low'] = df['low']
    df_res['close'] = df['close']
    df_res['volume'] = df.get('volume', 0)
    df_res['tenkan'] = tenkan_sen
    df_res['kijun'] = kijun_sen
    df_res['senkou_a'] = senkou_span_a
    df_res['senkou_b'] = senkou_span_b
    df_res['chikou'] = chikou_span
    
    return df_res

def calculateHtfTrendSeries(dfHtf: pd.DataFrame, tenkan: int, kijun: int, senkou: int, displacement: int) -> pd.Series:
    df_ichi = calculateIchimokuIndicators(dfHtf, tenkan, kijun, senkou, displacement)
    close = df_ichi['close']
    span_a = df_ichi['senkou_a']
    span_b = df_ichi['senkou_b']
    
    trend = pd.Series("NEUTRAL", index=dfHtf.index)
    bull_mask = (close > span_a) & (close > span_b)
    bear_mask = (close < span_a) & (close < span_b)
    trend[bull_mask] = "ALCISTA"
    trend[bear_mask] = "BAJISTA"
    return trend

def runIchimokuGridSearch() -> None:
    print("==========================================================")
    print("  INICIANDO GRID SEARCH OPTIMIZER (ICHIMOKU CLOUD SYSTEM)")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    tenkanCombos = [7, 9, 12]
    kijunCombos = [22, 26, 30]
    senkouCombos = [44, 52, 60]
    displacement = 26
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 600:
            print(f"⚠️ Datos insuficientes para {symbol} ({len(df5m)} velas).")
            fallbackParams = {"tenkan": 9, "kijun": 26, "senkou": 52, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('Ichimoku', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        df1h = df5m.resample('1h').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 220 or len(df1h) < 60:
            fallbackParams = {"tenkan": 9, "kijun": 26, "senkou": 52, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('Ichimoku', symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for tenkan in tenkanCombos:
            for kijun in kijunCombos:
                for senkou in senkouCombos:
                    htfTrendSeries = calculateHtfTrendSeries(df1h, tenkan, kijun, senkou, displacement)
                    htfTrendAligned = htfTrendSeries.reindex(df15m.index, method='ffill').fillna("NEUTRAL")
                    
                    df_ichi = calculateIchimokuIndicators(df15m, tenkan, kijun, senkou, displacement)
                    atr14 = ta.ATR(df15m['high'].values, df15m['low'].values, df15m['close'].values, timeperiod=14)
                    
                    closes = df15m['close'].values
                    highs = df15m['high'].values
                    lows = df15m['low'].values
                    opens = df15m['open'].values
                    n = len(df15m)
                    
                    tenkan_vals = df_ichi['tenkan'].values
                    kijun_vals = df_ichi['kijun'].values
                    span_a_vals = df_ichi['senkou_a'].values
                    span_b_vals = df_ichi['senkou_b'].values
                    trends = htfTrendAligned.values
                    
                    validIdx = np.where(~np.isnan(tenkan_vals) & ~np.isnan(kijun_vals) & ~np.isnan(span_a_vals) & ~np.isnan(span_b_vals) & ~np.isnan(atr14))[0]
                    if len(validIdx) < 100: continue
                    
                    candidates = []
                    idx = validIdx[0]
                    while idx < n - 1:
                        cPrice = closes[idx]
                        kPrice = kijun_vals[idx]
                        tPrice = tenkan_vals[idx]
                        spA = span_a_vals[idx]
                        spB = span_b_vals[idx]
                        atrV = atr14[idx]
                        tr = trends[idx]
                        
                        isKumoBull = spA > spB
                        kumoTop = max(spA, spB)
                        kumoBottom = min(spA, spB)
                        
                        isLong = (cPrice > kumoTop) and (tPrice > kPrice) and (cPrice >= kPrice) and (tr == "ALCISTA")
                        isShort = (cPrice < kumoBottom) and (tPrice < kPrice) and (cPrice <= kPrice) and (tr == "BAJISTA")
                        
                        direction = None
                        if isLong: direction = "LARGO"
                        elif isShort: direction = "CORTO"
                        
                        if direction:
                            candidates.append({
                                'idx': idx,
                                'direction': direction,
                                'price': cPrice,
                                'atrVal': atrV,
                                'kijunVal': kPrice,
                                'kumoTop': kumoTop,
                                'kumoBottom': kumoBottom
                            })
                        idx += 1
                        
                    for minRr in minRrCombos:
                        trades = []
                        last_exit_idx = -1
                        
                        for cand in candidates:
                            if cand['idx'] <= last_exit_idx:
                                continue
                                
                            direction = cand['direction']
                            price = cand['price']
                            atrVal = cand['atrVal']
                            idx_entry = cand['idx']
                            
                            if direction == "LARGO":
                                sl = min(cand['kijunVal'], cand['kumoTop']) - (atrVal * 0.2)
                            else:
                                sl = max(cand['kijunVal'], cand['kumoBottom']) + (atrVal * 0.2)
                                
                            slDist = abs(price - sl)
                            if slDist <= 0 or slDist > (atrVal * 4.0):
                                continue
                                
                            tpDist = slDist * minRr
                            tp = price + tpDist if direction == "LARGO" else price - tpDist
                            
                            lows_slice = lows[idx_entry:]
                            highs_slice = highs[idx_entry:]
                            
                            if direction == "LARGO":
                                sl_hits = np.where(lows_slice - (spreadPrice / 2.0) <= sl)[0]
                                tp_hits = np.where(highs_slice + (spreadPrice / 2.0) >= tp)[0]
                            else:
                                sl_hits = np.where(highs_slice + (spreadPrice / 2.0) >= sl)[0]
                                tp_hits = np.where(lows_slice - (spreadPrice / 2.0) <= tp)[0]
                                
                            first_sl = sl_hits[0] if len(sl_hits) > 0 else n
                            first_tp = tp_hits[0] if len(tp_hits) > 0 else n
                            
                            if first_sl < first_tp:
                                trades.append(-100.0)
                                last_exit_idx = idx_entry + first_sl
                            elif first_tp < first_sl:
                                trades.append(100.0 * minRr)
                                last_exit_idx = idx_entry + first_tp
                            else:
                                last_exit_idx = n
                                
                        tCount = len(trades)
                        if tCount > 3:
                            wCount = len([t for t in trades if t > 0])
                            wRate = (wCount / tCount) * 100
                            pnlNet = sum(trades)
                            
                            profitCount = sum([t for t in trades if t > 0])
                            lossCount = abs(sum([t for t in trades if t <= 0]))
                            profFactor = profitCount / lossCount if lossCount > 0 else float('inf')
                            
                            comboData = {
                                'Símbolo': symbol,
                                'Tenkan': tenkan,
                                'Kijun': kijun,
                                'Senkou': senkou,
                                'Min RR': minRr,
                                'Trades': tCount,
                                'Win Rate': f"{wRate:.1f}%",
                                'Profit Factor': round(profFactor, 2),
                                'PnL USD': pnlNet
                            }
                            allResultsRaw.append(comboData)
                            
                            if pnlNet > symbolBestProfit and profFactor >= 1.0:
                                symbolBestProfit = pnlNet
                                symbolBestCombo = comboData
                                
        if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            params = {
                "tenkan": symbolBestCombo['Tenkan'],
                "kijun": symbolBestCombo['Kijun'],
                "senkou": symbolBestCombo['Senkou'],
                "min_rr": symbolBestCombo['Min RR']
            }
            opt_db_helper.saveSymbolStrategyConfig('Ichimoku', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: Tenkan={symbolBestCombo['Tenkan']} | Kijun={symbolBestCombo['Kijun']} | Senkou={symbolBestCombo['Senkou']} | RR={symbolBestCombo['Min RR']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"tenkan": 9, "kijun": 26, "senkou": 52, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('Ichimoku', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")

    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("ichimoku_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("ichimoku_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runIchimokuGridSearch()
'''
write_script('run_ichimoku_optimization.py', ichimoku_code)

# 8. run_breakoutprobability_optimization.py
breakoutprob_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.utils.alertBuilder import adjustTPForMinRR
from Sentinel.backtesting import opt_db_helper

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def runBreakoutProbabilityGridSearch() -> None:
    print("==========================================================")
    print("  INICIANDO GRID SEARCH OPTIMIZER (BREAKOUT PROBABILITY)")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    channelLenCombos = [15, 20, 25, 30]
    atrMultCombos = [1.0, 1.2, 1.5, 2.0]
    minProbCombos = [0.0, 45.0, 50.0, 55.0, 60.0]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 600:
            print(f"⚠️ Datos insuficientes para {symbol} ({len(df5m)} velas).")
            fallbackParams = {"channel_len": 20, "atr_mult": 1.5, "min_prob": 50, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('BreakoutProbability', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 220:
            fallbackParams = {"channel_len": 20, "atr_mult": 1.5, "min_prob": 50, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('BreakoutProbability', symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        opens = df15m['open'].values
        highs = df15m['high'].values
        lows = df15m['low'].values
        closes = df15m['close'].values
        n = len(df15m)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        atr14 = ta.ATR(highs, lows, closes, timeperiod=14)
        adx14 = ta.ADX(highs, lows, closes, timeperiod=14)
        vol_sma20 = pd.Series(df15m['volume']).rolling(20).mean().values
        
        for cLen in channelLenCombos:
            upperChannel = pd.Series(highs).rolling(cLen).max().shift(1).values
            lowerChannel = pd.Series(lows).rolling(cLen).min().shift(1).values
            
            validIdx = np.where(~np.isnan(upperChannel) & ~np.isnan(lowerChannel) & ~np.isnan(atr14) & ~np.isnan(adx14) & ~np.isnan(vol_sma20))[0]
            if len(validIdx) < 100: continue
            
            for aMult in atrMultCombos:
                candidates = []
                idx = validIdx[0]
                while idx < n - 1:
                    cPrice = closes[idx]
                    uChan = upperChannel[idx]
                    lChan = lowerChannel[idx]
                    atrV = atr14[idx]
                    adxV = adx14[idx]
                    volV = df15m['volume'].iloc[idx]
                    volSmaV = vol_sma20[idx]
                    
                    isBreakoutUp = cPrice > uChan
                    isBreakoutDown = cPrice < lChan
                    
                    direction = None
                    if isBreakoutUp: direction = "LARGO"
                    elif isBreakoutDown: direction = "CORTO"
                    
                    if direction:
                        probScore = 50.0
                        if adxV > 25: probScore += 15.0
                        if volSmaV > 0 and volV > 1.2 * volSmaV: probScore += 15.0
                        if abs(cPrice - (uChan if isBreakoutUp else lChan)) > (0.2 * atrV): probScore += 10.0
                        probScore = min(95.0, probScore)
                        
                        candidates.append({
                            'idx': idx,
                            'direction': direction,
                            'price': cPrice,
                            'prob': probScore,
                            'atrVal': atrV,
                            'uChan': uChan,
                            'lChan': lChan,
                            'atrMult': aMult
                        })
                    idx += 1
                    
                for minProb in minProbCombos:
                    for minRr in minRrCombos:
                        trades = []
                        last_exit_idx = -1
                        
                        for cand in candidates:
                            if cand['idx'] <= last_exit_idx:
                                continue
                            if cand['prob'] < minProb:
                                continue
                                
                            direction = cand['direction']
                            price = cand['price']
                            atrVal = cand['atrVal']
                            atrM = cand['atrMult']
                            idx_entry = cand['idx']
                            
                            if direction == "LARGO":
                                sl = cand['uChan'] - (atrVal * atrM)
                            else:
                                sl = cand['lChan'] + (atrVal * atrM)
                                
                            slDist = abs(price - sl)
                            if slDist <= 0 or slDist > (atrVal * 4.0):
                                continue
                                
                            tpDist = slDist * minRr
                            tp = price + tpDist if direction == "LARGO" else price - tpDist
                            
                            lows_slice = lows[idx_entry:]
                            highs_slice = highs[idx_entry:]
                            
                            if direction == "LARGO":
                                sl_hits = np.where(lows_slice - (spreadPrice / 2.0) <= sl)[0]
                                tp_hits = np.where(highs_slice + (spreadPrice / 2.0) >= tp)[0]
                            else:
                                sl_hits = np.where(highs_slice + (spreadPrice / 2.0) >= sl)[0]
                                tp_hits = np.where(lows_slice - (spreadPrice / 2.0) <= tp)[0]
                                
                            first_sl = sl_hits[0] if len(sl_hits) > 0 else n
                            first_tp = tp_hits[0] if len(tp_hits) > 0 else n
                            
                            if first_sl < first_tp:
                                trades.append(-100.0)
                                last_exit_idx = idx_entry + first_sl
                            elif first_tp < first_sl:
                                trades.append(100.0 * minRr)
                                last_exit_idx = idx_entry + first_tp
                            else:
                                last_exit_idx = n
                                
                        tCount = len(trades)
                        if tCount > 3:
                            wCount = len([t for t in trades if t > 0])
                            wRate = (wCount / tCount) * 100
                            pnlNet = sum(trades)
                            
                            profitCount = sum([t for t in trades if t > 0])
                            lossCount = abs(sum([t for t in trades if t <= 0]))
                            profFactor = profitCount / lossCount if lossCount > 0 else float('inf')
                            
                            comboData = {
                                'Símbolo': symbol,
                                'Channel Len': cLen,
                                'ATR Mult': aMult,
                                'Min Prob': minProb,
                                'Min RR': minRr,
                                'Trades': tCount,
                                'Win Rate': f"{wRate:.1f}%",
                                'Profit Factor': round(profFactor, 2),
                                'PnL USD': pnlNet
                            }
                            allResultsRaw.append(comboData)
                            
                            if pnlNet > symbolBestProfit and profFactor >= 1.0:
                                symbolBestProfit = pnlNet
                                symbolBestCombo = comboData
                                
        if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            params = {
                "channel_len": symbolBestCombo.get("Channel Len", 20),
                "atr_mult": symbolBestCombo.get("ATR Mult", 1.5),
                "min_prob": symbolBestCombo.get("Min Prob", 0),
                "min_rr": symbolBestCombo["Min RR"]
            }
            opt_db_helper.saveSymbolStrategyConfig('BreakoutProbability', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: Channel={symbolBestCombo['Channel Len']} | ATR Mult={symbolBestCombo['ATR Mult']} | Min Prob={symbolBestCombo['Min Prob']}% | RR={symbolBestCombo['Min RR']} | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"channel_len": 20, "atr_mult": 1.5, "min_prob": 50, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('BreakoutProbability', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")

    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("breakoutprobability_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("breakoutprobability_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runBreakoutProbabilityGridSearch()
'''
write_script('run_breakoutprobability_optimization.py', breakoutprob_code)

print("Batch 1 completed!")
