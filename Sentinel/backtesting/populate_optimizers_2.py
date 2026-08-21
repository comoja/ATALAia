import os

base_dir = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/Sentinel/backtesting'

def write_script(filename, content):
    path = os.path.join(base_dir, filename)
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content.strip() + '\n')
    print(f"✅ Generated {filename}")

# 9. run_breakoutny_optimization.py
breakoutny_code = '''import sys
import os
import pandas as pd
import numpy as np
import json
import warnings
from datetime import datetime, timedelta, time as dt_time

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from Sentinel.backtesting import opt_db_helper

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def runBreakoutNYGridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (BREAKOUTNY)       ")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    rangeDurations = [15, 20, 25, 30, 40, 45, 50, 60]
    tradingWindows = [90, 120, 150, 180, 210, 240]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 200:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            fallbackParams = {"range_duration": 60, "trading_window": 180, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('BreakoutNY', symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        groupedDays = df5m.groupby(df5m.index.date)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for rangeDuration in rangeDurations:
            for tradingWindow in tradingWindows:
                for minRr in minRrCombos:
                    trades = []
                    
                    for dayDate, dfDay in groupedDays:
                        if len(dfDay) < 20:
                            continue
                            
                        startTs = pd.Timestamp(datetime.combine(dayDate, dt_time(9, 0)))
                        if dfDay.index.tz is not None:
                            startTs = startTs.tz_localize(dfDay.index.tz)
                            
                        endTs = startTs + timedelta(minutes=rangeDuration)
                        endTimeTs = endTs + timedelta(minutes=tradingWindow)
                        
                        dfRange = dfDay[(dfDay.index >= startTs) & (dfDay.index < endTs)]
                        if dfRange.empty:
                            continue
                            
                        rangeHigh = float(dfRange['high'].max())
                        rangeLow = float(dfRange['low'].min())
                        
                        dfWindow = dfDay[(dfDay.index >= endTs) & (dfDay.index <= endTimeTs)]
                        if dfWindow.empty:
                            continue
                            
                        openArr = dfWindow['open'].values
                        highArr = dfWindow['high'].values
                        lowArr = dfWindow['low'].values
                        closeArr = dfWindow['close'].values
                        timeArr = dfWindow.index
                        
                        firstWindowTime = timeArr[0]
                        dfPrior = dfDay[dfDay.index < firstWindowTime]
                        if dfPrior.empty:
                            continue
                        prevCloseVal = float(dfPrior['close'].iloc[-1])
                        
                        tradeClosed = False
                        pnlUsd = 0.0
                        
                        for idx in range(len(dfWindow)):
                            closePrice = float(closeArr[idx])
                            openPrice = float(openArr[idx])
                            
                            isExplosiveLong = closePrice > rangeHigh and prevCloseVal <= rangeHigh
                            isExplosiveShort = closePrice < rangeLow and prevCloseVal >= rangeLow
                            
                            direction = None
                            if isExplosiveLong: direction = "LARGO"
                            elif isExplosiveShort: direction = "CORTO"
                                
                            if direction:
                                entryPrice = closePrice + (spreadPrice / 2.0) if direction == "LARGO" else closePrice - (spreadPrice / 2.0)
                                sl = rangeLow if direction == "LARGO" else rangeHigh
                                riskDist = abs(entryPrice - sl)
                                if riskDist <= 0:
                                    prevCloseVal = closePrice
                                    continue
                                    
                                tp = entryPrice + (riskDist * minRr) if direction == "LARGO" else entryPrice - (riskDist * minRr)
                                
                                for j in range(idx + 1, len(dfWindow)):
                                    vHigh = float(highArr[j])
                                    vLow = float(lowArr[j])
                                    
                                    if direction == "LARGO":
                                        lowAdjusted = vLow - (spreadPrice / 2.0)
                                        highAdjusted = vHigh + (spreadPrice / 2.0)
                                        if lowAdjusted <= sl:
                                            pnlUsd = -100.0
                                            tradeClosed = True
                                        elif highAdjusted >= tp:
                                            pnlUsd = 100.0 * minRr
                                            tradeClosed = True
                                    else:
                                        highAdjusted = vHigh + (spreadPrice / 2.0)
                                        lowAdjusted = vLow - (spreadPrice / 2.0)
                                        if highAdjusted >= sl:
                                            pnlUsd = -100.0
                                            tradeClosed = True
                                        elif lowAdjusted <= tp:
                                            pnlUsd = 100.0 * minRr
                                            tradeClosed = True
                                            
                                    if tradeClosed:
                                        trades.append(pnlUsd)
                                        break
                                        
                            if tradeClosed:
                                break
                            prevCloseVal = closePrice
                            
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
                            'Range Duration': rangeDuration,
                            'Trading Window': tradingWindow,
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
                "range_duration": symbolBestCombo.get("Range Duration", 60),
                "trading_window": symbolBestCombo.get("Trading Window", 180),
                "min_rr": symbolBestCombo["Min RR"]
            }
            opt_db_helper.saveSymbolStrategyConfig('BreakoutNY', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: Range={symbolBestCombo['Range Duration']}m | Window={symbolBestCombo['Trading Window']}m | RR={symbolBestCombo['Min RR']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"range_duration": 60, "trading_window": 180, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('BreakoutNY', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")

    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("breakoutny_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("breakoutny_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runBreakoutNYGridSearch()
'''
write_script('run_breakoutny_optimization.py', breakoutny_code)

# 10. run_fvgdiario_optimization.py
fvgdiario_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from Sentinel.backtesting import opt_db_helper

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def runFVGDiarioGridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (FVGDIARIO)        ")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.3, 2.5, 2.8, 3.0, 3.5]
    minFvgPipsCombos = [2.0, 3.0, 4.0, 5.0, 6.0, 8.0, 10.0]
    minConfidenceCombos = [60.0, 65.0, 70.0, 75.0, 80.0, 85.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            fallbackParams = {"min_rr": 1.5, "min_fvg_pips": 5.0, "min_confidence": 60}
            opt_db_helper.saveSymbolStrategyConfig('FVGDiario', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        df1d = df5m.resample('1D').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 200 or len(df1d) < 3:
            fallbackParams = {"min_rr": 1.5, "min_fvg_pips": 5.0, "min_confidence": 60}
            opt_db_helper.saveSymbolStrategyConfig('FVGDiario', symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        dailyBiasMap = {}
        pdhMap = {}
        pdlMap = {}
        
        for i in range(1, len(df1d)):
            curDayDate = df1d.index[i].date()
            prevDayRow = df1d.iloc[i-1]
            closeVal = prevDayRow['close']
            openVal = prevDayRow['open']
            highVal = prevDayRow['high']
            lowVal = prevDayRow['low']
            
            body = closeVal - openVal
            totalRange = highVal - lowVal
            
            bias = "NEUTRAL"
            if totalRange > 0:
                if (body / totalRange) >= 0.5 and body > 0:
                    bias = "LARGO"
                elif (body / totalRange) >= 0.5 and body < 0:
                    bias = "CORTO"
                    
            dailyBiasMap[curDayDate] = bias
            pdhMap[curDayDate] = float(highVal)
            pdlMap[curDayDate] = float(lowVal)
            
        df15m = df15m.copy()
        df15m["atr"] = ta.ATR(df15m['high'].values, df15m['low'].values, df15m['close'].values, timeperiod=14)
        df15m.dropna(subset=["atr"], inplace=True)
        
        opens = df15m['open'].values
        highs = df15m['high'].values
        lows = df15m['low'].values
        closes = df15m['close'].values
        atrs = df15m['atr'].values
        times = df15m.index
        n = len(df15m)
        
        sHighs = df15m['high'].rolling(20).max().values
        sLows = df15m['low'].rolling(20).min().values
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for minRr in minRrCombos:
            for minFvgPips in minFvgPipsCombos:
                for minConfidence in minConfidenceCombos:
                    trades = []
                    activeTrade = None
                    
                    idx = 30
                    while idx < n:
                        if activeTrade:
                            vHigh = highs[idx]
                            vLow = lows[idx]
                            
                            if activeTrade['direction'] == 'LARGO':
                                lowAdj = vLow - (spreadPrice / 2.0)
                                highAdj = vHigh + (spreadPrice / 2.0)
                                if lowAdj <= activeTrade['sl']:
                                    trades.append(-100.0)
                                    activeTrade = None
                                elif highAdj >= activeTrade['tp']:
                                    trades.append(100.0 * minRr)
                                    activeTrade = None
                            else:
                                highAdj = vHigh + (spreadPrice / 2.0)
                                lowAdj = vLow - (spreadPrice / 2.0)
                                if highAdj >= activeTrade['sl']:
                                    trades.append(-100.0)
                                    activeTrade = None
                                elif lowAdj <= activeTrade['tp']:
                                    trades.append(100.0 * minRr)
                                    activeTrade = None
                                    
                            idx += 1
                            continue
                            
                        candleDate = times[idx].date()
                        dailyBias = dailyBiasMap.get(candleDate, "NEUTRAL")
                        pdh = pdhMap.get(candleDate, None)
                        pdl = pdlMap.get(candleDate, None)
                        
                        if dailyBias == "NEUTRAL" or pdh is None or pdl is None:
                            idx += 1
                            continue
                            
                        windowHighs = highs[idx-10:idx]
                        windowLows = lows[idx-10:idx]
                        
                        manipulation = None
                        if dailyBias == "LARGO" and np.min(windowLows) < pdl:
                            manipulation = {"type": "MANIPULATION_DOWN", "level": pdl, "idx": idx - 1}
                        elif dailyBias == "CORTO" and np.max(windowHighs) > pdh:
                            manipulation = {"type": "MANIPULATION_UP", "level": pdh, "idx": idx - 1}
                            
                        if not manipulation:
                            idx += 1
                            continue
                            
                        mss = False
                        if dailyBias == "LARGO":
                            for k in range(idx - 5, idx + 1):
                                if k < n and highs[k] > pdl:
                                    mss = True
                                    break
                        elif dailyBias == "CORTO":
                            for k in range(idx - 5, idx + 1):
                                if k < n and lows[k] < pdh:
                                    mss = True
                                    break
                                    
                        if not mss:
                            idx += 1
                            continue
                            
                        isBullishFvg = (lows[idx] > highs[idx-2]) and (closes[idx-1] > opens[idx-1])
                        isBearishFvg = (highs[idx] < lows[idx-2]) and (closes[idx-1] < opens[idx-1])
                        
                        fvg = None
                        if isBullishFvg and dailyBias == "LARGO":
                            fvgSize = lows[idx] - highs[idx-2]
                            fvg = {"type": "Bullish_FVG", "size": fvgSize, "mid": (highs[idx-2] + lows[idx]) / 2.0}
                        elif isBearishFvg and dailyBias == "CORTO":
                            fvgSize = lows[idx-2] - highs[idx]
                            fvg = {"type": "Bearish_FVG", "size": fvgSize, "mid": (lows[idx-2] + highs[idx]) / 2.0}
                            
                        if not fvg:
                            idx += 1
                            continue
                            
                        fvgPips = fvg['size'] * pipMult
                        if fvgPips < minFvgPips:
                            idx += 1
                            continue
                            
                        currentPrice = closes[idx]
                        atrVal = atrs[idx]
                        
                        if fvg['type'] == 'Bullish_FVG':
                            entryPrice = fvg['mid']
                            sl = (sLows[idx] if not np.isnan(sLows[idx]) else entryPrice - (atrVal * 1.5)) - (atrVal * 0.2)
                            riskDist = abs(entryPrice - sl)
                            if riskDist <= 0:
                                idx += 1
                                continue
                            tp = entryPrice + (riskDist * minRr)
                            activeTrade = {"direction": "LARGO", "entry": entryPrice, "sl": sl, "tp": tp}
                        else:
                            entryPrice = fvg['mid']
                            sl = (sHighs[idx] if not np.isnan(sHighs[idx]) else entryPrice + (atrVal * 1.5)) + (atrVal * 0.2)
                            riskDist = abs(entryPrice - sl)
                            if riskDist <= 0:
                                idx += 1
                                continue
                            tp = entryPrice - (riskDist * minRr)
                            activeTrade = {"direction": "CORTO", "entry": entryPrice, "sl": sl, "tp": tp}
                            
                        idx += 1
                        
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
                            'Min RR': minRr,
                            'Min FVG Pips': minFvgPips,
                            'Min Conf': minConfidence,
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
                "min_rr": symbolBestCombo["Min RR"],
                "min_fvg_pips": symbolBestCombo.get("Min FVG Pips", 5.0),
                "min_confidence": symbolBestCombo.get("Min Conf", 60.0)
            }
            opt_db_helper.saveSymbolStrategyConfig('FVGDiario', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: RR={symbolBestCombo['Min RR']} | Min FVG Pips={symbolBestCombo['Min FVG Pips']} | Conf={symbolBestCombo['Min Conf']}% | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"min_rr": 1.5, "min_fvg_pips": 5.0, "min_confidence": 60}
            opt_db_helper.saveSymbolStrategyConfig('FVGDiario', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")

    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("fvgdiario_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("fvgdiario_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runFVGDiarioGridSearch()
'''
write_script('run_fvgdiario_optimization.py', fvgdiario_code)

# 11. run_imbalance_optimization.py
imbalance_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
import pytz
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

ASSET_SL_MULTIPLIERS = {
    'divisas': 0.15,
    'BTC/USD': 0.35,
    'XAU/USD': 0.25,
    'XAG/USD': 0.25,
    'NAS100': 0.25,
}

def runImbalanceGridSearch(strategyName: str, sessionTzName: str, refStartH: int, refEndH: int, tradeStartH: int, tradeEndH: int) -> list:
    print(f"\n==========================================================")
    print(f"    INICIANDO GRID SEARCH OPTIMIZER ({strategyName.upper()})    ")
    print(f"==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    maxMinutosFvgCombos = [15, 30, 45, 60, 90, 120]
    minConfidenceCombos = [60.0, 65.0, 70.0, 75.0, 80.0]
    
    bestResults = []
    allResultsRaw = []
    
    sessionTz = pytz.timezone(sessionTzName)
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df = loadCandles(symbol, startDateStr, endDateStr)
        if df.empty or len(df) < 500:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            fallbackParams = {"min_rr": 1.5, "max_minutos_fvg": 60, "min_conf": 50}
            opt_db_helper.saveSymbolStrategyConfig(strategyName, symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        slMultiplier = ASSET_SL_MULTIPLIERS.get(symbol, ASSET_SL_MULTIPLIERS['divisas'])
        
        df = df.copy()
        df["atr"] = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
        df.dropna(subset=["atr"], inplace=True)
        
        try:
            df.index = df.index.tz_localize('America/Mexico_City', ambiguous='infer', nonexistent='shift_forward')
        except Exception:
            pass
            
        dfSession = df.tz_convert(sessionTz)
        groups = dfSession.groupby(dfSession.index.date)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for minRr in minRrCombos:
            for maxMinutosFvg in maxMinutosFvgCombos:
                for minConfidence in minConfidenceCombos:
                    trades = []
                    
                    for dateKey, dfDay in groups:
                        if len(dfDay) < 10:
                            continue
                            
                        refCandles = dfDay[(dfDay.index.hour >= refStartH) & (dfDay.index.hour < refEndH)]
                        if refCandles.empty:
                            continue
                            
                        refHigh = float(refCandles['high'].max())
                        refLow = float(refCandles['low'].min())
                        
                        tradeCandles = dfDay[(dfDay.index.hour >= tradeStartH) & (dfDay.index.hour < tradeEndH)]
                        if tradeCandles.empty or len(tradeCandles) < 5:
                            continue
                            
                        highs = tradeCandles['high'].values
                        lows = tradeCandles['low'].values
                        closes = tradeCandles['close'].values
                        opens = tradeCandles['open'].values
                        atrs = tradeCandles['atr'].values
                        times = tradeCandles.index
                        
                        sweep = None
                        idxSweep = -1
                        for i in range(len(tradeCandles)):
                            if lows[i] < refLow and closes[i] > refLow:
                                sweep = 'LIQUIDITY_SWEPT_DOWN'
                                idxSweep = i
                                break
                            elif highs[i] > refHigh and closes[i] < refHigh:
                                sweep = 'LIQUIDITY_SWEPT_UP'
                                idxSweep = i
                                break
                                
                        if not sweep or idxSweep < 0:
                            continue
                            
                        direction = 'LARGO' if sweep == 'LIQUIDITY_SWEPT_DOWN' else 'CORTO'
                        
                        idxFvg = -1
                        for i in range(idxSweep + 1, len(tradeCandles)):
                            if direction == 'LARGO':
                                if lows[i] > highs[i-2] if i >= 2 else False:
                                    idxFvg = i
                                    break
                            else:
                                if highs[i] < lows[i-2] if i >= 2 else False:
                                    idxFvg = i
                                    break
                                    
                        if idxFvg < 0:
                            continue
                            
                        entryPrice = (highs[idxFvg-2] + lows[idxFvg]) / 2.0 if direction == 'LARGO' else (lows[idxFvg-2] + highs[idxFvg]) / 2.0
                        atrVal = atrs[idxFvg]
                        paddingPips = (atrVal * slMultiplier)
                        
                        swingHigh = np.max(highs[max(0, idxFvg-5):idxFvg+1])
                        swingLow = np.min(lows[max(0, idxFvg-5):idxFvg+1])
                        
                        if direction == 'CORTO':
                            zonaHighFvg = np.max(highs[idxFvg:min(len(tradeCandles), idxFvg+2)])
                            stopRef = max(zonaHighFvg, swingHigh)
                            stopLoss = stopRef + paddingPips
                            tpStructural = swingLow
                            minTp = entryPrice - atrVal * 3.0
                            tpStructural = max(tpStructural, minTp)
                            tpFinal = adjustTPForMinRR(entryPrice, stopLoss, tpStructural, "CORTO", minRR=minRr)
                        else:
                            zonaLowFvg = np.min(lows[idxFvg:min(len(tradeCandles), idxFvg+2)])
                            stopRef = min(zonaLowFvg, swingLow)
                            stopLoss = stopRef - paddingPips
                            tpStructural = swingHigh
                            maxTp = entryPrice + atrVal * 3.0
                            tpStructural = min(tpStructural, maxTp)
                            tpFinal = adjustTPForMinRR(entryPrice, stopLoss, tpStructural, "LARGO", minRR=minRr)
                            
                        tradeActive = False
                        tradeEntryTime = None
                        
                        for k in range(idxFvg + 1, len(tradeCandles)):
                            kHigh = highs[k]
                            kLow = lows[k]
                            kTime = times[k]
                            
                            if not tradeActive:
                                minutosDesdeFvg = (kTime - times[idxFvg]).total_seconds() / 60.0
                                if minutosDesdeFvg > maxMinutosFvg:
                                    break
                                    
                                if direction == 'LARGO' and kLow <= entryPrice:
                                    tradeActive = True
                                    tradeEntryTime = kTime
                                elif direction == 'CORTO' and kHigh >= entryPrice:
                                    tradeActive = True
                                    tradeEntryTime = kTime
                                    
                            if tradeActive:
                                if direction == 'LARGO':
                                    lowAdj = kLow - (spreadPrice / 2.0)
                                    highAdj = kHigh + (spreadPrice / 2.0)
                                    if lowAdj <= stopLoss:
                                        trades.append(-100.0)
                                        break
                                    elif highAdj >= tpFinal:
                                        trades.append(100.0 * minRr)
                                        break
                                else:
                                    highAdj = kHigh + (spreadPrice / 2.0)
                                    lowAdj = kLow - (spreadPrice / 2.0)
                                    if highAdj >= stopLoss:
                                        trades.append(-100.0)
                                        break
                                    elif lowAdj <= tpFinal:
                                        trades.append(100.0 * minRr)
                                        break
                                        
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
                            'Min RR': minRr,
                            'Max Minutos Fvg': maxMinutosFvg,
                            'Min Conf': minConfidence,
                            'Trades': tCount,
                            'Win Rate': f"{wRate:.1f}%",
                            'Profit Factor': round(profFactor, 2),
                            'PnL USD': pnlNet
                        }
                        allResultsRaw.append(comboData)
                        
                        if pnlNet > symbolBestProfit and profFactor >= 1.25:
                            symbolBestProfit = pnlNet
                            symbolBestCombo = comboData
                            
        if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            params = {"min_rr": symbolBestCombo["Min RR"], "max_minutos_fvg": symbolBestCombo.get("Max Minutos Fvg", 60), "min_conf": symbolBestCombo.get("Min Conf", 50)}
            opt_db_helper.saveSymbolStrategyConfig(strategyName, symbol, True, params)
            print(f"✅ DB: Guardado {symbol} ({strategyName} TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: RR={symbolBestCombo['Min RR']} | Max Minutos={symbolBestCombo['Max Minutos Fvg']} | Conf={symbolBestCombo['Min Conf']}% | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"min_rr": 1.5, "max_minutos_fvg": 60, "min_conf": 50}
            opt_db_helper.saveSymbolStrategyConfig(strategyName, symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol} ({strategyName}). Guardado en DB (FALSE).")
            
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath(f"{strategyName.lower()}_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath(f"{strategyName.lower()}_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        
    return bestResults

def runAllImbalanceOptimizations():
    runImbalanceGridSearch("ImbalanceLDN", "Europe/London", 8, 9, 9, 14)
    runImbalanceGridSearch("ImbalanceNY", "America/New_York", 8, 9, 9, 14)
    runImbalanceGridSearch("ImbalancePMNY", "America/New_York", 14, 15, 15, 17)

if __name__ == '__main__':
    runAllImbalanceOptimizations()
'''
write_script('run_imbalance_optimization.py', imbalance_code)

# 12. run_silverbullet_optimization.py
silverbullet_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import json
import warnings
from datetime import datetime, time, timedelta
import pytz

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.utils.alertBuilder import adjustTPForMinRR
from Sentinel.backtesting import opt_db_helper

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

NY_TZ = pytz.timezone("America/New_York")
SILVER_BULLET_WINDOWS = {
    "london_open": {"start": time(3, 0), "end": time(4, 0)},
    "ny_am":       {"start": time(10, 0), "end": time(11, 0)},
    "ny_pm":       {"start": time(14, 0), "end": time(15, 0)}
}

def runBacktestForCombo(df5m: pd.DataFrame, symbol: str, fvgMinPct: float, minRrVal: float, minAdx: float) -> dict:
    df = df5m.copy()
    
    df["adx"] = pd.Series(ta.ADX(df['high'].values.astype(float), df['low'].values.astype(float), df['close'].values.astype(float), timeperiod=14), index=df.index)
    df["atr"] = pd.Series(ta.ATR(df['high'].values.astype(float), df['low'].values.astype(float), df['close'].values.astype(float), timeperiod=14), index=df.index)
    
    df.dropna(subset=["adx", "atr"], inplace=True)
    if len(df) < 50:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    try:
        idxNy = df.index.tz_convert("America/New_York")
    except Exception:
        df.index = df.index.tz_localize("America/Mexico_City", ambiguous='infer', nonexistent='shift_forward')
        idxNy = df.index.tz_convert("America/New_York")
        
    df["ny_time"] = idxNy.time
    df["ny_date"] = idxNy.date
    
    trades = []
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    
    dates = df["ny_date"].unique()
    
    for d in dates:
        df_day = df[df["ny_date"] == d]
        if len(df_day) < 12:
            continue
            
        for w_name, w in SILVER_BULLET_WINDOWS.items():
            w_start_ny = NY_TZ.localize(datetime.combine(d, w["start"]))
            w_end_ny = NY_TZ.localize(datetime.combine(d, w["end"]))
            
            idx_ny_day = df_day.index.tz_convert("America/New_York")
            df_w = df_day[(idx_ny_day >= w_start_ny) & (idx_ny_day < w_end_ny)]
            if len(df_w) < 5:
                continue
                
            ref_end = w_start_ny + timedelta(minutes=15)
            df_ref = df_w[df_w.index.tz_convert("America/New_York") < ref_end]
            if df_ref.empty or len(df_ref) < 3:
                continue
                
            ref = {
                "high": float(df_ref["high"].max()),
                "low": float(df_ref["low"].min())
            }
            
            sweep_start = w_start_ny + timedelta(minutes=15)
            df_post = df_w[df_w.index.tz_convert("America/New_York") >= sweep_start]
            if df_post.empty:
                continue
                
            sweep = None
            for idx_v, (time_v, v) in enumerate(df_post.iterrows()):
                adx_v = float(v["adx"])
                if adx_v < minAdx:
                    continue
                    
                if v["low"] < ref["low"] and v["close"] > ref["low"]:
                    sweep = {"type": "LARGO", "swept_level": ref["low"], "sweep_low": v["low"], "idx": idx_v, "time": time_v}
                    break
                elif v["high"] > ref["high"] and v["close"] < ref["high"]:
                    sweep = {"type": "CORTO", "swept_level": ref["high"], "sweep_high": v["high"], "idx": idx_v, "time": time_v}
                    break
                    
            if not sweep:
                continue
                
            df_signals = df_post.iloc[sweep["idx"]+1:]
            if len(df_signals) < 3:
                continue
                
            fvg = None
            for idx_s, (time_s, s) in enumerate(df_signals.iterrows()):
                abs_idx = df_day.index.get_loc(time_s)
                if abs_idx < 2:
                    continue
                    
                h2 = df_day["high"].iloc[abs_idx - 2]
                l2 = df_day["low"].iloc[abs_idx - 2]
                h = df_day["high"].iloc[abs_idx]
                l = df_day["low"].iloc[abs_idx]
                c = df_day["close"].iloc[abs_idx]
                
                h_vals = df_day["high"].iloc[max(0, abs_idx-6):abs_idx].values
                l_vals = df_day["low"].iloc[max(0, abs_idx-6):abs_idx].values
                if sweep["type"] == "LARGO":
                    mss_ok = float(c) > float(np.max(h_vals)) if len(h_vals) > 0 else False
                    if l > h2 and mss_ok:
                        gap_pct = (l - h2) / float(c)
                        if gap_pct >= fvgMinPct:
                            fvg = {
                                "direction": "LARGO",
                                "entry": (h2 + l) / 2.0,
                                "sl": float(sweep["sweep_low"]) - (float(s["atr"]) * 0.1),
                                "time": time_s,
                                "idx_day": abs_idx
                            }
                            break
                else:
                    mss_ok = float(c) < float(np.min(l_vals)) if len(l_vals) > 0 else False
                    if h < l2 and mss_ok:
                        gap_pct = (l2 - h) / float(c)
                        if gap_pct >= fvgMinPct:
                            fvg = {
                                "direction": "CORTO",
                                "entry": (l2 + h) / 2.0,
                                "sl": float(sweep["sweep_high"]) + (float(s["atr"]) * 0.1),
                                "time": time_s,
                                "idx_day": abs_idx
                            }
                            break
                            
            if not fvg:
                continue
                
            entry_p = fvg["entry"]
            sl_p = fvg["sl"]
            sl_dist = abs(entry_p - sl_p)
            if sl_dist <= 0:
                continue
                
            tp_p = entry_p + (sl_dist * minRrVal) if fvg["direction"] == "LARGO" else entry_p - (sl_dist * minRrVal)
            
            df_exec = df_day.iloc[fvg["idx_day"]+1:]
            if df_exec.empty:
                continue
                
            trade_active = False
            for time_e, e in df_exec.iterrows():
                high_e = float(e["high"])
                low_e = float(e["low"])
                
                if not trade_active:
                    if fvg["direction"] == "LARGO" and low_e <= entry_p:
                        trade_active = True
                    elif fvg["direction"] == "CORTO" and high_e >= entry_p:
                        trade_active = True
                    else:
                        continue
                        
                if trade_active:
                    if fvg["direction"] == "LARGO":
                        if low_e <= sl_p:
                            trades.append({"pnl": -100.0})
                            break
                        elif high_e >= tp_p:
                            trades.append({"pnl": 100.0 * minRrVal})
                            break
                    else:
                        if high_e >= sl_p:
                            trades.append({"pnl": -100.0})
                            break
                        elif low_e <= tp_p:
                            trades.append({"pnl": 100.0 * minRrVal})
                            break
                            
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

def runSilverBulletGridSearch():
    logger.info("==========================================================")
    logger.info("  INICIANDO GRID SEARCH OPTIMIZER (SILVER BULLET ICT)   ")
    logger.info("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    fvgMinPctCombos = [0.00005, 0.0001, 0.00015, 0.0002]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    minAdxCombos = [15.0, 20.0, 25.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos de 5m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"fvgMinPct": 0.0001, "minRr": 1.5, "minAdx": 20.0}
            opt_db_helper.saveSymbolStrategyConfig('SilverBullet', symbol, False, fallbackParams)
            continue
            
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for fvgMinPct in fvgMinPctCombos:
            for minRr in minRrCombos:
                for minAdx in minAdxCombos:
                    res = runBacktestForCombo(df5m, symbol, fvgMinPct, minRr, minAdx)
                    numTrades = len(res['trades'])
                    
                    row = {
                        "symbol": symbol,
                        "fvgMinPct": fvgMinPct,
                        "minRr": minRr,
                        "minAdx": minAdx,
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
            logger.info(f"  ✨ Mejor combo viable para {symbol}: FVG Min={bestCombo['fvgMinPct']}, Min R:R={bestCombo['minRr']}, Min ADX={bestCombo['minAdx']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
            params = {
                "fvgMinPct": bestCombo["fvgMinPct"],
                "minRr": bestCombo["minRr"],
                "minAdx": bestCombo["minAdx"]
            }
            opt_db_helper.saveSymbolStrategyConfig('SilverBullet', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0 y WR >= 35%) para {symbol}.")
            fallbackParams = {"fvgMinPct": 0.0001, "minRr": 1.5, "minAdx": 20.0}
            opt_db_helper.saveSymbolStrategyConfig('SilverBullet', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró combo viable para {symbol}. Guardado en DB (FALSE).")
            
    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: dfAll.to_csv(opt_db_helper.getOutputPath("silverbullet_grid_results_all.csv"), index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty: dfBest.to_csv(opt_db_helper.getOutputPath("silverbullet_grid_results_best.csv"), index=False)

if __name__ == "__main__":
    runSilverBulletGridSearch()
'''
write_script('run_silverbullet_optimization.py', silverbullet_code)

# 13. run_speedbot_optimization.py
speedbot_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from Sentinel.backtesting import opt_db_helper

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def runSpeedBotGridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (SPEEDBOT)         ")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    atrMults = [1.0, 1.2, 1.4, 1.6, 1.8, 2.0]
    bodyRatios = [0.65, 0.70, 0.75, 0.80, 0.85]
    confirmRatios = [0.30, 0.40, 0.50, 0.60, 0.70]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 200:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            fallbackParams = {"atr_mult": 1.5, "body_ratio": 0.6, "confirm_ratio": 0.5, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('SpeedBot', symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        openPrices = df5m['open'].values.astype(float)
        highPrices = df5m['high'].values.astype(float)
        lowPrices = df5m['low'].values.astype(float)
        closePrices = df5m['close'].values.astype(float)
        totalBars = len(df5m)
        
        atr14 = ta.ATR(highPrices, lowPrices, closePrices, timeperiod=14)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for atrMult in atrMults:
            for bodyRatio in bodyRatios:
                for confirmRatio in confirmRatios:
                    for minRr in minRrCombos:
                        trades = []
                        i = 50
                        while i < totalBars:
                            atrVal = atr14[i]
                            if np.isnan(atrVal):
                                i += 1
                                continue
                                
                            bodyLast = abs(closePrices[i] - openPrices[i])
                            bodyPrev = abs(closePrices[i - 1] - openPrices[i - 1])
                            rangePrev = highPrices[i - 1] - lowPrices[i - 1]
                            
                            isExplosive = bodyPrev > (atrVal * atrMult)
                            isSolid = (bodyPrev / rangePrev) > bodyRatio if rangePrev > 0 else False
                            
                            sameDir = (closePrices[i] > openPrices[i]) == (closePrices[i - 1] > openPrices[i - 1])
                            isConfirmed = sameDir and (bodyLast >= bodyPrev * confirmRatio)
                            
                            if not (isExplosive and isSolid and isConfirmed):
                                i += 1
                                continue
                                
                            direction = "LARGO" if closePrices[i] > openPrices[i] else "CORTO"
                            entryPrice = closePrices[i] + (spreadPrice / 2.0) if direction == "LARGO" else closePrices[i] - (spreadPrice / 2.0)
                            
                            if direction == "LARGO":
                                sl = min(lowPrices[i], lowPrices[i - 1]) - (atrVal * 0.1)
                            else:
                                sl = max(highPrices[i], highPrices[i - 1]) + (atrVal * 0.1)
                                
                            riskDist = abs(entryPrice - sl)
                            if riskDist <= 0.0:
                                i += 1
                                continue
                                
                            tp = entryPrice + (riskDist * minRr) if direction == "LARGO" else entryPrice - (riskDist * minRr)
                            
                            tradeClosed = False
                            pnlUsd = 0.0
                            for j in range(i + 1, totalBars):
                                vHigh = highPrices[j]
                                vLow = lowPrices[j]
                                
                                if direction == "LARGO":
                                    lowAdjusted = vLow - (spreadPrice / 2.0)
                                    highAdjusted = vHigh + (spreadPrice / 2.0)
                                    if lowAdjusted <= sl:
                                        pnlUsd = -100.0
                                        tradeClosed = True
                                    elif highAdjusted >= tp:
                                        pnlUsd = 100.0 * minRr
                                        tradeClosed = True
                                else:
                                    highAdjusted = vHigh + (spreadPrice / 2.0)
                                    lowAdjusted = vLow - (spreadPrice / 2.0)
                                    if highAdjusted >= sl:
                                        pnlUsd = -100.0
                                        tradeClosed = True
                                    elif lowAdjusted <= tp:
                                        pnlUsd = 100.0 * minRr
                                        tradeClosed = True
                                        
                                if tradeClosed:
                                    trades.append(pnlUsd)
                                    i = j
                                    break
                            if not tradeClosed:
                                break
                            i += 1
                            
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
                                'ATR Mult': atrMult,
                                'Body Ratio': bodyRatio,
                                'Confirm Ratio': confirmRatio,
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
                "atr_mult": symbolBestCombo.get("ATR Mult", 1.5),
                "body_ratio": symbolBestCombo.get("Body Ratio", 0.6),
                "confirm_ratio": symbolBestCombo.get("Confirm Ratio", 0.5),
                "min_rr": symbolBestCombo["Min RR"]
            }
            opt_db_helper.saveSymbolStrategyConfig('SpeedBot', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: ATR Mult={symbolBestCombo['ATR Mult']} | Body={symbolBestCombo['Body Ratio']} | Confirm={symbolBestCombo['Confirm Ratio']} | RR={symbolBestCombo['Min RR']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"atr_mult": 1.5, "body_ratio": 0.6, "confirm_ratio": 0.5, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('SpeedBot', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")
            
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("speedbot_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("speedbot_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runSpeedBotGridSearch()
'''
write_script('run_speedbot_optimization.py', speedbot_code)

# 14. run_patron4h_optimization.py
patron4h_code = '''import sys
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
from Sentinel.core.Patron4h import Patron4HBot
from Sentinel.analysis import technical
from Sentinel.analysis.fvg_analyzer import FvgAnalyzer
from Sentinel.backtesting import opt_db_helper

logging.getLogger('sentinel').setLevel(logging.ERROR)
logging.basicConfig(level=logging.ERROR)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

async def runPatron4HGridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (PATRON4H)         ")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    fvgMinPctCombos = [0.00005, 0.0001, 0.0002]
    displacementPctCombos = [0.0003, 0.0005, 0.001]
    minRrCombos = [1.2, 1.5, 2.0, 2.5]
    minConfidenceCombos = [50.0, 60.0, 70.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 600:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            fallbackParams = {"fvgMinPct": 0.0001, "displacementPct": 0.0005, "rrRatioMin": 1.5, "maxMinutosFvg": 240.0, "minConfidence": 50, "lookback": 50}
            opt_db_helper.saveSymbolStrategyConfig('Patron4h', symbol, False, fallbackParams)
            continue
            
        df4h = df5m.resample('4h').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df4h) < 50:
            fallbackParams = {"fvgMinPct": 0.0001, "displacementPct": 0.0005, "rrRatioMin": 1.5, "maxMinutosFvg": 240.0, "minConfidence": 50, "lookback": 50}
            opt_db_helper.saveSymbolStrategyConfig('Patron4h', symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        bot = Patron4HBot()
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for fvgMinPct in fvgMinPctCombos:
            for displacementPct in displacementPctCombos:
                for minRr in minRrCombos:
                    for minConf in minConfidenceCombos:
                        trades = []
                        last_exit_idx = -1
                        
                        df4h_fvgs = technical.detect_fvgs(df4h, min_gap_pct=fvgMinPct, validate_mitigation=False, apply_high_prob_filters=True)
                        
                        for fvg in df4h_fvgs:
                            fvg_time = fvg.get('time')
                            if not fvg_time: continue
                            
                            df5m_sub = df5m[df5m.index >= fvg_time]
                            if df5m_sub.empty or len(df5m_sub) < 10: continue
                            
                            fvg_dir = "LARGO" if fvg.get('type') == 'Bullish_FVG' else "CORTO"
                            entry_price = float(df5m_sub['close'].iloc[0])
                            atr_val = ta.ATR(df5m_sub['high'].values, df5m_sub['low'].values, df5m_sub['close'].values, timeperiod=14)[-1]
                            if np.isnan(atr_val) or atr_val <= 0: continue
                            
                            if fvg_dir == "LARGO":
                                sl = entry_price - (atr_val * 1.5)
                                tp = entry_price + (abs(entry_price - sl) * minRr)
                            else:
                                sl = entry_price + (atr_val * 1.5)
                                tp = entry_price - (abs(entry_price - sl) * minRr)
                                
                            for idx_k in range(1, min(len(df5m_sub), 48)): # Max 4 horas en velas de 5m
                                vH = float(df5m_sub['high'].iloc[idx_k])
                                vL = float(df5m_sub['low'].iloc[idx_k])
                                
                                if fvg_dir == "LARGO":
                                    if vL <= sl:
                                        trades.append(-100.0)
                                        break
                                    elif vH >= tp:
                                        trades.append(100.0 * minRr)
                                        break
                                else:
                                    if vH >= sl:
                                        trades.append(-100.0)
                                        break
                                    elif vL <= tp:
                                        trades.append(100.0 * minRr)
                                        break
                                        
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
                                'FVG Min Pct': fvgMinPct,
                                'Displacement Pct': displacementPct,
                                'Min RR': minRr,
                                'Min Conf': minConf,
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
                "fvgMinPct": symbolBestCombo["FVG Min Pct"],
                "displacementPct": symbolBestCombo["Displacement Pct"],
                "rrRatioMin": symbolBestCombo["Min RR"],
                "maxMinutosFvg": 240.0,
                "minConfidence": symbolBestCombo["Min Conf"],
                "lookback": 50
            }
            opt_db_helper.saveSymbolStrategyConfig('Patron4h', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: RR={symbolBestCombo['Min RR']} | Conf={symbolBestCombo['Min Conf']}% | Disp={symbolBestCombo['Displacement Pct']} | FVG={symbolBestCombo['FVG Min Pct']} | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"fvgMinPct": 0.0001, "displacementPct": 0.0005, "rrRatioMin": 1.5, "maxMinutosFvg": 240.0, "minConfidence": 50, "lookback": 50}
            opt_db_helper.saveSymbolStrategyConfig('Patron4h', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable y viable para {symbol}. Guardado en DB (FALSE).")
            
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("patron4h_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("patron4h_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    asyncio.run(runPatron4HGridSearch())
'''
write_script('run_patron4h_optimization.py', patron4h_code)

# 15. run_premiumconfluence_optimization.py
premiumconfluence_code = '''import os
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
'''
write_script('run_premiumconfluence_optimization.py', premiumconfluence_code)

print("Batch 2 completed!")
