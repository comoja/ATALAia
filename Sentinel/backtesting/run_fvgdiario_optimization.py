import sys
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
