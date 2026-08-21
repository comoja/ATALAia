import sys
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
