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
