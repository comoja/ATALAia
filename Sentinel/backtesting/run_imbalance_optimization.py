import sys
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
