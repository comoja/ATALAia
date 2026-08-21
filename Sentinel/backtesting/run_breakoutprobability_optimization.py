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
