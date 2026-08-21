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
from Sentinel.ml import model as mlModel
from middleware.config import constants as config
from Sentinel.backtesting import opt_db_helper

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def calculateSlope(series: np.ndarray) -> np.ndarray:
    n = len(series)
    slopes = np.zeros(n)
    for i in range(10, n):
        y = series[i-10:i]
        if np.isnan(y).any(): continue
        x = np.arange(10)
        m, _ = np.polyfit(x, y, 1)
        slopes[i] = (m / np.mean(y)) * 100
    return slopes

def runCruceEMAGridSearch() -> None:
    print("==========================================================")
    print("  INICIANDO GRID SEARCH OPTIMIZER (SMA Pullback 15m+IMACD)")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    modelClf = mlModel.loadModel(config.MODEL_FILE_PATH)
    if modelClf is None:
        print("❌ No se pudo cargar el modelo ML. Saliendo.")
        return
        
    fastPeriods = [5, 8, 10, 12, 15, 20]
    slowPeriods = [20, 25, 35, 50, 80, 100, 150]
    smaCombos = [(f, s) for f in fastPeriods for s in slowPeriods if f < s]

    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    minConfidenceCombos = [0.0, 50.0, 60.0, 70.0, 80.0]
    imacdCombos = [
        (10, 5),
        (12, 9),
        (20, 9),
        (26, 9),
        (34, 9),
        (50, 20)
    ]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 600:
            print(f"⚠️ Datos insuficientes para {symbol} ({len(df5m)} velas).")
            fallbackParams = {"emaFast": 9, "emaSlow": 21, "minRr": 1.5}
            fallbackImacd = {"useImpulseMacdFilter": 1, "macdFast": 12, "macdSlow": 26, "macdSignal": 9}
            opt_db_helper.saveSymbolStrategyConfig('CruceEMA', symbol, False, fallbackParams, fallbackImacd)
            continue
            
        # Resample a 15 min
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 220:
            print(f"⚠️ Velas 15m insuficientes para {symbol} ({len(df15m)} velas).")
            fallbackParams = {"emaFast": 9, "emaSlow": 21, "minRr": 1.5}
            fallbackImacd = {"useImpulseMacdFilter": 1, "macdFast": 12, "macdSlow": 26, "macdSignal": 9}
            opt_db_helper.saveSymbolStrategyConfig('CruceEMA', symbol, False, fallbackParams, fallbackImacd)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        opens = df15m['open'].values
        highs = df15m['high'].values
        lows = df15m['low'].values
        closes = df15m['close'].values
        n = len(df15m)
        
        hlc3 = (highs + lows + closes) / 3.0
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for smaFast, smaSlow in smaCombos:
            emaF = ta.EMA(closes, timeperiod=smaFast)
            emaS = ta.EMA(closes, timeperiod=smaSlow)
            atr14 = ta.ATR(highs, lows, closes, timeperiod=14)
            
            sHighs = df15m['high'].rolling(20).max().values
            sLows = df15m['low'].rolling(20).min().values
            hZones = df15m['high'].rolling(20).quantile(0.90).values
            lZones = df15m['low'].rolling(20).quantile(0.10).values
            
            emaSlopes = calculateSlope(emaF)
            
            for lengthMA, lengthSignal in imacdCombos:
                # Calcular IMACD específico
                hi_arr = pd.Series(highs).ewm(alpha=1.0/lengthMA, adjust=False).mean().values
                lo_arr = pd.Series(lows).ewm(alpha=1.0/lengthMA, adjust=False).mean().values
                ema1 = ta.EMA(hlc3, timeperiod=lengthMA)
                ema2 = ta.EMA(ema1, timeperiod=lengthMA)
                mi_arr = ema1 + (ema1 - ema2)
                md_arr = np.where(mi_arr > hi_arr, mi_arr - hi_arr, np.where(mi_arr < lo_arr, mi_arr - lo_arr, 0.0))
                sb_arr = ta.SMA(md_arr, timeperiod=lengthSignal)
                
                validIdx = np.where(~np.isnan(emaF) & ~np.isnan(emaS) & ~np.isnan(atr14) & ~np.isnan(sb_arr))[0]
                if len(validIdx) < 100: continue
                
                # ML probabilities
                probs = np.full(n, 0.5)
                # Extraer features para ML
                feat_df = pd.DataFrame(index=df15m.index)
                feat_df['rsi'] = ta.RSI(closes, timeperiod=14)
                feat_df['atr'] = atr14
                feat_df['macd'], _, _ = ta.MACD(closes)
                feat_df['adx'] = ta.ADX(highs, lows, closes, timeperiod=14)
                feat_df['ema_slope'] = emaSlopes
                feat_df['imacd'] = md_arr
                feat_df['imacd_signal'] = sb_arr
                feat_df.fillna(0.0, inplace=True)
                
                try:
                    features_matrix = feat_df.values
                    preds = modelClf.predict_proba(features_matrix)[:, 1]
                    probs = preds
                except Exception:
                    probs = np.full(n, 0.5)

                # Generar candidatos
                candidates = []
                idx = validIdx[0]
                while idx < n - 1:
                    # Cruce EMA Pullback Logic
                    isBull = closes[idx] > emaS[idx] and lows[idx] <= emaF[idx] and closes[idx] >= emaF[idx] and md_arr[idx] > sb_arr[idx] and emaSlopes[idx] > 0
                    isBear = closes[idx] < emaS[idx] and highs[idx] >= emaF[idx] and closes[idx] <= emaF[idx] and md_arr[idx] < sb_arr[idx] and emaSlopes[idx] < 0
                    
                    direction = None
                    if isBull: direction = "LARGO"
                    elif isBear: direction = "CORTO"
                    
                    if direction:
                        candidates.append({
                            'idx': idx,
                            'direction': direction,
                            'price': closes[idx],
                            'prob': probs[idx],
                            'atrVal': atr14[idx],
                            'swingHigh': sHighs[idx],
                            'swingLow': sLows[idx],
                            'hZone': hZones[idx],
                            'lZone': lZones[idx]
                        })
                    idx += 1

                for minRr in minRrCombos:
                    for minConfidence in minConfidenceCombos:
                        minConfVal = minConfidence / 100.0
                        
                        trades = []
                        last_exit_idx = -1
                        
                        for cand in candidates:
                            if cand['idx'] <= last_exit_idx:
                                continue
                            
                            if cand['prob'] < minConfVal:
                                continue
                                
                            direction = cand['direction']
                            price = cand['price']
                            atrVal = cand['atrVal']
                            swingHigh = cand['swingHigh']
                            swingLow = cand['swingLow']
                            hZone = cand['hZone']
                            lZone = cand['lZone']
                            idx_entry = cand['idx']
                            
                            if direction == "LARGO":
                                sl = swingLow - atrVal * 0.2
                                tpStruct = hZone
                            else:
                                sl = swingHigh + atrVal * 0.2
                                tpStruct = lZone
                                
                            slDist = abs(price - sl)
                            if slDist <= 0:
                                continue
                                
                            tpDist = max(abs(tpStruct - price), slDist * minRr)
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
                                'EMA Fast': smaFast,
                                'EMA Slow': smaSlow,
                                'IMACD Slow': lengthMA,
                                'IMACD Signal': lengthSignal,
                                'Min RR': minRr,
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
                "emaFast": symbolBestCombo['EMA Fast'],
                "emaSlow": symbolBestCombo['EMA Slow'],
                "minRr": symbolBestCombo['Min RR']
            }
            imacd_params = {
                "useImpulseMacdFilter": 1,
                "macdFast": 12,
                "macdSlow": symbolBestCombo['IMACD Slow'],
                "macdSignal": symbolBestCombo['IMACD Signal']
            }
            opt_db_helper.saveSymbolStrategyConfig('CruceEMA', symbol, True, params, imacd_params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: Fast={symbolBestCombo['EMA Fast']} | Slow={symbolBestCombo['EMA Slow']} | IMACD={symbolBestCombo['IMACD Slow']}/{symbolBestCombo['IMACD Signal']} | RR={symbolBestCombo['Min RR']} | Conf={symbolBestCombo['Min Conf']}% | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"emaFast": 9, "emaSlow": 21, "minRr": 1.5}
            fallbackImacd = {"useImpulseMacdFilter": 1, "macdFast": 12, "macdSlow": 26, "macdSignal": 9}
            opt_db_helper.saveSymbolStrategyConfig('CruceEMA', symbol, False, fallbackParams, fallbackImacd)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")

    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("cruceema_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("cruceema_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")


if __name__ == '__main__':
    runCruceEMAGridSearch()
