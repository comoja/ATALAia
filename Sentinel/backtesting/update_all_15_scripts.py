import os
import sys

base_dir = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/Sentinel/backtesting'

# 1. run_genericfvg_optimization.py
genericfvg_code = '''import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
import logging
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from Sentinel.analysis import technical
from middleware.utils.alertBuilder import adjustTPForMinRR
from Sentinel.backtesting import opt_db_helper

logging.getLogger('sentinel').setLevel(logging.ERROR)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def runGenericFVGGridSearch() -> None:
    print("==========================================================")
    print("  INICIANDO GRID SEARCH OPTIMIZER (GENERICFVG - FAST NUMPY) ")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    minRrCombos = [1.0, 1.2, 1.4, 1.5, 1.7, 1.8, 2.0, 2.2, 2.5, 2.8, 3.0, 3.5]
    minConfidenceCombos = [0.0, 40.0, 45.0, 50.0, 55.0, 60.0, 65.0, 70.0, 75.0, 80.0, 85.0, 90.0]
    requireHtfSweepCombos = [True, False]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            fallbackParams = {"minRr": 1.5, "minConfidence": 0.0, "requireHtfSweep": False}
            opt_db_helper.saveSymbolStrategyConfig('GenericFVG', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        df1d = df5m.resample('1D').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 200 or len(df1d) < 3:
            print(f"  ⚠️ Datos insuficientes en 15m/1d para {symbol}. Saltando.")
            fallbackParams = {"minRr": 1.5, "minConfidence": 0.0, "requireHtfSweep": False}
            opt_db_helper.saveSymbolStrategyConfig('GenericFVG', symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        pdhMap = {}
        pdlMap = {}
        for i in range(1, len(df1d)):
            curDayDate = df1d.index[i].date()
            prevDayRow = df1d.iloc[i-1]
            pdhMap[curDayDate] = float(prevDayRow['high'])
            pdlMap[curDayDate] = float(prevDayRow['low'])
            
        df15m = df15m.copy()
        df15m["atr"] = ta.ATR(df15m['high'].values, df15m['low'].values, df15m['close'].values, timeperiod=14)
        df15m.dropna(subset=["atr"], inplace=True)
        
        fvgsList = technical.detect_fvgs(df15m, validate_mitigation=False, apply_high_prob_filters=True)
        fvgMap = {fvg.get('idx'): fvg for fvg in fvgsList if fvg.get('idx') is not None}
        
        opens = df15m['open'].values
        highs = df15m['high'].values
        lows = df15m['low'].values
        closes = df15m['close'].values
        atrs = df15m['atr'].values
        times = df15m.index
        n = len(df15m)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        precomputed_signals = []
        for idx_fvg in sorted(fvgMap.keys()):
            latestFvg = fvgMap[idx_fvg]
            classification = latestFvg.get('classification', 'Alta Probabilidad')
            if classification == 'Rechazo/Baja Probabilidad':
                continue
                
            fvgDirection = "LARGO" if latestFvg['type'] == 'Bullish_FVG' else "CORTO"
            
            sweepLookback = 150
            hasSweep = False
            sweepType = None
            startK = max(0, idx_fvg - sweepLookback)
            
            for k in range(idx_fvg, startK - 1, -1):
                kDate = times[k].date()
                kPdh = pdhMap.get(kDate)
                kPdl = pdlMap.get(kDate)
                if kPdh is None or kPdl is None:
                    continue
                
                kHigh = highs[k]
                kLow = lows[k]
                kClose = closes[k]
                
                if kHigh > kPdh and kClose < kPdh:
                    sweepType = 'MANIPULATION_UP'
                    hasSweep = True
                    break
                elif kLow < kPdl and kClose > kPdl:
                    sweepType = 'MANIPULATION_DOWN'
                    hasSweep = True
                    break
                    
            precomputed_signals.append({
                'idx_fvg': idx_fvg,
                'fvgDirection': fvgDirection,
                'hasSweep': hasSweep,
                'sweepType': sweepType,
                'currentPrice': closes[idx_fvg],
                'atrVal': atrs[idx_fvg],
                'latestFvg': latestFvg
            })
            
        for minRr in minRrCombos:
            for minConfidence in minConfidenceCombos:
                for requireHtfSweep in requireHtfSweepCombos:
                    trades = []
                    last_exit_idx = -1
                    
                    for sig in precomputed_signals:
                        idx_fvg = sig['idx_fvg']
                        if idx_fvg <= last_exit_idx:
                            continue
                            
                        fvgDirection = sig['fvgDirection']
                        hasSweep = sig['hasSweep']
                        sweepType = sig['sweepType']
                        
                        skipSignal = False
                        if hasSweep:
                            if sweepType == 'MANIPULATION_UP' and fvgDirection == 'LARGO':
                                skipSignal = True
                            elif sweepType == 'MANIPULATION_DOWN' and fvgDirection == 'CORTO':
                                skipSignal = True
                        else:
                            if requireHtfSweep:
                                skipSignal = True
                                
                        if skipSignal:
                            continue
                            
                        currentPrice = sig['currentPrice']
                        atrVal = sig['atrVal']
                        latestFvg = sig['latestFvg']
                        
                        setupFvg = technical.calculate_fvg_setup(latestFvg, currentPrice, atrVal)
                        entryPrice = setupFvg['entry']
                        sl = setupFvg['sl']
                        
                        fvgIdxPrior = latestFvg.get('idx', idx_fvg)
                        startPrior = max(0, fvgIdxPrior - 19)
                        
                        if fvgDirection == "LARGO":
                            tpRef = np.max(np.maximum(opens[startPrior:fvgIdxPrior+1], closes[startPrior:fvgIdxPrior+1]))
                        else:
                            tpRef = np.min(np.minimum(opens[startPrior:fvgIdxPrior+1], closes[startPrior:fvgIdxPrior+1]))
                            
                        maxAtrMult = 3.0
                        if fvgDirection == "LARGO":
                            maxTp = currentPrice + atrVal * maxAtrMult
                            tpRef = min(tpRef, maxTp)
                        else:
                            minTp = currentPrice - atrVal * maxAtrMult
                            tpRef = max(tpRef, minTp)
                            
                        tp1 = adjustTPForMinRR(entryPrice, sl, tpRef, fvgDirection, minRR=minRr)
                        
                        riskDist = abs(entryPrice - sl)
                        rewardDist = abs(tp1 - entryPrice)
                        rrRatio = rewardDist / riskDist if riskDist > 0 else 0
                        
                        if rrRatio > 15:
                            continue
                            
                        progressPct = (currentPrice - entryPrice) / (entryPrice - sl) if fvgDirection == 'LARGO' else (entryPrice - currentPrice) / (sl - entryPrice)
                        if progressPct > 3.5:
                            continue
                            
                        if fvgDirection == "LARGO" and currentPrice <= sl:
                            continue
                        if fvgDirection == "CORTO" and currentPrice >= sl:
                            continue
                            
                        realRiskDist = abs(currentPrice - sl)
                        rrVal = round(abs(tp1 - currentPrice) / realRiskDist, 2) if realRiskDist > 0 else 0
                        minRealRr = minRr * 0.70
                        if rrVal < minRealRr:
                            continue
                            
                        baseConfidence = 85
                        if baseConfidence < minConfidence:
                            continue
                            
                        lows_slice = lows[idx_fvg:]
                        highs_slice = highs[idx_fvg:]
                        
                        if fvgDirection == "LARGO":
                            sl_hits = np.where(lows_slice - (spreadPrice / 2.0) <= sl)[0]
                            tp_hits = np.where(highs_slice + (spreadPrice / 2.0) >= tp1)[0]
                        else:
                            sl_hits = np.where(highs_slice + (spreadPrice / 2.0) >= sl)[0]
                            tp_hits = np.where(lows_slice - (spreadPrice / 2.0) <= tp1)[0]
                            
                        first_sl = sl_hits[0] if len(sl_hits) > 0 else n
                        first_tp = tp_hits[0] if len(tp_hits) > 0 else n
                        
                        if first_sl < first_tp:
                            trades.append(-100.0)
                            last_exit_idx = idx_fvg + first_sl
                        elif first_tp < first_sl:
                            trades.append(100.0 * minRr)
                            last_exit_idx = idx_fvg + first_tp
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
                            'Min RR': minRr,
                            'Min Conf': minConfidence,
                            'Require Sweep': requireHtfSweep,
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
            params = {
                "minRr": symbolBestCombo['Min RR'],
                "minConfidence": symbolBestCombo['Min Conf'],
                "requireHtfSweep": symbolBestCombo['Require Sweep']
            }
            opt_db_helper.saveSymbolStrategyConfig('GenericFVG', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: RR={symbolBestCombo['Min RR']} | Conf={symbolBestCombo['Min Conf']}% | Sweep={symbolBestCombo['Require Sweep']} | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"minRr": 1.5, "minConfidence": 0.0, "requireHtfSweep": False}
            opt_db_helper.saveSymbolStrategyConfig('GenericFVG', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable y viable para {symbol}. Guardado en DB (FALSE).")
            
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("genericfvg_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("genericfvg_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runGenericFVGGridSearch()
'''
with open(os.path.join(base_dir, 'run_genericfvg_optimization.py'), 'w', encoding='utf-8') as f:
    f.write(genericfvg_code.strip() + '\n')
print("Updated run_genericfvg_optimization.py")

# 2. run_sniper_optimization.py
sniper_code = '''import os
import sys
import pandas as pd
import numpy as np
import logging
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.config import constants as config
from Sentinel.analysis import technical
from Sentinel.ml import model as mlModel
from Sentinel.backtesting import opt_db_helper

logger = logging.getLogger("sniper_grid_search")

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def evaluateGrid(symbol: str, df15m: pd.DataFrame, model, probaThresholdLongCombos, minConfidenceCombos, minRrCombos):
    df_prepared = technical.calculateFeatures(df15m.copy())
    if df_prepared is None or df_prepared.empty: return []
    df_prepared = mlModel.defineMlTarget(df_prepared)
    if df_prepared is None or df_prepared.empty: return []
        
    X, _ = mlModel.cleanDataForModel(df_prepared)
    if len(X) < 100: return []
        
    features = mlModel.MODEL_FEATURES
    X_clean = X.dropna(subset=features)
    if X_clean.empty: return []
    
    probas = model.predict_proba(X_clean[features])[:, 1]
    
    df_op = df_prepared.loc[X_clean.index]
    close_arr = df_op['close'].values
    high_arr = df_op['high'].values
    low_arr = df_op['low'].values
    atr_arr = df_op['atr'].values
    
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    
    results = []
    
    for probaThresholdLong in probaThresholdLongCombos:
        probaThresholdShort = 1.0 - probaThresholdLong
        
        long_signals = probas >= probaThresholdLong
        short_signals = probas <= probaThresholdShort
        
        for minConfidence in minConfidenceCombos:
            for minRr in minRrCombos:
                
                trades = []
                in_trade = False
                
                for i in range(100, len(X_clean)):
                    if in_trade:
                        v_k_low = low_arr[i]
                        v_k_high = high_arr[i]
                        
                        if direction == "LARGO":
                            if v_k_low <= sl_price:
                                in_trade = False
                                trades.append((sl_price - entry_price) * pipMult - opt_db_helper.getSpread(symbol))
                            elif v_k_high >= tp_price:
                                in_trade = False
                                trades.append((tp_price - entry_price) * pipMult - opt_db_helper.getSpread(symbol))
                        else:
                            if v_k_high >= sl_price:
                                in_trade = False
                                trades.append((entry_price - sl_price) * pipMult - opt_db_helper.getSpread(symbol))
                            elif v_k_low <= tp_price:
                                in_trade = False
                                trades.append((entry_price - tp_price) * pipMult - opt_db_helper.getSpread(symbol))
                        continue
                        
                    is_long = long_signals[i-1]
                    is_short = short_signals[i-1]
                    
                    if not is_long and not is_short:
                        continue
                        
                    prob_val = probas[i-1]
                    confidence = prob_val if is_long else (1.0 - prob_val)
                    if (confidence * 100) < minConfidence:
                        continue
                        
                    atr_val = atr_arr[i-1]
                    if np.isnan(atr_val) or atr_val <= 0:
                        continue
                        
                    current_close = close_arr[i-1]
                    
                    if is_long:
                        direction = "LARGO"
                        entry_price = current_close
                        sl_price = entry_price - (atr_val * 1.5)
                        tp_price = entry_price + (abs(entry_price - sl_price) * minRr)
                        in_trade = True
                    elif is_short:
                        direction = "CORTO"
                        entry_price = current_close
                        sl_price = entry_price + (atr_val * 1.5)
                        tp_price = entry_price - (abs(entry_price - sl_price) * minRr)
                        in_trade = True
                        
                if trades:
                    wins = [t for t in trades if t > 0]
                    losses = [t for t in trades if t <= 0]
                    win_rate = (len(wins) / len(trades)) * 100
                    pnl = sum(trades)
                    profit_factor = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else (99.0 if wins else 0.0)
                    
                    if len(trades) >= 3 and profit_factor >= 1.0:
                        results.append({
                            "symbol": symbol,
                            "probaThresholdLong": probaThresholdLong,
                            "minConfidence": minConfidence,
                            "minRr": minRr,
                            "totalTrades": len(trades),
                            "winRate": round(win_rate, 2),
                            "profitFactor": round(profit_factor, 2),
                            "pnl": round(pnl, 2)
                        })
                        
    return results

def runSniperGridSearch():
    logger.info("==========================================================")
    logger.info(" INICIANDO GRID SEARCH OPTIMIZER PARA ESTRATEGIA SNIPER")
    logger.info("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    model = mlModel.loadModel(config.MODEL_FILE_PATH)
    if model is None:
        logger.error("No se pudo cargar el modelo ML para Sniper.")
        return
        
    probaThresholdLongCombos = [0.45, 0.50, 0.55, 0.60]
    minConfidenceCombos = [50, 55, 60, 65]
    minRrCombos = [1.2, 1.5, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"⚙️ Optimizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 500:
            logger.warning(f"  ⚠️ Datos insuficientes para {symbol} ({len(df5m)} velas).")
            fallbackParams = {"minRr": 1.5, "minConfidence": 55, "probaThresholdLong": 0.50}
            opt_db_helper.saveSymbolStrategyConfig('Sniper', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        results = evaluateGrid(symbol, df15m, model, probaThresholdLongCombos, minConfidenceCombos, minRrCombos)
        allResultsRaw.extend(results)
        
        if results:
            best = max(results, key=lambda x: (x['profitFactor'], x['winRate']))
            logger.info(f"  ✨ Mejor combo para {symbol}: PF={best['profitFactor']}, MinConf={best['minConfidence']}, R:R={best['minRr']}")
            bestResults.append(best)
            params = {
                "minRr": best['minRr'],
                "minConfidence": best['minConfidence'],
                "probaThresholdLong": best['probaThresholdLong']
            }
            opt_db_helper.saveSymbolStrategyConfig('Sniper', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0) para {symbol}.")
            fallbackParams = {"minRr": 1.5, "minConfidence": 55, "probaThresholdLong": 0.50}
            opt_db_helper.saveSymbolStrategyConfig('Sniper', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró combo viable para {symbol}. Guardado en DB (FALSE).")
            
    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: 
        dfAll.to_csv(opt_db_helper.getOutputPath("sniper_grid_results_all.csv"), index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty:
        dfBest.to_csv(opt_db_helper.getOutputPath("sniper_grid_results_best.csv"), index=False)

if __name__ == '__main__':
    runSniperGridSearch()
'''
with open(os.path.join(base_dir, 'run_sniper_optimization.py'), 'w', encoding='utf-8') as f:
    f.write(sniper_code.strip() + '\n')
print("Updated run_sniper_optimization.py")
