import os
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
