import sys
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
