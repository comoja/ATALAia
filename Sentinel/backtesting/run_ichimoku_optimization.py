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

def calculateIchimokuIndicators(df: pd.DataFrame, tenkan: int, kijun: int, senkou: int, displacement: int) -> pd.DataFrame:
    high_prices = df['high']
    low_prices = df['low']
    close_prices = df['close']

    tenkan_sen = (high_prices.rolling(window=tenkan).max() + low_prices.rolling(window=tenkan).min()) / 2
    kijun_sen = (high_prices.rolling(window=kijun).max() + low_prices.rolling(window=kijun).min()) / 2
    senkou_span_a = ((tenkan_sen + kijun_sen) / 2).shift(displacement)
    senkou_span_b = ((high_prices.rolling(window=senkou).max() + low_prices.rolling(window=senkou).min()) / 2).shift(displacement)
    chikou_span = close_prices.shift(-displacement)

    df_res = pd.DataFrame(index=df.index)
    df_res['open'] = df['open']
    df_res['high'] = df['high']
    df_res['low'] = df['low']
    df_res['close'] = df['close']
    df_res['volume'] = df.get('volume', 0)
    df_res['tenkan'] = tenkan_sen
    df_res['kijun'] = kijun_sen
    df_res['senkou_a'] = senkou_span_a
    df_res['senkou_b'] = senkou_span_b
    df_res['chikou'] = chikou_span
    
    return df_res

def calculateHtfTrendSeries(dfHtf: pd.DataFrame, tenkan: int, kijun: int, senkou: int, displacement: int) -> pd.Series:
    df_ichi = calculateIchimokuIndicators(dfHtf, tenkan, kijun, senkou, displacement)
    close = df_ichi['close']
    span_a = df_ichi['senkou_a']
    span_b = df_ichi['senkou_b']
    
    trend = pd.Series("NEUTRAL", index=dfHtf.index)
    bull_mask = (close > span_a) & (close > span_b)
    bear_mask = (close < span_a) & (close < span_b)
    trend[bull_mask] = "ALCISTA"
    trend[bear_mask] = "BAJISTA"
    return trend

def runIchimokuGridSearch() -> None:
    print("==========================================================")
    print("  INICIANDO GRID SEARCH OPTIMIZER (ICHIMOKU CLOUD SYSTEM)")
    print("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    tenkanCombos = [7, 9, 12]
    kijunCombos = [22, 26, 30]
    senkouCombos = [44, 52, 60]
    displacement = 26
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 600:
            print(f"⚠️ Datos insuficientes para {symbol} ({len(df5m)} velas).")
            fallbackParams = {"tenkan": 9, "kijun": 26, "senkou": 52, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('Ichimoku', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        df1h = df5m.resample('1h').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 220 or len(df1h) < 60:
            fallbackParams = {"tenkan": 9, "kijun": 26, "senkou": 52, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('Ichimoku', symbol, False, fallbackParams)
            continue
            
        pipMult = opt_db_helper.getPipMultiplier(symbol)
        spreadPrice = opt_db_helper.getSpread(symbol) / pipMult
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for tenkan in tenkanCombos:
            for kijun in kijunCombos:
                for senkou in senkouCombos:
                    htfTrendSeries = calculateHtfTrendSeries(df1h, tenkan, kijun, senkou, displacement)
                    htfTrendAligned = htfTrendSeries.reindex(df15m.index, method='ffill').fillna("NEUTRAL")
                    
                    df_ichi = calculateIchimokuIndicators(df15m, tenkan, kijun, senkou, displacement)
                    atr14 = ta.ATR(df15m['high'].values, df15m['low'].values, df15m['close'].values, timeperiod=14)
                    
                    closes = df15m['close'].values
                    highs = df15m['high'].values
                    lows = df15m['low'].values
                    opens = df15m['open'].values
                    n = len(df15m)
                    
                    tenkan_vals = df_ichi['tenkan'].values
                    kijun_vals = df_ichi['kijun'].values
                    span_a_vals = df_ichi['senkou_a'].values
                    span_b_vals = df_ichi['senkou_b'].values
                    trends = htfTrendAligned.values
                    
                    validIdx = np.where(~np.isnan(tenkan_vals) & ~np.isnan(kijun_vals) & ~np.isnan(span_a_vals) & ~np.isnan(span_b_vals) & ~np.isnan(atr14))[0]
                    if len(validIdx) < 100: continue
                    
                    candidates = []
                    idx = validIdx[0]
                    while idx < n - 1:
                        cPrice = closes[idx]
                        kPrice = kijun_vals[idx]
                        tPrice = tenkan_vals[idx]
                        spA = span_a_vals[idx]
                        spB = span_b_vals[idx]
                        atrV = atr14[idx]
                        tr = trends[idx]
                        
                        isKumoBull = spA > spB
                        kumoTop = max(spA, spB)
                        kumoBottom = min(spA, spB)
                        
                        isLong = (cPrice > kumoTop) and (tPrice > kPrice) and (cPrice >= kPrice) and (tr == "ALCISTA")
                        isShort = (cPrice < kumoBottom) and (tPrice < kPrice) and (cPrice <= kPrice) and (tr == "BAJISTA")
                        
                        direction = None
                        if isLong: direction = "LARGO"
                        elif isShort: direction = "CORTO"
                        
                        if direction:
                            candidates.append({
                                'idx': idx,
                                'direction': direction,
                                'price': cPrice,
                                'atrVal': atrV,
                                'kijunVal': kPrice,
                                'kumoTop': kumoTop,
                                'kumoBottom': kumoBottom
                            })
                        idx += 1
                        
                    for minRr in minRrCombos:
                        trades = []
                        last_exit_idx = -1
                        
                        for cand in candidates:
                            if cand['idx'] <= last_exit_idx:
                                continue
                                
                            direction = cand['direction']
                            price = cand['price']
                            atrVal = cand['atrVal']
                            idx_entry = cand['idx']
                            
                            if direction == "LARGO":
                                sl = min(cand['kijunVal'], cand['kumoTop']) - (atrVal * 0.2)
                            else:
                                sl = max(cand['kijunVal'], cand['kumoBottom']) + (atrVal * 0.2)
                                
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
                                'Tenkan': tenkan,
                                'Kijun': kijun,
                                'Senkou': senkou,
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
                "tenkan": symbolBestCombo['Tenkan'],
                "kijun": symbolBestCombo['Kijun'],
                "senkou": symbolBestCombo['Senkou'],
                "min_rr": symbolBestCombo['Min RR']
            }
            opt_db_helper.saveSymbolStrategyConfig('Ichimoku', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: Tenkan={symbolBestCombo['Tenkan']} | Kijun={symbolBestCombo['Kijun']} | Senkou={symbolBestCombo['Senkou']} | RR={symbolBestCombo['Min RR']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"tenkan": 9, "kijun": 26, "senkou": 52, "min_rr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('Ichimoku', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")

    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = opt_db_helper.getOutputPath("ichimoku_grid_results_all.csv")
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = opt_db_helper.getOutputPath("ichimoku_grid_results_best.csv")
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runIchimokuGridSearch()
