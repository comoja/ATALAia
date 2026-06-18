import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
from datetime import datetime

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from Sentinel.ml import model as mlModel
from middleware.config import constants as config

ALL_SYMBOLS = [
    'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
    'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
    'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
]

PIP_MULTIPLIERS = {
    'EUR/USD': 10000.0, 'GBP/USD': 10000.0, 'AUD/USD': 10000.0, 'NZD/USD': 10000.0,
    'USD/CAD': 10000.0, 'USD/CHF': 10000.0, 'EUR/GBP': 10000.0, 'GBP/CAD': 10000.0,
    'GBP/JPY': 100.0, 'USD/JPY': 100.0, 'USD/MXN': 10000.0, 'XAU/USD': 1.0, 'BTC/USD': 1.0,
}

SPREADS = {
    'EUR/USD': 1.0, 'GBP/USD': 1.5, 'AUD/USD': 1.2, 'NZD/USD': 1.5,
    'USD/CAD': 1.5, 'USD/CHF': 1.6, 'EUR/GBP': 1.5, 'GBP/CAD': 2.2,
    'GBP/JPY': 2.0, 'USD/JPY': 1.2, 'USD/MXN': 25.0, 'XAU/USD': 0.35, 'BTC/USD': 30.0,
}

def loadCandles(symbol: str, startDate: str, endDate: str) -> pd.DataFrame:
    try:
        connection = dbConnection.getConnection()
        if connection is None: return pd.DataFrame()
        query = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = '5min' AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, connection, params=(symbol, startDate, endDate))
        connection.close()
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
        return df
    except Exception as e:
        print(f"Error cargando velas: {e}")
        return pd.DataFrame()

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
    
    startDateStr = '2026-04-16 00:00:00' 
    endDateStr = '2026-06-16 23:59:59'
    
    modelClf = mlModel.loadModel(config.MODEL_FILE_PATH)
    if modelClf is None:
        print("❌ No se pudo cargar el modelo ML. Saliendo.")
        return
        
    smaCombos = [
        (10, 20),
        (15, 20),
        (20, 50)
    ]
    minRrCombos = [1.2, 1.5, 2.0]
    minConfidenceCombos = [65.0, 70.0, 75.0]
    
    bestResults = []
    allResultsRaw = []
    
    lengthMA = 34
    lengthSignal = 9
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 600:
            continue
            
        # Resample a 15 min
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 220:
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        opens = df15m['open'].values
        highs = df15m['high'].values
        lows = df15m['low'].values
        closes = df15m['close'].values
        n = len(df15m)
        
        # Calcular IMACD globalmente
        hlc3 = (highs + lows + closes) / 3.0
        hi_arr = pd.Series(highs).ewm(alpha=1.0/lengthMA, adjust=False).mean().values
        lo_arr = pd.Series(lows).ewm(alpha=1.0/lengthMA, adjust=False).mean().values
        ema1 = ta.EMA(hlc3, timeperiod=lengthMA)
        ema2 = ta.EMA(ema1, timeperiod=lengthMA)
        mi_arr = ema1 + (ema1 - ema2)
        md_arr = np.where(mi_arr > hi_arr, mi_arr - hi_arr, np.where(mi_arr < lo_arr, mi_arr - lo_arr, 0.0))
        sb_arr = ta.SMA(md_arr, timeperiod=lengthSignal)
        
        
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
            
            validIdx = np.where(~np.isnan(emaF) & ~np.isnan(emaS) & ~np.isnan(atr14) & ~np.isnan(sb_arr))[0]
            if len(validIdx) < 100: continue
                
            probs = np.zeros(n)
            featData = []
            featIndices = []
            for i in range(1, n):
                if i not in validIdx: continue
                featData.append({
                    "close": closes[i], "atr": atr14[i], "atr_norm": atr14[i]/closes[i],
                    "sma20": emaF[i], "sma200": emaS[i],
                    "dist_sma20": (closes[i] - emaF[i])/closes[i],
                    "dist_sma200": (closes[i] - emaS[i])/closes[i],
                    "log_return": np.log(closes[i]/closes[i-1]),
                    "range": (highs[i]-lows[i])/closes[i],
                    "sma_slope": emaSlopes[i]
                })
                featIndices.append(i)
                
            if featData:
                dfFeat = pd.DataFrame(featData)
                try:
                    predProbs = modelClf.predict_proba(dfFeat)[:, 1]
                    for idx_feat, i in enumerate(featIndices): probs[i] = predProbs[idx_feat]
                except:
                    for i in featIndices: probs[i] = 0.55
            
            for minRr in minRrCombos:
                for minConfidence in minConfidenceCombos:
                    minConfVal = minConfidence / 100.0
                    
                    trades = []
                    activeTrade = None
                    
                    idx = smaSlow + 20
                    while idx < n:
                        price = closes[idx]
                        if activeTrade:
                            vHigh = highs[idx]
                            vLow = lows[idx]
                            tradeClosed = False
                            
                            if False:
                                pass
 
                            if not tradeClosed:
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
                            
                        direction = None
                        v_curr_close = closes[idx]
                        v_curr_open = opens[idx]
                        v_curr_high = highs[idx]
                        v_curr_low = lows[idx]
                        v_prev_close = closes[idx-1]
                        v_prev_open = opens[idx-1]
                        
                        body_curr = abs(v_curr_close - v_curr_open)
                        
                        md_val = md_arr[idx]
                        sb_val = sb_arr[idx]
                        
                        if emaF[idx] > emaS[idx]:
                            in_zone = v_curr_low <= (emaF[idx] + (atr14[idx]*0.1))
                            lower_wick = min(v_curr_open, v_curr_close) - v_curr_low
                            is_pinbar = (lower_wick > (body_curr * 1.5)) and (v_curr_close > v_curr_open) and body_curr > 0
                            is_engulfing = (v_prev_close < v_prev_open) and (v_curr_close > v_curr_open) and (v_curr_close > v_prev_open) and (v_curr_open < v_prev_close)
                            
                            imacd_bullish = (md_val > sb_val) and (md_val > 0)
                            
                            if in_zone and (is_pinbar or is_engulfing) and imacd_bullish: direction = "LARGO"
                                
                        elif emaF[idx] < emaS[idx]:
                            in_zone = v_curr_high >= (emaF[idx] - (atr14[idx]*0.1))
                            upper_wick = v_curr_high - max(v_curr_open, v_curr_close)
                            is_pinbar = (upper_wick > (body_curr * 1.5)) and (v_curr_close < v_curr_open) and body_curr > 0
                            is_engulfing = (v_prev_close > v_prev_open) and (v_curr_close < v_curr_open) and (v_curr_close < v_prev_open) and (v_curr_open > v_prev_close)
                            
                            imacd_bearish = (md_val < sb_val) and (md_val < 0)
                            
                            if in_zone and (is_pinbar or is_engulfing) and imacd_bearish: direction = "CORTO"
                        
                        if direction:
                            prob = probs[idx]
                            if prob >= minConfVal:
                                atrVal = atr14[idx]
                                swingHigh = sHighs[idx]
                                swingLow = sLows[idx]
                                hZone = hZones[idx]
                                lZone = lZones[idx]
                                
                                if direction == "LARGO":
                                    sl = swingLow - atrVal * 0.2
                                    tpStruct = hZone
                                else:
                                    sl = swingHigh + atrVal * 0.2
                                    tpStruct = lZone
                                    
                                slDist = abs(price - sl)
                                if slDist > 0:
                                    tpDist = max(abs(tpStruct - price), slDist * minRr)
                                    tp = price + tpDist if direction == "LARGO" else price - tpDist
                                    
                                    activeTrade = {
                                        'direction': direction,
                                        'entry': price + (spreadPrice / 2.0) if direction == "LARGO" else price - (spreadPrice / 2.0),
                                        'sl': sl,
                                        'tp': tp,
                                        'sl_dist': slDist
                                    }
                                    
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
                            'EMA Fast': smaFast,
                            'EMA Slow': smaSlow,
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
            print(f"  🏆 Mejor combo para {symbol}: Fast={symbolBestCombo['EMA Fast']} | Slow={symbolBestCombo['EMA Slow']} | RR={symbolBestCombo['Min RR']} | Conf={symbolBestCombo['Min Conf']}% | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}.")

    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/cruceema_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/cruceema_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")
        
        try:
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            
            for combo in bestResults:
                sym = combo['Símbolo']
                params = {
                    "emaFast": combo['EMA Fast'],
                    "emaSlow": combo['EMA Slow'],
                    "minRr": combo['Min RR']
                }
                paramsJson = json.dumps(params)
                
                imacd_params = {
                    "useImpulseMacdFilter": 1,
                    "macdFast": 12,
                    "macdSlow": 26,
                    "macdSignal": 9
                }
                imacdJson = json.dumps(imacd_params)
                
                sql = """
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson, jsonIMACD)
                    VALUES ('CruceEMA', %s, TRUE, %s, %s)
                    ON DUPLICATE KEY UPDATE parametersJson = VALUES(parametersJson), jsonIMACD = VALUES(jsonIMACD), enabled = TRUE
                """
                cursor.execute(sql, (sym, paramsJson, imacdJson))
                
            conn.commit()
            print("✅ Parámetros rentables guardados automáticamente en la BD por símbolo (symbolStrategyConfig).")
        except Exception as e:
            print(f"❌ Error guardando parámetros en BD: {e}")
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals(): conn.close()

if __name__ == '__main__':
    runCruceEMAGridSearch()
