import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
from datetime import datetime

# Asegurar path del proyecto en sys.path
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
    'EUR/USD': 10000.0,
    'GBP/USD': 10000.0,
    'AUD/USD': 10000.0,
    'NZD/USD': 10000.0,
    'USD/CAD': 10000.0,
    'USD/CHF': 10000.0,
    'EUR/GBP': 10000.0,
    'GBP/CAD': 10000.0,
    'GBP/JPY': 100.0,
    'USD/JPY': 100.0,
    'USD/MXN': 10000.0,
    'XAU/USD': 1.0,
    'BTC/USD': 1.0,
}

SPREADS = {
    'EUR/USD': 1.0,
    'GBP/USD': 1.5,
    'AUD/USD': 1.2,
    'NZD/USD': 1.5,
    'USD/CAD': 1.5,
    'USD/CHF': 1.6,
    'EUR/GBP': 1.5,
    'GBP/CAD': 2.2,
    'GBP/JPY': 2.0,
    'USD/JPY': 1.2,
    'USD/MXN': 25.0,
    'XAU/USD': 0.35,
    'BTC/USD': 30.0,
}

def loadCandles(symbol: str, startDate: str, endDate: str) -> pd.DataFrame:
    try:
        connection = dbConnection.getConnection()
        if connection is None:
            return pd.DataFrame()
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
        if np.isnan(y).any():
            continue
        x = np.arange(10)
        m, _ = np.polyfit(x, y, 1)
        slopes[i] = (m / np.mean(y)) * 100
    return slopes

def runEMA20200GridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (EMA20200)         ")
    print("==========================================================")
    
    startDateStr = '2026-05-15 00:00:00' # Cargamos un poco más para EMAs lentas
    endDateStr = '2026-06-11 14:00:00'
    
    # Cargar modelo ML
    modelClf = mlModel.loadModel(config.MODEL_FILE_PATH)
    if modelClf is None:
        print("❌ No se pudo cargar el modelo ML. Saliendo.")
        return
        
    # Grid de Parámetros
    emaCombos = [
        (10, 100),
        (20, 150),
        (20, 200),
        (30, 200)
    ]
    pullbackTolerances = [0.0010, 0.0015, 0.0020]
    minRrCombos = [1.2, 1.5, 2.0]
    minConfidenceCombos = [65.0, 70.0, 75.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 600:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        # Resamplear a 1h (tiempo nativo de EMA20200)
        df1h = df5m.resample('1h').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        if len(df1h) < 220:
            print(f"  ⚠️ Datos insuficientes en 1h para {symbol} ({len(df1h)} velas). Saltando.")
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        opens = df1h['open'].values
        highs = df1h['high'].values
        lows = df1h['low'].values
        closes = df1h['close'].values
        n = len(df1h)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for emaFast, emaSlow in emaCombos:
            # Calcular indicadores en lote
            emaF = ta.EMA(closes, timeperiod=emaFast)
            emaS = ta.EMA(closes, timeperiod=emaSlow)
            atr14 = ta.ATR(highs, lows, closes, timeperiod=14)
            
            # Precalcular niveles estructurales (Swing High/Low en ventana de 20 velas)
            sHighs = df1h['high'].rolling(20).max().values
            sLows = df1h['low'].rolling(20).min().values
            hZones = df1h['high'].rolling(20).quantile(0.90).values
            lZones = df1h['low'].rolling(20).quantile(0.10).values
            
            # Precalcular slopes de la EMA rápida
            emaSlopes = calculateSlope(emaF)
            
            # Precalcular ML features y predicciones
            validIdx = np.where(~np.isnan(emaF) & ~np.isnan(emaS) & ~np.isnan(atr14))[0]
            if len(validIdx) < 100:
                continue
                
            probs = np.zeros(n)
            # Construir DataFrame de características para predecir en lote
            featData = []
            featIndices = []
            for i in range(1, n):
                if i not in validIdx:
                    continue
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
                    for idx_feat, i in enumerate(featIndices):
                        probs[i] = predProbs[idx_feat]
                except Exception as e:
                    # En caso de error, default 0.55
                    for i in featIndices:
                        probs[i] = 0.55
            
            # Precalcular Cruces (detectCross de 3 velas de lookback)
            crosses = [None] * n
            diff = emaF - emaS
            for i in range(4, n):
                # 1. actual
                if (diff[i] > 0) and (diff[i-1] <= 0): crosses[i] = "LARGO"
                elif (diff[i] < 0) and (diff[i-1] >= 0): crosses[i] = "CORTO"
                # 2. hace 1 vela
                elif (diff[i-1] > 0) and (diff[i-2] <= 0) and (diff[i] > 0): crosses[i] = "LARGO"
                elif (diff[i-1] < 0) and (diff[i-2] >= 0) and (diff[i] < 0): crosses[i] = "CORTO"
                # 3. hace 2 velas
                elif (diff[i-2] > 0) and (diff[i-3] <= 0) and (diff[i-1] > 0) and (diff[i] > 0): crosses[i] = "LARGO"
                elif (diff[i-2] < 0) and (diff[i-3] >= 0) and (diff[i-1] < 0) and (diff[i] < 0): crosses[i] = "CORTO"

            for pullbackTol in pullbackTolerances:
                for minRr in minRrCombos:
                    for minConfidence in minConfidenceCombos:
                        minConfVal = minConfidence / 100.0
                        
                        trades = []
                        waitingPullbackDir = None
                        activeTrade = None
                        
                        idx = emaSlow + 20
                        while idx < n:
                            if activeTrade:
                                # Evaluar salidas del trade
                                vHigh = highs[idx]
                                vLow = lows[idx]
                                
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
                                
                            # Evaluar cruce
                            cross = crosses[idx]
                            if cross:
                                waitingPullbackDir = cross
                                idx += 1
                                continue
                                
                            if waitingPullbackDir is not None:
                                price = closes[idx]
                                emaFastVal = emaF[idx]
                                atrVal = atr14[idx]
                                
                                # Límite de pullback adaptativo
                                pullbackLimit = atrVal * 1.5 if (atrVal and not np.isnan(atrVal)) else (emaFastVal * pullbackTol)
                                
                                if abs(price - emaFastVal) <= pullbackLimit:
                                    # Carga de probabilidad ML
                                    prob = probs[idx]
                                    if prob >= minConfVal:
                                        # Entrar al trade
                                        direction = waitingPullbackDir
                                        waitingPullbackDir = None
                                        
                                        # Calcular precios SL y TP estructurales
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
                                            # Ajustar TP por R:R mínimo
                                            tpDist = max(abs(tpStruct - price), slDist * minRr)
                                            tp = price + tpDist if direction == "LARGO" else price - tpDist
                                            
                                            activeTrade = {
                                                'direction': direction,
                                                'entry': price + (spreadPrice / 2.0) if direction == "LARGO" else price - (spreadPrice / 2.0),
                                                'sl': sl,
                                                'tp': tp
                                            }
                                else:
                                    # Si se aleja demasiado, cancelar pullback waiting
                                    if abs(price - emaFastVal) > pullbackLimit * 2.0:
                                        waitingPullbackDir = None
                                        
                            idx += 1
                            
                        # Calcular métricas del backtest
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
                                'EMA Fast': emaFast,
                                'EMA Slow': emaSlow,
                                'Pullback Tol': pullbackTol,
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
            print(f"  🏆 Mejor combo para {symbol}: Fast={symbolBestCombo['EMA Fast']} | Slow={symbolBestCombo['EMA Slow']} | Tol={symbolBestCombo['Pullback Tol']} | RR={symbolBestCombo['Min RR']} | Conf={symbolBestCombo['Min Conf']}% | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}.")

    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/ema20200_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/ema20200_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runEMA20200GridSearch()
