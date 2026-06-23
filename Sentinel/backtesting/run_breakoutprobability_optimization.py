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
from Sentinel.analysis import technical

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

def runBreakoutProbabilityGridSearch() -> None:
    print("==========================================================")
    print("  INICIANDO GRID SEARCH OPTIMIZER (BREAKOUTPROBABILITY)   ")
    print("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00' # Dos meses de datos
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros
    channelLens = [10, 15, 20, 25, 30, 35, 40]
    targetAtrMults = [1.0, 1.2, 1.4, 1.5, 1.6, 1.8, 2.0, 2.2, 2.5]
    minProbThresholds = [45.0, 50.0, 55.0, 60.0, 65.0]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        # Resamplear a 15min para BreakoutProbability
        df15m = df5m.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 350:
            print(f"  ⚠️ Datos insuficientes en 15min para {symbol} ({len(df15m)} velas). Saltando.")
            continue
            
        # Calcular Impulse MACD
        impulseMacd, impulseSignal = technical.calculateImpulseMacd(df15m)
        df15m["impulseMacd"] = impulseMacd
        df15m["impulseSignal"] = impulseSignal
        df15m["atr"] = ta.ATR(df15m['high'].values, df15m['low'].values, df15m['close'].values, timeperiod=14)
        
        # Eliminar nulos iniciales de indicadores
        df15m.dropna(subset=["impulseMacd", "impulseSignal", "atr"], inplace=True)
        if len(df15m) < 320:
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        # Guardar en Numpy para velocidad
        opens = df15m['open'].values
        highs = df15m['high'].values
        lows = df15m['low'].values
        closes = df15m['close'].values
        atrs = df15m['atr'].values
        macdVal = df15m['impulseMacd'].values
        macdSig = df15m['impulseSignal'].values
        times = df15m.index
        n = len(df15m)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for channelLen in channelLens:
            # Precalcular máximos y mínimos del canal de Donchian desplazados (excluyendo la vela actual)
            maxVals = df15m['high'].shift(1).rolling(channelLen).max().values
            minVals = df15m['low'].shift(1).rolling(channelLen).min().values
            
            for targetAtrMult in targetAtrMults:
                for minRr in minRrCombos:
                    # Precalcular si habría éxito en cada vela si rompe
                    # Para acelerar, lo hacemos en bucle Numpy corto (lookahead 10 velas)
                    successBull = np.zeros(n, dtype=bool)
                    successBear = np.zeros(n, dtype=bool)
                    
                    for i in range(channelLen, n - 10):
                        closeVal = closes[i]
                        atrVal = atrs[i]
                        maxVal = maxVals[i]
                        minVal = minVals[i]
                        if np.isnan(maxVal) or np.isnan(minVal) or np.isnan(atrVal):
                            continue
                            
                        # Éxito alcista
                        slDistBull = closeVal - minVal
                        if slDistBull > 0:
                            tpDistBull = max(targetAtrMult * atrVal, minRr * slDistBull)
                            tpBull = closeVal + tpDistBull
                            slBull = minVal
                            for k in range(i + 1, i + 11):
                                if highs[k] >= tpBull:
                                    successBull[i] = True
                                    break
                                if lows[k] <= slBull:
                                    break
                                    
                        # Éxito bajista
                        slDistBear = maxVal - closeVal
                        if slDistBear > 0:
                            tpDistBear = max(targetAtrMult * atrVal, minRr * slDistBear)
                            tpBear = closeVal - tpDistBear
                            slBear = maxVal
                            for k in range(i + 1, i + 11):
                                if lows[k] <= tpBear:
                                    successBear[i] = True
                                    break
                                if highs[k] >= slBear:
                                    break
                                    
                    for minProbThreshold in minProbThresholds:
                        trades = []
                        activeTrade = None
                        
                        # Simular probabilidades móviles (lookback 300)
                        # attempts y successes móviles
                        # Usando Pandas Series para calcular rolling sum rápido
                        isBullBreak = (closes > maxVals) & (macdVal > macdSig) & ((maxVals - minVals) <= 2.0 * atrs)
                        isBearBreak = (closes < minVals) & (macdVal < macdSig) & ((maxVals - minVals) <= 2.0 * atrs)
                        
                        # Convertir a series de pandas temporales para rolling sum
                        sBullBreak = pd.Series(isBullBreak)
                        sBearBreak = pd.Series(isBearBreak)
                        sBullSuccess = pd.Series(isBullBreak & successBull)
                        sBearSuccess = pd.Series(isBearBreak & successBear)
                        
                        attemptsBull = sBullBreak.rolling(300).sum().values
                        successesBull = sBullSuccess.rolling(300).sum().values
                        attemptsBear = sBearBreak.rolling(300).sum().values
                        successesBear = sBearSuccess.rolling(300).sum().values
                        
                        longProbs = np.where(attemptsBull > 0, (successesBull / attemptsBull) * 100.0, 50.0)
                        shortProbs = np.where(attemptsBear > 0, (successesBear / attemptsBear) * 100.0, 50.0)
                        
                        # Iniciar simulación de trading a partir del lookback (index 300)
                        idx = 300
                        while idx < n:
                            if activeTrade:
                                # Evaluar salidas
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
                                
                            # Evaluar entrada
                            # Ruptura Alcista
                            if isBullBreak[idx]:
                                prob = longProbs[idx]
                                if prob >= minProbThreshold:
                                    sl = minVals[idx]
                                    closeVal = closes[idx]
                                    atrVal = atrs[idx]
                                    tpDist = max(targetAtrMult * atrVal, minRr * (closeVal - sl))
                                    activeTrade = {
                                        'direction': 'LARGO',
                                        'entry': closeVal + (spreadPrice / 2.0),
                                        'sl': sl,
                                        'tp': closeVal + tpDist
                                    }
                            # Ruptura Bajista
                            elif isBearBreak[idx]:
                                prob = shortProbs[idx]
                                if prob >= minProbThreshold:
                                    sl = maxVals[idx]
                                    closeVal = closes[idx]
                                    atrVal = atrs[idx]
                                    tpDist = max(targetAtrMult * atrVal, minRr * (sl - closeVal))
                                    activeTrade = {
                                        'direction': 'CORTO',
                                        'entry': closeVal - (spreadPrice / 2.0),
                                        'sl': sl,
                                        'tp': closeVal - tpDist
                                    }
                                    
                            idx += 1
                            
                        # Métricas
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
                                'Channel Len': channelLen,
                                'ATR Mult': targetAtrMult,
                                'Min Prob': minProbThreshold,
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
            print(f"  🏆 Mejor combo para {symbol}: Channel={symbolBestCombo['Channel Len']} | ATR Mult={symbolBestCombo['ATR Mult']} | Min Prob={symbolBestCombo['Min Prob']}% | RR={symbolBestCombo['Min RR']} | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}.")

    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/breakoutprobability_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/breakoutprobability_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runBreakoutProbabilityGridSearch()
