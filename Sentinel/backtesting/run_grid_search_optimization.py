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

EXCLUDED_SYMBOLS = [
    'USD/MXN', 'AUD/USD', 'USD/JPY', 'XAU/USD',
    'EUR/USD', 'GBP/CAD', 'EUR/GBP', 'GBP/JPY', 'USD/CHF'
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

def calculateSmoothedHeikinAshi(df: pd.DataFrame, period1: int = 10, period2: int = 10) -> tuple:
    openSmooth = df['open'].ewm(span=period1, adjust=False).mean()
    highSmooth = df['high'].ewm(span=period1, adjust=False).mean()
    lowSmooth = df['low'].ewm(span=period1, adjust=False).mean()
    closeSmooth = df['close'].ewm(span=period1, adjust=False).mean()
    
    haClose = (openSmooth + highSmooth + lowSmooth + closeSmooth) / 4.0
    haOpen = np.zeros(len(df))
    haOpen[0] = (openSmooth.iloc[0] + closeSmooth.iloc[0]) / 2.0
    for i in range(1, len(df)):
        haOpen[i] = (haOpen[i - 1] + haClose.iloc[i - 1]) / 2.0
    haOpenSeries = pd.Series(haOpen, index=df.index)
    
    haCloseSmooth = haClose.ewm(span=period2, adjust=False).mean()
    haOpenSmooth = haOpenSeries.ewm(span=period2, adjust=False).mean()
    return haCloseSmooth, haOpenSmooth

def runGridSearch() -> None:
    print("==========================================================")
    print("        INICIANDO GRID SEARCH OPTIMIZER (EXCLUIDOS)       ")
    print("==========================================================")
    
    startDateStr = '2026-05-21 00:00:00'
    endDateStr = '2026-06-11 14:00:00'
    
    # Grid de Parámetros
    supertrendMults = [1.5, 2.0, 2.5, 3.0]
    haPeriods = [(10, 10), (12, 12)]
    mssLookbacks = [8, 12, 15]
    macdParams = [(20, 9), (34, 9)]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in EXCLUDED_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 150:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        df15m.ffill(inplace=True)
        df15m.bfill(inplace=True)
        
        if len(df15m) < 210:
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        closePrices = df15m['close'].values.astype(float)
        highPrices = df15m['high'].values.astype(float)
        lowPrices = df15m['low'].values.astype(float)
        
        ema200 = ta.EMA(closePrices, timeperiod=200)
        atr14 = ta.ATR(highPrices, lowPrices, closePrices, timeperiod=14)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        # Iterar sobre la cuadrícula
        for mult in supertrendMults:
            stTrend, stTrail = technical.calculateAtrStop(df15m, period=10, multiplier=mult)
            
            for p1, p2 in haPeriods:
                haCloseSmooth, haOpenSmooth = calculateSmoothedHeikinAshi(df15m, p1, p2)
                
                for mss in mssLookbacks:
                    for lengthMa, lengthSignal in macdParams:
                        impulseMacd, impulseSignal = technical.calculateImpulseMacd(df15m, lengthMa=lengthMa, lengthSignal=lengthSignal)
                        
                        trades = []
                        i = 200
                        while i < len(df15m):
                            cPrice = float(df15m['close'].iloc[i])
                            cEma200 = ema200[i]
                            if pd.isna(cEma200) or pd.isna(atr14[i]):
                                i += 1
                                continue
                                
                            macroBullish = cPrice > cEma200
                            macroBearish = cPrice < cEma200
                            
                            stBullish = stTrend[i] == 1
                            stBearish = stTrend[i] == -1
                            
                            haBullish = haCloseSmooth.iloc[i] > haOpenSmooth.iloc[i]
                            haBearish = haCloseSmooth.iloc[i] < haOpenSmooth.iloc[i]
                            
                            macdBullish = impulseMacd.iloc[i] > impulseSignal.iloc[i]
                            macdBearish = impulseMacd.iloc[i] < impulseSignal.iloc[i]
                            
                            maxHighPrev = float(df15m['high'].iloc[i-mss:i].max())
                            minLowPrev = float(df15m['low'].iloc[i-mss:i].min())
                            
                            mssBullish = cPrice > maxHighPrev
                            mssBearish = cPrice < minLowPrev
                            
                            direction = None
                            if macroBullish and stBullish and haBullish and macdBullish and mssBullish:
                                direction = 'LARGO'
                            elif macroBearish and stBearish and haBearish and macdBearish and mssBearish:
                                direction = 'CORTO'
                                
                            if direction:
                                slPrice = float(stTrail[i])
                                slDist = abs(cPrice - slPrice)
                                atrVal = float(atr14[i])
                                
                                triggerRange = float(df15m['high'].iloc[i] - df15m['low'].iloc[i])
                                if triggerRange > 2.0 * atrVal or slDist > 2.0 * atrVal or slDist <= 0.0:
                                    i += 1
                                    continue
                                    
                                entryPrice = cPrice + (spreadPrice / 2.0) if direction == 'LARGO' else cPrice - (spreadPrice / 2.0)
                                takeProfit = entryPrice + (1.5 * slDist) if direction == 'LARGO' else entryPrice - (1.5 * slDist)
                                
                                tradeClosed = False
                                pipsResult = 0.0
                                for j in range(i + 1, len(df15m)):
                                    vHigh = float(df15m['high'].iloc[j])
                                    vLow = float(df15m['low'].iloc[j])
                                    
                                    if direction == 'LARGO':
                                        lowAdjusted = vLow - (spreadPrice / 2.0)
                                        highAdjusted = vHigh + (spreadPrice / 2.0)
                                        if lowAdjusted <= slPrice:
                                            pipsResult = -abs(entryPrice - slPrice) * pipMult
                                            tradeClosed = True
                                        elif highAdjusted >= takeProfit:
                                            pipsResult = abs(takeProfit - entryPrice) * pipMult
                                            tradeClosed = True
                                    else:
                                        highAdjusted = vHigh + (spreadPrice / 2.0)
                                        lowAdjusted = vLow - (spreadPrice / 2.0)
                                        if highAdjusted >= slPrice:
                                            pipsResult = -abs(slPrice - entryPrice) * pipMult
                                            tradeClosed = True
                                        elif lowAdjusted <= takeProfit:
                                            pipsResult = abs(entryPrice - takeProfit) * pipMult
                                            tradeClosed = True
                                            
                                    if tradeClosed:
                                        pnlUsd = 150.0 if pipsResult > 0 else -100.0
                                        trades.append(pnlUsd)
                                        i = j
                                        break
                            i += 1
                            
                        # Metricas del combo
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
                                'Supertrend Mult': mult,
                                'HA Period': p1,
                                'MSS Lookback': mss,
                                'MACD Params': f"{lengthMa},{lengthSignal}",
                                'Trades': tCount,
                                'Win Rate': f"{wRate:.1f}%",
                                'Profit Factor': round(profFactor, 2),
                                'PnL USD': pnlNet
                            }
                            allResultsRaw.append(comboData)
                            
                            # Criterio del mejor combo por Profit Factor y PnL
                            if pnlNet > symbolBestProfit and profFactor >= 1.0:
                                symbolBestProfit = pnlNet
                                symbolBestCombo = comboData
                                
        if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            print(f"  🏆 Mejor combo para {symbol}: Mult={symbolBestCombo['Supertrend Mult']} | HA={symbolBestCombo['HA Period']} | MSS={symbolBestCombo['MSS Lookback']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}.")

    # Guardar reporte de combinaciones
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/optimization_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/optimization_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runGridSearch()
