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

def runSpeedBotGridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (SPEEDBOT)         ")
    print("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros de Desplazamiento
    atrMults = [1.0, 1.2, 1.4, 1.6, 1.8, 2.0]
    bodyRatios = [0.65, 0.70, 0.75, 0.80, 0.85]
    confirmRatios = [0.30, 0.40, 0.50, 0.60, 0.70]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 200:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        # Convertir a arrays de numpy para velocidad extrema
        openPrices = df5m['open'].values.astype(float)
        highPrices = df5m['high'].values.astype(float)
        lowPrices = df5m['low'].values.astype(float)
        closePrices = df5m['close'].values.astype(float)
        totalBars = len(df5m)
        
        # Calcular ATR 14
        atr14 = ta.ATR(highPrices, lowPrices, closePrices, timeperiod=14)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for atrMult in atrMults:
            for bodyRatio in bodyRatios:
                for confirmRatio in confirmRatios:
                    for minRr in minRrCombos:
                        
                        trades = []
                        i = 50
                        while i < totalBars:
                            atrVal = atr14[i]
                            if np.isnan(atrVal):
                                i += 1
                                continue
                                
                            bodyLast = abs(closePrices[i] - openPrices[i])
                            bodyPrev = abs(closePrices[i - 1] - openPrices[i - 1])
                            rangePrev = highPrices[i - 1] - lowPrices[i - 1]
                            
                            isExplosive = bodyPrev > (atrVal * atrMult)
                            isSolid = (bodyPrev / rangePrev) > bodyRatio if rangePrev > 0 else False
                            
                            sameDir = (closePrices[i] > openPrices[i]) == (closePrices[i - 1] > openPrices[i - 1])
                            isConfirmed = sameDir and (bodyLast >= bodyPrev * confirmRatio)
                            
                            if not (isExplosive and isSolid and isConfirmed):
                                i += 1
                                continue
                                
                            direction = "LARGO" if closePrices[i] > openPrices[i] else "CORTO"
                            
                            # Simular SL y TP
                            entryPrice = closePrices[i] + (spreadPrice / 2.0) if direction == "LARGO" else closePrices[i] - (spreadPrice / 2.0)
                            
                            if direction == "LARGO":
                                sl = min(lowPrices[i], lowPrices[i - 1]) - (atrVal * 0.1)
                            else:
                                sl = max(highPrices[i], highPrices[i - 1]) + (atrVal * 0.1)
                                
                            riskDist = abs(entryPrice - sl)
                            if riskDist <= 0.0:
                                i += 1
                                continue
                                
                            tp = entryPrice + (riskDist * minRr) if direction == "LARGO" else entryPrice - (riskDist * minRr)
                            
                            # Simular holding period hasta que toque SL o TP
                            tradeClosed = False
                            pnlUsd = 0.0
                            for j in range(i + 1, totalBars):
                                vHigh = highPrices[j]
                                vLow = lowPrices[j]
                                
                                if direction == "LARGO":
                                    lowAdjusted = vLow - (spreadPrice / 2.0)
                                    highAdjusted = vHigh + (spreadPrice / 2.0)
                                    if lowAdjusted <= sl:
                                        pnlUsd = -100.0
                                        tradeClosed = True
                                    elif highAdjusted >= tp:
                                        pnlUsd = 100.0 * minRr
                                        tradeClosed = True
                                else:
                                    highAdjusted = vHigh + (spreadPrice / 2.0)
                                    lowAdjusted = vLow - (spreadPrice / 2.0)
                                    if highAdjusted >= sl:
                                        pnlUsd = -100.0
                                        tradeClosed = True
                                    elif lowAdjusted <= tp:
                                        pnlUsd = 100.0 * minRr
                                        tradeClosed = True
                                        
                                if tradeClosed:
                                    trades.append(pnlUsd)
                                    i = j
                                    break
                            if not tradeClosed:
                                # Llegó al fin del dataset sin tocar SL o TP
                                break
                            i += 1
                            
                        # Métricas del combo
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
                                'ATR Mult': atrMult,
                                'Body Ratio': bodyRatio,
                                'Confirm Ratio': confirmRatio,
                                'Min RR': minRr,
                                'Trades': tCount,
                                'Win Rate': f"{wRate:.1f}%",
                                'Profit Factor': round(profFactor, 2),
                                'PnL USD': pnlNet
                            }
                            allResultsRaw.append(comboData)
                            
                            # Criterio del mejor combo
                            if pnlNet > symbolBestProfit and profFactor >= 1.0:
                                symbolBestProfit = pnlNet
                                symbolBestCombo = comboData
                                
        if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            print(f"  🏆 Mejor combo para {symbol}: ATR Mult={symbolBestCombo['ATR Mult']} | Body={symbolBestCombo['Body Ratio']} | Confirm={symbolBestCombo['Confirm Ratio']} | RR={symbolBestCombo['Min RR']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}.")
            
    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/speedbot_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/speedbot_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runSpeedBotGridSearch()
