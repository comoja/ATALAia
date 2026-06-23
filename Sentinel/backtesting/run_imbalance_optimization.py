import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
from datetime import datetime, timedelta
import pytz

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from Sentinel.analysis import technical
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR

# Silenciar logs
logging.getLogger('sentinel').setLevel(logging.ERROR)

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

ASSET_SL_MULTIPLIERS = {
    'XAU/USD': 1.5,
    'BTC/USD': 1.5,
    'divisas': 1.5,
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
        print(f"Error cargando velas para {symbol}: {e}")
        return pd.DataFrame()

def runImbalanceGridSearch(strategyName: str, sessionTzName: str, refStartH: int, refEndH: int, tradeStartH: int, tradeEndH: int) -> list:
    print(f"\n==========================================================")
    print(f"    INICIANDO GRID SEARCH OPTIMIZER ({strategyName.upper()})    ")
    print(f"==========================================================")
    
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    maxMinutosFvgCombos = [10, 15, 20, 25, 30, 45, 60]
    minConfidenceCombos = [60.0, 65.0, 70.0, 75.0, 80.0, 85.0]
    
    bestResults = []
    allResultsRaw = []
    
    sessionTz = pytz.timezone(sessionTzName)
    localTz = pytz.timezone('America/Mexico_City')
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df = loadCandles(symbol, startDateStr, endDateStr)
        if df.empty or len(df) < 500:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        slMultiplier = ASSET_SL_MULTIPLIERS.get(symbol, ASSET_SL_MULTIPLIERS['divisas'])
        
        # Precalcular ATR de 14 periodos en 5min
        df = df.copy()
        df["atr"] = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
        df.dropna(subset=["atr"], inplace=True)
        
        # Localizar el índice a México y convertir a la zona de la sesión
        try:
            df.index = df.index.tz_localize('America/Mexico_City', ambiguous='infer', nonexistent='shift_forward')
        except Exception:
            # Si ya tiene zona horaria
            pass
            
        dfSession = df.tz_convert(sessionTz)
        
        # Agrupar por el día de la sesión
        groups = dfSession.groupby(dfSession.index.date)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for minRr in minRrCombos:

            for maxMinutosFvg in maxMinutosFvgCombos:
                for minConfidence in minConfidenceCombos:
                    trades = []
                    
                    # Simulación día por día
                    for dateKey, dfDay in groups:
                        if len(dfDay) < 10:
                            continue
                            
                        # Velas de la sesión de referencia (ej. 8:00 a 9:00)
                        refCandles = dfDay.between_time(f"{refStartH:02d}:00", f"{refEndH:02d}:00")
                        # Velas de la ventana de trading (ej. 9:00 a 14:00)
                        tradeCandles = dfDay.between_time(f"{tradeStartH:02d}:00", f"{tradeEndH:02d}:00")

                        
                        if refCandles.empty or tradeCandles.empty:
                            continue
                            
                        precioMaximo = refCandles['high'].max()
                        precioMinimo = refCandles['low'].min()
                        
                        # Buscar Vela de Ruptura (velaCorte)
                        velaCorte = None
                        idxVelaCorte = None
                        
                        opens = tradeCandles['open'].values
                        highs = tradeCandles['high'].values
                        lows = tradeCandles['low'].values
                        closes = tradeCandles['close'].values
                        times = tradeCandles.index
                        
                        for i in range(len(tradeCandles)):
                            oVal = opens[i]
                            hVal = highs[i]
                            lVal = lows[i]
                            cVal = closes[i]
                            
                            cuerpo = abs(cVal - oVal)
                            rango = hVal - lVal
                            if rango == 0:
                                continue
                                
                            bodyTop = max(oVal, cVal)
                            bodyBottom = min(oVal, cVal)
                            
                            # Ruptura Alcista
                            if bodyTop > precioMaximo and bodyBottom > precioMaximo:
                                if cVal > oVal and (cuerpo / rango) > 0.6 and ((hVal - cVal) / rango) <= 0.25:
                                    velaCorte = {'type': 'LARGO', 'idx': i, 'precioRuptura': bodyTop}
                                    idxVelaCorte = i
                                    break
                            # Ruptura Bajista
                            if bodyBottom < precioMinimo and bodyTop < precioMinimo:
                                if cVal < oVal and (cuerpo / rango) > 0.6 and ((cVal - lVal) / rango) <= 0.25:
                                    velaCorte = {'type': 'CORTO', 'idx': i, 'precioRuptura': bodyBottom}
                                    idxVelaCorte = i
                                    break
                                    
                        if not velaCorte:
                            continue
                            
                        # Buscar FVG posterior en la ventana de trading
                        direction = velaCorte['type']
                        fvgFound = None
                        idxFvg = None
                        
                        for i in range(idxVelaCorte + 1, len(tradeCandles)):
                            # BISI (Bullish): Low[i] > High[i-2]
                            # SIBI (Bearish): High[i] < Low[i-2]
                            if i < 2:
                                continue
                                
                            isBullish = (lows[i] > highs[i-2]) and (closes[i-1] > opens[i-1])
                            isBearish = (highs[i] < lows[i-2]) and (closes[i-1] < opens[i-1])
                            
                            if isBullish and direction == 'LARGO':
                                fvgFound = {
                                    'type': 'Bullish_FVG',
                                    'mid': (highs[i-2] + lows[i]) / 2.0,
                                    'top': lows[i],
                                    'bottom': highs[i-2],
                                    'idx': i
                                }
                                idxFvg = i
                                break
                            elif isBearish and direction == 'CORTO':
                                fvgFound = {
                                    'type': 'Bearish_FVG',
                                    'mid': (lows[i-2] + highs[i]) / 2.0,
                                    'top': lows[i-2],
                                    'bottom': highs[i],
                                    'idx': i
                                }
                                idxFvg = i
                                break
                                
                        if not fvgFound:
                            continue
                            
                        # Simular Orden Límite y Trade
                        entryPrice = fvgFound['mid']
                        atrVal = tradeCandles['atr'].values[idxFvg]
                        paddingPips = atrVal * slMultiplier
                        
                        # Swing levels locales
                        startPrior = max(0, idxFvg - 29)
                        swingHigh = np.max(highs[startPrior:idxFvg+1])
                        swingLow = np.min(lows[startPrior:idxFvg+1])
                        
                        if direction == 'CORTO':
                            zonaHighFvg = np.max(highs[idxFvg:min(len(tradeCandles), idxFvg+2)])
                            stopRef = max(zonaHighFvg, swingHigh)
                            stopLoss = stopRef + paddingPips
                            
                            # TP estructural
                            tpStructural = swingLow
                            # Cap TP
                            minTp = entryPrice - atrVal * 3.0
                            tpStructural = max(tpStructural, minTp)
                            tpFinal = adjustTPForMinRR(entryPrice, stopLoss, tpStructural, "CORTO", minRR=minRr)
                        else:
                            zonaLowFvg = np.min(lows[idxFvg:min(len(tradeCandles), idxFvg+2)])
                            stopRef = min(zonaLowFvg, swingLow)
                            stopLoss = stopRef - paddingPips
                            
                            # TP estructural
                            tpStructural = swingHigh
                            # Cap TP
                            maxTp = entryPrice + atrVal * 3.0
                            tpStructural = min(tpStructural, maxTp)
                            tpFinal = adjustTPForMinRR(entryPrice, stopLoss, tpStructural, "LARGO", minRR=minRr)
                            
                        # Simular retest y evaluación del trade
                        tradeActive = False
                        tradeEntryTime = None
                        
                        for k in range(idxFvg + 1, len(tradeCandles)):
                            kHigh = highs[k]
                            kLow = lows[k]
                            kClose = closes[k]
                            kTime = times[k]
                            
                            # Verificar si expira el tiempo para activar la orden límite
                            if not tradeActive:
                                minutosDesdeFvg = (kTime - times[idxFvg]).total_seconds() / 60.0
                                if minutosDesdeFvg > maxMinutosFvg:
                                    break # Orden límite expirada
                                    
                                if direction == 'LARGO' and kLow <= entryPrice:
                                    tradeActive = True
                                    tradeEntryTime = kTime
                                elif direction == 'CORTO' and kHigh >= entryPrice:
                                    tradeActive = True
                                    tradeEntryTime = kTime
                                    
                            if tradeActive:
                                # Evaluar SL/TP
                                if direction == 'LARGO':
                                    lowAdj = kLow - (spreadPrice / 2.0)
                                    highAdj = kHigh + (spreadPrice / 2.0)
                                    if lowAdj <= stopLoss:
                                        trades.append(-100.0)
                                        break
                                    elif highAdj >= tpFinal:
                                        trades.append(100.0 * minRr)
                                        break
                                else:
                                    highAdj = kHigh + (spreadPrice / 2.0)
                                    lowAdj = kLow - (spreadPrice / 2.0)
                                    if highAdj >= stopLoss:
                                        trades.append(-100.0)
                                        break
                                    elif lowAdj <= tpFinal:
                                        trades.append(100.0 * minRr)
                                        break
                                        
                    # Métricas finales
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
                            'Max Minutos Fvg': maxMinutosFvg,
                            'Min Conf': minConfidence,
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
            print(f"  🏆 Mejor combo para {symbol}: RR={symbolBestCombo['Min RR']} | Max Minutos={symbolBestCombo['Max Minutos Fvg']} | Conf={symbolBestCombo['Min Conf']}% | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable y viable para {symbol}.")
            
    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = f"/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/{strategyName.lower()}_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = f"/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/{strategyName.lower()}_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        
    return bestResults

def runAllImbalanceOptimizations():
    # 1. ImbalanceLDN: Europe/London. Rango de ref: 8:00 a 9:00. Trade window: 9:00 a 14:00.
    runImbalanceGridSearch("ImbalanceLDN", "Europe/London", 8, 9, 9, 14)
    
    # 2. ImbalanceNY: America/New_York. Rango de ref: 8:00 a 9:00. Trade window: 9:00 a 14:00.
    runImbalanceGridSearch("ImbalanceNY", "America/New_York", 8, 9, 9, 14)
    
    # 3. ImbalancePMNY: America/New_York. Rango de ref: 14:00 a 15:00. Trade window: 15:00 a 17:00.
    runImbalanceGridSearch("ImbalancePMNY", "America/New_York", 14, 15, 15, 17)

if __name__ == '__main__':
    runAllImbalanceOptimizations()
