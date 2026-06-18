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

def runFVGDiarioGridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (FVGDIARIO)        ")
    print("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00' # Extra para inicialización
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros
    minRrCombos = [1.5, 2.0, 2.5, 3.0]
    minFvgPipsCombos = [3.0, 5.0, 8.0]
    minConfidenceCombos = [65.0, 70.0, 75.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        # Resamplear a 15min y 1D
        df15m = df5m.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        df1d = df5m.resample('1D').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 200 or len(df1d) < 3:
            print(f"  ⚠️ Datos insuficientes en 15m/1d para {symbol}. Saltando.")
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        # Precalcular Daily Bias y PDH/PDL por día
        dailyBiasMap = {}
        pdhMap = {}
        pdlMap = {}
        
        for i in range(1, len(df1d)):
            prevDayDate = df1d.index[i-1].date()
            curDayDate = df1d.index[i].date()
            
            prevDayRow = df1d.iloc[i-1]
            closeVal = prevDayRow['close']
            openVal = prevDayRow['open']
            highVal = prevDayRow['high']
            lowVal = prevDayRow['low']
            
            body = closeVal - openVal
            totalRange = highVal - lowVal
            
            bias = "NEUTRAL"
            if totalRange > 0:
                if (body / totalRange) >= 0.5 and body > 0:
                    bias = "LARGO"
                elif (body / totalRange) >= 0.5 and body < 0:
                    bias = "CORTO"
                    
            dailyBiasMap[curDayDate] = bias
            pdhMap[curDayDate] = float(highVal)
            pdlMap[curDayDate] = float(lowVal)
            
        # Precalcular ATR en 15m
        df15m = df15m.copy()
        df15m["atr"] = ta.ATR(df15m['high'].values, df15m['low'].values, df15m['close'].values, timeperiod=14)
        df15m.dropna(subset=["atr"], inplace=True)
        
        opens = df15m['open'].values
        highs = df15m['high'].values
        lows = df15m['low'].values
        closes = df15m['close'].values
        atrs = df15m['atr'].values
        times = df15m.index
        n = len(df15m)
        
        # Precalcular swing highs y lows de 20 velas
        sHighs = df15m['high'].rolling(20).max().values
        sLows = df15m['low'].rolling(20).min().values
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for minRr in minRrCombos:
            for minFvgPips in minFvgPipsCombos:
                for minConfidence in minConfidenceCombos:
                    trades = []
                    activeTrade = None
                    
                    # Simulación del backtest
                    # Empezamos en un índice seguro para tener histórico
                    idx = 30
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
                            
                        # Buscar parámetros del día anterior correspondiente a esta vela
                        candleDate = times[idx].date()
                        dailyBias = dailyBiasMap.get(candleDate, "NEUTRAL")
                        pdh = pdhMap.get(candleDate, None)
                        pdl = pdlMap.get(candleDate, None)
                        
                        if dailyBias == "NEUTRAL" or pdh is None or pdl is None:
                            idx += 1
                            continue
                            
                        # Detectar sweep de liquidez en las últimas 10 velas de 15m
                        # (technical.detectLiquiditySweep)
                        # Para emularlo de forma rápida:
                        # Si es LARGO, queremos ver si el low cruzó pdl en las últimas 10 velas
                        # Si es CORTO, queremos ver si el high cruzó pdh en las últimas 10 velas
                        windowHighs = highs[idx-10:idx]
                        windowLows = lows[idx-10:idx]
                        
                        manipulation = None
                        if dailyBias == "LARGO" and np.min(windowLows) < pdl:
                            manipulation = {"type": "MANIPULATION_DOWN", "level": pdl, "idx": idx - 1}
                        elif dailyBias == "CORTO" and np.max(windowHighs) > pdh:
                            manipulation = {"type": "MANIPULATION_UP", "level": pdh, "idx": idx - 1}
                            
                        if not manipulation:
                            idx += 1
                            continue
                            
                        # Verificar MSS (Market Structure Shift) posterior al sweep
                        # LARGO: ver si el high de las últimas velas superó el nivel
                        # CORTO: ver si el low de las últimas velas cayó del nivel
                        mss = False
                        if dailyBias == "LARGO":
                            for k in range(idx - 5, idx + 1):
                                if k < n and highs[k] > pdl:
                                    mss = True
                                    break
                        elif dailyBias == "CORTO":
                            for k in range(idx - 5, idx + 1):
                                if k < n and lows[k] < pdh:
                                    mss = True
                                    break
                                    
                        if not mss:
                            idx += 1
                            continue
                            
                        # Buscar FVG después de la manipulación
                        # Para simplificar y acelerar, detectamos un FVG local (velas i, i-1, i-2)
                        # BISI (Bullish): Low[i] > High[i-2]
                        # SIBI (Bearish): High[i] < Low[i-2]
                        isBullishFvg = (lows[idx] > highs[idx-2]) and (closes[idx-1] > opens[idx-1])
                        isBearishFvg = (highs[idx] < lows[idx-2]) and (closes[idx-1] < opens[idx-1])
                        
                        fvg = None
                        if isBullishFvg and dailyBias == "LARGO":
                            fvgSize = lows[idx] - highs[idx-2]
                            fvg = {"type": "Bullish_FVG", "size": fvgSize, "mid": (highs[idx-2] + lows[idx]) / 2.0}
                        elif isBearishFvg and dailyBias == "CORTO":
                            fvgSize = lows[idx-2] - highs[idx]
                            fvg = {"type": "Bearish_FVG", "size": fvgSize, "mid": (lows[idx-2] + highs[idx]) / 2.0}
                            
                        if not fvg:
                            idx += 1
                            continue
                            
                        fvgSizePips = fvg['size'] * pipMult
                        if fvgSizePips < minFvgPips:
                            idx += 1
                            continue
                            
                        # Confianza
                        baseConf = 85 if dailyBias != "NEUTRAL" else 75
                        if baseConf < minConfidence:
                            idx += 1
                            continue
                            
                        # Entrar al trade
                        direction = "LARGO" if dailyBias == "LARGO" else "CORTO"
                        entry = closes[idx]
                        
                        atrVal = atrs[idx]
                        swingHigh = sHighs[idx]
                        swingLow = sLows[idx]
                        
                        if direction == "LARGO":
                            sl = swingLow - atrVal * 0.2
                        else:
                            sl = swingHigh + atrVal * 0.2
                            
                        slDist = abs(entry - sl)
                        if slDist > 0:
                            tpDist = slDist * minRr
                            tp = entry + tpDist if direction == "LARGO" else entry - tpDist
                            
                            activeTrade = {
                                'direction': direction,
                                'entry': entry + (spreadPrice / 2.0) if direction == "LARGO" else entry - (spreadPrice / 2.0),
                                'sl': sl,
                                'tp': tp
                            }
                            
                        idx += 1
                        
                    # Evaluar métricas
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
                            'Min FVG Pips': minFvgPips,
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
            print(f"  🏆 Mejor combo para {symbol}: RR={symbolBestCombo['Min RR']} | Min FVG Pips={symbolBestCombo['Min FVG Pips']} | Conf={symbolBestCombo['Min Conf']}% | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}.")

    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/fvgdiario_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/fvgdiario_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runFVGDiarioGridSearch()
