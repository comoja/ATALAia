import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
import logging
from datetime import datetime

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from Sentinel.analysis import technical
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR

# Silenciar logs para que no saturen la pantalla
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

def runGenericFVGGridSearch() -> None:
    print("==========================================================")
    print("  INICIANDO GRID SEARCH OPTIMIZER (GENERICFVG - FAST NUMPY) ")
    print("==========================================================")
    
    startDateStr = '2026-05-15 00:00:00'
    endDateStr = '2026-06-11 14:00:00'
    
    # Grid de Parámetros
    minRrCombos = [1.5, 2.0, 2.5]
    minConfidenceCombos = [70.0, 80.0, 90.0]
    requireHtfSweepCombos = [True, False]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        # Resamplear a 15min y 1d (para sweep HTF)
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
        
        # Precalcular PDH y PDL por fecha
        pdhMap = {}
        pdlMap = {}
        for i in range(1, len(df1d)):
            curDayDate = df1d.index[i].date()
            prevDayRow = df1d.iloc[i-1]
            pdhMap[curDayDate] = float(prevDayRow['high'])
            pdlMap[curDayDate] = float(prevDayRow['low'])
            
        # Precalcular ATR en 15m
        df15m = df15m.copy()
        df15m["atr"] = ta.ATR(df15m['high'].values, df15m['low'].values, df15m['close'].values, timeperiod=14)
        df15m.dropna(subset=["atr"], inplace=True)
        
        # Precalcular FVGs usando la lógica centralizada una sola vez sin mitigación futura
        fvgsList = technical.detect_fvgs(df15m, validate_mitigation=False, apply_high_prob_filters=True)
        
        # Crear mapeo rápido de FVG por índice de confirmación (Vela 3)
        fvgMap = {fvg.get('idx'): fvg for fvg in fvgsList if fvg.get('idx') is not None}
        
        opens = df15m['open'].values
        highs = df15m['high'].values
        lows = df15m['low'].values
        closes = df15m['close'].values
        atrs = df15m['atr'].values
        times = df15m.index
        n = len(df15m)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for minRr in minRrCombos:
            for minConfidence in minConfidenceCombos:
                for requireHtfSweep in requireHtfSweepCombos:
                    trades = []
                    activeTrade = None
                    
                    # Simulación
                    idx = 50
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
                            
                        # Verificar si hay un FVG confirmado en esta vela idx
                        latestFvg = fvgMap.get(idx)
                        if not latestFvg:
                            idx += 1
                            continue
                            
                        classification = latestFvg.get('classification', 'Alta Probabilidad')
                        if classification == 'Rechazo/Baja Probabilidad':
                            idx += 1
                            continue
                            
                        fvgDirection = "LARGO" if latestFvg['type'] == 'Bullish_FVG' else "CORTO"
                        
                        # Filtro Sweep HTF (Numpy rápido)
                        # Lookback dinámico según el timeframe: para 15min es 150
                        sweepLookback = 150
                        hasSweep = False
                        sweepType = None
                        startK = max(0, idx - sweepLookback)
                        
                        for k in range(idx, startK - 1, -1):
                            kDate = times[k].date()
                            kPdh = pdhMap.get(kDate)
                            kPdl = pdlMap.get(kDate)
                            if kPdh is None or kPdl is None:
                                continue
                            
                            kHigh = highs[k]
                            kLow = lows[k]
                            kClose = closes[k]
                            
                            if kHigh > kPdh and kClose < kPdh:
                                sweepType = 'MANIPULATION_UP'
                                hasSweep = True
                                break
                            if kLow < kPdl and kClose > kPdl:
                                sweepType = 'MANIPULATION_DOWN'
                                hasSweep = True
                                break
                                
                        skipSignal = False
                        if hasSweep:
                            if sweepType == 'MANIPULATION_UP' and fvgDirection != 'CORTO':
                                skipSignal = True
                            elif sweepType == 'MANIPULATION_DOWN' and fvgDirection != 'LARGO':
                                skipSignal = True
                        else:
                            if requireHtfSweep:
                                skipSignal = True
                                
                        if skipSignal:
                            idx += 1
                            continue
                            
                        # Niveles SMC
                        currentPrice = float(closes[idx])
                        atrVal = atrs[idx]
                        setupFvg = technical.calculate_fvg_setup(latestFvg, currentPrice, atrVal)
                        
                        entryPrice = setupFvg['entry']
                        sl = setupFvg['sl']
                        
                        # TP Estructural
                        fvgIdxPrior = latestFvg.get('idx', idx)
                        startPrior = max(0, fvgIdxPrior - 19)
                        
                        if fvgDirection == "LARGO":
                            tpRef = np.max(np.maximum(opens[startPrior:fvgIdxPrior+1], closes[startPrior:fvgIdxPrior+1]))
                        else:
                            tpRef = np.min(np.minimum(opens[startPrior:fvgIdxPrior+1], closes[startPrior:fvgIdxPrior+1]))
                            
                        # Cap de TP por ATR
                        maxAtrMult = 3.0
                        if fvgDirection == "LARGO":
                            maxTp = currentPrice + atrVal * maxAtrMult
                            tpRef = min(tpRef, maxTp)
                        else:
                            minTp = currentPrice - atrVal * maxAtrMult
                            tpRef = max(tpRef, minTp)
                            
                        tp1 = adjustTPForMinRR(entryPrice, sl, tpRef, fvgDirection, minRR=minRr)
                        
                        riskDist = abs(entryPrice - sl)
                        rewardDist = abs(tp1 - entryPrice)
                        rrRatio = rewardDist / riskDist if riskDist > 0 else 0
                        
                        if rrRatio > 15:
                            idx += 1
                            continue
                            
                        # Health Check Simplificado
                        progressPct = (currentPrice - entryPrice) / (entryPrice - sl) if fvgDirection == 'LARGO' else (entryPrice - currentPrice) / (sl - entryPrice)
                        if progressPct > 3.5:
                            idx += 1
                            continue
                            
                        # Validar invalidación de SL inmediata
                        if fvgDirection == "LARGO" and currentPrice <= sl:
                            idx += 1
                            continue
                        if fvgDirection == "CORTO" and currentPrice >= sl:
                            idx += 1
                            continue
                            
                        # Validar RR Real
                        realRiskDist = abs(currentPrice - sl)
                        rrVal = round(abs(tp1 - currentPrice) / realRiskDist, 2) if realRiskDist > 0 else 0
                        minRealRr = minRr * 0.70
                        if rrVal < minRealRr:
                            idx += 1
                            continue
                            
                        # Confianza
                        baseConfidence = 85
                        if baseConfidence < minConfidence:
                            idx += 1
                            continue
                            
                        # Entrar al trade
                        activeTrade = {
                            'direction': fvgDirection,
                            'entry': currentPrice + (spreadPrice / 2.0) if fvgDirection == "LARGO" else currentPrice - (spreadPrice / 2.0),
                            'sl': sl,
                            'tp': tp1
                        }
                        
                        idx += 1
                        
                    # Métricas finales del combo
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
                            'Min Conf': minConfidence,
                            'Require Sweep': requireHtfSweep,
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
            print(f"  🏆 Mejor combo para {symbol}: RR={symbolBestCombo['Min RR']} | Conf={symbolBestCombo['Min Conf']}% | Sweep={symbolBestCombo['Require Sweep']} | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable y viable para {symbol}.")
            
    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/genericfvg_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/genericfvg_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runGenericFVGGridSearch()
