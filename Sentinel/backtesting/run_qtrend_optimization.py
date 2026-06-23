import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import asyncio
import json
from datetime import datetime
import pytz

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection, dbManager
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR
from Sentinel.analysis import technical

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def getActiveSymbols():
    try:
        connection = dbConnection.getConnection()
        if connection is None:
            return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']
        cursor = connection.cursor()
        cursor.execute("SELECT symbol FROM SentinelSymbol WHERE Activo = 1")
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        symbolsList = [row[0] for row in rows]
        if not symbolsList:
            return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']
        return symbolsList
    except Exception as e:
        print(f"Error cargando símbolos activos: {e}")
        return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']

ALL_SYMBOLS = getActiveSymbols()

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
        logger.error(f"Error cargando velas para {symbol}: {e}")
        return pd.DataFrame()

def runBacktestForCombo(df15m: pd.DataFrame, symbol: str, superPeriod: int, superMult: float, fastPeriod: int, slowPeriod: int, tpParam: float) -> dict:
    # 1. Calcular SuperTrend
    stTrend, stTrail = technical.calculateAtrStop(df15m, superPeriod, superMult)
    
    # 2. Calcular EMAs para QTrend
    closePrices = df15m['close'].values.astype(float)
    emaFast = ta.EMA(closePrices, timeperiod=fastPeriod)
    emaSlow = ta.EMA(closePrices, timeperiod=slowPeriod)
    
    if len(df15m) < max(superPeriod, slowPeriod) + 10:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    trades = []
    activeTrade = None
    pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
    spread = SPREADS.get(symbol, 1.0) / pipMult
    symbolType = 'CRYPTO' if symbol == 'BTC/USD' else 'METAL' if symbol == 'XAU/USD' else 'MONEDA'

    # Recorrer velas
    for i in range(max(superPeriod, slowPeriod) + 5, len(df15m)):
        currentPrice = float(df15m['close'].iloc[i])
        currentTime = df15m.index[i]
        
        # 1. Gestionar trade activo si existe
        if activeTrade:
            velaHigh = float(df15m['high'].iloc[i])
            velaLow = float(df15m['low'].iloc[i])
            direction = activeTrade['direction']
            sl = activeTrade['sl']
            tp = activeTrade['tp']
            
            closed = False
            exitPrice = 0.0
            pnlPips = 0.0
            
            if direction == "LARGO":
                if velaLow <= sl:
                    closed = True
                    exitPrice = sl
                    pnlPips = (sl - activeTrade['entry']) * pipMult
                elif velaHigh >= tp:
                    closed = True
                    exitPrice = tp
                    pnlPips = (tp - activeTrade['entry']) * pipMult
            else: # CORTO
                if velaHigh <= sl if sl < activeTrade['entry'] else velaHigh >= sl:
                    # En corto, SL está por encima
                    if velaHigh >= sl:
                        closed = True
                        exitPrice = sl
                        pnlPips = (activeTrade['entry'] - sl) * pipMult
                if velaLow <= tp:
                    closed = True
                    exitPrice = tp
                    pnlPips = (activeTrade['entry'] - tp) * pipMult
            
            if closed:
                # Restar spread como costo transaccional
                pnlPips -= SPREADS.get(symbol, 1.0)
                trades.append({
                    "direction": direction,
                    "entryTime": activeTrade['entryTime'],
                    "exitTime": currentTime,
                    "pnl": pnlPips,
                    "result": "WIN" if pnlPips > 0 else "LOSS"
                })
                activeTrade = None
            continue
            
        # 2. Buscar nuevas señales
        stBullish = stTrend[i] == 1
        stBearish = stTrend[i] == -1
        qBullish = emaFast[i] > emaSlow[i]
        qBearish = emaFast[i] < emaSlow[i]
        
        stBullishCross = stTrend[i-1] == -1 and stTrend[i] == 1
        stBearishCross = stTrend[i-1] == 1 and stTrend[i] == -1
        
        qBullishCross = emaFast[i-1] <= emaSlow[i-1] and emaFast[i] > emaSlow[i]
        qBearishCross = emaFast[i-1] >= emaSlow[i-1] and emaFast[i] < emaSlow[i]
        
        # Buscar crossovers recientes (máximo 3 velas)
        stBullishAge = -1
        stBearishAge = -1
        qBullishAge = -1
        qBearishAge = -1
        
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if stTrend[idx-1] == -1 and stTrend[idx] == 1:
                    stBullishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if stTrend[idx-1] == 1 and stTrend[idx] == -1:
                    stBearishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if emaFast[idx-1] <= emaSlow[idx-1] and emaFast[idx] > emaSlow[idx]:
                    qBullishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if emaFast[idx-1] >= emaSlow[idx-1] and emaFast[idx] < emaSlow[idx]:
                    qBearishAge = age
                    break
                    
        direction = None
        if stBullish and qBullish and 0 <= stBullishAge <= 3 and 0 <= qBullishAge <= 3:
            direction = "LARGO"
        elif stBearish and qBearish and 0 <= stBearishAge <= 3 and 0 <= qBearishAge <= 3:
            direction = "CORTO"
            
        if direction:
            # Calcular niveles
            slPrice = float(stTrail[i])
            slDist = abs(currentPrice - slPrice)
            if slDist <= 0:
                continue
                
            # Filtro de Stop Loss sobre-extendido (máximo 2.5 * ATR de 14)
            atrSeries = pd.Series(ta.ATR(df15m['high'].values.astype(float), df15m['low'].values.astype(float), df15m['close'].values.astype(float), 14))
            atrVal = atrSeries.iloc[i] if not np.isnan(atrSeries.iloc[i]) else 0.0
            if atrVal > 0 and slDist > 2.5 * atrVal:
                continue
                
            if symbolType == 'MONEDA':
                # TP basado en minRR
                if direction == "LARGO":
                    calculatedTp = currentPrice + (tpParam * slDist)
                else:
                    calculatedTp = currentPrice - (tpParam * slDist)
            else:
                # TP porcentual para Oro y Cripto
                tpPctDecimal = tpParam / 100.0
                if direction == "LARGO":
                    calculatedTp = currentPrice * (1.0 + tpPctDecimal)
                else:
                    calculatedTp = currentPrice * (1.0 - tpPctDecimal)
                    
            tpPrice = adjustTPForMinRR(currentPrice, slPrice, calculatedTp, direction, minRR=1.5 if symbolType != 'MONEDA' else tpParam)
            
            activeTrade = {
                "direction": direction,
                "entry": currentPrice,
                "sl": slPrice,
                "tp": tpPrice,
                "entryTime": currentTime
            }

    # Resumen de métricas
    if not trades:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    
    totalProfit = sum(t['pnl'] for t in wins)
    totalLoss = abs(sum(t['pnl'] for t in losses))
    
    winRate = (len(wins) / len(trades)) * 100.0
    profitFactor = totalProfit / totalLoss if totalLoss > 0 else 999.0 if totalProfit > 0 else 0.0
    pnlTotal = sum(t['pnl'] for t in trades)
    
    return {
        "trades": trades,
        "winRate": round(winRate, 2),
        "profitFactor": round(profitFactor, 2),
        "pnl": round(pnlTotal, 2)
    }

async def runQTrendGridSearch():
    logger.info("==========================================================")
    logger.info("  INICIANDO GRID SEARCH OPTIMIZER (QTREND - 15MIN DATA)  ")
    logger.info("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros
    supertrendPeriodCombos = [8, 10, 12, 14, 16]
    supertrendMultiplierCombos = [1.5, 2.0, 2.5, 3.0, 3.5]
    qtrendFastCombos = [7, 9, 11, 12, 14]
    qtrendSlowCombos = [18, 21, 24, 26, 30]
    
    # Parámetro de Take Profit (tpParam): representa minRR para Forex, y tpPercent (porcentual, ej. 2.0%) para Cripto/Oro
    tpParamCombosMoneda = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    tpParamCombosExotic = [1.0, 1.5, 2.0, 2.5, 3.0]
    
    allResultsRaw = []
    bestResults = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos de 5m insuficientes para {symbol}. Saltando.")
            continue
            
        # Resamplear a 15min (marco preferido de QTrend)
        df15m = df5m.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 100:
            logger.warning(f"  ⚠️ Datos resampleados a 15m insuficientes para {symbol}. Saltando.")
            continue
            
        isMoneda = symbol not in ['XAU/USD', 'BTC/USD']
        tpCombos = tpParamCombosMoneda if isMoneda else tpParamCombosExotic
        
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        # Bucle de combinaciones
        for sPeriod in supertrendPeriodCombos:
            for sMult in supertrendMultiplierCombos:
                for qFast in qtrendFastCombos:
                    for qSlow in qtrendSlowCombos:
                        for tpParam in tpCombos:
                            metrics = runBacktestForCombo(df15m, symbol, sPeriod, sMult, qFast, qSlow, tpParam)
                            nTrades = len(metrics['trades'])
                            
                            resRow = {
                                "symbol": symbol,
                                "supertrendPeriod": sPeriod,
                                "supertrendMultiplier": sMult,
                                "qtrendFast": qFast,
                                "qtrendSlow": qSlow,
                                "tpParam": tpParam,
                                "nTrades": nTrades,
                                "winRate": metrics['winRate'],
                                "profitFactor": metrics['profitFactor'],
                                "pnl": metrics['pnl']
                            }
                            allResultsRaw.append(resRow)
                            
                            # Criterio de mejor combo por activo: viabilidad (PF >= 1.25, WR >= 42%, con al menos 1 trade)
                            if nTrades > 0 and metrics['profitFactor'] >= 1.00 and metrics['winRate'] >= 35.0:
                                if metrics['profitFactor'] > bestPf or (metrics['profitFactor'] == bestPf and metrics['winRate'] > bestWr):
                                    bestPf = metrics['profitFactor']
                                    bestWr = metrics['winRate']
                                    bestCombo = resRow
        
        if bestCombo:
            bestResults.append(bestCombo)
            logger.info(f"  🏆 Mejor combo viable para {symbol}: ST_P={bestCombo['supertrendPeriod']}, ST_M={bestCombo['supertrendMultiplier']}, QF={bestCombo['qtrendFast']}, QS={bestCombo['qtrendSlow']}, TP={bestCombo['tpParam']} | PF={bestCombo['profitFactor']}, WR={bestCombo['winRate']}%, PnL={bestCombo['pnl']:.2f}")
        else:
            logger.info(f"  ❌ Ningún combo cumplió con el umbral de viabilidad para {symbol}.")

    # Guardar resultados detallados en CSV
    dfAll = pd.DataFrame(allResultsRaw)
    os.makedirs("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting", exist_ok=True)
    dfAll.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/qtrend_grid_results_all.csv", index=False)
    
    dfBest = pd.DataFrame(bestResults)
    dfBest.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/qtrend_grid_results_best.csv", index=False)
    
    logger.info("\n==========================================================")
    logger.info("  GRID SEARCH OPTIMIZATION COMPLETADO PARA QTREND")
    logger.info(f"  Resultados totales guardados en Sentinel/backtesting/qtrend_grid_results_all.csv")
    logger.info(f"  Combinaciones ganadoras guardadas en Sentinel/backtesting/qtrend_grid_results_best.csv")
    logger.info("==========================================================")

if __name__ == "__main__":
    asyncio.run(runQTrendGridSearch())
