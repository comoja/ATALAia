import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR
from Sentinel.analysis import technical

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def getActiveSymbols():
    try:
        from middleware.database import dbConnection
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
        print(f"Error fetching active symbols: {e}")
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

def calculateLrc(closePrices: np.ndarray, period: int = 100, dev: float = 2.0):
    n = len(closePrices)
    center = np.full(n, np.nan)
    upper = np.full(n, np.nan)
    lower = np.full(n, np.nan)
    slope = np.full(n, np.nan)
    
    if n < period:
        return center, upper, lower, slope
        
    x = np.arange(period)
    
    for i in range(period - 1, n):
        y = closePrices[i - period + 1 : i + 1]
        m, c = np.polyfit(x, y, 1)
        predVal = m * (period - 1) + c
        center[i] = predVal
        slope[i] = m
        
        yFit = m * x + c
        residuals = y - yFit
        stdDev = np.std(residuals)
        
        upper[i] = predVal + (dev * stdDev)
        lower[i] = predVal - (dev * stdDev)
        
    return center, upper, lower, slope

def checkDivergence(df: pd.DataFrame, rsiSeries: pd.Series, lookback: int = 5) -> dict:
    divergences = {"bullish": False, "bearish": False}
    if len(df) < lookback + 1:
        return divergences

    pricesLow = df['low'].tail(lookback)
    pricesHigh = df['high'].tail(lookback)
    rsiVals = rsiSeries.tail(lookback)

    # 1. Divergencia Alcista (Bullish Divergence)
    if pricesLow.iloc[-1] <= pricesLow.iloc[:-1].min():
        minPriceIdx = pricesLow.iloc[:-1].idxmin()
        if minPriceIdx in rsiVals.index:
            if rsiVals.iloc[-1] > rsiVals.loc[minPriceIdx]:
                divergences["bullish"] = True

    # 2. Divergencia Bajista (Bearish Divergence)
    if pricesHigh.iloc[-1] >= pricesHigh.iloc[:-1].max():
        maxPriceIdx = pricesHigh.iloc[:-1].idxmax()
        if maxPriceIdx in rsiVals.index:
            if rsiVals.iloc[-1] < rsiVals.loc[maxPriceIdx]:
                divergences["bearish"] = True

    return divergences

def runBacktestForCombo(df1h: pd.DataFrame, symbol: str, lrcPeriod: int, lrcDev: float, minRrVal: float) -> dict:
    df = df1h.copy()
    
    # Calcular LRC
    closePrices = df['close'].values.astype(float)
    centerChannel, upperChannel, lowerChannel, slopeChannel = calculateLrc(closePrices, period=lrcPeriod, dev=lrcDev)
    
    df["lrcCenter"] = centerChannel
    df["lrcUpper"] = upperChannel
    df["lrcLower"] = lowerChannel
    df["lrcSlope"] = slopeChannel
    
    # Calcular RSI y ATR
    df["rsi"] = pd.Series(ta.RSI(closePrices, timeperiod=14), index=df.index)
    df["atr"] = pd.Series(ta.ATR(df['high'].values.astype(float), df['low'].values.astype(float), closePrices, timeperiod=14), index=df.index)
    
    # Calcular Impulse MACD
    impulseMacd, impulseSignal = technical.calculateImpulseMacd(df)
    df["impulseMacd"] = impulseMacd
    df["impulseSignal"] = impulseSignal
    
    # Filtrar nulos al inicio
    startIdx = max(lrcPeriod, 20) + 5
    if len(df) < startIdx:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    trades = []
    activeTrade = None
    pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
    spread = SPREADS.get(symbol, 1.0) / pipMult
    
    for i in range(startIdx, len(df)):
        currentPrice = float(df['close'].iloc[i])
        currentTime = df.index[i]
        
        # 1. Gestionar trade activo si existe
        if activeTrade:
            velaHigh = float(df['high'].iloc[i])
            velaLow = float(df['low'].iloc[i])
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
                if velaHigh >= sl:
                    closed = True
                    exitPrice = sl
                    pnlPips = (activeTrade['entry'] - sl) * pipMult
                elif velaLow <= tp:
                    closed = True
                    exitPrice = tp
                    pnlPips = (activeTrade['entry'] - tp) * pipMult
            
            if closed:
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
        # Volume Breakout Protection
        avgVolume = df['volume'].rolling(window=20).mean().iloc[i]
        currentVolume = df['volume'].iloc[i]
        if currentVolume > 1.5 * avgVolume and avgVolume > 0:
            continue
            
        currentClose = float(df['close'].iloc[i])
        currentHigh = float(df['high'].iloc[i])
        currentLow = float(df['low'].iloc[i])
        currentRsi = float(df['rsi'].iloc[i])
        currentAtr = float(df['atr'].iloc[i])
        
        currentLrcUpper = float(df['lrcUpper'].iloc[i])
        currentLrcLower = float(df['lrcLower'].iloc[i])
        currentLrcSlope = float(df['lrcSlope'].iloc[i])
        
        if np.isnan(currentLrcUpper) or np.isnan(currentRsi) or np.isnan(currentAtr) or np.isnan(currentLrcSlope):
            continue
            
        divergences = checkDivergence(df.iloc[:i+1], df["rsi"].iloc[:i+1], lookback=5)
        
        currentImpulse = df["impulseMacd"].iloc[i]
        prevImpulse = df["impulseMacd"].iloc[i-1]
        currentSignal = df["impulseSignal"].iloc[i]
        prevSignal = df["impulseSignal"].iloc[i-1]
        
        impulseGiroLong = (currentImpulse > currentSignal) and (prevImpulse <= prevSignal)
        impulseGiroShort = (currentImpulse < currentSignal) and (prevImpulse >= prevSignal)
        
        isTrendBullish = (currentLrcSlope > 0)
        direction = None
        
        # Configuración para COMPRA (Long Trigger)
        if currentClose < currentLrcLower and isTrendBullish:
            if (currentRsi < 30 or divergences["bullish"]) and impulseGiroLong:
                direction = "LARGO"
                
        # Configuración para VENTA (Short Trigger)
        elif currentClose > currentLrcUpper and not isTrendBullish:
            if (currentRsi > 70 or divergences["bearish"]) and impulseGiroShort:
                direction = "CORTO"
                
        if direction:
            # Stop Loss Estructural Adaptativo
            swingLow = df['low'].iloc[max(0, i-14):i+1].min()
            swingHigh = df['high'].iloc[max(0, i-14):i+1].max()
            
            if direction == "LARGO":
                slPrice = min(currentLow - (1.5 * currentAtr), swingLow - (0.2 * currentAtr))
            else:
                slPrice = max(currentHigh + (1.5 * currentAtr), swingHigh + (0.2 * currentAtr))
                
            slDist = abs(currentClose - slPrice)
            if slDist <= 0:
                continue
                
            calculatedTp = currentClose + (slDist * minRrVal) if direction == "LARGO" else currentClose - (slDist * minRrVal)
            tpPrice = adjustTPForMinRR(currentClose, slPrice, calculatedTp, direction, minRR=minRrVal)
            
            baseConfidence = 0.70
            if currentRsi < 20 or currentRsi > 80:
                baseConfidence += 0.10
            if (direction == "LARGO" and divergences["bullish"]) or (direction == "CORTO" and divergences["bearish"]):
                baseConfidence += 0.15
                
            # Filtro de confianza mínimo (0.70)
            if baseConfidence < 0.70:
                continue
                
            activeTrade = {
                "direction": direction,
                "entry": currentClose,
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
        "winRate": winRate,
        "profitFactor": profitFactor,
        "pnl": pnlTotal
    }

def runReversionMediaGridSearch():
    logger.info("==========================================================")
    logger.info(" INICIANDO GRID SEARCH OPTIMIZER (REVERSIONMEDIA - 1H) ")
    logger.info("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros
    lrcPeriodCombos = [50, 75, 100, 125, 150]
    lrcDevCombos = [1.5, 1.8, 2.0, 2.2, 2.5, 3.0]
    minRrCombos = [1.5, 1.8, 2.0, 2.2, 2.5, 3.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        from middleware.config.constants import TIMEZONE
        df5m.index = df5m.index.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward')
            
        # Resamplear a 1h
        df1h = df5m.resample('1h').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        if len(df1h) < 120:
            logger.warning(f"  ⚠️ Datos insuficientes en 1h para {symbol}. Saltando.")
            continue
            
        # Bucle de Grid Search
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for lrcPeriod in lrcPeriodCombos:
            for lrcDev in lrcDevCombos:
                for minRr in minRrCombos:
                    res = runBacktestForCombo(df1h, symbol, lrcPeriod, lrcDev, minRr)
                    numTrades = len(res['trades'])
                    
                    row = {
                        "symbol": symbol,
                        "lrcPeriod": lrcPeriod,
                        "lrcDev": lrcDev,
                        "minRr": minRr,
                        "totalTrades": numTrades,
                        "winRate": round(res['winRate'], 2),
                        "profitFactor": round(res['profitFactor'], 2),
                        "pnl": round(res['pnl'], 2)
                    }
                    allResultsRaw.append(row)
                    
                    # Criterio de viabilidad: WR >= 42% y PF >= 1.25, al menos 1 trade
                    if numTrades >= 1 and res['winRate'] >= 35.0 and res['profitFactor'] >= 1.00:
                        # Seleccionar el mejor por PF, luego por WR
                        if res['profitFactor'] > bestPf or (res['profitFactor'] == bestPf and res['winRate'] > bestWr):
                            bestPf = res['profitFactor']
                            bestWr = res['winRate']
                            bestCombo = row
                            
        if bestCombo:
            logger.info(f"  ✨ Mejor combo viable para {symbol}: LRC Period={bestCombo['lrcPeriod']}, LRC Dev={bestCombo['lrcDev']}, Min R:R={bestCombo['minRr']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
            try:
                import json
                from middleware.database import dbConnection
                conn = dbConnection.getConnection()
                cursor = conn.cursor()
                combo = bestCombo
                params = {"ema_period": combo["EMA Period"], "z_score_threshold": combo["Z-Score"], "min_rr": combo["Min RR"]}
                paramsJson = json.dumps(params)
                
                sql = """
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                    VALUES ('ReversionMedia', %s, TRUE, %s)
                    ON DUPLICATE KEY UPDATE parametersJson = VALUES(parametersJson), enabled = TRUE
                """
                cursor.execute(sql, (symbol, paramsJson))
                conn.commit()
                print(f"✅ DB: Guardado {symbol} (TRUE)")
            except Exception as e:
                print(f"❌ Error DB {symbol}: {e}")
            finally:
                if 'cursor' in locals(): cursor.close()
                if 'conn' in locals() and hasattr(conn, 'close'): conn.close()

        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0 y WR >= 35%) para {symbol}.")
            
    # Guardar resultados en CSV
    dfAll = pd.DataFrame(allResultsRaw)
    dfAll.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/reversionmedia_grid_results_all.csv", index=False)
    
    dfBest = pd.DataFrame(bestResults)
    dfBest.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/reversionmedia_grid_results_best.csv", index=False)
    
    # NUEVO: Guardar en base de datos inmediatamente
    if bestResults:
        import json
        from middleware.database import dbConnection
        try:
            conn = dbConnection.getConnection()
            if conn:
                cursor = conn.cursor()
                strategy_name = "ReversionMedia"
                
                # Deshabilitar los que no fueron rentables
                successful_symbols = {r['symbol'] for r in bestResults}
                for s in ALL_SYMBOLS:
                    if s not in successful_symbols:
                        cursor.execute("""
                            INSERT INTO symbolStrategyConfig (strategy, symbol, enabled)
                            VALUES (%s, %s, FALSE)
                            ON DUPLICATE KEY UPDATE enabled = FALSE
                        """, (strategy_name, s))
                
                for combo in bestResults:
                    symbol = combo['symbol']
                    params_dict = {"lrcPeriod": combo["lrcPeriod"], "lrcDev": combo["lrcDev"], "minRr": combo["minRr"]}
                    params_json = json.dumps(params_dict)
                    cursor.execute("""
                        INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                        VALUES (%s, %s, TRUE, %s)
                        ON DUPLICATE KEY UPDATE enabled = TRUE, parametersJson = %s
                    """, (strategy_name, symbol, params_json, params_json))
                conn.commit()
                cursor.close()
                conn.close()
                logger.info(f"💾 Se guardaron en BD los resultados de {strategy_name}")
        except Exception as e:
            logger.error(f"❌ Error al guardar en BD: {e}")
    
    logger.info("==========================================================")
    logger.info(" GRID SEARCH COMPLETADO. Archivos CSV generados con éxito.")
    logger.info("==========================================================")

if __name__ == "__main__":
    runReversionMediaGridSearch()
