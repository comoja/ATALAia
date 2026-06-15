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

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

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
        logger.error(f"Error cargando velas para {symbol}: {e}")
        return pd.DataFrame()

def getPendiente(serie: pd.Series, periodos: int = 3) -> float:
    y = serie.iloc[-periodos:].values
    x = np.arange(periodos)
    if len(y) < periodos:
        return 0.0
    m, b = np.polyfit(x, y, 1)
    return float(m)

def detectar_rebote_sma_doble(df: pd.DataFrame, sma20: float, i: int, tendencia: str) -> tuple:
    velas_analisis = 25
    touches = []
    
    start = max(0, i - velas_analisis - 1)
    for k in range(start, i + 1):
        price = df["close"].iloc[k]
        sma = df["sma20"].iloc[k]
        atr = df["atr"].iloc[k]
        low = df["low"].iloc[k]
        high = df["high"].iloc[k]
        open_price = df["open"].iloc[k]
        close_price = df["close"].iloc[k]
        
        tolerancia_pct = 0.8
        max_wick_pct = 1.2
        tolerancia_pips = (atr / price) * tolerancia_pct * price
        max_wick_pips = atr * max_wick_pct
        
        dist_low = (sma - low) if low < sma else float('inf')
        dist_high = (high - sma) if high > sma else float('inf')
        
        toque_direccion = None
        toque_dist = None
        if dist_low < tolerancia_pips and dist_low < max_wick_pips:
            toque_direccion = "ALCISTA"
            toque_dist = dist_low
        elif dist_high > 0 and dist_high < tolerancia_pips and dist_high < max_wick_pips:
            toque_direccion = "BAJISTA"
            toque_dist = dist_high
            
        if toque_direccion:
            touches.append({
                "idx": k,
                "direccion": toque_direccion,
                "time": df.index[k],
                "open": open_price,
                "high": high,
                "low": low,
                "close": close_price,
                "sma": sma,
                "atr": atr,
                "dist": toque_dist
            })
            
    if len(touches) < 2:
        return None, None
        
    for idx in range(len(touches) - 1, 0, -1):
        t1 = touches[idx - 1]
        t2 = touches[idx]
        if t1["direccion"] != t2["direccion"] or (t1["close"] > t1["open"]) != (t2["close"] > t2["open"]):
            continue
        if t1["direccion"] != tendencia:
            continue
            
        separacion = t2["idx"] - t1["idx"]
        if separacion < 0 or separacion > 25:
            continue
            
        close_actual = df["close"].iloc[i]
        
        if t1["direccion"] == "BAJISTA" and close_actual <= sma20 * 1.005:
            return "CORTO", t2["time"]
        elif t1["direccion"] == "ALCISTA" and close_actual >= sma20 * 0.995:
            return "LARGO", t2["time"]
            
    return None, None

def detectar_consolidacion_oro_puro(df: pd.DataFrame, sma20: float, i: int, tendencia: str) -> dict | None:
    velas_consolidacion = []
    start = max(0, i - 10)
    for k in range(start, i + 1):
        precio_medio = (df["close"].iloc[k] + df["open"].iloc[k]) / 2
        if abs(precio_medio - sma20) / sma20 < 0.003:
            velas_consolidacion.append({"high": df["high"].iloc[k], "low": df["low"].iloc[k]})
            
    if len(velas_consolidacion) < 5:
        return None
        
    base_high = max(v["high"] for v in velas_consolidacion)
    base_low = min(v["low"] for v in velas_consolidacion)
    rango_base = base_high - base_low
    close_actual = df["close"].iloc[i]
    
    if tendencia == "BAJISTA" and close_actual < (base_low - rango_base * 0.3):
        return {"type": "CORTO", "sl": base_high + rango_base * 0.2}
    elif tendencia == "ALCISTA" and close_actual > (base_high + rango_base * 0.3):
        return {"type": "LARGO", "sl": base_low - rango_base * 0.2}
    return None

def validar_filtros_basicos(df: pd.DataFrame, i: int, close: float, sma20: float, sma200: float, atr: float, direction: str) -> bool:
    if abs(close - sma20) / close * 100 < (atr / close * 100) * 0.5:
        return False
        
    start_rango = max(0, i - 19)
    rango = (df["high"].iloc[start_rango:i+1].max() - df["low"].iloc[start_rango:i+1].min()) / close
    if rango < (atr / close) * 3:
        return False
        
    velas_contrarias = 0
    for offset in range(-4, 0):
        idx = i + offset
        if idx >= 0:
            es_bajista = df["close"].iloc[idx] < df["open"].iloc[idx]
            es_alcista = df["close"].iloc[idx] > df["open"].iloc[idx]
            if direction == "LARGO" and es_bajista:
                velas_contrarias += 1
            elif direction == "CORTO" and es_alcista:
                velas_contrarias += 1
                
    if velas_contrarias >= 2:
        return False
        
    if (direction == "LARGO" and close < sma200) or (direction == "CORTO" and close > sma200):
        return False
        
    return True

def runBacktestForCombo(df15m: pd.DataFrame, df1h: pd.DataFrame, symbol: str, slopeThreshold: float, minRrVal: float) -> dict:
    df = df15m.copy()
    
    # Calcular medias móviles e indicadores técnicos en 15m
    closePrices = df['close'].values.astype(float)
    df["sma20"] = pd.Series(ta.SMA(closePrices, timeperiod=20), index=df.index)
    df["sma200"] = pd.Series(ta.SMA(closePrices, timeperiod=200), index=df.index)
    df["atr"] = pd.Series(ta.ATR(df['high'].values.astype(float), df['low'].values.astype(float), closePrices, timeperiod=14), index=df.index)
    df["adx"] = pd.Series(ta.ADX(df['high'].values.astype(float), df['low'].values.astype(float), closePrices, timeperiod=14), index=df.index)
    
    df.dropna(subset=["sma20", "sma200", "atr", "adx"], inplace=True)
    if len(df) < 50:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    trades = []
    activeTrade = None
    pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
    spread = SPREADS.get(symbol, 1.0) / pipMult
    
    # Mapeo del índice de 1h a 15m para la tendencia de 1h
    idx1h_map = df1h.index.get_indexer(df.index, method='pad')
    
    for i in range(50, len(df)):
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
        close = float(df['close'].iloc[i])
        sma20 = float(df['sma20'].iloc[i])
        sma200 = float(df['sma200'].iloc[i])
        atr = float(df['atr'].iloc[i])
        adx = float(df['adx'].iloc[i])
        
        if adx < 20:
            continue
            
        # Identificar tendencia de 15m
        pendienteSma20 = (getPendiente(df["sma20"].iloc[max(0, i-9):i+1], 10) / sma20) * 100
        if close > sma20 and pendienteSma20 > slopeThreshold:
            tendencia = "ALCISTA"
        elif close < sma20 and pendienteSma20 < -slopeThreshold:
            tendencia = "BAJISTA"
        else:
            tendencia = "NEUTRAL"
            
        if tendencia == "NEUTRAL":
            continue
            
        # Validar Tendencia 1h offline
        i_1h = idx1h_map[i]
        if i_1h < 20:
            continue
        close1h = df1h["close"].iloc[i_1h]
        sma20_1h = df1h["sma20"].iloc[i_1h]
        pendienteSma20_1h = (getPendiente(df1h["sma20"].iloc[max(0, i_1h-9):i_1h+1], 10) / sma20_1h) * 100
        
        # Umbral fijo para 1h (0.005)
        if close1h > sma20_1h and pendienteSma20_1h > 0.005:
            tendencia1h = "ALCISTA"
        elif close1h < sma20_1h and pendienteSma20_1h < -0.005:
            tendencia1h = "BAJISTA"
        else:
            tendencia1h = "NEUTRAL"
            
        if tendencia1h != tendencia:
            continue
            
        # Detectar entrada
        direction, double_touch_time = detectar_rebote_sma_doble(df, sma20, i, tendencia)
        consolidacion = None
        if not direction:
            consolidacion = detectar_consolidacion_oro_puro(df, sma20, i, tendencia)
            if not consolidacion:
                continue
            direction = consolidacion["type"]
            double_touch_time = currentTime
            
        if not validar_filtros_basicos(df, i, close, sma20, sma200, atr, direction):
            continue
            
        if double_touch_time and (currentTime - double_touch_time).total_seconds() / 60 > 60:
            continue
            
        # Calcular SL e TP
        if consolidacion:
            slPrice = consolidacion["sl"]
        else:
            # Swing High/Low de las últimas 40 velas
            swingLow = df['low'].iloc[max(0, i-39):i+1].min()
            swingHigh = df['high'].iloc[max(0, i-39):i+1].max()
            atr_padding = atr * 0.2
            
            if direction == "LARGO":
                slPrice = min(close - (atr * 1.2), swingLow - atr_padding)
            else:
                slPrice = max(close + (atr * 1.2), swingHigh + atr_padding)
                
        slDist = abs(close - slPrice)
        if slDist <= 0:
            continue
            
        # TP basado en expected_return = 0.5 (tp_factor = 1.5)
        tp_ml = close + slDist * 1.5 if direction == "LARGO" else close - slDist * 1.5
        swingLow40 = df['low'].iloc[max(0, i-39):i+1].min()
        swingHigh40 = df['high'].iloc[max(0, i-39):i+1].max()
        
        if direction == "LARGO":
            take_profit = min(swingHigh40, tp_ml) if swingHigh40 > tp_ml else swingHigh40
        else:
            take_profit = max(swingLow40, tp_ml) if swingLow40 < tp_ml else swingLow40
            
        tpPrice = adjustTPForMinRR(close, slPrice, take_profit, direction, minRR=minRrVal)
        
        activeTrade = {
            "direction": direction,
            "entry": close,
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

def runSMA20200GridSearch():
    logger.info("==========================================================")
    logger.info(" INICIANDO GRID SEARCH OPTIMIZER (SMA20_200 - 15MIN) ")
    logger.info("==========================================================")
    
    startDateStr = '2026-05-15 00:00:00'
    endDateStr = '2026-06-11 14:00:00'
    
    # Grid de Parámetros
    slopeThresholdCombos = [0.003, 0.005, 0.007]
    minRrCombos = [1.5, 2.0]
    
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
            
        # Resamplear a 15min
        df15m = df5m.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # Resamplear a 1h
        df1h = df5m.resample('1h').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        # Precalcular SMA 20 en 1h para la validación de tendencia
        df1h["sma20"] = pd.Series(ta.SMA(df1h['close'].values.astype(float), timeperiod=20), index=df1h.index)
        df1h.dropna(subset=["sma20"], inplace=True)
        
        if len(df15m) < 120 or len(df1h) < 30:
            logger.warning(f"  ⚠️ Datos insuficientes en 15m/1h para {symbol}. Saltando.")
            continue
            
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for slopeThreshold in slopeThresholdCombos:
            for minRr in minRrCombos:
                res = runBacktestForCombo(df15m, df1h, symbol, slopeThreshold, minRr)
                numTrades = len(res['trades'])
                
                row = {
                    "symbol": symbol,
                    "slopeThreshold": slopeThreshold,
                    "minRr": minRr,
                    "totalTrades": numTrades,
                    "winRate": round(res['winRate'], 2),
                    "profitFactor": round(res['profitFactor'], 2),
                    "pnl": round(res['pnl'], 2)
                }
                allResultsRaw.append(row)
                
                # Criterio de viabilidad: WR >= 42% y PF >= 1.25, al menos 1 trade
                if numTrades >= 1 and res['winRate'] >= 42.0 and res['profitFactor'] >= 1.25:
                    if res['profitFactor'] > bestPf or (res['profitFactor'] == bestPf and res['winRate'] > bestWr):
                        bestPf = res['profitFactor']
                        bestWr = res['winRate']
                        bestCombo = row
                        
        if bestCombo:
            logger.info(f"  ✨ Mejor combo viable para {symbol}: Slope={bestCombo['slopeThreshold']}, Min R:R={bestCombo['minRr']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.25 y WR >= 42%) para {symbol}.")
            
    # Guardar resultados en CSV
    dfAll = pd.DataFrame(allResultsRaw)
    dfAll.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/sma20_200_grid_results_all.csv", index=False)
    
    dfBest = pd.DataFrame(bestResults)
    dfBest.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/sma20_200_grid_results_best.csv", index=False)
    
    logger.info("==========================================================")
    logger.info(" GRID SEARCH COMPLETADO. Archivos CSV generados con éxito.")
    logger.info("==========================================================")

if __name__ == "__main__":
    runSMA20200GridSearch()
