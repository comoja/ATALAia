"""
Module for calculating technical indicators and features.
"""
import logging
import pandas as pd
import numpy as np
import talib as ta
from middleware.database import dbManager

logger = logging.getLogger(__name__)

def _calculate_dynamic_periods(df: pd.DataFrame) -> dict:
    """
    Calculates dynamic periods for indicators based on market volatility.
    (Original logic from `calcularPeriodosDinamicos`)
    """
    dfCopy = df.copy()
    dfCopy['volatility'] = ta.ATR(dfCopy.high, dfCopy.low, dfCopy.close, timeperiod=14) / dfCopy.close
    
    # Handle NaNs from ATR calculation at the beginning of the series
    dfCopy['volatility'] = dfCopy['volatility'].ffill().bfill()
    
    rollingStats = dfCopy['volatility'].rolling(window=100)
    avgVol = rollingStats.mean().iloc[-1]
    stdVol = rollingStats.std().iloc[-1]
    currentVol = dfCopy['volatility'].iloc[-1]

    # High volatility
    if currentVol > (avgVol + stdVol): 
        return {"cci": 20, "rsi": 21, "macd": (24, 52, 18)}
    # Low volatility
    elif currentVol < (avgVol - stdVol): 
        return {"cci": 9, "rsi": 7, "macd": (6, 13, 5)}
    # Standard volatility
    else:
        return {"cci": 14, "rsi": 14, "macd": (12, 26, 9)}

def _calculate_slope(series: pd.Series, window: int = 3) -> pd.Series:
    """
    Calculates the slope of a series using linear regression over a rolling window.
    (Original logic from `pendienteRSI`)
    """
    x = np.arange(window)
    
    def getSlope(y):
        if len(y) < window:
            return np.nan
        # polyfit returns [slope, intercept]
        m, _ = np.polyfit(x, y, 1)
        return m

    return series.rolling(window=window).apply(getSlope, raw=True)

def calculateFeatures(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates all technical indicators and features for the given DataFrame.
    Returns the DataFrame with the new feature columns.
    """
    dfFeatured = df.copy()
    
    #logger.info(f"Columnas recibidas en calculateFeatures: {dfFeatured.columns.tolist()}")
    
    # --- Base Indicators ---
    ema20 = dfFeatured["close"].ewm(span=20, adjust=False).mean()
    ema50 = dfFeatured["close"].ewm(span=50, adjust=False).mean()
    dfFeatured["ema20"] = ema20
    dfFeatured["ema50"] = ema50
    dfFeatured["emaDist"] = (dfFeatured["close"] - ema20) / dfFeatured["close"]
    dfFeatured["emaTrend"] = (ema20 - ema50) / dfFeatured["close"]
    dfFeatured["slopeEma50"] = ema50.pct_change(12)
    dfFeatured["atr"] = ta.ATR(dfFeatured["high"], dfFeatured["low"], dfFeatured["close"], 14)
    dfFeatured["sar"] = ta.SAR(dfFeatured["high"], dfFeatured["low"], acceleration=0.02, maximum=0.2)
    dfFeatured["sarTrend"] = np.where(dfFeatured["close"] > dfFeatured["sar"], 1, -1)
    dfFeatured["sarDist"] = (dfFeatured["close"] - dfFeatured["sar"]) / dfFeatured["close"]
    
    # --- Dynamic Period Indicators ---
    periods = _calculate_dynamic_periods(dfFeatured)
    dfFeatured["rsi"] = ta.RSI(dfFeatured["close"], timeperiod=periods['rsi'])
    dfFeatured['cci'] = ta.CCI(dfFeatured['high'], dfFeatured['low'], dfFeatured['close'], timeperiod=periods['cci'])
    
    # --- Volume and Momentum ---
    volSma = dfFeatured["volume"].rolling(window=24).mean()
    dfFeatured["volRatio"] = np.where((volSma > 0) & (dfFeatured["volume"] > 0), dfFeatured["volume"] / volSma, 1.0)
    dfFeatured["volRegime"] = dfFeatured["atr"] / dfFeatured["atr"].rolling(60).mean()
    
    for i in range(1, 4): 
        dfFeatured[f"lag{i}"] = dfFeatured["close"].pct_change(i)
        
    macd, macdsignal, macdhist = ta.MACD(
        dfFeatured['close'], 
        fastperiod=periods['macd'][0], 
        slowperiod=periods['macd'][1], 
        signalperiod=periods['macd'][2]
    )
    dfFeatured["macd"] = macd
    dfFeatured["macdSig"] = macdsignal
    dfFeatured["macdHist"] = macdhist
    dfFeatured["macdNorm"] = dfFeatured["macdHist"] / dfFeatured["atr"]
    
    # --- Slopes ---
    dfFeatured["pendienteRsi"] = _calculate_slope(dfFeatured["rsi"], window=3)
    dfFeatured["pendienteCci"] = _calculate_slope(dfFeatured["cci"], window=3)

    # --- Candlestick Patterns ---
    dfFeatured["cdlEngulfing"] = ta.CDLENGULFING(dfFeatured['open'], dfFeatured['high'], dfFeatured['low'], dfFeatured['close'])
    dfFeatured["cdlHammer"] = ta.CDLHAMMER(dfFeatured['open'], dfFeatured['high'], dfFeatured['low'], dfFeatured['close'])
    dfFeatured["cdlShootingStar"] = ta.CDLSHOOTINGSTAR(dfFeatured['open'], dfFeatured['high'], dfFeatured['low'], dfFeatured['close'])

    return dfFeatured

def get_prev_day_high_low(df: pd.DataFrame) -> dict:
    """
    Obtiene el High y Low del día anterior cerrado.
    Utiliza resampling diario para asegurar precisión.
    """
    try:
        if df is None or len(df) < 10:
            return {"pdh": None, "pdl": None}
            
        # Resample a diario para obtener máximo y mínimo de cada día
        df_daily = df.resample('D').agg({'high': 'max', 'low': 'min'}).dropna()
        
        if len(df_daily) < 2:
            return {"pdh": None, "pdl": None}
            
        # El día anterior cerrado es el penúltimo del resample (el último es el día en curso)
        prev_day = df_daily.iloc[-2]
        return {
            "pdh": float(prev_day['high']),
            "pdl": float(prev_day['low'])
        }
    except Exception as e:
        logger.error(f"Error calculando PDH/PDL: {e}")
        return {"pdh": None, "pdl": None}

def get_structural_levels(df: pd.DataFrame, lookback: int = 40, lookback_macro: int = 120) -> dict:
    """
    Identifica niveles estructurales (Swing High/Low) en el periodo reciente y macro.
    Retorna el máximo y mínimo absoluto del periodo, junto con zonas de liquidez macro.
    """
    if len(df) < lookback:
        lookback = len(df)
        
    recent = df.iloc[-lookback:]
    macro = df.iloc[-min(len(df), lookback_macro):]
    
    return {
        "swing_high": float(recent['high'].max()),
        "swing_low": float(recent['low'].min()),
        "high_zone": float(np.percentile(recent['high'], 90)),
        "low_zone": float(np.percentile(recent['low'], 10)),
        "macro_high": float(macro['high'].max()),
        "macro_low": float(macro['low'].min())
    }

def calculate_ote_zone(
    swing_start: float,
    swing_end: float,
    direction: str,
    fib_min: float = 0.62,
    fib_max: float = 0.79,
) -> dict:
    """
    Calcula la zona OTE (Optimal Trade Entry) de ICT basada en retroceso Fibonacci.
    
    La zona OTE representa el 62-79% de retroceso del impulso más reciente.
    Es la zona de entrada de mayor probabilidad según ICT.

    Args:
        swing_start : Inicio del impulso (punto A — donde empezó el movimiento).
        swing_end   : Fin del impulso (punto B — extremo del movimiento, e.g. sweep).
        direction   : 'LONG' o 'LARGO' → buscamos compras (retroceso hacia abajo).
                      'SHORT' o 'CORTO' → buscamos ventas (retroceso hacia arriba).
        fib_min     : Nivel Fibonacci mínimo de la zona OTE (default 0.62).
        fib_max     : Nivel Fibonacci máximo de la zona OTE (default 0.79).

    Returns:
        dict con 'ote_low', 'ote_high', 'sweet_spot' (70.5%), 'rango'.
    """
    rango = abs(swing_end - swing_start)
    if rango == 0:
        mid = (swing_start + swing_end) / 2
        return {"ote_low": mid, "ote_high": mid, "sweet_spot": mid, "rango": 0}

    direction_upper = direction.upper()

    if direction_upper in ("LONG", "LARGO"):
        # Impulso alcista: swing_start < swing_end (precio subió).
        # El retroceso va hacia abajo. OTE = retracement 62-79% desde swing_end.
        ote_high = swing_end - rango * fib_min    # 62% retracement → precio más alto de la zona
        ote_low  = swing_end - rango * fib_max    # 79% retracement → precio más bajo de la zona
        sweet_spot = swing_end - rango * 0.705    # 70.5% — "sweet spot" ICT
    else:  # SHORT / CORTO
        # Impulso bajista: swing_start > swing_end (precio bajó).
        # El retroceso va hacia arriba. OTE = retracement 62-79% desde swing_end.
        ote_low  = swing_end + rango * fib_min    # 62% retracement → precio más bajo de la zona
        ote_high = swing_end + rango * fib_max    # 79% retracement → precio más alto de la zona
        sweet_spot = swing_end + rango * 0.705

    return {
        "ote_low":    float(ote_low),
        "ote_high":   float(ote_high),
        "sweet_spot": float(sweet_spot),
        "rango":      float(rango),
    }


def is_in_ote_zone(
    price: float,
    swing_start: float,
    swing_end: float,
    direction: str,
    fib_min: float = 0.62,
    fib_max: float = 0.79,
) -> tuple:
    """
    Verifica si `price` está dentro de la zona OTE (Fibonacci 62-79%).

    Returns:
        (bool, dict) — True/False + detalles de la zona calculada.
    """
    zone = calculate_ote_zone(swing_start, swing_end, direction, fib_min, fib_max)
    in_zone = zone["ote_low"] <= price <= zone["ote_high"]
    return in_zone, zone


def is_spread_safe(df: pd.DataFrame, max_spread_atr_percent: float = 20.0) -> bool:
    """
    Verifica si el spread actual es seguro para operar.
    Como solemos tener solo precios OHLC, calculamos el spread promedio reciente.
    """
    if len(df) < 20: return True
    
    # Calculamos el ATR actual
    high_low = df['high'] - df['low']
    atr = high_low.rolling(window=14).mean().iloc[-1]
    
    # Supongamos un spread estimado de 0.2 ATR para brokers estándar.
    # En producción real, este valor vendría de client.get_spread()
    estimated_spread = atr * 0.1 # Placeholder conservador
    
    threshold = atr * (max_spread_atr_percent / 100)
    
    if estimated_spread > threshold:
        return False
    return True

def detect_fvgs(df: pd.DataFrame, min_gap_pct: float = 0.0001, min_adx: float = 0) -> list:
    """
    Detecta Fair Value Gaps (FVG) en un DataFrame usando definición ICT.
    
    Un FVG clásico requiere 3 velas consecutivas donde:
    - Vela central (i-1): tiene el rango más pequeño (vela pequeña)
    - Vela izquierda (i-2) y derecha (i): tienen rangos mayores
    
    Bullish FVG:
    - Low(i-1) > Low(i-2) Y Low(i-1) > Low(i)  (vela central tiene el low más alto)
    - La vela central está "entre" las otras dos en términos de precio
    
    Bearish FVG:
    - High(i-1) < High(i-2) Y High(i-1) < High(i)  (vela central tiene el high más bajo)
    - La vela central está "entre" las otras dos en términos de precio
    
    Args:
        df: DataFrame con OHLC.
        min_gap_pct: Tamaño mínimo del gap respecto al precio (filtro de ruido).
        min_adx: ADX mínimo para filtrar mercados laterales (0 = no evaluar).
                 Valores típicos: 20=mercado con tendencia, <20=mercado lateral.
        
    Returns:
        Lista de diccionarios con la información de cada FVG.
    """
    fvgs = []
    
    # --- Filtrar mercado lateral con ADX ---
    if min_adx > 0 and len(df) >= 14:
        try:
            adx = float(ta.ADX(df['high'], df['low'], df['close'], timeperiod=14).dropna().iloc[-1])
            if adx < min_adx:
                logger.debug(f"[FVG] ADX={adx:.1f} < {min_adx} → mercado lateral, sin FVG")
                return fvgs
        except Exception as e:
            logger.debug(f"[FVG] Error calculando ADX: {e}")
    if len(df) < 3:
        return fvgs
        
    highs = df['high'].values
    lows = df['low'].values
    closes = df['close'].values
    opens = df['open'].values
    times = df.index
    
    # Análisis de 3 velas: (i-2), (i-1), (i)
    for i in range(2, len(df)):
        # Range de cada vela
        range_prev2 = highs[i-2] - lows[i-2]
        range_mid = highs[i-1] - lows[i-1]
        range_curr = highs[i] - lows[i]
        
        # Vela central (i-1) debe tener el rango más pequeño (vela pequeña)
        if not (range_mid < range_prev2 and range_mid < range_curr):
            continue
        
        # ---- Bullish FVG ----
        # Low(i-1) > Low(i-2) Y Low(i-1) > Low(i)
        # La vela central tiene el low más alto (está arriba de las otras dos)
        if lows[i-1] > lows[i-2] and lows[i-1] > lows[i]:
            gap_size = lows[i-1] - max(lows[i-2], lows[i])
            if gap_size / closes[i] >= min_gap_pct:
                fvgs.append({
                    'type': 'Bullish_FVG',
                    'top': float(lows[i-1]),
                    'bottom': float(max(lows[i-2], lows[i])),
                    'mid': float((lows[i-1] + max(lows[i-2], lows[i])) / 2),
                    'size': float(gap_size),
                    'timestamp': str(times[i-1]),
                    'idx': i-1
                })
        
        # ---- Bearish FVG ----
        # High(i-1) < High(i-2) Y High(i-1) < High(i)
        # La vela central tiene el high más bajo (está abajo de las otras dos)
        elif highs[i-1] < highs[i-2] and highs[i-1] < highs[i]:
            gap_size = min(highs[i-2], highs[i]) - highs[i-1]
            if gap_size / closes[i] >= min_gap_pct:
                fvgs.append({
                    'type': 'Bearish_FVG',
                    'top': float(min(highs[i-2], highs[i])),
                    'bottom': float(highs[i-1]),
                    'mid': float((min(highs[i-2], highs[i]) + highs[i-1]) / 2),
                    'size': float(gap_size),
                    'timestamp': str(times[i-1]),
                    'idx': i-1
                })
                
    return fvgs

def get_pip_multiplier(symbol: str) -> float:
    """Obtiene el multiplicador de pips para un activo.
    
    Convierte distancia de precio a pips. Ejemplos:
    - EUR/USD: 0.00048 precio -> 4.8 pips (multiplicador 10000)
    - USD/JPY: 0.01 precio -> 1 pip (multiplicador 100)
    - XAU/USD: 0.50 precio -> 0.5 pips (multiplicador 1)
    """
    symbol_up = symbol.upper()
    
    # Fix prioritario: JPY/HUF siempre usan 100.0
    if any(s in symbol_up for s in ["JPY", "HUF"]):
        return 100.0
    
    try:
        symbolData = dbManager.getSymbol(symbol)
        if symbolData and 'pip' in symbolData and symbolData['pip'] is not None:
            pip_val = float(symbolData['pip'])
            # DB tiene el valor de 1 pip en precio, convertir a multiplicador
            if pip_val > 0:
                return 1.0 / pip_val
    except Exception as e:
        logger.debug(f"Error obteniendo pip multiplier desde DB para {symbol}: {e}")
    
    # Fallback si no hay DB o falla
    if any(s in symbol_up for s in ["XAU", "GOLD", "BTC", "ETH", "SOL", "BNB"]):
        return 1.0
    if "MXN" in symbol_up:
        return 10000.0
    return 10000.0
