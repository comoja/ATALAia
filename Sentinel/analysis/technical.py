"""
Module for calculating technical indicators and features.
"""
import logging
import pandas as pd
import numpy as np
import talib as ta
from typing import Optional
from middleware.database import dbManager

logger = logging.getLogger("sentinel")

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

def detect_fvgs(df: pd.DataFrame, min_gap_pct: float = 0.0001, min_adx: float = 0, validate_mitigation: bool = True) -> list:
    """
    Detecta Fair Value Gaps (FVG) usando definición ICT estricta.
    
    Estructura de 3 Velas (Vela 1 = i-2, Vela 2 = i-1, Vela 3 = i):
    
    FVG Alcista (Bullish):
    - High(Vela 1) < Low(Vela 3) →gap entre el máximo de Vela 1 y mínimo de Vela 3
    - Ignora el color de las velas; solo importan las mechas
    
    FVG Bajista (Bearish):
    - Low(Vela 1) > High(Vela 3) → gap entre el mínimo de Vela 1 y máximo de Vela 3
    
    Filtro de Desplazamiento (Vela 2):
    - El cuerpo de Vela 2 debe ser al menos 50% del rango total
    - Evita gaps por ruido de mechas
    
    Validación de Cierre:
    - El FVG solo se considera confirmado cuando Vela 3 ha cerrado
    
    Regla de Mitigación:
    - El gap se invalida si cualquier vela posterior cierra dentro del espacio
    
    Args:
        df: DataFrame con OHLC.
        min_gap_pct: Tamaño mínimo del gap respecto al precio (filtro de ruido).
        min_adx: ADX mínimo para filtrar mercados laterales (0 = no evaluar).
        validate_mitigation: Si True, valida que el gap no haya sido llenado por velas posteriores.
        
    Returns:
        Lista de diccionarios con la información de cada FVG.
    """
    fvgs = []
    
    if min_adx > 0 and len(df) >= 14:
        try:
            adx = float(ta.ADX(df['high'], df['low'], df['close'], timeperiod=14).dropna().iloc[-1])
            if adx < min_adx:
                logger.debug(f"[FVG] ADX={adx:.1f} < {min_adx} → mercado lateral, sin FVG")
                return fvgs
        except Exception as e:
            logger.debug(f"[FVG] Error calculando ADX: {e}")
    
    if len(df) < 4:
        return fvgs
    
    highs = df['high'].values
    lows = df['low'].values
    closes = df['close'].values
    opens = df['open'].values
    times = df.index
    
    for i in range(2, len(df) - 1):
        v1_high = highs[i-2]
        v1_low = lows[i-2]
        v2_high = highs[i-1]
        v2_low = lows[i-1]
        v2_open = opens[i-1]
        v2_close = closes[i-1]
        v3_high = highs[i]
        v3_low = lows[i]
        
        v2_range = v2_high - v2_low
        if v2_range == 0:
            continue
        
        v2_body = abs(v2_close - v2_open)
        v2_body_pct = v2_body / v2_range
        
        if v2_body_pct < 0.50:
            continue
        
        if v1_high < v3_low:
            gap_size = v3_low - v1_high
            if gap_size / closes[i] >= min_gap_pct:
                fvg = {
                    'type': 'Bullish_FVG',
                    'top': float(v3_low),
                    'bottom': float(v1_high),
                    'mid': float((v1_high + v3_low) / 2),
                    'size': float(gap_size),
                    'timestamp': str(times[i]),
                    'idx': i
                }
                if validate_mitigation and _is_fvg_mitigated(df, i, fvg):
                    logger.debug(f"[FVG] Bullish FVG en idx {i} invalidado por mitigación")
                    continue
                fvgs.append(fvg)
        
        elif v1_low > v3_high:
            gap_size = v1_low - v3_high
            if gap_size / closes[i] >= min_gap_pct:
                fvg = {
                    'type': 'Bearish_FVG',
                    'top': float(v1_low),
                    'bottom': float(v3_high),
                    'mid': float((v1_low + v3_high) / 2),
                    'size': float(gap_size),
                    'timestamp': str(times[i]),
                    'idx': i
                }
                if validate_mitigation and _is_fvg_mitigated(df, i, fvg):
                    logger.debug(f"[FVG] Bearish FVG en idx {i} invalidado por mitigación")
                    continue
                fvgs.append(fvg)
    
    return fvgs


def _is_fvg_mitigated(df: pd.DataFrame, fvg_start_idx: int, fvg: Dict) -> bool:
    """
    Valida si un FVG ha sido llenado (mitigado) por alguna vela posterior.
    
    El gap se considera invalidado desde que cualquier vela posterior entra
    a negociar dentro del espacio del FVG:
    - FVG Alcista: Se invalida si el Low de una vela posterior es <= Top del FVG
    - FVG Bajista: Se invalida si el High de una vela posterior es >= Bottom del FVG
    
    Args:
        df: DataFrame con OHLC.
        fvg_start_idx: Índice de Vela 3 (confirmación del FVG).
        fvg: Diccionario con información del FVG.
    """
    gap_bottom = fvg['bottom']
    gap_top = fvg['top']
    
    for i in range(fvg_start_idx + 1, len(df)):
        candle_high = float(df['high'].iloc[i])
        candle_low = float(df['low'].iloc[i])
        
        if fvg['type'] == 'Bullish_FVG':
            if candle_low <= gap_top:
                return True
        else:
            if candle_high >= gap_bottom:
                return True
    
    return False


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


def detect_fvg_closed(
    df_source: pd.DataFrame,
    interval: str,
    min_gap_pct: float = 0.0005,
    min_adx: float = 20,
    lookback: int = 10
) -> Optional[Dict]:
    """
    Detecta el FVG más reciente usando SOLO Velas Terminadas.
    
    Args:
        df_source: DataFrame de velas (preferiblemente 5min para mayor precision)
        interval: Intervalo objetivo ('15min', '1h', '4h', '1d')
        min_gap_pct: Tamaño mínimo del gap respecto al precio (default 0.0005 = 5 pips)
        min_adx: ADX mínimo para filtrar mercados laterales (default 20)
        lookback: Número de velas hacia atrás para buscar (default 10)
    
    Returns:
        Dict con info del FVG más reciente de vela cerrada, o None si no hay.
        El FVG usa idx <= len(df_resampled) - 2 (última vela cerrada)
    """
    df = resample_to_interval(df_source, interval)
    
    if len(df) < 3:
        return None
    
    last_closed_idx = len(df) - 2
    
    fvgs = detect_fvgs(df, min_gap_pct=min_gap_pct, min_adx=min_adx)
    
    if not fvgs:
        return None
    
    closed_fvgs = [f for f in fvgs if f['idx'] <= last_closed_idx]
    
    if not closed_fvgs:
        return None
    
    latest_fvg = closed_fvgs[-1]
    
    vela4_idx = latest_fvg['idx'] + 1
    if vela4_idx <= last_closed_idx:
        vela4_high = float(df['high'].iloc[vela4_idx])
        vela4_low = float(df['low'].iloc[vela4_idx])
        
        if latest_fvg['type'] == 'Bullish_FVG':
            if vela4_low <= latest_fvg['top']:
                logger.debug(f"[FVG] FVG Bullish mitigado por Vela 4 - descartado")
                return None
        else:
            if vela4_high >= latest_fvg['bottom']:
                logger.debug(f"[FVG] FVG Bearish mitigado por Vela 4 - descartado")
                return None
    
    return latest_fvg


def resample_to_interval(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """
    Resamplea un DataFrame OHLCV al intervalo deseado.
    
    Args:
        df: DataFrame con columnas OHLCV.
        interval: Intervalo objetivo ('5min', '15min', '1h', '4h', '1d')
    
    Returns:
        DataFrame resampleado con las mismas columnas.
    """
    if df is None or len(df) < 1:
        return df
    
    rule_map = {
        '5min': '5min',
        '15min': '15min',
        '30min': '30min',
        '1h': '1h',
        '2h': '2h',
        '4h': '4h',
        '1d': '1D',
        '1M': '1ME'
    }
    rule = rule_map.get(interval, interval)
    
    if rule == '5min' or interval == '5min':
        return df
    
    agg_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }
    if 'volume' in df.columns:
        agg_dict['volume'] = 'sum'
    
    df_resampled = df.resample(rule, label='right', closed='right').agg(agg_dict).dropna()
    
    return df_resampled


def calculate_adx(df: pd.DataFrame, period: int = 14) -> float:
    """
    Calcula el ADX (Average Directional Index) para determinar si el mercado tiene tendencia.
    
    Args:
        df: DataFrame con columnas high, low, close.
        period: Periodo para el cálculo del ADX (default 14).
        
    Returns:
        Valor del ADX. Retorna 25.0 por defecto si no se puede calcular.
    """
    if df is None or len(df) < period * 2:
        return 25.0
    
    try:
        adx_series = ta.ADX(df['high'].values, df['low'].values, df['close'].values, timeperiod=period)
        adx_val = float(adx_series.dropna().iloc[-1])
        return adx_val if not np.isnan(adx_val) else 25.0
    except Exception as e:
        logger.debug(f"[ADX] Error calculando ADX: {e}")
        return 25.0


def is_market_trending(df: pd.DataFrame, min_adx: float = 20, period: int = 14) -> tuple:
    """
    Determina si el mercado tiene tendencia suficiente para operar.
    
    Args:
        df: DataFrame con columnas high, low, close.
        min_adx: ADX mínimo requerido (default 20).
        period: Periodo para el cálculo del ADX (default 14).
        
    Returns:
        Tupla (is_trending: bool, adx_value: float)
    """
    adx = calculate_adx(df, period)
    return (adx >= min_adx, adx)


def check_tp_exhaustion(df: pd.DataFrame, vela_origen_idx: int, entry: float, tp: float, direction: str, threshold: float = 0.60) -> tuple:
    """
    Verifica si el precio ya recorrió demasiado hacia el TP desde la vela origen.
    Si el mercado ya caminó > threshold hacia el TP, la señal está "gastada".
    
    Args:
        df: DataFrame con columnas high, low, close.
        vela_origen_idx: Índice de la vela donde se formó el patrón/displacement original.
        entry: Precio de entrada.
        tp: Precio del take profit.
        direction: Dirección de la operación ('LONG'/'LARGO' o 'SHORT'/'CORTO').
        threshold: Umbral de bloqueo (default 0.60 = 60%).
        
    Returns:
        Tupla (is_valid: bool, recorrido_pct: float, message: str)
    """
    logger = logging.getLogger("sentinel")
    
    if vela_origen_idx is None or vela_origen_idx < 0 or vela_origen_idx >= len(df):
        return (True, 0.0, "Sin índice de vela origen, se permite")
    
    if df is None or len(df) <= vela_origen_idx:
        return (True, 0.0, "DataFrame insuficiente, se permite")
    
    try:
        direction_upper = direction.upper() if direction else ""
        precio_origen = float(df['close'].iloc[vela_origen_idx])
        distancia_total = abs(tp - precio_origen)
        
        if distancia_total == 0:
            return (True, 0.0, "Distancia TP cero, se permite")
        
        if direction_upper in ("LONG", "LARGO"):
            max_since_origin = float(df['high'].iloc[vela_origen_idx:].max())
            recorrido_pct = (max_since_origin - precio_origen) / distancia_total
        else:  # SHORT / CORTO
            min_since_origin = float(df['low'].iloc[vela_origen_idx:].min())
            recorrido_pct = (precio_origen - min_since_origin) / distancia_total
        
        if recorrido_pct > threshold:
            logger.info(f"[Exhaustion] Señal descartada: Precio ya recorrió {recorrido_pct*100:.1f}% hacia TP (umbral: {threshold*100:.0f}%)")
            return (False, recorrido_pct, f"Bloqueado: {recorrido_pct*100:.1f}% > {threshold*100:.0f}%")
        
        logger.info(f"[Exhaustion] Recorrido hacia TP: {recorrido_pct*100:.1f}% (umbral: {threshold*100:.0f}%) ✅")
        return (True, recorrido_pct, "Válido")
        
    except Exception as e:
        logger.debug(f"[Exhaustion] Error calculando exhaustion: {e}")
        return (True, 0.0, f"Error: {e}")
