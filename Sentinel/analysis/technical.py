"""
Module for calculating technical indicators and features.
"""
import logging
import pandas as pd
import numpy as np
import talib as ta
from typing import Optional
from datetime import datetime
import pytz

from middleware.database import dbManager

logger = logging.getLogger("sentinel")

def _format_timestamp(ts) -> str:
    """Formatea timestamp a string estándar para logging."""
    if ts is None:
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(ts, str):
        return ts[:19] if len(ts) >= 19 else ts
    if hasattr(ts, 'strftime'):
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)[:19]

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
    ema200 = dfFeatured["close"].ewm(span=200, adjust=False).mean()
    dfFeatured["ema20"] = ema20
    dfFeatured["ema50"] = ema50
    dfFeatured["ema200"] = ema200

    # --- Punto 3: SMAs and Bollinger Bands for SMA20_200 Bot ---
    dfFeatured["sma20"] = ta.SMA(dfFeatured["close"].values, timeperiod=20)
    dfFeatured["sma200"] = ta.SMA(dfFeatured["close"].values, timeperiod=200)
    dfFeatured["bb_upper"], dfFeatured["bb_middle"], dfFeatured["bb_lower"] = ta.BBANDS(
        dfFeatured["close"].values, timeperiod=20, nbdevup=2, nbdevdn=2
    )

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

def detect_mss(df: pd.DataFrame, direction: str, lookback: int = 15) -> bool:
    """
    Detecta un Market Structure Shift (MSS).
    Un MSS ocurre cuando el precio rompe el último máximo/mínimo estructural.
    
    Args:
        df: DataFrame con OHLC.
        direction: 'LARGO' (buscamos ruptura de máximo) o 'CORTO' (buscamos ruptura de mínimo).
        lookback: Cuántas velas mirar atrás para encontrar el swing relevante.
    """
    if len(df) < lookback + 2:
        return False
        
    recent_df = df.iloc[-(lookback+1):-1] # No incluir la vela actual
    current_close = float(df['close'].iloc[-1])
    
    if direction.upper() == "LARGO":
        # Ruptura de máximo previo (Bearish to Bullish shift)
        last_high = float(recent_df['high'].max())
        return current_close > last_high
    else:
        # Ruptura de mínimo previo (Bullish to Bearish shift)
        last_low = float(recent_df['low'].min())
        return current_close < last_low

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
        direction   : 'LARGO' → buscamos compras (retroceso hacia abajo).
                      'CORTO' → buscamos ventas (retroceso hacia arriba).
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

    if direction_upper == "LARGO":
        # Impulso alcista: swing_start < swing_end (precio subió).
        # El retroceso va hacia abajo. OTE = retracement 62-79% desde swing_end.
        ote_high = swing_end - rango * fib_min    # 62% retracement → precio más alto de la zona
        ote_low  = swing_end - rango * fib_max    # 79% retracement → precio más bajo de la zona
        sweet_spot = swing_end - rango * 0.705    # 70.5% — "sweet spot" ICT
    else:  # CORTO
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
    
    for i in range(2, len(df)):  # Incluye la última vela (seguro: DB solo guarda velas cerradas)
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
        
        if v1_high < v3_low and v2_close > v2_open:
            gap_size = v3_low - v1_high
            if gap_size / closes[i] >= min_gap_pct:
                fvg = {
                    'type': 'Bullish_FVG',
                    'top': float(v3_low),
                    'bottom': float(v1_high),
                    'gap_low': float(v1_high),
                    'gap_high': float(v3_low),
                    'mid': float((v1_high + v3_low) / 2),
                    'size': float(gap_size),
                    'v1_low': float(v1_low),
                    'v1_high': float(v1_high),
                    'timestamp': _format_timestamp(times[i]),
                    'idx': i
                }
                if validate_mitigation and _is_fvg_mitigated(df, i, fvg):
                    logger.debug(f"[FVG] Bullish FVG en idx {i} invalidado por mitigación")
                    continue
                fvgs.append(fvg)
        
        elif v1_low > v3_high and v2_close < v2_open:
            gap_size = v1_low - v3_high
            if gap_size / closes[i] >= min_gap_pct:
                fvg = {
                    'type': 'Bearish_FVG',
                    'top': float(v1_low),
                    'bottom': float(v3_high),
                    'gap_low': float(v3_high),
                    'gap_high': float(v1_low),
                    'mid': float((v1_low + v3_high) / 2),
                    'size': float(gap_size),
                    'v1_low': float(v1_low),
                    'v1_high': float(v1_high),
                    'timestamp': _format_timestamp(times[i]),
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

# Setup FVG Centralizado v1.1
def calculate_fvg_setup(fvg: dict, current_price: float, atr: float = 0) -> dict:
    """
    Calcula los niveles de entrada y SL para un setup de FVG centralizado.
    Aplica la lógica de 'Entrada Oportunista': si el precio actual es mejor 
    que el punto medio (50%), se usa el precio actual.
    
    Args:
        fvg: Diccionario del FVG (detectado por detect_fvgs)
        current_price: Precio actual del mercado
        atr: Valor ATR para el buffer del SL (opcional)
        
    Returns:
        dict: {entry, sl, sl_dist, direction, is_opportunistic}
    """
    direction = "LARGO" if "Bullish" in fvg['type'] else "CORTO"
    # target_entry: Ahora es más agresivo (primer 25% del gap en lugar del 50%)
    # Para Largo: top - 25% del gap. Para Corto: bottom + 25% del gap.
    gap_size = fvg['top'] - fvg['bottom']
    if direction == "LARGO":
        target_entry = fvg['top'] - (gap_size * 0.25)
    else:
        target_entry = fvg['bottom'] + (gap_size * 0.25)
    
    # 1. Definir SL estructural (Vela 1 Low/High + buffer)
    buffer = atr * 0.1 if atr > 0 else 0
    if direction == "LARGO":
        # Usar v1_low si existe, si no, usar el bottom del FVG
        sl_base = fvg.get('v1_low', fvg.get('bottom', target_entry * 0.99))
        sl = sl_base - buffer
    else:
        # Usar v1_high si existe, si no, usar el top del FVG
        sl_base = fvg.get('v1_high', fvg.get('top', target_entry * 1.01))
        sl = sl_base + buffer
        
    # 2. Lógica de Entrada Oportunista
    # Si el precio actual es MEJOR que el 50% y NO ha cruzado el SL
    is_opportunistic = False
    if direction == "LARGO":
        if sl < current_price < target_entry:
            entry = current_price
            is_opportunistic = True
        else:
            entry = target_entry
    else: # CORTO
        if target_entry < current_price < sl:
            entry = current_price
            is_opportunistic = True
        else:
            entry = target_entry
            
    return {
        "entry": float(entry),
        "sl": float(sl),
        "sl_dist": abs(entry - sl),
        "direction": direction,
        "is_opportunistic": is_opportunistic
    }


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
    """
    adx = calculate_adx(df, period)
    return (adx >= min_adx, adx)


def check_tp_exhaustion(df: pd.DataFrame, vela_origen_idx: int, entry: float, tp: float, sl: float, direction: str, threshold: float = 0.60, timeframe: str = "15M") -> tuple:
    """
    Verifica si el precio ya recorrió demasiado hacia el TP desde la vela origen.
    """
    logger = logging.getLogger("sentinel")
    
    if vela_origen_idx is None or vela_origen_idx < 0 or vela_origen_idx >= len(df):
        logger.warning(f"[Exhaustion] Sin índice de vela origen válido ({vela_origen_idx}), se BLOQUEA señal por seguridad")
        return (False, 0.0, "Sin índice de vela origen, se bloquea")
    
    try:
        direction_upper = direction.upper() if direction else ""
        current_close = float(df['close'].iloc[-1])
        
        # 1. Verificar si TP/SL ya fueron alcanzados en el pasado (desde origen hasta ahora)
        df_since_origen = df.iloc[vela_origen_idx:]
        max_since_origen = float(df_since_origen['high'].max())
        min_since_origen = float(df_since_origen['low'].min())
        
        if direction_upper == "LARGO":
            if max_since_origen >= tp:
                return (False, 1.0, "TP ya alcanzado")
            if min_since_origen <= sl:
                return (False, 0.0, "SL ya alcanzado")
                
            distancia_total = abs(tp - entry)
            if distancia_total > 0:
                recorrido_actual = (current_close - entry) / distancia_total
                recorrido_max = (max_since_origen - entry) / distancia_total
                recorrido_pct = max(recorrido_actual, recorrido_max)
            else:
                recorrido_pct = 0
        else: # CORTO
            if min_since_origen <= tp:
                return (False, 1.0, "TP ya alcanzado")
            if max_since_origen >= sl:
                return (False, 0.0, "SL ya alcanzado")
                
            distancia_total = abs(entry - tp)
            if distancia_total > 0:
                recorrido_actual = (entry - current_close) / distancia_total
                recorrido_max = (entry - min_since_origen) / distancia_total
                recorrido_pct = max(recorrido_actual, recorrido_max)
            else:
                recorrido_pct = 0
        
        if recorrido_pct > threshold:
            logger.info(f"[Exhaustion] 🚫 Señal descartada: Recorrido {recorrido_pct*100:.1f}% > {threshold*100:.0f}%")
            return (False, recorrido_pct, f"Agotado: {recorrido_pct*100:.1f}%")
            
        return (True, recorrido_pct, "Válido")
        
    except Exception as e:
        logger.error(f"[Exhaustion] Error: {e}", exc_info=True)
        return (True, 0.0, f"Error: {e}")


def check_signal_health(entry: float, tp: float, sl: float, direction: str, current_price: float, threshold: float = 0.65, candle_time: str = "") -> tuple:
    """
    Verifica la salud de una señal potencial:
    1. Si el precio ya cruzó el SL (se acercó demasiado al SL)
    2. Si el precio ya recorrió más del threshold hacia el TP
    
    Args:
        entry: Precio de entrada
        tp: Take profit
        sl: Stop loss
        direction: 'LARGO' o 'CORTO'
        current_price: Precio actual
        threshold: Porcentaje máximo de recorrido hacia TP (default 0.65 = 65%)
        candle_time: Timestamp de la vela de entrada (opcional)
    
    Returns:
        (is_valid, progress_pct, message)
    """
    logger = logging.getLogger("sentinel")
    time_prefix = f"[{candle_time}] " if candle_time else ""
    
    try:
        direction_upper = direction.upper()
        risk_dist = abs(entry - sl)
        
        if risk_dist == 0:
            return (True, 0.0, "Risk_dist=0, skip check")
        
        if direction_upper == "LARGO":
            if current_price <= sl:
                logger.info(f"[Health] {time_prefix}🚫 LARGO: precio {current_price:.5f} <= SL {sl:.5f} - descartando")
                return (False, 0.0, "Precio bajo SL")
            
            distancia_total = abs(tp - entry)
            if distancia_total > 0:
                progress_pct = (current_price - entry) / distancia_total
            else:
                progress_pct = 0
        else:
            if current_price >= sl:
                logger.info(f"[Health] {time_prefix}🚫 CORTO: precio {current_price:.5f} >= SL {sl:.5f} - descartando")
                return (False, 0.0, "Precio sobre SL")
            
            distancia_total = abs(entry - tp)
            if distancia_total > 0:
                progress_pct = (entry - current_price) / distancia_total
            else:
                progress_pct = 0
        
        if progress_pct > threshold:
            status_msg = "TP ya alcanzado" if progress_pct >= 1.0 else "Agotado"
            logger.info(f"[Health] {time_prefix}🚫 {status_msg}: Progreso {progress_pct*100:.1f}% > {threshold*100:.0f}% hacia TP - descartando")
            return (False, progress_pct, f"{status_msg}: {progress_pct*100:.1f}%")
        
        return (True, progress_pct, "Válido")
    
    except Exception as e:
        logger.error(f"[Health] Error: {e}", exc_info=True)
        return (True, 0.0, f"Error: {e}")
