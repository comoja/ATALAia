"""
Module for calculating technical indicators and features.
"""
import logging
import pandas as pd
import numpy as np
import talib as ta

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
