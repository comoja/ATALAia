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

    #logger.info(f"Features calculadas. Columnas: {dfFeatured.columns.tolist()}")
    
    return dfFeatured
