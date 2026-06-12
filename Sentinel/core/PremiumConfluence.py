import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple
import pandas as pd
import numpy as np
import talib as ta

from middleware.database import dbManager
from Sentinel.analysis import technical
from Sentinel.analysis.technical import check_tp_exhaustion, check_signal_health, detect_mss
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateBEPrice
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal
from middleware.utils import momentum

logger = logging.getLogger("sentinel")

class PremiumConfluenceBot:
    """
    Estrategia PremiumConfluence que implementa la confluencia de 5 indicadores mostrados en la imagen:
    1. Filtro Macro (EMA 200)
    2. Supertrend (10, 1.0)
    3. Velas Heikin Ashi Suavizadas (10, 10)
    4. Impulse MACD (20, 9)
    5. Market Structure Shift (MSS)
    """
    def __init__(self):
        logger.info("Bot PremiumConfluence iniciado")

    def calculateSuperTrend(self, df: pd.DataFrame, supertrendPeriod: int, supertrendMultiplier: float) -> tuple:
        """
        Calcula el canal de SuperTrend y el Trailing Stop delegando al módulo de análisis técnico.
        """
        return technical.calculateAtrStop(df, supertrendPeriod, supertrendMultiplier)

    def calculateSmoothedHeikinAshi(self, df: pd.DataFrame, period1: int = 10, period2: int = 10) -> Tuple[pd.Series, pd.Series]:
        """
        Calcula velas Heikin Ashi Suavizadas (Smoothed Heikin Ashi) usando EMAs.
        Retorna dos series (haCloseSmooth, haOpenSmooth).
        """
        # 1. Suavizado inicial de los precios OHLC con una EMA de period1
        openSmooth = df['open'].ewm(span=period1, adjust=False).mean()
        highSmooth = df['high'].ewm(span=period1, adjust=False).mean()
        lowSmooth = df['low'].ewm(span=period1, adjust=False).mean()
        closeSmooth = df['close'].ewm(span=period1, adjust=False).mean()
        
        # 2. Calcular velas Heikin Ashi sobre los precios suavizados
        haClose = (openSmooth + highSmooth + lowSmooth + closeSmooth) / 4.0
        
        # Heikin Ashi Open es recursivo: HA_Open = (HA_Open_prev + HA_Close_prev) / 2
        haOpen = np.zeros(len(df))
        haOpen[0] = (openSmooth.iloc[0] + closeSmooth.iloc[0]) / 2.0
        
        for i in range(1, len(df)):
            haOpen[i] = (haOpen[i - 1] + haClose.iloc[i - 1]) / 2.0
            
        haOpenSeries = pd.Series(haOpen, index=df.index)
        
        # 3. Suavizado final de las series HA con una EMA de period2
        haCloseSmooth = haClose.ewm(span=period2, adjust=False).mean()
        haOpenSmooth = haOpenSeries.ewm(span=period2, adjust=False).mean()
        
        return haCloseSmooth, haOpenSmooth

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f"[{symbol}] Analizando con PremiumConfluence...")
        
        master = preloadedData.get(symbol) if preloadedData else None
        
        # Operamos en timeframe de 15min para mayor estabilidad, con fallback a 5min
        if isinstance(master, dict):
            df = master.get('15min') if master.get('15min') is not None else master.get('5min')
        else:
            df = master

        if df is None or len(df) < 200:
            logger.info(f"[{symbol}] Datos insuficientes para PremiumConfluence")
            return None

        # Cargar parámetros desde base de datos, usando valores por defecto para confluencia perfecta
        stratConfig = dbManager.getSymbolStrategyConfig("PremiumConfluence", symbol) or {}
        
        # Parámetros del Supertrend (10, 1.5 por defecto para equilibrio óptimo)
        supertrendPeriod = int(stratConfig.get('supertrend_period', 10))
        supertrendMultiplier = float(stratConfig.get('supertrend_mult', 1.5))
        
        # Parámetros de Heikin Ashi Suavizado (10, 10 por defecto según la imagen)
        haPeriod1 = int(stratConfig.get('ha_period1', 10))
        haPeriod2 = int(stratConfig.get('ha_period2', 10))
        
        # Parámetros del Impulse MACD (20, 9 por defecto según la imagen)
        macdLength = int(stratConfig.get('macd_length', 20))
        macdSignal = int(stratConfig.get('macd_signal', 9))
        
        # Take Profit y ratio de riesgo mínimos
        minRrVal = float(stratConfig.get('min_rr', 1.5))
        if minRrVal <= 0:
            minRrVal = 1.5
            
        tpPercent = float(stratConfig.get('tp_percent', 2.0)) / 100.0
        if tpPercent <= 0:
            tpPercent = 0.02

        # 1. Filtro Macro: calcular EMA 200
        closePrices = df['close'].values.astype(float)
        ema200 = ta.EMA(closePrices, timeperiod=200)
        ema200Series = pd.Series(ema200).ffill().bfill().values
        
        # 2. Supertrend (10, 1.0)
        stTrend, stTrail = self.calculateSuperTrend(df, supertrendPeriod, supertrendMultiplier)
        
        # 3. Velas Heikin Ashi Suavizadas (10, 10)
        haCloseSmooth, haOpenSmooth = self.calculateSmoothedHeikinAshi(df, haPeriod1, haPeriod2)
        
        # 4. Impulse MACD (20, 9)
        # Si no está en el dataframe base, lo calculamos
        if "impulseMacd" in df.columns and "impulseSignal" in df.columns:
            impulseMacdSeries = df["impulseMacd"]
            impulseSignalSeries = df["impulseSignal"]
        else:
            impulseMacdSeries, impulseSignalSeries = technical.calculateImpulseMacd(df, lengthMa=macdLength, lengthSignal=macdSignal)
            
        if len(stTrend) < 3 or len(haCloseSmooth) < 3 or len(impulseMacdSeries) < 3:
            return None

        currentPrice = float(df['close'].iloc[-1])
        currentEma200 = float(ema200Series[-1])
        
        # Estados actuales
        macroBullish = currentPrice > currentEma200
        macroBearish = currentPrice < currentEma200
        
        supertrendBullish = stTrend[-1] == 1
        supertrendBearish = stTrend[-1] == -1
        
        haBullish = haCloseSmooth.iloc[-1] > haOpenSmooth.iloc[-1]
        haBearish = haCloseSmooth.iloc[-1] < haOpenSmooth.iloc[-1]
        
        macdBullish = impulseMacdSeries.iloc[-1] > impulseSignalSeries.iloc[-1]
        macdBearish = impulseMacdSeries.iloc[-1] < impulseSignalSeries.iloc[-1]

        # 5. Market Structure Shift (MSS)
        # Validamos si ocurrió un MSS o ruptura estructural reciente en las últimas 8 velas
        mssBullish = detect_mss(df, direction="LARGO", lookback=8)
        mssBearish = detect_mss(df, direction="CORTO", lookback=8)

        direction = None
        
        # --- LÓGICA DE CONFLUENCIA ---
        if macroBullish and supertrendBullish and haBullish and macdBullish and mssBullish:
            direction = "LARGO"
            logger.info(f"[{symbol}] PremiumConfluence: Confluencia LARGO confirmada. Price > EMA200, Supertrend Verde, Heikin Ashi Verde, MACD Alcista, MSS Alcista.")
        elif macroBearish and supertrendBearish and haBearish and macdBearish and mssBearish:
            direction = "CORTO"
            logger.info(f"[{symbol}] PremiumConfluence: Confluencia CORTO confirmada. Price < EMA200, Supertrend Rojo, Heikin Ashi Rojo, MACD Bajista, MSS Bajista.")

        if not direction:
            return None
            
        logger.info(f"[{symbol}] ¡Señal detectada en PremiumConfluence! Dirección: {direction}")

        # Calcular distancia del Stop Loss (usamos el trailing de Supertrend o el swing estructural)
        slPrice = float(stTrail[-1])
        slDist = abs(currentPrice - slPrice)
        
        if slDist <= 0:
            return None

        # Calcular ATR de 14 periodos para filtros de sobre-extensión
        atrSeries = ta.ATR(df['high'].values.astype(float), df['low'].values.astype(float), df['close'].values.astype(float), timeperiod=14)
        if len(atrSeries) > 0 and not np.isnan(atrSeries[-1]):
            atrVal = float(atrSeries[-1])
        else:
            atrVal = 0.0

        if atrVal > 0:
            # Filtro Spike
            triggerCandleRange = float(df['high'].iloc[-1] - df['low'].iloc[-1])
            maxTriggerAtrMult = float(stratConfig.get('max_trigger_atr_mult', 2.0))
            if maxTriggerAtrMult <= 0:
                maxTriggerAtrMult = 2.0
            
            if triggerCandleRange > maxTriggerAtrMult * atrVal:
                logger.info(f"[{symbol}] PremiumConfluence: Señal descartada por vela de disparo gigante (Spike). Rango: {triggerCandleRange:.5f} > {maxTriggerAtrMult} * ATR")
                return None
                
            # Filtro Stop Loss sobre-extendido
            maxSlAtrMult = float(stratConfig.get('max_sl_atr_mult', 2.0))
            if maxSlAtrMult <= 0:
                maxSlAtrMult = 2.0
                
            if slDist > maxSlAtrMult * atrVal:
                logger.info(f"[{symbol}] PremiumConfluence: Señal descartada por Stop Loss sobre-extendido. Distancia SL: {slDist:.5f} > {maxSlAtrMult} * ATR")
                return None

        # Calcular precio del Take Profit en base a tipo de símbolo y minRrVal
        symbolType = symbolInfo.get('tipo', 'MONEDA').upper()
        if symbolType in ["MONEDA", "EXOTIC"]:
            # Para Forex calculamos en base a pips/SL dist y el minRrVal
            if direction == "LARGO":
                calculatedTp = currentPrice + (minRrVal * slDist)
            else:
                calculatedTp = currentPrice - (minRrVal * slDist)
        else:
            if direction == "LARGO":
                calculatedTp = currentPrice * (1.0 + tpPercent)
            else:
                calculatedTp = currentPrice * (1.0 - tpPercent)

        tpPrice = adjustTPForMinRR(currentPrice, slPrice, calculatedTp, direction, minRR=minRrVal)

        # Validaciones de salud y agotamiento
        isValid, _, _ = check_tp_exhaustion(
            df, len(df) - 5, currentPrice, tpPrice, slPrice, direction, threshold=0.60, timeframe="15min"
        )
        if not isValid:
            logger.info(f"[{symbol}] PremiumConfluence: Señal rechazada por agotamiento (exhaustion)")
            return None

        lastClosedCandle = get_last_closed_candle(datetime.now(ZoneInfo(TIMEZONE)), 15, df=df)
        candleTimeStr = (lastClosedCandle.name if hasattr(lastClosedCandle, 'name') else lastClosedCandle).strftime("%Y-%m-%d %H:%M:%S")
        
        isValid, _, _ = check_signal_health(
            currentPrice, tpPrice, slPrice, direction, currentPrice, threshold=0.65, candle_time=candleTimeStr
        )
        if not isValid:
            logger.info(f"[{symbol}] PremiumConfluence: Señal rechazada por filtro de salud")
            return None

        # --- Cálculo de Tamaño de Posición y Gestión de Riesgo ---
        from Sentinel.analysis import risk as riskAnalysis
        refCapital = symbolInfo.get('refCapital', 10000.0)
        refRiskPct = symbolInfo.get('refRiskPct', 1.0)
        
        size, riskUsdActual, marginUsed = riskAnalysis.calculatePositionSize(
            refCapital, refRiskPct, slDist, symbolInfo, entryPrice=currentPrice
        )
        
        if size is None or size <= 0:
            logger.info(f"[{symbol}] PremiumConfluence: Tamaño de posición inválido o margen insuficiente - saltando")
            return None

        rrVal = round(abs(tpPrice - currentPrice) / slDist, 2)
        expectedProfit = riskUsdActual * rrVal
        
        minUsdProfit = float(stratConfig.get('min_usd_profit', 10.0))
        if minUsdProfit < 6.0:
            minUsdProfit = 6.0
            
        if expectedProfit < minUsdProfit:
            logger.info(f"[{symbol}] PremiumConfluence: Beneficio Est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - descartando por orden insignificante")
            return None

        multiplier = getPipMultiplier(symbol)
        momentumState = symbolInfo.get('momentum', '☁️ SIN DATOS')
        momentumBonus, _ = momentum.getMomentumBonus(momentumState, direction)

        beTrigger = calculateBEPrice(currentPrice, slPrice, tpPrice, direction)
        
        return Signal(
            strategy="PremiumConfluence",
            symbol=symbol,
            direction=direction,
            entry_price=currentPrice,
            stop_loss=slPrice,
            take_profit=tpPrice,
            sl_distance=slDist,
            confidence=int(stratConfig.get('min_confidence', 80)) + momentumBonus,
            setup="Premium Confluence Alignment",
            status="EN ZONA ✅",
            candleTime=candleTimeStr,
            intervalo="15min",
            riesgo_pips=round(slDist * multiplier, 1),
            rr_ratio=rrVal,
            break_even=beTrigger,
            size=size,
            metadata={
                "supertrendPeriod": supertrendPeriod,
                "supertrendMultiplier": supertrendMultiplier,
                "haPeriod1": haPeriod1,
                "haPeriod2": haPeriod2,
                "macdLength": macdLength,
                "macdSignal": macdSignal,
                "tpPercent": tpPercent,
                "momentum": momentumState,
                "risk_usd": round(riskUsdActual, 2),
                "expected_profit": round(expectedProfit, 2),
                "margin_used": round(marginUsed, 2)
            }
        )
