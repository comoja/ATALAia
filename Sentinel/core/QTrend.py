import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional
import pandas as pd
import numpy as np
import talib as ta

from middleware.database import dbManager
from Sentinel.analysis import technical
from Sentinel.analysis.technical import check_tp_exhaustion, check_signal_health
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateBEPrice
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal
from middleware.utils import momentum

logger = logging.getLogger("sentinel")

class QTrendBot:
    """
    Estrategia QTrendSuperTrendStrategy adaptada a Sentinel.
    Utiliza el canal de SuperTrend y el indicador Q-Trend (EMAs Rápidas/Lentas) en confluencia,
    con objetivos de Take Profit porcentuales fijos.
    """
    def __init__(self):
        logger.info("Bot QTrend iniciado")

    def calculateSuperTrend(self, df: pd.DataFrame, supertrendPeriod: int, supertrendMultiplier: float) -> tuple:
        """
        Calcula el canal de SuperTrend y el Trailing Stop dinámico delegando al módulo centralizado.
        """
        return technical.calculateAtrStop(df, supertrendPeriod, supertrendMultiplier)


    def calculateQTrend(self, df: pd.DataFrame, qtrendFast: int, qtrendSlow: int) -> tuple:
        """
        Calcula las EMAs rápidas y lentas para el indicador Q-Trend.
        """
        close = df['close'].values.astype(float)
        emaFast = ta.EMA(close, timeperiod=qtrendFast)
        emaSlow = ta.EMA(close, timeperiod=qtrendSlow)
        
        # Convertir a Series para manejo seguro de nulos
        emaFastSeries = pd.Series(emaFast).ffill().bfill().values
        emaSlowSeries = pd.Series(emaSlow).ffill().bfill().values
        
        return emaFastSeries, emaSlowSeries

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f"[{symbol}] Analizando con QTrend...")
        
        master = preloadedData.get(symbol) if preloadedData else None
        
        # Obtener el timeframe de operación (15min preferido)
        if isinstance(master, dict):
            df = master.get('15min') if master.get('15min') is not None else master.get('5min')
        else:
            df = master

        if df is None or len(df) < 50:
            logger.info(f"[{symbol}] Datos insuficientes para QTrend")
            return None

        # Cargar parámetros de configuración de la BD (con soporte para parámetros dinámicos por símbolo)
        stratConfig = dbManager.getSymbolStrategyConfig("QTrend", symbol) or {}
        
        # Parámetros del SuperTrend
        supertrendPeriod = int(stratConfig.get('supertrendPeriod') or stratConfig.get('supertrend_period') or 
                               stratConfig.get('max_minutos_fvg', 10))
        if supertrendPeriod > 100 or supertrendPeriod <= 0:
            supertrendPeriod = 10
            
        supertrendMultiplier = float(stratConfig.get('supertrendMultiplier') or stratConfig.get('supertrend_multiplier') or 
                                     stratConfig.get('min_rr', 3.0))
        if supertrendMultiplier <= 0:
            supertrendMultiplier = 3.0
            
        # Parámetros de Q-Trend (EMAs)
        qtrendFast = int(stratConfig.get('qtrendFast') or stratConfig.get('qtrend_fast') or 
                         stratConfig.get('start_hour', 9))
        if qtrendFast <= 0:
            qtrendFast = 9
            
        qtrendSlow = int(stratConfig.get('qtrendSlow') or stratConfig.get('qtrend_slow') or 
                         stratConfig.get('max_minutos_signal', 21))
        if qtrendSlow <= 0:
            qtrendSlow = 21
            
        # Parámetro de Take Profit (guardado como 2.5% -> 2.5 en la base de datos)
        tpPercentVal = float(stratConfig.get('tpPercent') or stratConfig.get('tp_percent') or 
                             stratConfig.get('max_rr', 2.5))
        # Si viene de la configuración global anterior (max_rr), suele dividirse por 100
        if 'tpPercent' not in stratConfig and 'tp_percent' not in stratConfig:
            tpPercent = tpPercentVal / 100.0
        else:
            # Si el valor de tpPercent está en formato porcentual (ej. 2.5), lo dividimos por 100.0
            tpPercent = tpPercentVal / 100.0 if tpPercentVal > 0.5 else tpPercentVal
            
        if tpPercent <= 0:
            tpPercent = 0.025

        # Calcular indicadores
        stTrend, stTrail = self.calculateSuperTrend(df, supertrendPeriod, supertrendMultiplier)
        emaFast, emaSlow = self.calculateQTrend(df, qtrendFast, qtrendSlow)
        
        if len(stTrend) < 3 or len(emaFast) < 3:
            return None

        currentPrice = float(df['close'].iloc[-1])
        
        # Definición de estados actuales
        supertrendBullish = stTrend[-1] == 1
        supertrendBearish = stTrend[-1] == -1
        
        qtrendBullish = emaFast[-1] > emaSlow[-1]
        qtrendBearish = emaFast[-1] < emaSlow[-1]

        # Calcular antigüedad del crossover de SuperTrend alcista/bajista
        stBullishCrossAge = -1
        stBearishCrossAge = -1
        for age in range(0, 10):
            idx = len(stTrend) - 1 - age
            if idx > 0:
                if stTrend[idx - 1] == -1 and stTrend[idx] == 1:
                    stBullishCrossAge = age
                    break
        for age in range(0, 10):
            idx = len(stTrend) - 1 - age
            if idx > 0:
                if stTrend[idx - 1] == 1 and stTrend[idx] == -1:
                    stBearishCrossAge = age
                    break

        # Calcular antigüedad del crossover de QTrend (EMAs) alcista/bajista
        qtrendBullishCrossAge = -1
        qtrendBearishCrossAge = -1
        for age in range(0, 10):
            idx = len(emaFast) - 1 - age
            if idx > 0:
                if emaFast[idx - 1] <= emaSlow[idx - 1] and emaFast[idx] > emaSlow[idx]:
                    qtrendBullishCrossAge = age
                    break
        for age in range(0, 10):
            idx = len(emaFast) - 1 - age
            if idx > 0:
                if emaFast[idx - 1] >= emaSlow[idx - 1] and emaFast[idx] < emaSlow[idx]:
                    qtrendBearishCrossAge = age
                    break

        direction = None
        isBrokerEnabled = bool(symbolInfo.get('broker', 0))
        maxCrossAge = 3
        
        # --- LÓGICA DE ENTRADA (CONFLUENCIA DE AMBOS INDICADORES CON RESTRICCIÓN TEMPORAL) ---
        if supertrendBullish and qtrendBullish:
            # Validamos que ambos crossovers hayan ocurrido recientemente (dentro de la ventana maxCrossAge)
            if (0 <= stBullishCrossAge <= maxCrossAge) and (0 <= qtrendBullishCrossAge <= maxCrossAge):
                direction = "LARGO"
                logger.info(f"[{symbol}] QTrend: Confluencia LARGO confirmada. Antigüedad ST Cross: {stBullishCrossAge} velas, QTrend Cross: {qtrendBullishCrossAge} velas")
            else:
                logger.info(f"[{symbol}] QTrend: Señal LARGO ignorada por desincronización o madurez de tendencia. Antigüedad ST Cross: {stBullishCrossAge} velas, QTrend Cross: {qtrendBullishCrossAge} velas")
                
        elif supertrendBearish and qtrendBearish:
            # Validamos que ambos crossovers hayan ocurrido recientemente
            if (0 <= stBearishCrossAge <= maxCrossAge) and (0 <= qtrendBearishCrossAge <= maxCrossAge):
                direction = "CORTO"
                logger.info(f"[{symbol}] QTrend: Confluencia CORTO confirmada. Antigüedad ST Cross: {stBearishCrossAge} velas, QTrend Cross: {qtrendBearishCrossAge} velas")
            else:
                logger.info(f"[{symbol}] QTrend: Señal CORTO ignorada por desincronización o madurez de tendencia. Antigüedad ST Cross: {stBearishCrossAge} velas, QTrend Cross: {qtrendBearishCrossAge} velas")
            
        if not direction:
            return None
            
        # --- FILTRO DE CONTRADICCIÓN DE IMPULSE MACD ---
        if "impulseMacd" in df.columns and "impulseSignal" in df.columns:
            currentImpulse = df["impulseMacd"].iloc[-1]
            currentSignal = df["impulseSignal"].iloc[-1]
            macdAlcista = currentImpulse > currentSignal
            
            if direction == "LARGO" and not macdAlcista:
                logger.info(f"[{symbol}] QTrend: Señal LARGO descartada por contradicción con Impulse MACD (bajista)")
                return None
            elif direction == "CORTO" and macdAlcista:
                logger.info(f"[{symbol}] QTrend: Señal CORTO descartada por contradicción con Impulse MACD (alcista)")
                return None
            
        logger.info(f"[{symbol}] ¡Señal detectada en QTrend! Dirección: {direction}")

        # Calcular niveles de trade
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
            # 1. Filtro de vela Spike (vela de disparo anormalmente grande)
            triggerCandleRange = float(df['high'].iloc[-1] - df['low'].iloc[-1])
            maxTriggerAtrMult = float(stratConfig.get('max_trigger_atr_mult', 2.0))
            if maxTriggerAtrMult <= 0:
                maxTriggerAtrMult = 2.0
            
            if triggerCandleRange > maxTriggerAtrMult * atrVal:
                logger.info(f"[{symbol}] QTrend: Señal {direction} descartada por vela de disparo gigante (Spike). Rango de vela: {triggerCandleRange:.5f} > {maxTriggerAtrMult} * ATR ({maxTriggerAtrMult * atrVal:.5f})")
                return None
                
            # 2. Filtro de Stop Loss sobre-extendido (distancia de stop loss muy grande)
            maxSlAtrMult = float(stratConfig.get('max_sl_atr_mult', 2.0))
            if maxSlAtrMult <= 0:
                maxSlAtrMult = 2.0
                
            if slDist > maxSlAtrMult * atrVal:
                logger.info(f"[{symbol}] QTrend: Señal {direction} descartada por Stop Loss sobre-extendido. Distancia SL: {slDist:.5f} > {maxSlAtrMult} * ATR ({maxSlAtrMult * atrVal:.5f})")
                return None

        minRrVal = float(stratConfig.get('min_rr', 1.5))
        if minRrVal <= 0:
            minRrVal = 1.5

        symbolType = symbolInfo.get('tipo', 'MONEDA').upper()
        if symbolType in ["MONEDA", "EXOTIC"]:
            # Para Forex, el TP de 1.5% de precio es excesivo. Calculamos con base en la distancia del SL y el minRrVal.
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

        # Validaciones de salud y agotamiento de la señal
        isValid, _, messageExhaustion = check_tp_exhaustion(
            df, len(df) - 5, currentPrice, tpPrice, slPrice, direction, threshold=0.60, timeframe="15min"
        )
        if not isValid:
            logger.info(f"[{symbol}] QTrend: Señal rechazada por agotamiento (exhaustion)")
            return None

        lastClosedCandle = get_last_closed_candle(datetime.now(ZoneInfo(TIMEZONE)), 15, df=df)
        candleTimeStr = (lastClosedCandle.name if hasattr(lastClosedCandle, 'name') else lastClosedCandle).strftime("%Y-%m-%d %H:%M:%S")
        
        isValid, _, messageHealth = check_signal_health(
            currentPrice, tpPrice, slPrice, direction, currentPrice, threshold=0.65, candle_time=candleTimeStr
        )
        if not isValid:
            logger.info(f"[{symbol}] QTrend: Señal rechazada por filtro de salud")
            return None

        # --- Cálculo de Tamaño de Posición y Gestión de Riesgo ---
        from Sentinel.analysis import risk as riskAnalysis
        refCapital = symbolInfo.get('refCapital', 10000.0)
        refRiskPct = symbolInfo.get('refRiskPct', 1.0)
        
        size, riskUsdActual, marginUsed = riskAnalysis.calculatePositionSize(
            refCapital, refRiskPct, slDist, symbolInfo, entryPrice=currentPrice
        )
        
        if size is None or size <= 0:
            logger.info(f"[{symbol}] QTrend: Tamaño de posición inválido o margen insuficiente - saltando")
            return None

        rrVal = round(abs(tpPrice - currentPrice) / slDist, 2)
        expectedProfit = riskUsdActual * rrVal
        
        minUsdProfit = float(stratConfig.get('min_usd_profit', 10.0))
        if minUsdProfit < 6.0:
            minUsdProfit = 6.0
            
        if expectedProfit < minUsdProfit:
            logger.info(f"[{symbol}] QTrend: Beneficio Est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - descartando señal por órdenes de centavos")
            return None

        multiplier = getPipMultiplier(symbol)
        momentumState = symbolInfo.get('momentum', '☁️ SIN DATOS')
        momentumBonus, _ = momentum.getMomentumBonus(momentumState, direction)

        beTrigger = calculateBEPrice(currentPrice, slPrice, tpPrice, direction)
        
        return Signal(
            strategy="QTrend",
            symbol=symbol,
            direction=direction,
            entry_price=currentPrice,
            stop_loss=slPrice,
            take_profit=tpPrice,
            sl_distance=slDist,
            confidence=int(stratConfig.get('min_confidence', 70)) + momentumBonus,
            setup="QTrend Crossing",
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
                "qtrendFast": qtrendFast,
                "qtrendSlow": qtrendSlow,
                "tpPercent": tpPercent,
                "momentum": momentumState,
                "risk_usd": round(riskUsdActual, 2),
                "expected_profit": round(expectedProfit, 2),
                "margin_used": round(marginUsed, 2)
            }
        )
