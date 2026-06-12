import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple
import pandas as pd
import numpy as np
import talib as ta

from middleware.database import dbManager
from Sentinel.analysis import technical
from Sentinel.analysis.technical import check_signal_health
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateBEPrice
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class BreakoutProbabilityBot:
    """
    Estrategia Breakout Probability con Impulse MACD.
    Calcula dinámicamente la probabilidad de que una ruptura de rango continúe con éxito,
    utilizando confluencia con el indicador Impulse MACD para ingresar al mercado.
    """
    def __init__(self):
        logger.info("Bot Breakout Probability iniciado")

    def calculateBreakoutProbability(
        self, 
        df: pd.DataFrame, 
        lookback: int = 300, 
        channelLen: int = 20, 
        targetAtrMult: float = 1.5,
        minRrVal: float = 1.5
    ) -> Tuple[float, float]:
        """
        Recorre el historial de velas hacia atrás para calcular la probabilidad estadística
        de que una ruptura alcista o bajista sea real y alcance el target esperado.
        Simula el ajuste de Take Profit por R:R mínimo real para alinear con ejecución.
        """
        try:
            n = len(df)
            if n < lookback + channelLen + 10:
                return 50.0, 50.0  # Retorno neutral si hay pocos datos

            # Obtener datos requeridos
            highs = df['high'].values
            lows = df['low'].values
            closes = df['close'].values
            atrs = df['atr'].values if 'atr' in df.columns else ta.ATR(highs, lows, closes, timeperiod=14)

            longAttempts = 0
            longSuccesses = 0
            shortAttempts = 0
            shortSuccesses = 0

            # Bucle histórico sobre la ventana de lookback
            startIndex = max(channelLen, n - lookback)
            for i in range(startIndex, n - 10):
                # Rango del canal local de las últimas N velas
                maxVal = np.max(highs[i - channelLen : i])
                minVal = np.min(lows[i - channelLen : i])
                atrVal = atrs[i]

                # Filtro de consolidación: evitar canales excesivamente anchos
                if (maxVal - minVal) > 2.0 * atrVal:
                    continue

                # Identificar ruptura en la vela i (cierre fuera del rango)
                isBullishBreak = closes[i] > maxVal
                isBearishBreak = closes[i] < minVal

                if isBullishBreak:
                    longAttempts += 1
                    slDist = closes[i] - minVal
                    if slDist <= 0:
                        continue
                    # Simular el mismo ajuste de TP por R:R mínimo que se usa en vivo
                    tpDist = max(targetAtrMult * atrVal, minRrVal * slDist)
                    targetPrice = closes[i] + tpDist
                    stopPrice = minVal
                    
                    success = False
                    for k in range(i + 1, min(i + 11, n)):
                        if highs[k] >= targetPrice:
                            success = True
                            break
                        if lows[k] <= stopPrice:
                            break
                    if success:
                        longSuccesses += 1

                elif isBearishBreak:
                    shortAttempts += 1
                    slDist = maxVal - closes[i]
                    if slDist <= 0:
                        continue
                    # Simular el mismo ajuste de TP por R:R mínimo que se usa en vivo
                    tpDist = max(targetAtrMult * atrVal, minRrVal * slDist)
                    targetPrice = closes[i] - tpDist
                    stopPrice = maxVal
                    
                    success = False
                    for k in range(i + 1, min(i + 11, n)):
                        if lows[k] <= targetPrice:
                            success = True
                            break
                        if highs[k] >= stopPrice:
                            break
                    if success:
                        shortSuccesses += 1

            # Calcular probabilidades porcentuales finales
            longProb = (longSuccesses / longAttempts * 100.0) if longAttempts > 0 else 50.0
            shortProb = (shortSuccesses / shortAttempts * 100.0) if shortAttempts > 0 else 50.0

            return round(longProb, 1), round(shortProb, 1)

        except Exception as e:
            logger.error(f"Error en calculateBreakoutProbability: {e}", exc_info=True)
            return 50.0, 50.0

    async def runAnalysisCycleForSymbol(
        self, 
        symbolInfo: Dict, 
        preloadedData: Dict = None, 
        apiKey: str = None
    ) -> Optional[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f"[{symbol}] Analizando con Breakout Probability...")

        master = preloadedData.get(symbol) if preloadedData else None
        
        # Usar la temporalidad de 15min por defecto para balancear ruido y probabilidad
        if isinstance(master, dict):
            df = master.get('15min') if master.get('15min') is not None else master.get('5min')
        else:
            df = master

        if df is None or len(df) < 100:
            logger.info(f"[{symbol}] Datos insuficientes para Breakout Probability (se requieren mínimo 100 velas)")
            return None

        # Asegurar que ATR esté en las columnas del DataFrame
        if 'atr' not in df.columns:
            df = df.copy()
            df['atr'] = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)

        # Cargar parámetros de configuración de la BD
        stratConfig = dbManager.getStrategyConfig("BreakoutProbability") or {}
        
        # Longitud del canal de consolidación (Donchian/Rangos)
        channelLen = int(stratConfig.get('start_hour', 20))  # Mapeamos a columna desocupada o genérica
        if channelLen <= 0 or channelLen > 100:
            channelLen = 20
            
        # Múltiplo de ATR para target de probabilidad
        targetAtrMult = float(stratConfig.get('min_rr', 1.5))
        if targetAtrMult <= 0:
            targetAtrMult = 1.5

        # Umbral de probabilidad mínima para validar la ruptura (default 60%)
        minProbThreshold = float(stratConfig.get('min_confidence', 60))
        if minProbThreshold <= 0:
            minProbThreshold = 60.0

        # Calcular canal dinámico del rango previo (excluyendo la vela actual cerrándose)
        currentMax = df['high'].iloc[-channelLen - 1 : -1].max()
        currentMin = df['low'].iloc[-channelLen - 1 : -1].min()

        currentClose = float(df['close'].iloc[-1])
        currentAtr = float(df['atr'].iloc[-1])

        # Validar si hubo ruptura en el cierre de la última vela
        isBullishBreak = currentClose > currentMax
        isBearishBreak = currentClose < currentMin

        if not (isBullishBreak or isBearishBreak):
            return None

        # Confluencia con Impulse MACD
        currentImpulse = df["impulseMacd"].iloc[-1] if "impulseMacd" in df.columns else 0.0
        currentSignal = df["impulseSignal"].iloc[-1] if "impulseSignal" in df.columns else 0.0
        macdAlcista = currentImpulse > currentSignal

        direction = None
        if isBullishBreak and macdAlcista:
            direction = "LARGO"
        elif isBearishBreak and not macdAlcista:
            direction = "CORTO"

        if not direction:
            logger.info(f"[{symbol}] Ruptura detectada pero descartada por contradicción con Impulse MACD")
            return None

        # Ajuste de Take Profit con R:R mínimo
        minRrVal = float(stratConfig.get('min_rr', 1.5))
        if minRrVal <= 0:
            minRrVal = 1.5

        # Calcular probabilidad de ruptura histórica
        longProb, shortProb = self.calculateBreakoutProbability(
            df=df, 
            lookback=300, 
            channelLen=channelLen, 
            targetAtrMult=targetAtrMult,
            minRrVal=minRrVal
        )

        currentProb = longProb if direction == "LARGO" else shortProb

        # Validar si la probabilidad supera el umbral exigido
        if currentProb < minProbThreshold:
            logger.info(f"[{symbol}] Ruptura {direction} descartada: Probabilidad {currentProb}% < Mínimo {minProbThreshold}%")
            return None

        logger.info(f"[{symbol}] ¡Señal confirmada de BreakoutProbability! Probabilidad: {currentProb}% | Dirección: {direction}")

        # Definir niveles de precio
        if direction == "LARGO":
            slPrice = currentMin
            calculatedTp = currentClose + (targetAtrMult * currentAtr)
        else:
            slPrice = currentMax
            calculatedTp = currentClose - (targetAtrMult * currentAtr)

        tpPrice = adjustTPForMinRR(currentClose, slPrice, calculatedTp, direction, minRR=minRrVal)

        slDist = abs(currentClose - slPrice)
        if slDist <= 0:
            return None

        # Validaciones de salud del trade
        lastClosedCandle = get_last_closed_candle(datetime.now(ZoneInfo(TIMEZONE)), 15, df=df)
        candleTimeStr = (lastClosedCandle.name if hasattr(lastClosedCandle, 'name') else lastClosedCandle).strftime("%Y-%m-%d %H:%M:%S")
        
        isHealthy, _, messageHealth = check_signal_health(
            currentClose, tpPrice, slPrice, direction, currentClose, threshold=0.65, candle_time=candleTimeStr
        )
        if not isHealthy:
            logger.info(f"[{symbol}] BreakoutProbability: Señal descartada por filtro de salud - {messageHealth}")
            return None

        # Tamaño de posición y riesgo
        from Sentinel.analysis import risk as riskAnalysis
        refCapital = symbolInfo.get('refCapital', 10000.0)
        refRiskPct = symbolInfo.get('refRiskPct', 1.0)
        
        size, riskUsdActual, marginUsed = riskAnalysis.calculatePositionSize(
            refCapital, refRiskPct, slDist, symbolInfo, entryPrice=currentClose
        )
        
        if size is None or size <= 0:
            logger.info(f"[{symbol}] BreakoutProbability: Tamaño de posición inválido o margen insuficiente")
            return None

        rrVal = round(abs(tpPrice - currentClose) / slDist, 2)
        expectedProfit = riskUsdActual * rrVal
        
        minUsdProfit = float(stratConfig.get('min_usd_profit', 10.0))
        if expectedProfit < minUsdProfit:
            logger.info(f"[{symbol}] BreakoutProbability: Beneficio est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - saltando")
            return None

        multiplier = getPipMultiplier(symbol)
        beTrigger = calculateBEPrice(currentClose, slPrice, tpPrice, direction)

        return Signal(
            strategy="BreakoutProbability",
            symbol=symbol,
            direction=direction,
            entry_price=currentClose,
            stop_loss=slPrice,
            take_profit=tpPrice,
            sl_distance=slDist,
            confidence=int(currentProb),
            setup=f"Prob {direction} Breakout",
            status="EN ZONA ✅",
            candleTime=candleTimeStr,
            intervalo="15min",
            riesgo_pips=round(slDist * multiplier, 1),
            rr_ratio=rrVal,
            break_even=beTrigger,
            size=size,
            metadata={
                "breakout_probability": currentProb,
                "impulse_macd": round(currentImpulse, 5),
                "channel_max": round(currentMax, 5),
                "channel_min": round(currentMin, 5),
                "risk_usd": round(riskUsdActual, 2),
                "expected_profit": round(expectedProfit, 2),
                "margin_used": round(marginUsed, 2)
            }
        )
