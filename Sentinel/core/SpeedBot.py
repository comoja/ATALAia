import logging
import pandas as pd
import talib as ta
from datetime import datetime
from typing import Dict, Any, List, Optional

from middleware.database import dbManager
from Sentinel.analysis import technical
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateBEPrice
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class SpeedBot:
    """
    Estrategia de Impulso Institucional (Speed/Displacement).
    Busca velas con cuerpo extremo (>2.5x ATR) y entra a favor del movimiento 
    sin esperar retrocesos, ideal para capturar 'corridas' de liquidez.
    """
    
    def __init__(self, intervals=['5min']):
        self.strategy_name = "SpeedBot"
        self.intervals = intervals
        self._sent_signals = {} # {symbol_timestamp: True}

    def _now_mx(self):
        import pytz
        return datetime.now(pytz.timezone('America/Mexico_City'))

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None) -> List[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f" Analizando {symbol}...")
        preloadedMaster = preloadedData.get(symbol) if preloadedData else None
        if preloadedMaster is None: return []

        signals = []
        # Cargar parametros dinamicamente desde base de datos
        stratConfig = dbManager.getSymbolStrategyConfig(self.strategy_name, symbol) or {}
        minRr = float(stratConfig.get('min_rr', 1.5))
        atrMultTrigger = float(stratConfig.get('atr_mult_trigger', 1.4))
        bodyRatioThreshold = float(stratConfig.get('body_ratio_threshold', 0.78))
        confirmRatio = float(stratConfig.get('confirm_ratio', 0.50))
        
        for interval in self.intervals:
            df = preloadedMaster.get(interval)
            if df is None or len(df) < 50: continue

            # 1. Detectar Desplazamiento Extremo y Confirmado (Logica SMC Sostenida)
            # Exigimos dos velas consecutivas en la misma direccion, donde:
            # - La primera vela (prevCandle) es explosiva (>atrMultTriggerx ATR) y solida (>bodyRatioThreshold cuerpo/rango).
            # - La segunda vela (lastCandle, actual) confirma la direccion y recorre >= confirmRatio de la primera.
            dfFeat = df.tail(5).copy()
            atrSeries = ta.ATR(df['high'], df['low'], df['close'], 14).dropna()
            if atrSeries.empty: continue
            atr = atrSeries.iloc[-1]
            
            lastCandle = dfFeat.iloc[-1]
            prevCandle = dfFeat.iloc[-2]
            
            bodyLast = abs(lastCandle['close'] - lastCandle['open'])
            bodyPrev = abs(prevCandle['close'] - prevCandle['open'])
            rangePrev = prevCandle['high'] - prevCandle['low']
            
            # Validar la vela detonadora (prevCandle)
            isExplosive = bodyPrev > (atr * atrMultTrigger)
            isSolid = (bodyPrev / rangePrev) > bodyRatioThreshold if rangePrev > 0 else False
            
            # Validar la vela de confirmacion (lastCandle)
            sameDir = (lastCandle['close'] > lastCandle['open']) == (prevCandle['close'] > prevCandle['open'])
            isConfirmed = sameDir and (bodyLast >= bodyPrev * confirmRatio)
            
            if not (isExplosive and isSolid and isConfirmed):
                continue
                
            direction = "LARGO" if lastCandle['close'] > lastCandle['open'] else "CORTO"
            
            # Filtro e integracion inteligente del RSI basado en momentum (nivel 50) y agotamiento real extremo (85/15) centralizado
            rsiVal = lastCandle['rsi'] if 'rsi' in lastCandle else 50.0
            rsiOk, rsiReason = technical.check_rsi_momentum(rsiVal, direction)
            if not rsiOk:
                logger.info(f"[{symbol}] {interval}: Impulso {direction} descartado por {rsiReason}")
                continue
            
            # 2. Filtro de Momentum (No entrar contra tendencia)
            momentumState = symbolInfo.get('momentum', 'NEUTRAL')
            if direction == "LARGO" and momentumState == "BAJISTA": continue
            if direction == "CORTO" and momentumState == "ALCISTA": continue
            
            # 3. Evitar duplicados
            signalKey = f"{symbol}_{interval}_{df.index[-1]}"
            if signalKey in self._sent_signals: continue
            
            # 4. Calcular SL y TP con precision SMC
            entryPrice = float(lastCandle['close'])
            # SL por debajo/encima del extremo del impulso completo de 2 velas mas buffer ATR
            if direction == "LARGO":
                sl = min(float(lastCandle['low']), float(prevCandle['low'])) - (atr * 0.1)
            else:
                sl = max(float(lastCandle['high']), float(prevCandle['high'])) + (atr * 0.1)
                
            riskDist = abs(entryPrice - sl)
            if riskDist == 0: continue
            
            # Logica SMC Estricta: Buscar el primer Swing High/Low local previo al inicio del desplazamiento
            dfPrior = df.iloc[:-2]
            levels = technical.get_structural_levels(dfPrior, lookback=30)
            tpRef = levels['swing_high'] if direction == "LARGO" else levels['swing_low']
            
            # Si el TP estructural esta muy cerca o no existe, usar RR fijo
            tp1 = adjustTPForMinRR(entryPrice, sl, tpRef, direction, minRR=minRr)
            
            # 5. Crear Senal
            multiplier = getPipMultiplier(symbol)
            rrRatio = abs(tp1 - entryPrice) / riskDist
            
            sig = Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction=direction,
                entry_price=entryPrice,
                stop_loss=sl,
                take_profit=tp1,
                sl_distance=riskDist,
                confidence=85,
                setup=f"DESPLAZAMIENTO RAPIDO {interval}",
                status="IMPULSO",
                candleTime=df.index[-1].strftime("%Y-%m-%d %H:%M:%S"),
                intervalo=interval,
                riesgo_pips=round(riskDist * multiplier, 1),
                rr_ratio=round(rrRatio, 2),
                break_even=calculateBEPrice(entryPrice, sl, tp1, direction),
                metadata={
                    "atr_multiplier": round(bodyLast/atr, 2),
                    "momentum": momentumState
                }
            )
            
            signals.append(sig)
            self._sent_signals[signalKey] = True
            logger.info(f"[{symbol}] {interval}: ¡DESPLAZAMIENTO DETECTADO! RR={rrRatio:.2f}")

        return signals
