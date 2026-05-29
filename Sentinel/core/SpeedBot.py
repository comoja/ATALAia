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

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloaded_data: Dict = None) -> List[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f" Analizando {symbol}...")
        preloaded_master = preloaded_data.get(symbol) if preloaded_data else None
        if preloaded_master is None: return []

        signals = []
        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        min_rr = float(strat_config.get('min_rr', 1.5))
        
        for interval in self.intervals:
            df = preloaded_master.get(interval)
            if df is None or len(df) < 50: continue

            # 1. Detectar Desplazamiento Extremo y Confirmado (Lógica SMC Sostenida)
            # Exigimos dos velas consecutivas en la misma dirección, donde:
            # - La primera vela (prev_candle) es explosiva (>1.4x ATR) y sólida (>78% cuerpo/rango).
            # - La segunda vela (last_candle, actual) confirma la dirección y recorre >= 50% de la primera.
            df_feat = df.tail(5).copy()
            atr_series = ta.ATR(df['high'], df['low'], df['close'], 14).dropna()
            if atr_series.empty: continue
            atr = atr_series.iloc[-1]
            
            last_candle = df_feat.iloc[-1]
            prev_candle = df_feat.iloc[-2]
            
            body_last = abs(last_candle['close'] - last_candle['open'])
            body_prev = abs(prev_candle['close'] - prev_candle['open'])
            range_prev = prev_candle['high'] - prev_candle['low']
            
            # Validar la vela detonadora (prev_candle)
            is_explosive = body_prev > (atr * 1.4)
            is_solid = (body_prev / range_prev) > 0.78 if range_prev > 0 else False
            
            # Validar la vela de confirmación (last_candle)
            same_dir = (last_candle['close'] > last_candle['open']) == (prev_candle['close'] > prev_candle['open'])
            is_confirmed = same_dir and (body_last >= body_prev * 0.50)
            
            if not (is_explosive and is_solid and is_confirmed):
                continue
                
            direction = "LARGO" if last_candle['close'] > last_candle['open'] else "CORTO"
            
            # Filtro e integración inteligente del RSI basado en momentum (nivel 50) y agotamiento real extremo (85/15) centralizado
            rsi_val = last_candle['rsi'] if 'rsi' in last_candle else 50.0
            rsi_ok, rsi_reason = technical.check_rsi_momentum(rsi_val, direction)
            if not rsi_ok:
                logger.info(f"[{symbol}] {interval}: Impulso {direction} descartado por {rsi_reason}")
                continue
            
            # 2. Filtro de Momentum (No entrar contra tendencia)
            momentum_state = symbolInfo.get('momentum', 'NEUTRAL')
            if direction == "LARGO" and momentum_state == "BAJISTA": continue
            if direction == "CORTO" and momentum_state == "ALCISTA": continue
            
            # 3. Evitar duplicados
            signal_key = f"{symbol}_{interval}_{df.index[-1]}"
            if signal_key in self._sent_signals: continue
            
            # 4. Calcular SL y TP con precisión SMC
            entry_price = float(last_candle['close'])
            # SL por debajo/encima del extremo del impulso completo de 2 velas más buffer ATR
            if direction == "LARGO":
                sl = min(float(last_candle['low']), float(prev_candle['low'])) - (atr * 0.1)
            else:
                sl = max(float(last_candle['high']), float(prev_candle['high'])) + (atr * 0.1)
                
            risk_dist = abs(entry_price - sl)
            if risk_dist == 0: continue
            
            # Lógica SMC Estricta: Buscar el primer Swing High/Low local previo al inicio del desplazamiento
            df_prior = df.iloc[:-2]
            levels = technical.get_structural_levels(df_prior, lookback=30)
            tp_ref = levels['swing_high'] if direction == "LARGO" else levels['swing_low']
            
            # Si el TP estructural está muy cerca o no existe, usar RR fijo 1.5
            tp1 = adjustTPForMinRR(entry_price, sl, tp_ref, direction, minRR=min_rr)
            
            # 5. Crear Señal
            multiplier = getPipMultiplier(symbol)
            rr_ratio = abs(tp1 - entry_price) / risk_dist
            
            sig = Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction=direction,
                entry_price=entry_price,
                stop_loss=sl,
                take_profit=tp1,
                sl_distance=risk_dist,
                confidence=85,
                setup=f"DESPLAZAMIENTO RAPIDO {interval}",
                status="IMPULSO",
                candleTime=df.index[-1].strftime("%Y-%m-%d %H:%M:%S"),
                intervalo=interval,
                riesgo_pips=round(risk_dist * multiplier, 1),
                rr_ratio=round(rr_ratio, 2),
                break_even=calculateBEPrice(entry_price, sl, tp1, direction),
                metadata={
                    "atr_multiplier": round(body_last/atr, 2),
                    "momentum": momentum_state
                }
            )
            
            signals.append(sig)
            self._sent_signals[signal_key] = True
            logger.info(f"[{symbol}] {interval}: ¡DESPLAZAMIENTO DETECTADO! RR={rr_ratio:.2f}")

        return signals
