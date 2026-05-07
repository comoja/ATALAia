import asyncio
import logging
import pandas as pd
from datetime import datetime
import pytz
import talib as ta

from Sentinel.core.models import Signal
from Sentinel.analysis import technical
from middleware.database import dbManager
from dataSymbol.mainOrchestrator import get_last_closed_candle
from middleware.utils.alertBuilder import getPipMultiplier, calculateBEPrice, adjustTPForMinRR

logger = logging.getLogger('sentinel')

class GenericFVGBot:
    """
    Bot evolucionado a SMC (Toni Maura/ICT) que busca re-tests de FVGs
    tras un Market Structure Shift (MSS) con objetivos estructurales.
    """
    def __init__(self, intervals=['5min', '15min', '1h', '4h']):
        self.strategy_name = "GenericFVG"
        self.intervals = intervals
        self._sent_signals = {}
        
    def _now_mx(self):
        return datetime.now(pytz.timezone('America/Mexico_City'))

    async def runAnalysisCycleForSymbol(self, symbolInfo, preloaded_data):
        """
        Analiza un símbolo en múltiples intervalos usando datos pre-cargados.
        """
        symbol = symbolInfo['symbol']
        logger.info(f"Analizando {symbol} en intervalos {self.intervals}")
        
        signals = []
        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        min_confidence = float(strat_config.get('min_confidence', 80))
        min_rr_val = float(strat_config.get('min_rr', 1.5))
        
        multiplier = getPipMultiplier(symbol)
        preloaded_master = preloaded_data.get(symbol)
        if preloaded_master is None:
            return []
            
        logger.info(f"Analizando {symbol} (Toni Maura SMC) en {self.intervals}")
        for interval in self.intervals:
            df = preloaded_master.get(interval)
            if df is None or len(df) < 200:
                continue
            
            # 1. Detectar FVGs
            fvgs = technical.detect_fvgs(df)
            if not fvgs:
                continue
            
            # Tomar el más reciente
            latest_fvg = fvgs[-1]
            signal_direction = "LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO"
            
            # --- FILTRO MAURA 1: MSS (Market Structure Shift) ---
            if not technical.detect_mss(df, signal_direction, lookback=15):
                continue

            # Evitar señales duplicadas
            signal_key = f"{symbol}_{interval}_{latest_fvg['timestamp']}"
            if signal_key in self._sent_signals:
                continue
            
            # 2. Calcular niveles SMC
            # --- FILTRO SEGURIDAD: Antigüedad del FVG (Máx 24h) ---
            fvg_time = pd.to_datetime(latest_fvg['timestamp'])
            ahora = self._now_mx().replace(tzinfo=None)
            fvg_time_naive = fvg_time.replace(tzinfo=None)
            age_hours = (ahora - fvg_time_naive).total_seconds() / 3600
            
            if age_hours > 24:
                # logger.debug(f"[{symbol}] {interval}: FVG demasiado antiguo ({age_hours:.1f}h) - saltando")
                continue

            # --- Cálculo de Niveles Centralizado (Maura SMC) ---
            atr = ta.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
            current_price = float(df['close'].iloc[-1])
            setup_fvg = technical.calculate_fvg_setup(latest_fvg, current_price, atr)
            
            entry_price = setup_fvg['entry']
            sl = setup_fvg['sl']
            signal_direction = setup_fvg['direction']
            sl_dist = setup_fvg['sl_dist']
            levels = technical.get_structural_levels(df, lookback=60)
            if signal_direction == "LARGO":
                tp_ref = max(levels['swing_high'], latest_fvg['gap_high'] * 1.001)
            else:
                tp_ref = min(levels['swing_low'], latest_fvg['gap_low'] * 0.999)
            
            tp1 = adjustTPForMinRR(entry_price, sl, tp_ref, signal_direction, minRR=min_rr_val)
            
            risk_dist = abs(entry_price - sl)
            reward_dist = abs(tp1 - entry_price)
            rr_ratio = reward_dist / risk_dist if risk_dist > 0 else 0
            
            # --- FILTRO SEGURIDAD: RR Máximo (Evitar errores de data) ---
            if rr_ratio > 15:
                # logger.info(f"[{symbol}] {interval}: RR={rr_ratio:.2f} irreal - descartando")
                continue

            # --- Health Check ---
            current_price = float(df['close'].iloc[-1])
            fvg_time = latest_fvg.get('candle_time', '')
            fvg_time_str = fvg_time.strftime("%Y-%m-%d %H:%M:%S") if fvg_time else ""
            
            is_healthy, progress_pct, reason = technical.check_signal_health(
                entry_price, tp1, sl, 
                "LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO",
                current_price,
                threshold=3.5, # Permitir hasta 350% (re-tests lejanos)
                candle_time=fvg_time_str
            )
            
            if not is_healthy:
                # Si el precio ya tocó SL no insistir (ya logueado en technical)
                if current_price >= sl if latest_fvg['type'] == 'Bearish_FVG' else current_price <= sl:
                    continue
                
                # Si ya avanzó demasiado (ej. 350% del tamaño del gap), descartar
                if progress_pct > 3.5:
                    continue
            
            # 3. Verificar que precio actual no haya invalidado el SL
            if latest_fvg['type'] == 'Bullish_FVG':
                if current_price <= sl:
                    logger.info(f"[{symbol}] {interval}: Precio tocó SL (price={current_price:.5f}, sl={sl:.5f}) - descartando")
                    continue
            else:
                if current_price >= sl:
                    logger.info(f"[{symbol}] {interval}: Precio tocó SL (price={current_price:.5f}, sl={sl:.5f}) - descartando")
                    continue
            
            # --- FILTRO: Ganancia Mínima Est. ---
            multiplier = getPipMultiplier(symbol)
            risk_usd = float(strat_config.get('risk_usd', 100.0))
            # Calcular size localmente para validación
            size = (risk_usd / (risk_dist * multiplier)) if (risk_dist > 0 and multiplier > 0) else 0
            
            min_usd_profit = float(strat_config.get('min_usd_profit', 10.0))
            rr_val = round(abs(tp1 - entry_price) / risk_dist, 2)
            expected_profit = (risk_dist * multiplier * size) * rr_val
            
            if expected_profit < min_usd_profit:
                logger.info(f"[{symbol}] {interval}: Beneficio Est. ${expected_profit:.2f} < ${min_usd_profit:.2f} - descartando")
                continue

            # 4. Filtrar por confianza mínima
            base_confidence = 85
            if base_confidence < min_confidence:
                logger.info(f"[{symbol}] {interval}: confidence={base_confidence} < min_confidence={min_confidence} - descartando")
                continue
            
            # 5. Filtrar por tendencia HTF (21 días con filtro de neutralidad)
            monthly_trend = symbolInfo.get('weekly_trend', 'NEUTRAL')
            signal_direction = "LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO"
            
            if monthly_trend == "BAJISTA" and signal_direction == "LARGO":
                logger.info(f"[{symbol}] {interval}: Señal LARGO descartada - tendencia mensual BAJISTA")
                continue
            elif monthly_trend == "ALCISTA" and signal_direction == "CORTO":
                logger.info(f"[{symbol}] {interval}: Señal CORTO descartada - tendencia mensual ALCISTA")
                continue
            
            # Marcar como enviada en RAM
            self._sent_signals[signal_key] = True
            
            # Crear objeto Signal
            # Usar la hora de la vela origen para trazabilidad
            origin_candle_time = latest_fvg.get('candle_time', get_last_closed_candle(self._now_mx(), 5))
            
            # Calcular Break Even inteligente
            be_trigger = calculateBEPrice(entry_price, sl, tp1, signal_direction)

            sig = Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction=signal_direction,
                entry_price=entry_price,
                stop_loss=sl,
                take_profit=tp1,
                sl_distance=risk_dist,
                confidence=base_confidence,
                setup=f"FVG {interval}",
                status="EN ZONA ✅",
                candleTime=origin_candle_time.strftime("%Y-%m-%d %H:%M:%S"),
                intervalo=interval,
                riesgo_pips=round(risk_dist * multiplier, 1),
                rr_ratio=rr_val,
                break_even=be_trigger,
                metadata={
                    "fvg": latest_fvg.get('type'),
                    "vela_origen": latest_fvg.get('timestamp')
                }
            )
            signals.append(sig)
            
        return signals
