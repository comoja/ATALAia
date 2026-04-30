import asyncio
import logging
import pandas as pd
from datetime import datetime
import pytz

from Sentinel.core.models import Signal
from Sentinel.analysis import technical
from middleware.database import dbManager
from dataSymbol.mainOrchestrator import get_last_closed_candle
from middleware.utils.alertBuilder import getPipMultiplier

logger = logging.getLogger('sentinel')

class GenericFVGBot:
    """
    Bot genérico que escanea múltiples intervalos buscando FVGs frescos
    y operables.
    """
    def __init__(self, intervals=['15min', '1h', '4h']):
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
        
        # Cargar multiplicador una vez por símbolo
        multiplier = getPipMultiplier(symbol)
        
        preloaded_master = preloaded_data.get(symbol)
        if preloaded_master is None:
            return []
            
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
            
            # Evitar señales duplicadas en el mismo bot (basado en timestamp del FVG)
            signal_key = f"{symbol}_{interval}_{latest_fvg['timestamp']}"
            if signal_key in self._sent_signals:
                continue
            
            # 2. Calcular niveles (Entry, TP, SL)
            # Entrada: Al tocar el borde del FVG (mitigación)
            entry_price = latest_fvg['gap_low'] if latest_fvg['type'] == 'Bullish_FVG' else latest_fvg['gap_high']
            tp1 = latest_fvg['gap_high'] if latest_fvg['type'] == 'Bullish_FVG' else latest_fvg['gap_low']
            
            # SL: Por debajo/encima de la mecha de la vela 1 (v1_low/v1_high)
            sl = latest_fvg['v1_low'] if latest_fvg['type'] == 'Bullish_FVG' else latest_fvg['v1_high']
            
            # Validar RR mínimo (usando TP1 como referencia conservadora)
            risk_dist = abs(entry_price - sl)
            reward_dist = abs(tp1 - entry_price)
            rr_ratio = reward_dist / risk_dist if risk_dist > 0 else 0
            
            if rr_ratio < 0.25: # Muy pequeño, buscar una estructura mayor?
                logger.info(f"[{symbol}] {interval}: RR={rr_ratio:.2f} < 0.25 - descartando señal")
                continue

            # --- Health Check ---
            current_price = float(df['close'].iloc[-1])
            fvg_time = latest_fvg.get('candle_time', '')
            fvg_time_str = fvg_time.strftime("%Y-%m-%d %H:%M:%S") if fvg_time else ""
            
            is_healthy, progress_pct, reason = technical.check_signal_health(
                entry_price, tp1, sl, 
                "LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO",
                current_price,
                threshold=0.65,
                candle_time=fvg_time_str
            )
            
            if not is_healthy:
                # Si el precio ya tocó SL no insistir
                if current_price >= sl if latest_fvg['type'] == 'Bearish_FVG' else current_price <= sl:
                    continue
                # Si ya avanzó mucho hacia el TP
                progress_pct = abs(current_price - entry_price) / reward_dist if reward_dist > 0 else 0
                max_progress = 0.65
                if progress_pct > max_progress:
                    logger.info(f"[{symbol}] {interval} [{latest_fvg['timestamp']}]: Progreso {progress_pct:.1%} > {max_progress:.0%} hacia TP", extra={"color": "yellow"})
                    logger.info(f"(SL={sl:.5f}, Entrada={entry_price:.5f}, TP={tp1:.5f}, Actual={current_price:.5f}) - descartando", extra={"color": "yellow"})
                    continue
            
            # 3. Verificar que precio actual esté dentro de la zona del FVG
            if latest_fvg['type'] == 'Bullish_FVG':
                if current_price <= sl or current_price >= tp1:
                    logger.info(f"[{symbol}] {interval}: Precio fuera de zona FVG (price={current_price:.5f}, sl={sl:.5f}, tp1={tp1:.5f}) - descartando")
                    continue
            else:
                if current_price >= sl or current_price <= tp1:
                    logger.info(f"[{symbol}] {interval}: Precio fuera de zona FVG (price={current_price:.5f}, sl={sl:.5f}, tp1={tp1:.5f}) - descartando")
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
                candleTime=origin_candle_time.strftime("%Y-%m-%d %H:%M:%S")
            )
            signals.append(sig)
            
        return signals
