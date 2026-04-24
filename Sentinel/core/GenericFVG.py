import logging
import pandas as pd
import pytz
from datetime import datetime
from typing import Dict, List, Any
from zoneinfo import ZoneInfo

import os
import sys

# Path setup
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbManager
from Sentinel.analysis import technical, risk
from middleware.execution.broker_gateway import gateway
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from middleware.utils.alertBuilder import getPipMultiplier


from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class GenericFVGBot:
    """
    Bot especializado en la detección de FVGs (Fair Value Gaps) en múltiples temporalidades
    de forma continua durante todo el día.
    """

    def __init__(self):
        self.intervals = ['15min', '1h', '4h']
        self._sent_signals = {}
        self.strategy_name = "GenericFVG"
        
        logger.info(f"GenericFVGBot iniciado para intervalos: {self.intervals}")

    def getMexicoTime(self) -> datetime:
        return datetime.now(pytz.timezone(TIMEZONE))

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None) -> List[Signal]:
        """Analiza un símbolo en todas las temporalidades configuradas y devuelve una lista de señales."""
        symbol = symbolInfo['symbol']
        logger.info(f"Analizando {symbol} en intervalos {self.intervals}")
        signals = []
        
        # Punto 3: Master Dictionary integration
        master = preloadedData.get(symbol) if preloadedData else None
        
        if not master or '5min' not in master:
            logger.warning(f"[{symbol}] Sin datos base (5min) para GenericFVG")
            return []
            
        df_5m = master.get('5min')

        for interval in self.intervals:
            df_tf = master.get(interval)
            
            # Si no hay master o no tiene el intervalo, detect_fvg_closed hará el resampleo
            latest_fvg = technical.detect_fvg_closed(
                df_source=df_tf if df_tf is not None else df_5m,
                interval=interval,
                min_gap_pct=0.0001,  # Reducido para detectar FVGs más pequeños
                min_adx=15  # Reducido para no perder señales
            )

            if not latest_fvg:
                continue
            
            logger.info(f"[{symbol}] {interval}: FVG detectado - {latest_fvg['type']} @ idx={latest_fvg['idx']}")
            
            # Control de duplicados usando el timestamp del FVG
            signal_key = f"{symbol}_{interval}_{latest_fvg['timestamp']}"
            if signal_key in self._sent_signals:
                logger.debug(f" Señal ya enviada en RAM: {signal_key}")
                continue
            
            # Usar el dataframe de la temporalidad correspondiente
            df = df_tf if df_tf is not None else technical.resample_to_interval(df_5m, interval)
            if len(df) < 3:
                continue
            
            # El idx del FVG es respecto al df resampleado
            last_closed_idx = len(df) - 2
            if latest_fvg['idx'] > last_closed_idx:
                logger.debug(f" FVG idx={latest_fvg['idx']} > last_closed_idx={last_closed_idx} - descartado")
                continue
            
            v1_idx = latest_fvg['idx'] - 1
            if v1_idx < 0 or v1_idx >= len(df):
                continue
            
            vela1 = df.iloc[v1_idx]
            structural = technical.get_structural_levels(df, lookback=30)
            
            entry_price = float(df['close'].iloc[-2])
            sl = 0
            tp1 = 0
            
            if latest_fvg['type'] == 'Bullish_FVG':
                sl = float(vela1['low'])
                tp1 = structural['swing_high']
            else:
                sl = float(vela1['high'])
                tp1 = structural['swing_low']
            
            risk_dist = abs(entry_price - sl)
            if risk_dist == 0: 
                continue
            
            if latest_fvg['type'] == 'Bullish_FVG':
                if tp1 <= entry_price: tp1 = entry_price + (risk_dist * 1.5)
            else:
                if tp1 >= entry_price: tp1 = entry_price - (risk_dist * 1.5)

            tp2 = entry_price + (risk_dist * 2) if latest_fvg['type'] == 'Bullish_FVG' else entry_price - (risk_dist * 2)

            fvg_mid = float(latest_fvg['mid'])
            total_path = abs(tp1 - fvg_mid)
            if total_path == 0: total_path = 0.001
            
            if latest_fvg['type'] == 'Bullish_FVG':
                progress_pct = (entry_price - fvg_mid) / total_path
            else:
                progress_pct = (fvg_mid - entry_price) / total_path
                
            if progress_pct >= 1.0:
                status_msg = "META ALCANZADA 🚨"
            elif progress_pct > 0.5:
                status_msg = "ALEJÁNDOSE ⚠️"
            else:
                status_msg = "EN ZONA ✅"

# ===== FILTROS DE CALIDAD =====
            from middleware.database import dbManager
            strat_config = dbManager.getStrategyConfig("GenericFVG") or {}
            
            current_price = float(df['close'].iloc[-1])
            rr_ratio = round(abs(tp1 - entry_price) / risk_dist, 2)
            min_rr = float(strat_config.get('min_rr', 0.35))
            min_confidence = float(strat_config.get('min_confidence', 70))
            max_sl_proximity = 0.2
            
            # 1. Filtrar RR muy bajo
            if rr_ratio < min_rr:
                logger.info(f"[{symbol}] {interval}: RR={rr_ratio:.2f} < {min_rr} - descartando señal")
                continue

            
            # 2. Verificar que precio actual no esté muy cerca del SL
            #    y que no haya recorrido más del 65% hacia el TP
            max_progress = 0.65
            
            if latest_fvg['type'] == 'Bullish_FVG':
                # Para LONG: descartar si precio está por debajo del SL (se acercó al SL)
                # o si ya avanzó más del 65% hacia el TP
                dist_to_sl = (current_price - sl) / risk_dist if risk_dist > 0 else 0
                if current_price <= sl:
                    logger.info(f"[{symbol}] {interval}: Precio bajo SL (price={current_price:.5f}, sl={sl:.5f}) - descartando")
                    continue
                if progress_pct > max_progress:
                    logger.info(f"[{symbol}] {interval} [{latest_fvg['timestamp']}]: Progreso {progress_pct:.1%} > {max_progress:.0%} hacia TP (entry={entry_price:.5f}, tp1={tp1:.5f}, current={current_price:.5f}) - descartando")
                    continue
            else:
                # Para SHORT: descartar si precio está por encima del SL (se acercó al SL)
                # o si ya avanzó más del 65% hacia el TP
                dist_to_sl = (sl - current_price) / risk_dist if risk_dist > 0 else 0
                if current_price >= sl:
                    logger.info(f"[{symbol}] {interval} [{latest_fvg['timestamp']}]: Precio sobre SL (price={current_price:.5f}, sl={sl:.5f}) - descartando")
                    continue
                if progress_pct > max_progress:
                    logger.info(f"[{symbol}] {interval} [{latest_fvg['timestamp']}]: Progreso {progress_pct:.1%} > {max_progress:.0%} hacia TP (entry={entry_price:.5f}, tp1={tp1:.5f}, current={current_price:.5f}) - descartando")
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
            
            # 4. Filtrar por confianza mínima
            base_confidence = 85
            if base_confidence < min_confidence:
                logger.info(f"[{symbol}] {interval}: confidence={base_confidence} < min_confidence={min_confidence} - descartando")
                continue
            
            # 5. Filtrar por tendencia mensual
            monthly_trend = symbolInfo.get('weekly_trend', 'NEUTRAL')
            signal_direction = "LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO"
            
            if monthly_trend == "BAJISTA" and signal_direction == "LARGO":
                logger.info(f"[{symbol}] {interval}: Señal LARGO descartada - tendencia mensual BAJISTA")
                continue
            elif monthly_trend == "ALCISTA" and signal_direction == "CORTO":
                logger.info(f"[{symbol}] {interval}: Señal CORTO descartada - tendencia mensual ALCISTA")
                continue
            
            # Marcar como enviada en RAM (el motor se encargará de persistir si es necesario)
            self._sent_signals[signal_key] = True
            
            # Crear objeto Signal con confianza calculada
            calc_confidence = 85 - (0 if latest_fvg['type'] == 'Bullish_FVG' else 0)  # Placeholder para ajustes futuros
            
            signal = Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction="LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO",
                entry_price=entry_price,
                stop_loss=sl,
                take_profit=tp1,
                sl_distance=risk_dist,
                riesgo_pips=round(risk_dist * getPipMultiplier(symbol), 1),
                rr_ratio=round(abs(tp1 - entry_price) / risk_dist, 2),
                confidence=calc_confidence,
                setup=f"FVG {interval}",
                status=status_msg,
                candleTime=latest_fvg['timestamp'],
                intervalo=interval,
                metadata={
                    "fvg": latest_fvg['type'],
                    "tp2": tp2
                }
            )
            signals.append(signal)

        return signals


