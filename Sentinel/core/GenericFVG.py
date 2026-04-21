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
                min_gap_pct=0.0005,
                min_adx=20
            )

            if not latest_fvg:
                continue
            
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
            min_rr = float(strat_config.get('min_rr', 0.5))
            max_sl_proximity = 0.2
            
            # 1. Filtrar RR muy bajo
            if rr_ratio < min_rr:
                logger.info(f" RR={rr_ratio:.2f} < {min_rr} - descartando señal")
                continue

            
            # 2. Verificar que precio actual no esté muy cerca del SL
            if latest_fvg['type'] == 'Bullish_FVG':
                dist_to_sl = (sl - current_price) / risk_dist if risk_dist > 0 else 0
                if dist_to_sl < max_sl_proximity:
                    logger.info(f" Precio muy cerca del SL ({dist_to_sl:.2f}) - descartando")
                    continue
            else:
                dist_to_sl = (current_price - sl) / risk_dist if risk_dist > 0 else 0
                if dist_to_sl < max_sl_proximity:
                    logger.info(f" Precio muy cerca del SL ({dist_to_sl:.2f}) - descartando")
                    continue
            
            # 3. Verificar que precio actual esté dentro de la zona del FVG
            if latest_fvg['type'] == 'Bullish_FVG':
                if current_price <= sl or current_price >= tp1:
                    logger.info(f" Precio fuera de zona FVG - descartando")
                    continue
            else:
                if current_price >= sl or current_price <= tp1:
                    logger.info(f" Precio fuera de zona FVG - descartando")
                    continue

            # Marcar como enviada en RAM (el motor se encargará de persistir si es necesario)
            self._sent_signals[signal_key] = True
            
            # Crear objeto Signal
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
                confidence=85,
                setup=f"FVG {interval}",
                status=status_msg,
                candle_time=latest_fvg['timestamp'],
                intervalo=interval,
                metadata={
                    "fvg": latest_fvg['type'],
                    "tp2": tp2
                }
            )
            signals.append(signal)

        return signals


