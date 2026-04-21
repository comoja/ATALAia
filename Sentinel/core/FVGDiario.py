"""
Estrategia FVG Diario - Manipulación de Liquidez + Daily Bias

Esta estrategia identifica escenarios de alta probabilidad siguiendo:
1. Daily Bias: Identificar tendencia diaria (alcista/bajista)
2. Manipulación: Detectar cuando el precio supera PDH/PDL
3. Confirmación: FVG en 15m/1h tras el rompimiento
4. Entry: Retest del FVG formado

Autor: Sentinel Trading System
"""
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from middleware.database import dbManager
from middleware.execution.broker_gateway import gateway
from middleware.utils import momentum
from Sentinel.analysis import technical, risk
from Sentinel.analysis.technical import check_tp_exhaustion
from middleware.utils.alertBuilder import getPipMultiplier

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")


class FVGDiarioBot:
    """
    Estrategia de trading basada en manipulación de liquidez del día anterior y continuación del Daily Bias.
    """
    
    def __init__(self, config: Dict = None):
        self.strategy_name = "FVGDiario"
        
        # Configuración por defecto
        self.config = {
            # Temporalidades a analizar
            "timeframes": ["15min", "1H", "4H", "1D"],
            
            # Parámetros de Daily Bias
            "daily_bullish_threshold": 0.0,  # close > open = bullish
            "min_adx_filter": 20.0,  # Filtrar mercados laterales
            
            # Parámetros de manipulación
            "manipulation_sessions": ["asian", "london", "ny"],  # Sesiones a monitorear
            
            # Parámetros de FVG
            "min_fvg_pips": 5,  # Mínimo 5 pips para FVG válido
            
            # Gestión de riesgo
            "min_rr_ratio": 2.0,  # Mínimo 1:2 R:R
            "max_rr_ratio": 4.0,  # Objetivo 1:4
            
            # Niveles a visualizar
            "show_pdh_pdl": True,
            
            # Alertas
            "alert_on_manipulation": True,
            "alert_on_fvg": True,
        }
        
        if config:
            self.config.update(config)
        
        self._sent_signals = {}
        
        logger.info(f"🤖 {self.strategy_name} iniciado")
    
    async def runAnalysisCycleForSymbol(self, symbolData: Dict, preloadedData: Dict = None) -> Optional[Signal]:
        """Analiza un símbolo en busca de señales y devuelve un objeto Signal si existe."""
        symbol = symbolData['symbol']
        logger.info(f"▶ Iniciando análisis para {symbol}")
        
        try:
            # Punto 3: Master Dictionary integration
            master = preloadedData.get(symbol) if preloadedData else None
            
            if isinstance(master, dict):

                df_intraday = master.get('15min')
                df_daily = master.get('1d')
            else:
                df_intraday = df_input
                df_daily = technical.resample_to_interval(df_input, '1D') if (df_input is not None and len(df_input) > 100) else None

            if df_intraday is None or len(df_intraday) < 20:
                logger.info(f"[{symbol}] Datos 15m insuficientes")
                return None
            
            if df_daily is None or len(df_daily) < 2:
                logger.info(f"[{symbol}] Datos diarios insuficientes para Daily Bias")
                return None

            
            # Identificar Daily Bias
            daily_bias = self._get_daily_bias(df_daily)
            
            # Obtener PDH y PDL del día anterior
            pdh, pdl = self._get_pdh_pdl(df_daily)
            
            # Obtener nivel de liquidez opuesto (para TP)
            opposite_liquidity = pdl if daily_bias == "BULLISH" else pdh
            
            # Buscar manipulación
            manipulation = self._detect_manipulation(
                df_intraday, pdh, pdl, daily_bias
            )
            
            if not manipulation:
                return None
            
            # Verificar cambio de estructura tras manipulación
            if not self._check_market_structure_shift( df_intraday, manipulation, daily_bias):
                return None
            
            # Buscar FVG tras manipulación
            fvg = self._find_fvg_after_manipulation(df_intraday, manipulation, daily_bias)
            
            if not fvg:
                return None
            
            # Generar señal
            return await self._generate_signal(symbolData, daily_bias, pdh, pdl, manipulation, fvg, opposite_liquidity, df_intraday)
            
        except Exception as e:
            logger.error(f" Error análisis {symbol}: {e}")
            return None
    
    def _get_daily_bias(self, df_daily: pd.DataFrame) -> str:
        """Determina el Daily Bias basándose en el día anterior completo."""
        if df_daily is None or len(df_daily) < 2:
            return "NEUTRAL"
        
        last_candle = df_daily.iloc[-2]
        close = last_candle['close']
        open_price = last_candle['open']
        
        body = close - open_price
        high_val = last_candle.get('high', last_candle.get('High', 0))
        low_val = last_candle.get('low', last_candle.get('Low', 0))
        total_range = high_val - low_val
        
        if total_range == 0:
            return "NEUTRAL"
        
        if body / total_range >= 0.5 and body > 0:
            return "BULLISH"
        elif body / total_range >= 0.5 and body < 0:
            return "BEARISH"
        
        return "NEUTRAL"
    
    def _get_pdh_pdl(self, df_daily: pd.DataFrame) -> tuple:
        """Obtiene el Previous Day High (PDH) y Previous Day Low (PDL)."""
        if df_daily is None or len(df_daily) < 2:
            return None, None
        
        prev_day = df_daily.iloc[-2]
        pdh = float(prev_day.get('high', prev_day.get('High', 0)))
        pdl = float(prev_day.get('low', prev_day.get('Low', 0)))
        
        return pdh, pdl
    
    def _detect_manipulation(self, df: pd.DataFrame, pdh: float, pdl: float, daily_bias: str) -> Optional[Dict]:
        """Detecta manipulación del nivel diario."""
        if df is None or len(df) < 10:
            return None
        
        recent = df.tail(10)
        
        if daily_bias == "BEARISH":
            for i in range(len(recent) - 1, -1, -1):
                row_high = recent.iloc[i].get('high', recent.iloc[i].get('High', 0))
                if row_high >= pdh:
                    return {"type": "MANIPULATION_UP", "level": pdh, "index": i, "timestamp": str(recent.index[i]), "price": float(row_high)}
        
        elif daily_bias == "BULLISH":
            for i in range(len(recent) - 1, -1, -1):
                row_low = recent.iloc[i].get('low', recent.iloc[i].get('Low', 0))
                if row_low <= pdl:
                    return {"type": "MANIPULATION_DOWN", "level": pdl, "index": i, "timestamp": str(recent.index[i]), "price": float(row_low)}
        
        return None
    
    def _check_market_structure_shift(self, df: pd.DataFrame, manipulation: Dict, daily_bias: str) -> bool:
        """Verifica si hay un Market Structure Shift (MSS) tras la manipulación."""
        if df is None or manipulation is None:
            return False
        
        manip_idx = manipulation['index']
        if manip_idx + 2 >= len(df):
            return False
        
        post_manip = df.iloc[manip_idx + 1:]
        if len(post_manip) < 2:
            return False
        
        if daily_bias == "BEARISH":
            recent_lows = post_manip['low'].values
            for low in recent_lows:
                if low < manipulation['level']:
                    return True
        elif daily_bias == "BULLISH":
            col_high = 'high' if 'high' in post_manip.columns else 'High'
            recent_highs = post_manip[col_high].values if col_high in post_manip.columns else []
            for high in recent_highs:
                if high > manipulation['level']:
                    return True
        
        return False
    
    def _find_fvg_after_manipulation(self, df: pd.DataFrame, manipulation: Dict, daily_bias: str) -> Optional[Dict]:
        """Busca un Fair Value Gap Formación después de la manipulación."""
        if df is None or manipulation is None:
            return None
        
        manip_idx = manipulation['index']
        start_idx = max(0, manip_idx + 2)
        end_idx = min(len(df), start_idx + 5)
        
        if end_idx - start_idx < 3:
            return None
        
        df_after = df.iloc[start_idx:end_idx]
        latest_fvg = technical.detect_fvg_closed(df_source=df_after, interval='15min', min_gap_pct=0.0005, min_adx=20, lookback=10)
        
        if not latest_fvg:
            return None
        
        if daily_bias == "BULLISH" and latest_fvg.get("type") == "Bullish_FVG":
            return latest_fvg
        elif daily_bias == "BEARISH" and latest_fvg.get("type") == "Bearish_FVG":
            return latest_fvg
        
        return None
    
    async def _generate_signal(self, symbolData: Dict, daily_bias: str, pdh: float, pdl: float, manipulation: Dict, fvg: Dict, opposite_liquidity: float, df: pd.DataFrame = None) -> Optional[Signal]:
        """Genera y devuelve una señal."""
        symbol = symbolData['symbol']
        
        # Verificar si la señal ya fue enviada en RAM
        signal_key = f"{symbol}_{fvg['timestamp']}"
        if signal_key in self._sent_signals:
            return None
        
        # ── FILTRO: ADX ──
        if df is not None:
            adx_ok, adx_value = technical.is_market_trending(df, min_adx=20, period=14)
            if not adx_ok:
                logger.info(f"[{symbol}] Señal descartada: ADX={adx_value:.1f} (< 20)")
                return None
        
        # Calcular niveles
        entry_price = float(fvg['mid'])
        
        if daily_bias == "BULLISH":
            stop_loss = float(fvg['bottom'])
            take_profit = entry_price + (entry_price - stop_loss) * self.config['min_rr_ratio']
            direction = "LARGO"
        else:
            stop_loss = float(fvg['top'])
            take_profit = entry_price - (stop_loss - entry_price) * self.config['min_rr_ratio']
            direction = "CORTO"
        
        sl_distance = abs(entry_price - stop_loss)
        
        # ── FILTRO: Exhaustion ──
        if df is not None:
            vela_origen_idx = len(df) - 5
            is_valid, _, mensaje = check_tp_exhaustion(df, vela_origen_idx, entry_price, take_profit, stop_loss, direction, threshold=0.60, timeframe="15M")
            if not is_valid:
                logger.info(f"[{symbol}] Señal descartada: Exhaustion - {mensaje}")
                return None
        
        # Marcar como enviada
        self._sent_signals[signal_key] = True
        
        return Signal(
            strategy=self.strategy_name,
            symbol=symbol,
            direction=direction,
            entry_price=entry_price,
            stop_loss=stop_loss,
            take_profit=take_profit,
            sl_distance=sl_distance,
            confidence=85 if daily_bias != "NEUTRAL" else 75,
            setup="FVG Diario + Manipulación",
            status="EN ZONA ✅",
            candle_time=fvg['timestamp'],
            intervalo="15min",
            riesgo_pips=round(sl_distance * getPipMultiplier(symbol), 1),
            rr_ratio=round(abs(take_profit - entry_price) / sl_distance, 2),
            metadata={
                "daily_bias": daily_bias,
                "pdh": pdh,
                "pdl": pdl,
                "manipulation_type": manipulation['type'],
                "fvg_type": fvg['type']
            }
        )

    

    
    def get_mexico_time(self) -> datetime:
        """Obtiene la hora actual en timezone México."""
        from datetime import datetime, timezone
        import pytz
        
        mexico_tz = pytz.timezone('America/Mexico_City')
        return datetime.now(mexico_tz)


async def main():
    """Función principal para ejecutar la estrategia."""
    strategy = FVGDiarioBot()
    await strategy.start()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())