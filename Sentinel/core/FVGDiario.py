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
import talib as ta
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from middleware.database import dbManager
from middleware.execution.broker_gateway import gateway
from middleware.utils import momentum
from Sentinel.analysis import technical, risk
from Sentinel.analysis.technical import check_tp_exhaustion, check_signal_health
from middleware.utils.alertBuilder import getPipMultiplier, calculateBEPrice

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
                df_intraday = master
                df_daily = technical.resample_to_interval(master, '1D') if (master is not None and len(master) > 100) else None

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
            opposite_liquidity = pdl if daily_bias == "LARGO" else pdh
            
            manipulation = self._detectManipulation(
                df_intraday, pdh, pdl, daily_bias
            )
            
            if not manipulation:
                return None
            
            # Verificar cambio de estructura tras manipulación
            if not self._checkMarketStructureShift(df_intraday, manipulation, daily_bias):
                return None
            
            # Buscar FVG tras el sweep de liquidez (MTF Alignment)
            fvg = self._findFvgAfterManipulation(df_intraday, manipulation, daily_bias)
            
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
            return "LARGO"
        elif body / total_range >= 0.5 and body < 0:
            return "CORTO"
        
        return "NEUTRAL"
    
    def _get_pdh_pdl(self, df_daily: pd.DataFrame) -> tuple:
        """Obtiene el Previous Day High (PDH) y Previous Day Low (PDL)."""
        if df_daily is None or len(df_daily) < 2:
            return None, None
        
        prev_day = df_daily.iloc[-2]
        pdh = float(prev_day.get('high', prev_day.get('High', 0)))
        pdl = float(prev_day.get('low', prev_day.get('Low', 0)))
        
        return pdh, pdl
    
    def _detectManipulation(self, df: pd.DataFrame, pdh: float, pdl: float, dailyBias: str) -> Optional[Dict]:
        """
        Detecta manipulación del nivel diario usando la función centralizada.

        Regla SMC/ICT (video - MTF Alignment):
        Un sweep real exige que el precio SUPERE el nivel PDH/PDL Y CIERRE de vuelta
        dentro del rango anterior. Un simple toque no es manipulación válida.
        """
        return technical.detectLiquiditySweep(df, htfHigh=pdh, htfLow=pdl, lookback=10)
    
    def _checkMarketStructureShift(self, df: pd.DataFrame, manipulation: Dict, dailyBias: str) -> bool:
        """Verifica si hay un Market Structure Shift (MSS) tras el sweep de liquidez."""
        if df is None or manipulation is None:
            return False

        # detectLiquiditySweep retorna 'idx'; compatibilidad con formato anterior
        manipIdx = manipulation.get('idx', manipulation.get('index', None))
        if manipIdx is None or manipIdx + 2 >= len(df):
            return False

        postManip = df.iloc[manipIdx + 1:]
        if len(postManip) < 2:
            return False

        # Inferir dirección del MSS esperado según el tipo de sweep
        sweepType = manipulation.get('type', '')
        if dailyBias == "CORTO" or sweepType == "MANIPULATION_UP":
            # Tras sweep de máximos, esperar ruptura de mínimo (MSS bajista)
            for low in postManip['low'].values:
                if low < manipulation['level']:
                    return True
        elif dailyBias == "LARGO" or sweepType == "MANIPULATION_DOWN":
            # Tras sweep de mínimos, esperar ruptura de máximo (MSS alcista)
            colHigh = 'high' if 'high' in postManip.columns else 'High'
            for high in (postManip[colHigh].values if colHigh in postManip.columns else []):
                if high > manipulation['level']:
                    return True

        return False
    
    def _findFvgAfterManipulation(self, df: pd.DataFrame, manipulation: Dict, dailyBias: str) -> Optional[Dict]:
        """Busca un Fair Value Gap formado después del sweep de liquidez."""
        if df is None or manipulation is None:
            return None

        # Compatibilidad con ambas versiones del dict (idx o index)
        manipIdx = manipulation.get('idx', manipulation.get('index', None))
        if manipIdx is None:
            return None

        startIdx = max(0, manipIdx + 2)
        endIdx   = min(len(df), startIdx + 5)

        if endIdx - startIdx < 3:
            return None

        dfAfter = df.iloc[startIdx:endIdx]
        latestFvg = technical.detect_fvg_closed(
            df_source=dfAfter, interval='15min', min_gap_pct=0.0001, min_adx=15, lookback=10
        )
        return latestFvg  # None si no hay FVG válido
    
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
        
        # Calcular niveles usando la función centralizada
        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        min_rr_val = float(strat_config.get('min_rr', 2.0))
        min_confidence = float(strat_config.get('min_confidence', 70))
        
        atr = 0
        if df is not None:
            atr_series = ta.ATR(df['high'], df['low'], df['close'], 14).dropna()
            atr = atr_series.iloc[-1] if not atr_series.empty else 0
            
        current_price = float(df['close'].iloc[-1]) if df is not None else float(fvg['mid'])
        setup_fvg = technical.calculate_fvg_setup(fvg, current_price, atr)
        
        entry_price = setup_fvg['entry']
        stop_loss = setup_fvg['sl']
        direction = setup_fvg['direction']
        take_profit = entry_price + (entry_price - stop_loss) * min_rr_val if direction == "LARGO" else entry_price - (stop_loss - entry_price) * min_rr_val
        
        sl_distance = abs(entry_price - stop_loss)
        
        # ── FILTRO: Exhaustion ──
        if df is not None:
            vela_origen_idx = len(df) - 5
            is_valid, _, mensaje = check_tp_exhaustion(df, vela_origen_idx, entry_price, take_profit, stop_loss, direction, threshold=0.60, timeframe="15M")
            if not is_valid:
                logger.info(f"[{symbol}] Señal descartada: Exhaustion - {mensaje}")
                return None
        
        if df is not None:
            current_price = float(df['close'].iloc[-1])
            fvg_time = fvg.get('timestamp', '')
            is_valid, _, mensaje = check_signal_health(entry_price, take_profit, stop_loss, direction, current_price, threshold=0.65, candle_time=fvg_time)
            if not is_valid:
                logger.info(f"[{symbol}] Señal descartada: Health - {mensaje}")
                return None
        
        # Calcular confianza
        base_confidence = 85 if daily_bias != "NEUTRAL" else 75
        if base_confidence < min_confidence:
            logger.info(f"[{symbol}] Señal descartada: confidence={base_confidence} < min_confidence={min_confidence}")
            return None
        
        # ── FILTRO: Tendencia mensual ──
        monthly_trend = symbolData.get('weekly_trend', 'NEUTRAL')
        if monthly_trend == "BAJISTA" and direction == "LARGO":
            logger.info(f"[{symbol}] Señal LARGO descartada - tendencia mensual BAJISTA")
            return None
        elif monthly_trend == "ALCISTA" and direction == "CORTO":
            logger.info(f"[{symbol}] Señal CORTO descartada - tendencia mensual ALCISTA")
            return None
        
        # ── FILTRO: Ganancia Mínima Estimada ──
        multiplier = getPipMultiplier(symbol)
        risk_usd = float(strat_config.get('risk_usd', 100.0))
        size = (risk_usd / (sl_distance * multiplier)) if (sl_distance > 0 and multiplier > 0) else 0

        min_usd_profit = float(strat_config.get('min_usd_profit', 10.0))
        rr_ratio = round(abs(take_profit - entry_price) / sl_distance, 2)
        expected_profit = (sl_distance * multiplier * size) * rr_ratio
        
        if expected_profit < min_usd_profit:
            logger.info(f"[{symbol}] {self.strategy_name}: Beneficio Est. ${expected_profit:.2f} < ${min_usd_profit:.2f} - descartando")
            return None

        # Calcular Break Even inteligente
        be_trigger = calculateBEPrice(entry_price, stop_loss, take_profit, direction)

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
            confidence=base_confidence,
            setup="FVG Diario + Manipulación",
            status="EN ZONA ✅",
            candleTime=fvg['timestamp'],
            intervalo="15min",
            riesgo_pips=round(sl_distance * multiplier, 1),
            rr_ratio=rr_ratio,
            break_even=be_trigger,
            metadata={
                "daily_bias": daily_bias,
                "pdh": pdh,
                "pdl": pdl,
                "manipulation_type": manipulation['type'],
                "fvg_type": fvg['type'],
                "vela_origen": fvg['timestamp']
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