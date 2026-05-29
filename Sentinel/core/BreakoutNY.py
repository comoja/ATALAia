import logging
from datetime import datetime, time as dt_time, timedelta
from typing import Dict, List

import pandas as pd
import pytz

from middleware.config.constants import TIMEZONE
from middleware.database import dbManager
from middleware.utils.alertBuilder import getPipMultiplier, calculateBEPrice, calculateRR
from Sentinel.analysis import technical
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")


class BreakoutNYBot:
    """
    Breakout de apertura NY.

    Toma el rango de la vela M15 que abre en start_hour:start_minute y entra
    cuando una vela M5 cerrada rompe por encima/debajo del rango.
    """

    def __init__(self):
        self.strategy_name = "BreakoutNY"
        self._triggered_days = set()

    def _now_local(self) -> datetime:
        return datetime.now(pytz.timezone(TIMEZONE))

    def _timestamp_for_today(self, hour: int, minute: int, df: pd.DataFrame) -> pd.Timestamp:
        ts = pd.Timestamp(datetime.combine(self._now_local().date(), dt_time(hour, minute)))
        if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
            return ts.tz_localize(pytz.timezone(TIMEZONE)).tz_convert(df.index.tz)
        return ts

    def _get_range_candle(self, df_15m: pd.DataFrame, start_hour: int, start_minute: int):
        target_ts = self._timestamp_for_today(start_hour, start_minute, df_15m)
        matches = df_15m[df_15m.index == target_ts]
        if matches.empty:
            return None, target_ts
        return matches.iloc[0], target_ts

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloaded_data: Dict = None, apiKey: str = None) -> List[Signal]:
        symbol = symbolInfo["symbol"]
        logger.info(f"[{symbol}] Analizando...")
        master = preloaded_data.get(symbol) if preloaded_data else None
        if not isinstance(master, dict):
            logger.info(f"[{symbol}] No se encontraron datos en master")
            return []

        df_5m = master.get("5min")
        df_15m = master.get("15min")
        if df_5m is None or df_15m is None or len(df_5m) < 10 or len(df_15m) < 2:
            logger.info(f"[{symbol}] Datos insuficientes")
            return []

        df_5m = technical.filter_to_closed_candles(df_5m)
        df_15m = technical.filter_to_closed_candles(df_15m)
        if df_5m.empty or df_15m.empty:
            logger.info(f"[{symbol}] Datos insuficientes")
            return []

        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        start_hour = int(strat_config.get("start_hour", 9))
        start_minute = int(strat_config.get("start_minute", 0))
        rr = float(strat_config.get("min_rr", 2.0))
        confidence = float(strat_config.get("min_confidence", 75))

        # Rango de 30 minutos de apertura (ej. 09:00 a 09:30 local/NY)
        start_ts = self._timestamp_for_today(start_hour, start_minute, df_5m)
        end_ts = start_ts + timedelta(minutes=30)

        df_range = df_5m[(df_5m.index >= start_ts) & (df_5m.index < end_ts)]
        if df_range.empty:
            logger.info(f"[{symbol}] No se encontraron velas de M5 para el rango de 30 minutos ({start_hour:02d}:{start_minute:02d} a 09:30)")
            return []

        now_ts = pd.Timestamp(self._now_local())
        if start_ts.tzinfo is not None:
            now_ts = now_ts.tz_convert(start_ts.tzinfo)
        elif now_ts.tzinfo is not None:
            now_ts = now_ts.tz_localize(None)

        if now_ts < end_ts:
            logger.info(f"[{symbol}] El rango de 30min ({start_hour:02d}:{start_minute:02d} a 09:30) aún no ha concluido")
            return []

        # Hora límite: 12:00 PM (mediodía) hora local (2h 30m desde las 09:30)
        end_time_ts = end_ts + timedelta(hours=2, minutes=30)
        if now_ts > end_time_ts:
            logger.info(f"[{symbol}] Hora actual {now_ts} excede la hora límite de trading {end_time_ts}")
            return []

        trade_day = start_ts.date()
        day_key = f"{symbol}_{trade_day.isoformat()}"
        if day_key in self._triggered_days:
            logger.info(f"[{symbol}] Estrategia ya ejecutada hoy para {symbol}")
            return []

        range_high = float(df_range["high"].max())
        range_low = float(df_range["low"].min())
        last_m5 = df_5m.iloc[-1]
        close_price = float(last_m5["close"])
        candle_time = df_5m.index[-1].strftime("%Y-%m-%d %H:%M:%S")

        # --- FILTRO SEGURIDAD 1: Evitar re-entradas tardías en velas subsiguientes ---
        # Solo operar si la vela anterior (iloc[-2]) cerró DENTRO del rango.
        if len(df_5m) >= 2:
            prevClose = float(df_5m.iloc[-2]["close"])
            if close_price > range_high and prevClose > range_high:
                logger.info(f"[{symbol}] BreakoutNY descartada: la vela anterior ya había roto por encima del rango (re-entrada)")
                return []
            if close_price < range_low and prevClose < range_low:
                logger.info(f"[{symbol}] BreakoutNY descartada: la vela anterior ya había roto por debajo del rango (re-entrada)")
                return []

        # --- FILTRO SEGURIDAD 2: Evitar desfase temporal por lag de API o ejecución tardía ---
        # Si la vela cerró hace más de 90 segundos del tiempo actual, se descarta.
        candleEnd = df_5m.index[-1] + timedelta(minutes=5)
        nowNaive = now_ts.tz_localize(None) if now_ts.tzinfo is not None else now_ts
        candleEndNaive = candleEnd.tz_localize(None) if candleEnd.tzinfo is not None else candleEnd
        delaySeconds = (nowNaive - candleEndNaive).total_seconds()
        
        if delaySeconds > 90:
            logger.info(f"[{symbol}] BreakoutNY descartada: señal tardía (delay de {delaySeconds:.1f}s > 90s)")
            return []

        df_5m_range = df_5m[(df_5m.index >= start_ts) & (df_5m.index < end_ts)]
        if not df_5m_range.empty:
            max_time = df_5m_range['high'].idxmax().strftime("%Y-%m-%d %H:%M:%S")
            min_time = df_5m_range['low'].idxmin().strftime("%Y-%m-%d %H:%M:%S")
        else:
            max_time = start_ts.strftime("%Y-%m-%d %H:%M:%S")
            min_time = start_ts.strftime("%Y-%m-%d %H:%M:%S")

        if close_price > range_high:
            direction = "LARGO"
            entry = close_price
            stop_loss = range_low
            risk_dist = entry - stop_loss
            take_profit = entry + (risk_dist * rr)
        elif close_price < range_low:
            direction = "CORTO"
            entry = close_price
            stop_loss = range_high
            risk_dist = stop_loss - entry
            take_profit = entry - (risk_dist * rr)
        else:
            return []

        if risk_dist <= 0:
            logger.info(f"[{symbol}] BreakoutNY descartada: riesgo invalido")
            return []

        signal_key = f"{day_key}_{candle_time}_{direction}"
        if signal_key in self._triggered_days:
            return []

        multiplier = getPipMultiplier(symbol)
        rr_ratio = calculateRR(entry, stop_loss, take_profit)
        self._triggered_days.add(day_key)
        self._triggered_days.add(signal_key)

        return [
            Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction=direction,
                entry_price=entry,
                stop_loss=stop_loss,
                take_profit=take_profit,
                sl_distance=risk_dist,
                confidence=confidence,
                setup="NY APERTURA 30MIN",
                status="RUPTURA CONFIRMADA",
                candleTime=candle_time,
                intervalo="5min",
                riesgo_pips=round(risk_dist * multiplier, 1),
                rr_ratio=rr_ratio,
                break_even=calculateBEPrice(entry, stop_loss, take_profit, direction),
                metadata={
                    "range_high": range_high,
                    "range_low": range_low,
                    "range_time": start_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "high_time": max_time,
                    "low_time": min_time,
                    "breakout_close": close_price,
                },
            )
        ]
