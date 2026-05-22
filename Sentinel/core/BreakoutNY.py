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
        master = preloaded_data.get(symbol) if preloaded_data else None
        if not isinstance(master, dict):
            return []

        df_5m = master.get("5min")
        df_15m = master.get("15min")
        if df_5m is None or df_15m is None or len(df_5m) < 10 or len(df_15m) < 2:
            return []

        df_5m = technical.filter_to_closed_candles(df_5m)
        df_15m = technical.filter_to_closed_candles(df_15m)
        if df_5m.empty or df_15m.empty:
            return []

        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        start_hour = int(strat_config.get("start_hour", 9))
        start_minute = int(strat_config.get("start_minute", 30))
        rr = float(strat_config.get("min_rr", 1.0))
        confidence = float(strat_config.get("min_confidence", 75))

        range_candle, target_ts = self._get_range_candle(df_15m, start_hour, start_minute)
        if range_candle is None:
            return []

        now_ts = pd.Timestamp(self._now_local())
        if target_ts.tzinfo is not None:
            now_ts = now_ts.tz_convert(target_ts.tzinfo)
        elif now_ts.tzinfo is not None:
            now_ts = now_ts.tz_localize(None)

        if now_ts < target_ts + timedelta(minutes=15):
            return []

        trade_day = target_ts.date()
        day_key = f"{symbol}_{trade_day.isoformat()}"
        if day_key in self._triggered_days:
            return []

        range_high = float(range_candle["high"])
        range_low = float(range_candle["low"])
        last_m5 = df_5m.iloc[-1]
        close_price = float(last_m5["close"])
        candle_time = df_5m.index[-1].strftime("%Y-%m-%d %H:%M:%S")

        df_5m_range = df_5m[(df_5m.index >= target_ts) & (df_5m.index < target_ts + timedelta(minutes=15))]
        if not df_5m_range.empty:
            max_time = df_5m_range['high'].idxmax().strftime("%Y-%m-%d %H:%M:%S")
            min_time = df_5m_range['low'].idxmin().strftime("%Y-%m-%d %H:%M:%S")
        else:
            max_time = target_ts.strftime("%Y-%m-%d %H:%M:%S")
            min_time = target_ts.strftime("%Y-%m-%d %H:%M:%S")

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
                setup="NY APERTURA 15MIN",
                status="RUPTURA CONFIRMADA",
                candleTime=candle_time,
                intervalo="5min",
                riesgo_pips=round(risk_dist * multiplier, 1),
                rr_ratio=rr_ratio,
                break_even=calculateBEPrice(entry, stop_loss, take_profit, direction),
                metadata={
                    "range_high": range_high,
                    "range_low": range_low,
                    "range_time": target_ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "high_time": max_time,
                    "low_time": min_time,
                    "breakout_close": close_price,
                },
            )
        ]
