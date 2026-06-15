import logging
from datetime import datetime, time as dt_time, timedelta
from typing import Dict, List

import pandas as pd
import talib as ta
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

    def _timestamp_for_ny(self, ny_hour: int, ny_minute: int, df: pd.DataFrame) -> pd.Timestamp:
        ny_tz = pytz.timezone("America/New_York")
        # Usar fecha local ya que la mañana en NY coincide con la fecha de la mañana en CDMX
        local_date = self._now_local().date()
        ny_ts = pd.Timestamp(datetime.combine(local_date, dt_time(ny_hour, ny_minute)))
        ny_ts = ny_ts.tz_localize(ny_tz)
        
        if isinstance(df.index, pd.DatetimeIndex) and df.index.tz is not None:
            return ny_ts.tz_convert(df.index.tz)
        
        # Si el dataframe es ingenuo (naive), asumimos que esta en TIMEZONE local (CDMX)
        local_tz = pytz.timezone(TIMEZONE)
        return ny_ts.tz_convert(local_tz).tz_localize(None)

    def _get_range_candle(self, df_15m: pd.DataFrame, ny_hour: int, ny_minute: int):
        target_ts = self._timestamp_for_ny(ny_hour, ny_minute, df_15m)
        matches = df_15m[df_15m.index == target_ts]
        if matches.empty:
            return None, target_ts
        return matches.iloc[0], target_ts

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> List[Signal]:
        symbol = symbolInfo["symbol"]
        logger.info(f"[{symbol}] Analizando...")
        master = preloadedData.get(symbol) if preloadedData else None
        if not isinstance(master, dict):
            logger.info(f"[{symbol}] No se encontraron datos en master")
            return []

        df5m = master.get("5min")
        df15m = master.get("15min")
        if df5m is None or df15m is None or len(df5m) < 10 or len(df15m) < 2:
            logger.info(f"[{symbol}] Datos insuficientes")
            return []

        df5m = technical.filter_to_closed_candles(df5m)
        df15m = technical.filter_to_closed_candles(df15m)
        if df5m.empty or df15m.empty:
            logger.info(f"[{symbol}] Datos insuficientes")
            return []

        # Cargar parametros dinamicamente desde base de datos
        stratConfig = dbManager.getSymbolStrategyConfig(self.strategy_name, symbol) or {}
        # Por defecto 8:30 AM en Hora Nueva York (Apertura Forex NY 8:00 AM + 30 min)
        # O 10:00 si se refiere a Equities (9:30 AM + 30 min)
        nyStartHour = int(stratConfig.get("start_hour", 8))
        nyStartMinute = int(stratConfig.get("start_minute", 30))
        minRr = 1.0  # Forzado a 1:1 (User Rule 5)
        confidence = float(stratConfig.get("min_confidence", 75))
        rangeDuration = 15  # Forzado a 15 min (User Rule 1)
        tradingWindow = int(stratConfig.get("trading_window", 150))

        # Rango de apertura NY usando huso horario de America/New_York
        startTs = self._timestamp_for_ny(nyStartHour, nyStartMinute, df5m)
        endTs = startTs + timedelta(minutes=rangeDuration)

        dfRange = df5m[(df5m.index >= startTs) & (df5m.index < endTs)]
        if dfRange.empty:
            logger.info(f"[{symbol}] No se encontraron velas de M5 para el rango de {rangeDuration} minutos")
            return []

        nowTs = pd.Timestamp(self._now_local())
        if startTs.tzinfo is not None:
            nowTs = nowTs.tz_convert(startTs.tzinfo)
        elif nowTs.tzinfo is not None:
            nowTs = nowTs.tz_localize(None)

        if nowTs < endTs:
            logger.info(f"[{symbol}] El rango de {rangeDuration}min aun no ha concluido")
            return []

        # Hora limite: tradingWindow minutos desde la finalizacion del rango (ej. 150 minutos = 2h 30m)
        endTimeTs = endTs + timedelta(minutes=tradingWindow)
        if nowTs > endTimeTs:
            logger.info(f"[{symbol}] Hora actual {nowTs} excede la hora limite de trading {endTimeTs}")
            return []

        tradeDay = startTs.date()
        dayKey = f"{symbol}_{tradeDay.isoformat()}"
        if dayKey in self._triggered_days:
            logger.info(f"[{symbol}] Estrategia ya ejecutada hoy para {symbol}")
            return []

        rangeHigh = float(dfRange["high"].max())
        rangeLow = float(dfRange["low"].min())
        rangeSize = rangeHigh - rangeLow
        
        # --- FILTRO NUEVO: Rango Estrecho (< 1.5x ATR) ---
        atr_series = ta.ATR(df5m['high'].values, df5m['low'].values, df5m['close'].values, timeperiod=14)
        current_atr = float(atr_series[-1])
        if current_atr > 0 and rangeSize > (current_atr * 1.5):
            logger.info(f"[{symbol}] BreakoutNY descartada: Rango muy grande ({rangeSize:.5f} > 1.5x ATR {current_atr:.5f})")
            return []

        lastM5 = df5m.iloc[-1]
        closePrice = float(lastM5["close"])
        candleTime = df5m.index[-1].strftime("%Y-%m-%d %H:%M:%S")

        # --- FILTRO SEGURIDAD 1: Evitar re-entradas tardias en velas subsiguientes ---
        # Solo operar si la vela anterior (iloc[-2]) cerro DENTRO del rango.
        if len(df5m) >= 2:
            prevClose = float(df5m.iloc[-2]["close"])
            if closePrice > rangeHigh and prevClose > rangeHigh:
                logger.info(f"[{symbol}] BreakoutNY descartada: la vela anterior ya habia roto por encima del rango (re-entrada)")
                return []
            if closePrice < rangeLow and prevClose < rangeLow:
                logger.info(f"[{symbol}] BreakoutNY descartada: la vela anterior ya habia roto por debajo del rango (re-entrada)")
                return []

        # --- FILTRO SEGURIDAD 2: Evitar desfase temporal por lag de API o ejecucion tardia ---
        # Si la vela cerro hace mas de 90 segundos del tiempo actual, se descarta.
        candleEnd = df5m.index[-1] + timedelta(minutes=5)
        nowNaive = nowTs.tz_localize(None) if nowTs.tzinfo is not None else nowTs
        candleEndNaive = candleEnd.tz_localize(None) if candleEnd.tzinfo is not None else candleEnd
        delaySeconds = (nowNaive - candleEndNaive).total_seconds()
        
        if delaySeconds > 90:
            logger.info(f"[{symbol}] BreakoutNY descartada: senal tardia (delay de {delaySeconds:.1f}s > 90s)")
            return []

        df5mRange = df5m[(df5m.index >= startTs) & (df5m.index < endTs)]
        if not df5mRange.empty:
            maxTime = df5mRange['high'].idxmax().strftime("%Y-%m-%d %H:%M:%S")
            minTime = df5mRange['low'].idxmin().strftime("%Y-%m-%d %H:%M:%S")
        else:
            maxTime = startTs.strftime("%Y-%m-%d %H:%M:%S")
            minTime = startTs.strftime("%Y-%m-%d %H:%M:%S")

        # --- FILTRO DE SEGURIDAD 3: Tendencia Macro (EMA 200) ---
        ema200 = df5m["close"].ewm(span=200, adjust=False).mean().iloc[-1]
        if closePrice > rangeHigh and closePrice < ema200:
            logger.info(f"[{symbol}] BreakoutNY descartada: Ruptura alcista pero debajo de EMA 200 ({ema200:.5f})")
            return []
        if closePrice < rangeLow and closePrice > ema200:
            logger.info(f"[{symbol}] BreakoutNY descartada: Ruptura bajista pero sobre EMA 200 ({ema200:.5f})")
            return []

        # --- FILTRO DE SEGURIDAD 4: Calidad de la Vela (Cuerpo >= 60%) ---
        body_size = abs(lastM5["close"] - lastM5["open"])
        total_size = lastM5["high"] - lastM5["low"]
        body_pct = body_size / total_size if total_size > 0 else 0
        if body_pct < 0.60:
            logger.info(f"[{symbol}] BreakoutNY descartada: Vela debil (Cuerpo {body_pct*100:.1f}% < 60%)")
            return []

        if closePrice > rangeHigh:
            direction = "LARGO"
            entry = closePrice
            stopLoss = rangeLow
            riskDist = entry - stopLoss
            takeProfit = entry + (riskDist * minRr)
        elif closePrice < rangeLow:
            direction = "CORTO"
            entry = closePrice
            stopLoss = rangeHigh
            riskDist = stopLoss - entry
            takeProfit = entry - (riskDist * minRr)
        else:
            return []

        if riskDist <= 0:
            logger.info(f"[{symbol}] BreakoutNY descartada: riesgo invalido")
            return []

        signalKey = f"{dayKey}_{candleTime}_{direction}"
        if signalKey in self._triggered_days:
            return []

        multiplier = getPipMultiplier(symbol)
        rrRatio = abs(takeProfit - entry) / riskDist
        self._triggered_days.add(dayKey)
        self._triggered_days.add(signalKey)

        return [
            Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction=direction,
                entry_price=entry,
                stop_loss=stopLoss,
                take_profit=takeProfit,
                sl_distance=riskDist,
                confidence=confidence,
                setup=f"NY APERTURA {rangeDuration}MIN",
                status="RUPTURA CONFIRMADA",
                candleTime=candleTime,
                intervalo="5min",
                riesgo_pips=round(riskDist * multiplier, 1),
                rr_ratio=round(rrRatio, 2),
                break_even=calculateBEPrice(entry, stopLoss, takeProfit, direction),
                metadata={
                    "range_high": rangeHigh,
                    "range_low": rangeLow,
                    "range_time": startTs.strftime("%Y-%m-%d %H:%M:%S"),
                    "high_time": maxTime,
                    "low_time": minTime,
                    "breakout_close": closePrice,
                },
            )
        ]
