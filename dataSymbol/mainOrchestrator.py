import sys
import os
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import List, Tuple
from zoneinfo import ZoneInfo
import pytz
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.utils.loggerConfig import setupLogging
setupLogging(logPara="dataSymbol", projectDir=os.path.dirname(os.path.abspath(__file__)))

from middleware.database import dbManager as middlewareDb
from middleware.scheduler.autoScheduler import isRestTime
from middleware.config.constants import API_KEYS, FESTIVOS, TIMEZONE
from middleware.api.twelvedata import _callTimeSeriesApi 
from core.databaseManager import DatabaseManager

logger = logging.getLogger(__name__)

RATE_LIMIT_PER_MINUTE = 7
RATE_LIMIT_PER_DAY = 750
SLEEP_BETWEEN_CALLS = 60 / RATE_LIMIT_PER_MINUTE
DAYS_PER_CALL = 30

ACCOUNT_NAMES = ["Jaime", "Raul", "Sebastian"]
TIMEZONE_LOCAL = ZoneInfo(TIMEZONE)

MAX_CANDLES_PER_CALL = 5000
CANDLE_INTERVAL_MINUTES = 5
MAX_MINUTES_PER_CALL = MAX_CANDLES_PER_CALL * CANDLE_INTERVAL_MINUTES


def next_5min_time(now):
    next_minute = (now.minute // 5 + 1) * 5

    if next_minute == 60:
        return now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        return now.replace(minute=next_minute, second=0, microsecond=0)


def seconds_until_next_5min(now, buffer_seconds=120):
    next_time = next_5min_time(now)

    # aplicar buffer directamente al next_time
    next_time = next_time + timedelta(seconds=buffer_seconds)

    sleep_seconds = int((next_time - now).total_seconds())

    return max(0, sleep_seconds), next_time

def isMarketOpen() -> bool:
    now = datetime.now(TIMEZONE_LOCAL)
    return not isRestTime(now)

def round5min(timestamp):
    if isinstance(timestamp, str):
        dt = datetime.strptime(timestamp, "%Y/%m/%d %H:%M:%S")
    else:
        dt = timestamp
    
    minute = (dt.minute // 5) * 5
    rounded = dt.replace(minute=minute, second=0, microsecond=0)
    
    return rounded

def is_forex_market_open(dt):
    # domingo antes de apertura → cerrado
    if dt.weekday() == 6 and dt.hour < 17:
        return False

    # sábado → cerrado
    if dt.weekday() == 5:
        return False

    # viernes después del cierre → cerrado
    if dt.weekday() == 4 and dt.hour >= 17:
        return False

    return True

def get_safe_last_candle(now, interval=5):
    # buffer real (clave)
    safe_now = now - timedelta(minutes=3)

    minute = (safe_now.minute // interval) * interval

    return safe_now.replace(minute=minute, second=0, microsecond=0)

def adjust_to_market_open(dt):
    while not is_forex_market_open(dt):
        dt += timedelta(minutes=5)
    return dt

def get_last_closed_candle(now, interval=5):
    """
    Devuelve el último timestamp de vela CERRADA
    """
    minute = (now.minute // interval) * interval
    candle_time = now.replace(minute=minute, second=0, microsecond=0)

    # si estamos justo en el inicio de vela → usar anterior
    if now == candle_time:
        candle_time -= timedelta(minutes=interval)

    return candle_time

def normalize_datetime(dt, tz):
    """
    Convierte datetime o pandas Timestamp a timezone-aware consistente
    """
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()

    if dt.tzinfo is None:
        return tz.localize(dt)
    else:
        return dt.astimezone(tz)

class MultiAccountRateLimiter:
    def __init__(self, apiKeys: List[str], accountNames: List[str]):
        self.apiKeys = apiKeys
        self.accountNames = accountNames
        self.callsToday = {key: 0 for key in apiKeys}
        self.keyIndex = 0

    def getNextAccount(self) -> Tuple[str, str]:
        maxAttempts = len(self.apiKeys)
        for _ in range(maxAttempts):
            if self.callsToday[self.apiKeys[self.keyIndex]] < RATE_LIMIT_PER_DAY:
                key = self.apiKeys[self.keyIndex]
                name = self.accountNames[self.keyIndex]
                self.keyIndex = (self.keyIndex + 1) % len(self.apiKeys)
                return key, name
            self.keyIndex = (self.keyIndex + 1) % len(self.apiKeys)
        return None, None

    def recordCall(self, key: str):
        if key in self.callsToday:
            self.callsToday[key] += 1

    def getStatus(self) -> dict:
        return {
            name: {"calls": self.callsToday[key], "remaining": RATE_LIMIT_PER_DAY - self.callsToday[key]}
            for key, name in zip(self.apiKeys, self.accountNames)
        }

    def allExhausted(self) -> bool:
        return all(count >= RATE_LIMIT_PER_DAY for count in self.callsToday.values())

def get_safe_last_candle(now, interval=5):
    # buffer dinámico
    delay = 3 if now.minute % 5 == 0 else 2

    safe_now = now - timedelta(minutes=delay)
    minute = (safe_now.minute // interval) * interval

    return safe_now.replace(minute=minute, second=0, microsecond=0)
def split_range(start, end, chunk_minutes=60):
    ranges = []
    current = start

    while current < end:
        chunk_end = current + timedelta(minutes=chunk_minutes)

        if chunk_end > end:
            chunk_end = end

        ranges.append((current, chunk_end))
        current = chunk_end

    return ranges

async def main():
    logger.info("=" * 60)
    logger.info("DataSymbol - Descarga Histórica de Velas 5min")
    logger.info("Presiona Ctrl+C para detener")
    logger.info("=" * 60)
    
    limiter = MultiAccountRateLimiter(API_KEYS, ACCOUNT_NAMES)
    db = DatabaseManager()
    lastResetDate = datetime.now().date()
    symbolIndex = 0
    
    while True:
        
        now = datetime.now(TIMEZONE_LOCAL)
        today = now.date()
        if today > lastResetDate:
            logger.info("Nuevo día detectado. Reseteando contadores de API.")
            limiter.callsToday = {key: 0 for key in limiter.apiKeys}
            lastResetDate = today
            symbolIndex = 0
        
        symbols = middlewareDb.getSymbols()
        if not symbols:
            logger.warning("No hay símbolos activos. Esperando...")
            await asyncio.sleep(60)
            continue
        
        if limiter.allExhausted():
            logger.warning("Todas las cuentas agotadas (750/día). Esperando hasta medianoche...")
            time.sleep(3600 * 6)
            limiter.callsToday = {key: 0 for key in limiter.apiKeys}
            continue
        
        if symbolIndex >= len(symbols):
            symbolIndex = 0
            sleep_seconds, next_time = seconds_until_next_5min(now)
            logger.info(f"============== Nueva ronda de símbolos ({len(symbols)} símbolos descansara hasta {next_time.strftime('%Y-%m-%d %H:%M:%S')} )==============")
            await asyncio.sleep(sleep_seconds)
            continue
        
        symbolData = symbols[symbolIndex]
        symbol = str(symbolData['symbol'])        
        
        lastDb = db.getLastTimestamp(symbol, "5min")
        symbolIndex += 1
        isNewSymbol = False
        if lastDb:
            startDate = lastDb #+ timedelta(minutes=5)
            logger.info(f"[{symbol}] Símbolo existente, lastDB={lastDb} startDate={startDate} ")
        else:
            startDateRaw = symbolData.get('startDate')
            if isinstance(startDateRaw, str):
                startDate = datetime.strptime(startDateRaw, '%Y-%m-%d')
            elif startDateRaw:
                startDate = datetime.combine(startDateRaw, datetime.min.time())
            else:
                startDate = datetime(2005, 1, 1)

            logger.info(f"[{symbol}] Símbolo nuevo, startDate={startDate}")
            isNewSymbol = True
        
        startDateDay = startDate.date() if startDate.tzinfo else startDate.replace(tzinfo=TIMEZONE_LOCAL).date()
        
        TZ = pytz.timezone(TIMEZONE)
        startDate = normalize_datetime(startDate, TZ)
        
        if startDateDay == today and not isMarketOpen():
            logger.info(f"[{symbol}] Mercado cerrado")
            continue
        
        if startDateDay == today:
            nextCandleTime = startDate             
            if now.timestamp() < nextCandleTime.timestamp():
                logger.info(f"1.- [{symbol}] now {now} < nextCandleTime {nextCandleTime} ")
                continue
        
        lastClosed = get_safe_last_candle(now)
        
        if startDateDay == today:
            endDate = lastClosed
            if endDate <= startDate:
                logger.info(f"2.- [{symbol}] lastClosed={lastClosed} startDate={startDate} >= endDate={endDate}")                
                continue

        else:
            endDate = round5min(startDate + timedelta(minutes=MAX_MINUTES_PER_CALL))
            # nunca permitir futuro
            if endDate > lastClosed:
                endDate = lastClosed
        
        # validación final
        startDate = adjust_to_market_open(startDate)
        endDate = adjust_to_market_open(endDate)
        
        if endDate <= startDate:
            logger.info(f"3.-startDate {startDate} >= endDate {endDate}")
            continue

        endDate   = normalize_datetime(endDate, TZ)

        
        apiKey, accountName = limiter.getNextAccount()
        logger.info(f"----> [{symbol}] startDate={startDate} endDate={endDate} cuenta de {accountName} ")
        if not apiKey:
            continue
        try:
            params = {
                "symbol": symbol,
                "interval": "5min",
                "apikey": apiKey,
                "start_date": startDate,
                "end_date": endDate
            }
            df = await _callTimeSeriesApi (params)
            
            limiter.recordCall(apiKey)
            
            if df is not None and not df.empty:
                logger.info(f"[{symbol}] df.head()={df['datetime'].iloc[0]} df.tail()={df['datetime'].iloc[-1]}")
                inserted = db.saveBulkData(df, symbol, "5min")
                if inserted > 0:
                    logger.info(f"[{symbol}] +{inserted} velas insertadas (5min)")
                
                results = db.resampleStandardIntervals(symbol, startDate)
                new15 = results.get("15min", 0)
                new1h = results.get("1h", 0)
                if new15 > 0 or new1h > 0:
                    logger.info(f"[{symbol}] Generadas: 15min: +{new15}, 1h: +{new1h}")
                await asyncio.sleep(SLEEP_BETWEEN_CALLS)
        except Exception as e:
            logger.error(f"[{symbol}] Error: {e}")
        
        


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario")
