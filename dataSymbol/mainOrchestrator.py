import sys
import os
import asyncio
import logging
import time
from datetime import datetime, timedelta
from typing import List, Tuple
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.database import dbManager as middlewareDb
from middleware.scheduler.autoScheduler import isRestTime
from middleware.config.constants import API_KEYS, FESTIVOS, TIMEZONE
from core.twelveDataClient import getTimeSeriesWithStartDate
from core.databaseManager import DatabaseManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

RATE_LIMIT_PER_MINUTE = 7
RATE_LIMIT_PER_DAY = 750
SLEEP_BETWEEN_CALLS = 60 / RATE_LIMIT_PER_MINUTE
DAYS_PER_CALL = 30

ACCOUNT_NAMES = ["Jaime", "Raul", "Sebastian"]
TIMEZONE_LOCAL = ZoneInfo(TIMEZONE)

MAX_CANDLES_PER_CALL = 5000
CANDLE_INTERVAL_MINUTES = 5


def isMarketOpen() -> bool:
    now = datetime.now(TIMEZONE_LOCAL)
    return not isRestTime(now)


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
            logger.info(f"=== Nueva ronda de símbolos ({len(symbols)} símbolos) ===")
            await asyncio.sleep(SLEEP_BETWEEN_CALLS)
            continue
        
        symbolData = symbols[symbolIndex]
        symbol = str(symbolData['symbol'])
        symbolIndex += 1
        
        lastDb = db.getLastTimestamp(symbol, "5min")
        logger.info(f"[{symbol}] getLastTimestamp(5min)={lastDb}")
        
        isNewSymbol = False
        if lastDb:
            startDate = lastDb + timedelta(minutes=5)
            logger.info(f"[{symbol}] startDate={startDate}")
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
        
        if startDateDay == today and not isMarketOpen():
            logger.info(f"[{symbol}] Mercado cerrado")
            await asyncio.sleep(SLEEP_BETWEEN_CALLS)
            continue
        
        if startDateDay == today:
            nextCandleTime = startDate + timedelta(minutes=5)
            if now.timestamp() < nextCandleTime.timestamp():
                await asyncio.sleep(SLEEP_BETWEEN_CALLS)
                continue
        
        apiKey, accountName = limiter.getNextAccount()
        if not apiKey:
            await asyncio.sleep(SLEEP_BETWEEN_CALLS)
            continue
        
        if startDateDay == today:
            endDate = now.replace(tzinfo=None) - timedelta(minutes=5)
            if endDate <= startDate:
                await asyncio.sleep(SLEEP_BETWEEN_CALLS)
                continue
        else:
            maxMinutesPerCall = MAX_CANDLES_PER_CALL * CANDLE_INTERVAL_MINUTES
            endDate = startDate + timedelta(minutes=maxMinutesPerCall)
            if endDate.timestamp() > now.timestamp():
                endDate = now - timedelta(minutes=5)
        
        if startDate.timestamp() >= endDate.timestamp():
            await asyncio.sleep(SLEEP_BETWEEN_CALLS)
            continue
        
        try:
            df = await getTimeSeriesWithStartDate(
                symbol=symbol,
                interval="5min",
                apiKey=apiKey,
                startDate=startDate,
                endDate=endDate,
                outputsize=5000
            )
            
            limiter.recordCall(apiKey)
            
            if df is not None and not df.empty:
                inserted = db.saveBulkData(df, symbol, "5min")
                if inserted > 0:
                    logger.info(f"[{symbol}] +{inserted} velas insertadas")
            
            results = db.resampleStandardIntervals(symbol, startDate)
            new15 = results.get("15min", 0)
            new1h = results.get("1h", 0)
            if new15 > 0 or new1h > 0:
                logger.info(f"[{symbol}] 15min: +{new15}, 1h: +{new1h} velas generadas")
            
        except Exception as e:
            logger.error(f"[{symbol}] Error: {e}")
        
        await asyncio.sleep(SLEEP_BETWEEN_CALLS)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario")
