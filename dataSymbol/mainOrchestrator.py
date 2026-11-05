import os
import sys
import time
import logging
import asyncio
import subprocess
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional
import pandas as pd
import pytz
from zoneinfo import ZoneInfo

# Configuración de rutas
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from middleware.utils.loggerConfig import setupLogging
setupLogging(logPara="dataSymbol", projectDir=os.path.dirname(os.path.abspath(__file__)), enableConsole=True)

from middleware.database import dbManager as middlewareDb
from middleware.scheduler.autoScheduler import isRestTime
from middleware.utils.time_utils import get_sleep_minutes, get_seconds_to_next_sync, get_seconds_until_market_opens, is_market_closed
from middleware.config.constants import API_KEYS, FESTIVOS, TIMEZONE, DATA_SOURCE
from middleware.api.twelvedata import _callTimeSeriesApi
from middleware.database.dbManager import DatabaseManager, get_api_usage, update_api_usage

logger = logging.getLogger("dataSymbol")

# Límites por cuenta (Twelvedata Basic - 4 cuentas)
# Real: 8 llamadas/min, 800/día por cuenta → 32/min y 3,200/día en total
RATE_LIMIT_PER_MINUTE = 8   # Límite real por cuenta
RATE_LIMIT_PER_DAY = 800    # Límite real por cuenta
TOTAL_ACCOUNTS = len(API_KEYS)
# Delay mínimo entre llamadas para no exceder el límite por minuto combinado
SLEEP_BETWEEN_CALLS = max(1.0, 60 / (RATE_LIMIT_PER_MINUTE * TOTAL_ACCOUNTS))

DAYS_PER_CALL = 30
ACCOUNT_NAMES = ["Jaime", "Raul", "Sebastian", "Ana"]
TIMEZONE_LOCAL = pytz.timezone(TIMEZONE)

MAX_CANDLES_PER_CALL = 100000
CANDLE_INTERVAL_MINUTES = 1
MAX_MINUTES_PER_CALL = MAX_CANDLES_PER_CALL * CANDLE_INTERVAL_MINUTES
DEFAULT_START_DATE_1MIN = datetime(2026, 9, 1, 0, 0, 0)

def next_1min_time(now):
    return now.replace(second=0, microsecond=0) + timedelta(minutes=1)

def seconds_until_next_1min(now, buffer_seconds=2):
    next_time = next_1min_time(now) + timedelta(seconds=buffer_seconds)
    sleep_seconds = int((next_time - now).total_seconds())
    return max(0, sleep_seconds), next_time

# --- Funciones Auxiliares ---
def round1min(timestamp):
    if isinstance(timestamp, str):
        fmt = "%Y-%m-%d %H:%M:%S" if "-" in timestamp else "%Y/%m/%d %H:%M:%S"
        dt = datetime.strptime(timestamp[:19], fmt)
    else:
        dt = timestamp
    return dt.replace(second=0, microsecond=0)

def adjust_to_market_open(dt):
    attempts = 0
    while isRestTime(dt) and attempts < 2000: 
        dt += timedelta(minutes=1)
        attempts += 1
    return dt

def get_safe_last_candle(now, interval=1):
    """
    Calcula el timestamp de la última vela CERRADA usando la función estándar del middleware.
    A las 18:35:02 con interval=1, devuelve 18:34 (la vela 18:34-18:35 que acaba de cerrar a las 18:35:00).
    """
    return _get_last_closed_candle(now, interval)

from middleware.utils.time_utils import get_last_closed_candle as _get_last_closed_candle

def get_last_closed_candle(now, interval=1, df=None):
    """Alias for middleware version to support legacy bot imports."""
    return _get_last_closed_candle(now, interval, df=df)



def normalize_datetime(dt, tz):
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
        # Cargar consumo diario desde la DB
        self.callsToday = {key: get_api_usage(name) for key, name in zip(apiKeys, accountNames)}
        # Registro para el límite por minuto (ventana deslizante)
        self.callsThisMinute = {key: [] for key in apiKeys}
        self.keyIndex = 0
        self.blockedUntil = {key: None for key in apiKeys}

    def getNextAccount(self) -> Tuple[Optional[str], Optional[str]]:
        now = time.time()
        maxAttempts = len(self.apiKeys)
        
        for _ in range(maxAttempts):
            idx = self.keyIndex
            key = self.apiKeys[idx]
            name = self.accountNames[idx]
            self.keyIndex = (self.keyIndex + 1) % len(self.apiKeys)

            # 1. Verificar bloqueo temporal (por errores de API)
            if self.blockedUntil[key] and now < self.blockedUntil[key]:
                continue

            # 2. Verificar límite diario
            if self.callsToday[key] >= RATE_LIMIT_PER_DAY:
                continue

            # 3. Verificar límite por minuto (ventana deslizante de 60s)
            # Limpiar timestamps viejos
            self.callsThisMinute[key] = [t for t in self.callsThisMinute[key] if now - t < 60]
            if len(self.callsThisMinute[key]) >= RATE_LIMIT_PER_MINUTE:
                continue

            return key, name
            
        return None, None

    def recordCall(self, key: str, name: str):
        now = time.time()
        if key in self.callsToday:
            self.callsToday[key] += 1
            self.callsThisMinute[key].append(now)
            # Persistir en DB
            update_api_usage(name, self.callsToday[key])

    def blockAccount(self, key: str, minutes: int = 1):
        """Bloquea una cuenta temporalmente si recibimos un error de límite."""
        self.blockedUntil[key] = time.time() + (minutes * 60)
        logger.warning(f"⚠️ Cuenta bloqueada temporalmente por {minutes} min debido a error de rate limit.")

    def getStatus(self) -> str:
        status_lines = []
        for key, name in zip(self.apiKeys, self.accountNames):
            rem_day = RATE_LIMIT_PER_DAY - self.callsToday[key]
            min_now = len([t for t in self.callsThisMinute[key] if time.time() - t < 60])
            status_lines.append(f"{name}: {self.callsToday[key]}/{RATE_LIMIT_PER_DAY} (Min: {min_now}/{RATE_LIMIT_PER_MINUTE})")
        return " | ".join(status_lines)

    def allExhausted(self) -> bool:
        return all(self.callsToday[k] >= RATE_LIMIT_PER_DAY for k in self.apiKeys)


def get_symbols_needing_candles(symbols, db, now_local, completed_in_rest=None):
    """
    Identifica qué símbolos tienen velas pendientes de descargar o están incompletos,
    incluso si el mercado está actualmente cerrado.
    """
    if completed_in_rest is None:
        completed_in_rest = set()
    pendientes = []
    for s_data in symbols:
        sym = s_data['symbol']
        if sym in completed_in_rest:
            continue
        cnt = db.getCandleCount(sym, "1min")
        if cnt == 0:
            pendientes.append(s_data)
            continue
        last_db = db.getLastTimestamp(sym, "1min")
        if not last_db:
            pendientes.append(s_data)
            continue
        last_norm = normalize_datetime(last_db, pytz.timezone(TIMEZONE))
        if (now_local - last_norm).total_seconds() > 4 * 86400:
            pendientes.append(s_data)
    return pendientes


MAX_WORKERS = int(os.getenv("DATASYMBOL_WORKERS", "5"))

async def process_single_symbol(symbolData: dict, db: DatabaseManager, limiter: MultiAccountRateLimiter, semaphore: asyncio.Semaphore, now_local: datetime) -> dict:
    async with semaphore:
        symbol = str(symbolData['symbol'])
        try:
            lastDb = await asyncio.to_thread(db.getLastTimestamp, symbol, "1min")
            
            if lastDb:
                startDate = lastDb + timedelta(minutes=1)
            else:
                if DATA_SOURCE == "forex":
                    startDate = DEFAULT_START_DATE_1MIN
                else:
                    startDateRaw = symbolData.get('startDate')
                    if isinstance(startDateRaw, str):
                        startDate = datetime.strptime(startDateRaw, '%Y-%m-%d')
                    elif startDateRaw:
                        startDate = datetime.combine(startDateRaw, datetime.min.time())
                    else:
                        startDate = DEFAULT_START_DATE_1MIN

            TZ = pytz.timezone(TIMEZONE)
            startDate = normalize_datetime(startDate, TZ)
            lastClosed = get_safe_last_candle(now_local, interval=1)
            
            if startDate.date() == now_local.date():
                endDate = lastClosed
                if endDate < startDate:
                    return {"symbol": symbol, "status": "up_to_date", "inserted": 0}
            else:
                endDate = round1min(startDate + timedelta(minutes=MAX_MINUTES_PER_CALL))
                if endDate > lastClosed:
                    endDate = lastClosed
            
            if endDate < startDate:
                return {"symbol": symbol, "status": "up_to_date", "inserted": 0}

            # Obtener cuenta o bypass si es forex
            if DATA_SOURCE == "forex":
                apiKey = None
                accountName = "MetaTrader5"
                params = {
                    "symbol": symbol,
                    "interval": "1min",
                    "start_date": startDate,
                    "end_date": endDate
                }
            else:
                apiKey, accountName = limiter.getNextAccount()
                if not apiKey:
                    return {"symbol": symbol, "status": "rate_limited", "inserted": 0}
                params = {
                    "symbol": symbol,
                    "interval": "1min",
                    "apikey": apiKey,
                    "start_date": startDate,
                    "end_date": endDate
                }            

            if DATA_SOURCE == "forex":
                from middleware.api import forex
                df = await forex.getTimeSeries(params)
            else:
                df = await _callTimeSeriesApi(params)
                limiter.recordCall(apiKey, accountName)
            
            inserted = 0
            if df is not None and not df.empty:
                inserted = await asyncio.to_thread(db.saveBulkData, df, symbol, "1min")
                if inserted > 0:
                    logger.info(f"[{symbol}] -> {accountName} | {startDate.strftime('%Y-%m-%d %H:%M')} a {endDate.strftime('%Y-%m-%d %H:%M')} --> {inserted} velas de 1min.")
            
            if DATA_SOURCE != "forex":
                await asyncio.sleep(SLEEP_BETWEEN_CALLS)

            return {"symbol": symbol, "status": "ok", "inserted": inserted}

        except Exception as e:
            error_msg = str(e).lower()
            if DATA_SOURCE != "forex" and 'apiKey' in locals() and apiKey:
                if "rate limit" in error_msg or "too many requests" in error_msg:
                    limiter.blockAccount(apiKey)
            logger.error(f"❌ Error API [{symbol}]: {e}")
            return {"symbol": symbol, "status": "error", "error": str(e), "inserted": 0}


async def main():
    logger.info("=" * 60)
    logger.info(f"DataSymbol - Descarga Paralela Multi-Hilo ({MAX_WORKERS} Workers)")
    logger.info(f"Fuente: {DATA_SOURCE} | Concurrencia: {MAX_WORKERS} hilos")
    logger.info("=" * 60)
    
    limiter = MultiAccountRateLimiter(API_KEYS, ACCOUNT_NAMES)
    db = DatabaseManager()
    
    # TwelveData resetea a las 00:00 UTC según la observación del usuario
    api_tz = pytz.utc
    lastResetDate = datetime.now(api_tz).date()
    _last_hourly_stockprices_sync = None
    
    while True:
        try:
            now_local = datetime.now(TIMEZONE_LOCAL)
            now_api = datetime.now(api_tz)
            today_api = now_api.date()

            # --- Sincronización de StockPrices a los :05 min de cada hora ---
            if now_local.minute >= 5:
                current_hour_str = now_local.strftime("%Y-%m-%d %H:05:00")
                if _last_hourly_stockprices_sync != current_hour_str:
                    logger.info("⏳ Detectado minuto :05 de la hora, sincronizando StockPrices desde velas cerradas...")
                    try:
                        from dataSymbol.dailyStockPrices import syncDailyStockPrices
                        asyncio.create_task(syncDailyStockPrices())
                        _last_hourly_stockprices_sync = current_hour_str
                        logger.info("✅ Sincronización de StockPrices iniciada en background (async task).")
                    except Exception as e:
                        logger.error(f"Error iniciando sincronización de StockPrices: {e}")

            # --- Optimización Semanal (Viernes 00:15) ---
            current_minute_str = now_local.strftime("%Y-%m-%d %H:%M")
            if now_local.weekday() == 4 and now_local.hour == 0 and now_local.minute == 15:
                global _last_weekly_optimization
                if '_last_weekly_optimization' not in globals():
                    _last_weekly_optimization = None
                
                if _last_weekly_optimization != current_minute_str:
                    logger.info("⏳ Detectado Viernes 00:15, iniciando run_all_optimizations en background...")
                    optScriptPath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Sentinel", "backtesting", "run_all_optimizations.py"))
                    try:
                        subprocess.Popen([sys.executable, optScriptPath], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                        _last_weekly_optimization = current_minute_str
                        logger.info("✅ Optimización semanal iniciada en background.")
                    except Exception as e:
                        logger.error(f"Error iniciando optimización semanal: {e}")
            
            # Reset diario basado en el reloj de TwelveData (UTC)
            if DATA_SOURCE != "forex" and today_api > lastResetDate:
                logger.info(f"📅 Nuevo día en TwelveData detectado ({today_api}). Reseteando contadores de API.")
                for name in ACCOUNT_NAMES:
                    update_api_usage(name, 0, reset=True)
                limiter.callsToday = {key: 0 for key in limiter.apiKeys}
                limiter.callsThisMinute = {key: [] for key in limiter.apiKeys}
                lastResetDate = today_api
            
            symbols = middlewareDb.getDataSymbols()
            if not symbols:
                logger.warning("No hay símbolos activos. Esperando...")
                await asyncio.sleep(60)
                continue
            
            if DATA_SOURCE != "forex" and limiter.allExhausted():
                logger.warning("🚨 Todas las cuentas agotadas por hoy. Esperando ciclo de descanso (bucle)...")
                segundosEspera = 3600.0
                while segundosEspera > 0 and limiter.allExhausted():
                    tiempoSueñoParcial = min(60.0, segundosEspera)
                    await asyncio.sleep(tiempoSueñoParcial)
                    segundosEspera -= tiempoSueñoParcial
                continue

            # Procesamiento concurrente de toda la ronda con 5 hilos
            round_start = time.time()
            semaphore = asyncio.Semaphore(MAX_WORKERS)
            tasks = [
                process_single_symbol(sData, db, limiter, semaphore, now_local)
                for sData in symbols
            ]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            elapsed = time.time() - round_start
            total_inserted = sum(r.get("inserted", 0) for r in results if isinstance(r, dict))

            sleepSeconds, nextTime = seconds_until_next_1min(datetime.now(TIMEZONE_LOCAL))
            statusStr = f"MetaTrader5 Local ({MAX_WORKERS} hilos)" if DATA_SOURCE == "forex" else limiter.getStatus()
            logger.info(f"✅ Ronda completada ({len(symbols)} símbolos en {MAX_WORKERS} hilos en {elapsed:.1f}s | {total_inserted} velas). Próximo escaneo: {nextTime.strftime('%H:%M:%S')} (Status: {statusStr}) \n")

            while sleepSeconds > 0:
                tiempoSueñoParcial = min(60.0, sleepSeconds)
                await asyncio.sleep(tiempoSueñoParcial)
                sleepSeconds -= tiempoSueñoParcial
                
        except Exception as e:
            logger.error(f"⚠️ Error en ciclo principal: {e}", exc_info=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario")
