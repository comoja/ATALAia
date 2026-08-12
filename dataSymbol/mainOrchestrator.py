import os
import sys
import time
import logging
import asyncio
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
    next_time = next_time + timedelta(seconds=buffer_seconds if DATA_SOURCE != "forex" else 2)
    sleep_seconds = int((next_time - now).total_seconds())
    return max(0, sleep_seconds), next_time

# --- Funciones Auxiliares ---
def round5min(timestamp):
    if isinstance(timestamp, str):
        dt = datetime.strptime(timestamp, "%Y/%m/%d %H:%M:%S")
    else:
        dt = timestamp
    minute = (dt.minute // 5) * 5
    return dt.replace(minute=minute, second=0, microsecond=0)

def adjust_to_market_open(dt):
    attempts = 0
    while isRestTime(dt) and attempts < 2000: 
        dt += timedelta(minutes=5)
        attempts += 1
    return dt

def get_safe_last_candle(now, interval=5):
    """
    Calcula el timestamp de la última vela CERRADA.
    Restamos el intervalo completo (5 min) más un buffer de seguridad (30s) 
    para asegurar que los datos ya están disponibles en la API.
    """
    safe_now = now - timedelta(minutes=interval) - timedelta(seconds=30)
    minute = (safe_now.minute // interval) * interval
    return safe_now.replace(minute=minute, second=0, microsecond=0)

from middleware.utils.time_utils import get_last_closed_candle as _get_last_closed_candle

def get_last_closed_candle(now, interval=5, df=None):
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

async def main():
    logger.info("=" * 60)
    logger.info("DataSymbol - Descarga Optimizada (Multi-Account 4x Speed)")
    logger.info(f"Configuración: {TOTAL_ACCOUNTS} cuentas | Límite: {RATE_LIMIT_PER_MINUTE}/min | Delay: {SLEEP_BETWEEN_CALLS:.2f}s")
    logger.info("=" * 60)
    
    limiter = MultiAccountRateLimiter(API_KEYS, ACCOUNT_NAMES)
    db = DatabaseManager()
    
    # TwelveData resetea a las 00:00 UTC según la observación del usuario
    api_tz = pytz.utc
    lastResetDate = datetime.now(api_tz).date()
    symbolIndex = 0
    ultimoLogMinuto = -1
    _last_hourly_stockprices_sync = None
    
    while True:
        try:
            now_local = datetime.now(TIMEZONE_LOCAL)
            now_api = datetime.now(api_tz)
            today_api = now_api.date()

            # --- Sincronización de StockPrices cada hora ---
            current_hour_str = now_local.strftime("%Y-%m-%d %H:00:00")
            if _last_hourly_stockprices_sync != current_hour_str:
                logger.info("⏳ Detectada nueva hora, sincronizando StockPrices desde candles...")
                import subprocess
                import sys, os
                scriptPath = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "fill_stockprices.py"))
                try:
                    subprocess.Popen([sys.executable, scriptPath], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    _last_hourly_stockprices_sync = current_hour_str
                    logger.info("✅ Sincronización de StockPrices iniciada en background.")
                except Exception as e:
                    logger.error(f"Error iniciando sincronización de StockPrices: {e}")

            # --- Optimización Semanal (Viernes 00:15) ---
            # Se ejecuta a las 00:15 de cada viernes (weekday 4 = Viernes)
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

            if isRestTime():
                segundosSueño = get_seconds_until_market_opens()
                segundosSueño += 10.0
                horasRestantes = int(segundosSueño // 3600)
                minutosRestantes = int((segundosSueño % 3600) // 60)
                segundosRestantes = int(segundosSueño % 60)
                tiempoSueñoParcial = min(60.0, segundosSueño)
                
                mensajeDescanso = f"💤 Periodo de descanso detectado. Apertura en {horasRestantes}h {minutosRestantes}m {segundosRestantes}s."
                if minutosRestantes % 15 == 0 and minutosRestantes != ultimoLogMinuto:
                    logger.info(mensajeDescanso)
                    ultimoLogMinuto = minutosRestantes
                else:
                    logger.debug(mensajeDescanso + f" (durmiendo ciclo de {int(tiempoSueñoParcial)}s)")
                
                await asyncio.sleep(tiempoSueñoParcial)
                continue
            
            # Reset diario basado en el reloj de TwelveData (UTC)
            if DATA_SOURCE != "forex" and today_api > lastResetDate:
                logger.info(f"📅 Nuevo día en TwelveData detectado ({today_api}). Reseteando contadores de API.")
                for name in ACCOUNT_NAMES:
                    update_api_usage(name, 0, reset=True)
                limiter.callsToday = {key: 0 for key in limiter.apiKeys}
                limiter.callsThisMinute = {key: [] for key in limiter.apiKeys}
                lastResetDate = today_api
                symbolIndex = 0
            
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
        
            if symbolIndex >= len(symbols):
                symbolIndex = 0
                sleepSeconds, nextTime = seconds_until_next_5min(now_local)
                statusStr = "MetaTrader5 Local" if DATA_SOURCE == "forex" else limiter.getStatus()
                logger.info(f"✅ Ronda completada. Próximo escaneo: {nextTime.strftime('%H:%M:%S')} (Status: {statusStr}) \n")
                while sleepSeconds > 0:
                    tiempoSueñoParcial = min(60.0, sleepSeconds)
                    await asyncio.sleep(tiempoSueñoParcial)
                    sleepSeconds -= tiempoSueñoParcial
                continue
            
            symbolData = symbols[symbolIndex]
            symbol = str(symbolData['symbol'])        
            
            lastDb = db.getLastTimestamp(symbol, "5min")
            symbolIndex += 1
            
            if lastDb:
                startDate = lastDb + timedelta(minutes=5)
            else:
                startDateRaw = symbolData.get('startDate')
                if isinstance(startDateRaw, str):
                    startDate = datetime.strptime(startDateRaw, '%Y-%m-%d')
                elif startDateRaw:
                    startDate = datetime.combine(startDateRaw, datetime.min.time())
                else:
                    startDate = datetime(2020, 1, 1) # Fallback más reciente para no saturar

            TZ = pytz.timezone(TIMEZONE)
            startDate = normalize_datetime(startDate, TZ)
            lastClosed = get_safe_last_candle(now_local)
            
            if startDate.date() == now_local.date():
                endDate = lastClosed
                if endDate < startDate:
                    continue
            else:
                endDate = round5min(startDate + timedelta(minutes=MAX_MINUTES_PER_CALL))
                if endDate > lastClosed:
                    endDate = lastClosed
            
            if endDate < startDate:
                continue

            # Obtener cuenta o bypass si es forex
            if DATA_SOURCE == "forex":
                apiKey = None
                accountName = "MetaTrader5"
                params = {
                    "symbol": symbol,
                    "interval": "5min",
                    "start_date": startDate,
                    "end_date": endDate
                }
            else:
                # Obtener siguiente cuenta disponible para Twelve Data
                apiKey, accountName = limiter.getNextAccount()
                if not apiKey:
                    # Si no hay cuenta disponible ahora (límite por minuto alcanzado), esperamos un poco
                    await asyncio.sleep(2)
                    continue
                params = {
                    "symbol": symbol,
                    "interval": "5min",
                    "apikey": apiKey,
                    "start_date": startDate,
                    "end_date": endDate
                }            
            
            
            try:               
                
                if DATA_SOURCE == "forex":
                    from middleware.api import forex
                    df = await forex.getTimeSeries(params)
                else:
                    df = await _callTimeSeriesApi(params)
                    # Registrar llamada exitosa en Twelve Data
                    limiter.recordCall(apiKey, accountName)
                
                if df is not None and not df.empty:
                    inserted = db.saveBulkData(df, symbol, "5min")
                    if inserted > 0:
                        logger.info(f"[{symbol}] -> {accountName} | {startDate.strftime('%Y-%m-%d %H:%M')} a {endDate.strftime('%Y-%m-%d %H:%M')} --> {inserted} velas de {DATA_SOURCE}.")
                
                # Espera dinámica entre llamadas
                await asyncio.sleep(SLEEP_BETWEEN_CALLS)
                
            except Exception as e:
                error_msg = str(e).lower()
                if DATA_SOURCE != "forex" and apiKey:
                    if "rate limit" in error_msg or "too many requests" in error_msg:
                        limiter.blockAccount(apiKey)
                logger.error(f"❌ Error API [{symbol}] con {accountName}: {e}")
                await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"⚠️ Error en ciclo principal: {e}", exc_info=True)
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario")
