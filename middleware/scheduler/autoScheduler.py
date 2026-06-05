
import os
import pytz
from datetime import datetime, timedelta
import asyncio

from middleware.config.constants import timeZone, FESTIVOS
import logging
logger = logging.getLogger("sentinel")
from apscheduler.schedulers.blocking import BlockingScheduler

try:
    from middleware.database.dbManager import get_sleep_time
except ImportError:
    get_sleep_time = None

try:
    from middleware.config.constants import DATA_SOURCE
except ImportError:
    DATA_SOURCE = "db"


from middleware.utils.time_utils import isRestTime, get_sleep_minutes, get_seconds_to_next_sync, get_seconds_until_market_opens

def startScheduler(jobFunction):

    scheduler = BlockingScheduler(timezone=timeZone)

    scheduler.addJob(
        jobFunction,
        trigger='cron',
        minute=0
    )

    print("Scheduler started...")
    scheduler.start()

def get_next_sync_time(intervaloMinutos):
    """
    Calcula el objeto datetime de la próxima ejecución sincronizada.
    Aterriza en minutos: 3, 8, 13, 18... (+3.5 min offset)
    """
    tz = pytz.timezone(timeZone)
    now = datetime.now(tz)
    
    offset_segundos = 2.1 * 60
    intervalo_segundos = intervaloMinutos * 60
    current_seconds_in_hour = now.minute * 60 + now.second
    
    segundosEspera = (offset_segundos - current_seconds_in_hour) % intervalo_segundos
    if segundosEspera < 20:
        segundosEspera += intervalo_segundos
        
    return now + timedelta(seconds=segundosEspera)



async def getTiempoEspera(intervaloMinutos):
    # 1. Ajustar intervalo según DATA_SOURCE si aplica
    if get_sleep_time:
        intervaloMinutos = get_sleep_time(intervaloMinutos)
    
    # 2. Manejo de Festivos/Descanso
    tz = pytz.timezone(timeZone)
    now = datetime.now(tz)
    if now.strftime("%Y-%m-%d") in FESTIVOS:
        logger.info(f"Dia festivo: {now.strftime('%Y-%m-%d')}. Esperando 1 hora...")
        await asyncio.sleep(3600)
        return
        
    if isRestTime():
        segundos_sueño = get_seconds_until_market_opens()
        segundos_sueño += 10.0
        horas = int(segundos_sueño // 3600)
        minutos = int((segundos_sueño % 3600) // 60)
        segundos = int(segundos_sueño % 60)
        logger.info(f"Mercado en descanso. Esperando {horas}h {minutos}m {segundos}s hasta la apertura...")
        await asyncio.sleep(segundos_sueño)
        return

    # 3. Cálculo de Sincronización
    proximo_escaneo = get_next_sync_time(intervaloMinutos)
    segundosEspera = (proximo_escaneo - datetime.now(tz)).total_seconds()
    
    hora_str = proximo_escaneo.strftime("%H:%M:%S")
    logger.info(f"⏳ Sincronizando: Próximo escaneo a las {hora_str} (faltan {int(segundosEspera // 60)}m {int(segundosEspera % 60)}s)\n\n")
    
    if segundosEspera > 0:
        await asyncio.sleep(segundosEspera)