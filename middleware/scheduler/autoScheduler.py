
import os
import pytz
from datetime import datetime, timedelta
import asyncio

from middleware.config.constants import timeZone, FESTIVOS
import logging
logger = logging.getLogger(__name__)
from apscheduler.schedulers.blocking import BlockingScheduler

try:
    from middleware.database.dbManager import get_sleep_time
except ImportError:
    get_sleep_time = None

try:
    from middleware.config.constants import DATA_SOURCE
except ImportError:
    DATA_SOURCE = "db"


from middleware.utils.time_utils import is_market_closed

def isRestTime(dt=None):
    """
    Determina si el mercado está en periodo de descanso/cierre.
    Utiliza la lógica centralizada de time_utils.
    """
    return is_market_closed(dt)

def startScheduler(jobFunction):

    scheduler = BlockingScheduler(timezone=timeZone)

    scheduler.addJob(
        jobFunction,
        trigger='cron',
        minute=0
    )

    print("Scheduler started...")
    scheduler.start()



async def getTiempoEspera(intervaloMinutos):
    # 1. Ajustar intervalo según DATA_SOURCE si aplica
    if get_sleep_time:
        intervaloMinutos = get_sleep_time(intervaloMinutos)
    
    tz = pytz.timezone(timeZone)
    now = datetime.now(tz)
    
    # 2. Manejo de Festivos
    if now.strftime("%Y-%m-%d") in FESTIVOS:
        logger.info(f"Dia festivo: {now.strftime('%Y-%m-%d')}. Esperando 1 hora...")
        await asyncio.sleep(3600)
        return
        
    # 3. Horario de descanso (isRestTime)
    if isRestTime():
        intervaloMinutos = 60 # Forzar espera de 1 hora si está en descanso

    # 4. Cálculo de Sincronización con OFFSET (+3 minutos)
    # Queremos aterrizar en minutos: 3, 8, 13, 18, 23, 28, 33, 38, 43, 48, 53, 58 (si intervalo=5)
    offset_segundos = 3.5 * 60
    intervalo_segundos = intervaloMinutos * 60
    current_seconds_in_hour = now.minute * 60 + now.second
    
    # Segundos restantes para el próximo bloque con offset
    segundosEspera = (offset_segundos - current_seconds_in_hour) % intervalo_segundos
    
    # Si la espera es demasiado corta (menos de 20s), probablemente acabamos de terminar el bloque actual, 
    # esperamos al siguiente para evitar re-ejecuciones inmediatas.
    if segundosEspera < 20:
        segundosEspera += intervalo_segundos
    
    # 5. Cálculo de hora exacta del próximo escaneo
    proximo_escaneo = now + timedelta(seconds=segundosEspera)
    hora_str = proximo_escaneo.strftime("%H:%M:%S")
    
    logger.info(f"⏳ Sincronizando: Próximo escaneo a las {hora_str} (faltan {segundosEspera // 60}m {segundosEspera % 60}s)\n\n")
    
    if segundosEspera > 0:
        await asyncio.sleep(segundosEspera)