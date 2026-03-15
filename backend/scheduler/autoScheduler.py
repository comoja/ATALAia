
import os
import sys
import pytz
from datetime import datetime
import asyncio

# Esto permite que el archivo encuentre la carpeta 'config'
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ruta_raiz not in sys.path:
    sys.path.insert(0, ruta_raiz)

from config.settings import timeZone, FESTIVOS
import logging
logger = logging.getLogger(__name__)
from apscheduler.schedulers.blocking import BlockingScheduler


def isRestTime():
    # Configuración de la zona horaria
    tz = pytz.timezone(timeZone)
    now = datetime.now(tz)

    weekday = now.weekday()
    hour = now.hour

    # Lunes-Viernes 00-06
    if weekday <= 4 and 0 <= hour < 6:
        logger.info("horario nocturno laboral")
        return True

    # Lunes-Viernes 16-17
    if weekday <= 4 and 16 <= hour < 17:
        logger.info("horario de comida laboral")
        return True

    # Viernes después 17:00
    if weekday == 4 and hour >= 17:
        logger.info("horario de fin de semana")
        return True

    # Sábado
    if weekday == 5:
        logger.info("horario sabatino ")
        return True

    # Domingo antes de 17
    if weekday == 6 and hour < 17:
        logger.info("horario dominical")
        return True

    return False

def startScheduler(jobFunction):

    scheduler = BlockingScheduler(timezone=timeZone)

    scheduler.add_job(
        jobFunction,
        trigger='cron',
        minute=0
    )

    print("Scheduler started...")
    scheduler.start()



async def getTiempoEspera(intervalo_minutos):
        tz = pytz.timezone(timeZone)
        now = datetime.now(tz)
        if now.strftime("%Y-%m-%d") in FESTIVOS:
            logger.info(f"Dia festivo: {now.strftime('%Y-%m-%d')}")
            await asyncio.sleep(segundos_espera)
        if now.hour == 23 and now.minute >= 0 and now.second <= 55:
            logger.info("🎯 DETECTADO CIERRE DIARIO (23:00). Iniciando escaneo de 8h...")
        if isRestTime():
            intervalo_minutos = 60
        minutos_proximos = intervalo_minutos - (now.minute % intervalo_minutos)
        segundos_espera = (minutos_proximos * 60) - now.second + 2
        if segundos_espera > 20:
            logger.info(f"⏳ Sincronizando: Próximo escaneo en {segundos_espera // 60}m {segundos_espera % 60}s")
            await asyncio.sleep(segundos_espera)