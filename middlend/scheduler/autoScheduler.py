
import os
import pytz
from datetime import datetime
import asyncio

from middlend.config.settings import timeZone, FESTIVOS
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

    scheduler.addJob(
        jobFunction,
        trigger='cron',
        minute=0
    )

    print("Scheduler started...")
    scheduler.start()



async def getTiempoEspera(intervaloMinutos):
        tz = pytz.timezone(timeZone)
        now = datetime.now(tz)
        if now.strftime("%Y-%m-%d") in FESTIVOS:
            logger.info(f"Dia festivo: {now.strftime('%Y-%m-%d')}")
            await asyncio.sleep(segundosEspera)
        if isRestTime():
            intervaloMinutos = 60
        minutosProximos = intervaloMinutos - (now.minute % intervaloMinutos)
        segundosEspera = (minutosProximos * 60) - now.second + 2
        if segundosEspera > 20:
            logger.info(f"⏳ Sincronizando: Próximo escaneo en {segundosEspera // 60}m {segundosEspera % 60}s")
            await asyncio.sleep(segundosEspera)