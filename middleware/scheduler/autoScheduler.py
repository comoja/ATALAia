
import os
import pytz
from datetime import datetime
import asyncio

from middleware.config.settings import timeZone, FESTIVOS
import logging
logger = logging.getLogger(__name__)
from apscheduler.schedulers.blocking import BlockingScheduler


def isRestTime():
    # Configuración de la zona horaria
    tz = pytz.timezone(timeZone)
    now = datetime.now(tz)

    weekday = now.weekday()
    hour = now.hour
    logger.info( f"------ hora de hoy  {now.strftime("%Y-%m-%d %H:%M:%S" )} weekday: {weekday} hour: {hour} minute: {now.minute} ")

    # Lunes-Viernes 00:01-06:00 (horario nocturno - no opera)
    #if weekday <= 4:
     #   if hour == 0 and now.minute >= 1:
      #      logger.info("horario nocturno - no opera")
      #      return True
      #  if 1 <= hour < 6:
      #      logger.info("horario nocturno - no opera")
      #      return True

    # Lunes-Jueves 16:03-17:00 (horario de lunch - no opera)
    if weekday <= 3 and (hour == 16 and now.minute >= 3) and hour < 17 :
        logger.info("horario de lunch - no opera")
        return True

    # Viernes desde 17:00 - no opera hasta domingo 17:00
    if weekday == 4 and hour >= 17:
        logger.info("viernes noche - no opera")
        return True

    # Sábado y Domingo - no opera
    if weekday >= 5:
        logger.info("fin de semana - no opera")
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
            logger.info(f"⏳ Sincronizando: Próximo escaneo en {segundosEspera // 60}m {segundosEspera % 60}s\n\n")
            await asyncio.sleep(segundosEspera)