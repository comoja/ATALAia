import requests
import pandas as pd
import time
from datetime import datetime
import asyncio
import logging

from middleware.config import constants

logger = logging.getLogger(__name__)

lastCallTime = 0
indice = -1

keyActiva = constants.API_KEYS[indice]
nombreKey = "JAIME" if indice == 0 else "RAUL" if indice == 1 else "SEBASTIAN"
ULTIMA_KEY_USADA = keyActiva
ULTIMO_INTERVALO_USADO = constants.INTERVAL

def getParametros():
    global ULTIMA_KEY_USADA, ULTIMO_INTERVALO_USADO, indice, keyActiva, nombreKey
    ahora = datetime.now()
    
    indice = (indice + 1) % len(constants.API_KEYS)
    keyActiva = constants.API_KEYS[indice]
    nombres = ["JAIME", "RAUL", "SEBASTIAN"]
    nombreKey = f"{nombres[indice]} ({indice})"
    
    intervaloActual = constants.INTERVALmax if (ahora.hour in constants.timeframes and ahora.minute < 15) else constants.INTERVAL

    esperaMin = 5

    if "min" in intervaloActual:
        mins = int(intervaloActual.replace("min", ""))
        velasAPedir = 2000
        esperaMin = mins
    elif "h" in intervaloActual:
        horas = int(intervaloActual.replace("h", ""))
        velasAPedir = 500
        esperaMin = horas * 60
    elif "day" in intervaloActual:
        velasAPedir = 100
        esperaMin = 24 * 60
    elif "week" in intervaloActual:
        velasAPedir = 300
        esperaMin = 24 * 5 * 60    
    elif "month" in intervaloActual:
        velasAPedir = 1200
        esperaMin = 24 * 30 * 60    
    else:
        velasAPedir = 500
    
    velasAPedir = max(100, min(velasAPedir, 5000))

    return keyActiva, intervaloActual, nombreKey, velasAPedir, esperaMin


def rateLimit():
    global lastCallTime
    now = time.time()
    if now - lastCallTime < int(constants.minutosXdia / (constants.MaxXdia / constants.MaxXminuto)):
        time.sleep(10 - (now - lastCallTime))
    lastCallTime = time.time()


def getPriceData(url,symbol, interval, outputsize):
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": outputsize,
        "apikey": keyActiva
    }
    try:
        r = response = requests.get(url, params=params).json()
        df = pd.DataFrame(r["values"])
    except:
        raise Exception("Error TwelveData (JSON)")
        logger.error("Error TwelveData (JSON)")
    if "values" not in r: raise Exception(f"API Error: {r.get('message')}")

    return df