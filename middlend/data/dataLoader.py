import requests
import pandas as pd
import time

from middlend.config.settings import MaxXminuto, MaxXdia, minutosXdia, INTERVAL, API_KEYS, INTERVALmax, timeframes


from datetime import datetime
import asyncio

import logging
logger = logging.getLogger(__name__)

lastCallTime = 0
indice = -1 # Empezamos con la primera API Key

keyActiva = API_KEYS[indice]
nombreKey = "JAIME" if indice == 0 else "RAUL" if indice == 1 else "SEBASTIAN"
ULTIMA_KEY_USADA = keyActiva
ULTIMO_INTERVALO_USADO = INTERVAL

def getParametros():
    global ULTIMA_KEY_USADA, ULTIMO_INTERVALO_USADO, indice, keyActiva, nombreKey
    ahora = datetime.now()
    
    # 1. Rotación de Cuenta de TwelveData
    indice = (indice + 1) % len(API_KEYS)
    keyActiva = API_KEYS[indice]
    nombres = ["JAIME", "RAUL", "SEBASTIAN"]
    nombreKey = f"{nombres[indice]} ({indice})"
    
    # 2. Selección de Intervalo (hora = 1h, resto = 15min)
    # Si el minuto es 01-14, usamos 1h (para caso de inicio temprano); si es 15, 30 o 45, usamos 15min
    intervaloActual = INTERVALmax if (ahora.hour in timeframes and  ahora.minute < 15) else INTERVAL

    # 3. CÁLCULO DINÁMICO DE VELAS (~20 días para ambos)
    esperaMin = 5

    if "min" in intervaloActual:
        mins = int(intervaloActual.replace("min", ""))
        # 15min * 2000 = 30000 min = ~20 días
        velasAPedir = 2000
        esperaMin = mins
    elif "h" in intervaloActual:
        horas = int(intervaloActual.replace("h", ""))
        # 1h * 500 = 500 horas = ~20 días
        velasAPedir = 500
        esperaMin = horas * 60

    elif "day" in intervaloActual:
        velasAPedir = 100 # Para diario, 100 velas es más que suficiente
        esperaMin = 24 * 60
    elif "week" in intervaloActual:
        velasAPedir = 300 # Para diario, 100 velas es más que suficiente
        esperaMin = 24 * 5 * 60    
    elif "month" in intervaloActual:
        velasAPedir = 1200 # Para diario, 100 velas es más que suficiente
        esperaMin = 24 * 30 * 60    
    else:
        velasAPedir = 500 # Valor por defecto
    # Sincronización normal de 15 minutos (00, 15, 30, 45)
    
    # Limites de seguridad para la API (Mínimo 100, Máximo 5000)
    velasAPedir = max(100, min(velasAPedir, 5000))

    return keyActiva, intervaloActual, nombreKey, velasAPedir, esperaMin


def rateLimit():
    global lastCallTime
    now = time.time()
    if now - lastCallTime < int(minutosXdia / (MaxXdia / MaxXminuto)):  # llamadas por minuto seguras
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