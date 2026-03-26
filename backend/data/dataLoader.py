import requests
import pandas as pd
import time

from middleware.config.constants import MaxXminuto, MaxXdia, minutosXdia, INTERVAL, API_KEYS, INTERVALmax, timeframes
from datetime import datetime
import asyncio

import logging
logger = logging.getLogger(__name__)

lastCallTime = 0
indice = -1 # Empezamos con la primera API Key

key_activa = API_KEYS[indice]
nombre_key = "JAIME" if indice == 0 else "RAUL" if indice == 1 else "SEBASTIAN"
ULTIMA_KEY_USADA = key_activa
ULTIMO_INTERVALO_USADO = INTERVAL

def getParametros():
    global ULTIMA_KEY_USADA, ULTIMO_INTERVALO_USADO, indice, key_activa, nombre_key
    ahora = datetime.now()
    
    # 1. Rotación de Cuenta de TwelveData
    indice = (indice + 1) % len(API_KEYS)
    key_activa = API_KEYS[indice]
    nombres = ["JAIME", "RAUL", "SEBASTIAN"]
    nombre_key = f"{nombres[indice]} ({indice})"
    
    # 2. Selección de Intervalo (Cierre 23:00)
    intervalo_actual = INTERVALmax if (ahora.hour in timeframes and 0 < ahora.minute < 15) else INTERVAL

    # 3. CÁLCULO DINÁMICO DE VELAS (Lookback de ~10 días)
    # Definimos cuántos minutos queremos ver hacia atrás (10 días = 14400 min)
    minutos_objetivo = 14400 
    esperaMin = 5 # Valor por defecto para el tiempo de espera entre llamadas a la API

    if "min" in intervalo_actual:
        mins = int(intervalo_actual.replace("min", ""))
        velas_a_pedir = minutos_objetivo // mins
        esperaMin = mins 
    elif "h" in intervalo_actual:
        horas = int(intervalo_actual.replace("h", ""))
        # Si es por horas, pedimos 40 días (57600 min) para asegurar > 100 velas
        minutos_objetivo_horas = 57600 
        velas_a_pedir = minutos_objetivo_horas // (horas * 60)
        esperaMin = horas * 60

    elif "day" in intervalo_actual:
        velas_a_pedir = 100 # Para diario, 100 velas es más que suficiente
        esperaMin = 24 * 60
    elif "week" in intervalo_actual:
        velas_a_pedir = 300 # Para diario, 100 velas es más que suficiente
        esperaMin = 24 * 5 * 60    
    elif "month" in intervalo_actual:
        velas_a_pedir = 1200 # Para diario, 100 velas es más que suficiente
        esperaMin = 24 * 30 * 60    
    else:
        velas_a_pedir = 500 # Valor por defecto
    # Sincronización normal de 15 minutos (00, 15, 30, 45)
    
    # Limites de seguridad para la API (Mínimo 100, Máximo 5000)
    velas_a_pedir = max(100, min(velas_a_pedir, 5000))

    return key_activa, intervalo_actual, nombre_key, velas_a_pedir, esperaMin


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
        "apikey": key_activa
    }
    try:
        r = response = requests.get(url, params=params).json()
        df = pd.DataFrame(r["values"])
    except:
        raise Exception("Error TwelveData (JSON)")
        logger.error("Error TwelveData (JSON)")
    if "values" not in r: raise Exception(f"API Error: {r.get('message')}")

    return df