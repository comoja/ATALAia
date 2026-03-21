import sys
import os
import asyncio
import requests

import mysql.connector
import requests
import pytz
from datetime import datetime, timedelta

from Sentinel.database import dbConnection

# Esto detecta la carpeta 'backend' y la registra en Python
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ruta_raiz not in sys.path:
    sys.path.insert(0, ruta_raiz)

from core.logger_config import setup_logging

# 1. Configura el sistema de logs antes que nada
setup_logging()
from config import settings
from scheduler.autoScheduler import getTiempoEspera
from core.comm import enviar_alerta
from database import dbManager
from data.dataLoader import getParametros, nombre_key

import logging
logger = logging.getLogger(__name__) 


def fetchTwelveData(symbol, isNewLoad, tiempo=5,intervalo="1day"):
    key, inter, nom, nVelas, esperaMin = getParametros()
    logger.info(f"Cargando datos de la cuenta 12Data ({nom})")
    
    dateStart = (datetime.now() - timedelta(days=tiempo)).strftime('%Y-%m-%d')
    apiUrl = f"https://api.twelvedata.com/time_series?apikey={key}&symbol={symbol}&interval={intervalo}&start_date={dateStart}"
    
    try:
        # Usamos un timeout para evitar que el bot se quede colgado
        apiResponse = requests.get(apiUrl, timeout=15)
        apiResponse.raise_for_status() 
        data = apiResponse.json()
        # Verificar si la API devolvió un error interno (como símbolo no encontrado)
        if data.get("status") == "error":
            logger.error(f"Error de 12Data para {symbol}: {data.get('message')}")
            return {}
            
        return data
        
    except Exception as e:
        logger.error(f"Error de conexión para {symbol}: {e}")
        return {}


async def priceUpdateBot():
    # Manejo de horarios: NY cierra a las 16:00 EST
    nyZone = pytz.timezone('America/New_York')
    nyTime = datetime.now(nyZone)
    logger.info(f"Sincronizando con NY. Hora actual: {nyTime.strftime('%H:%M:%S')}")
    tiempo = 20 * 365 # en años  

    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)

        # 1. Obtener símbolos activos de tu tabla catálogo
        dbCursor.execute("SELECT symbol FROM RatioSymbol WHERE Activo = 1")
        activeSymbols = dbCursor.fetchall()

        for row in activeSymbols:
            currentSymbol = row['symbol']
            # 2. Verificar si ya tenemos datos históricos para este símbolo
            dbCursor.execute("SELECT COUNT(*) as total FROM StockPrices WHERE symbol = %s", (currentSymbol,))
            hasHistory = dbCursor.fetchone()['total'] > 0
            if not hasHistory:
                tiempo = 20 * 365 # en años
            else:
                tiempo = 5
            logger.info(f"⌛ Cargando {tiempo} dia(s) de datos para: {currentSymbol}")
            historicalResponse = fetchTwelveData(currentSymbol, not hasHistory, tiempo)
            
            if "values" in historicalResponse:
                for dataPoint in historicalResponse["values"]:
                    insertQuery = """
                        INSERT INTO StockPrices (symbol, priceDate, closePrice, volume)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            closePrice = VALUES(closePrice),
                            priceDate = VALUES(priceDate),
                            volume = VALUES(volume);
                    """
                    dbCursor.execute(insertQuery, (currentSymbol, dataPoint['datetime'], dataPoint['close'], dataPoint.get('volume', 0)))
                dbConn.commit()
                logger.info(f"✅ Historial finalizado para {currentSymbol}\n")
            await asyncio.sleep(9)
        dbCursor.close()
        dbConn.close()
        logger.info("🚀 Bot ejecutado correctamente.\n")

    except Exception as error:
        logger.info(f"❌ Error en la ejecución al descargar los precios de cierre de {currentSymbol}: {error}")

if __name__ == "__main__":
    # Usa asyncio.run() para ejecutar funciones asíncronas
    try:
        asyncio.run(priceUpdateBot())
    except KeyboardInterrupt:
        pass