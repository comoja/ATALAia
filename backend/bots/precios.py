import sys
import os
import asyncio
import requests

import mysql.connector
import requests
import pytz
from datetime import datetime, timedelta

# Esto detecta la carpeta 'backend' y la registra en Python
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ruta_raiz not in sys.path:
    sys.path.insert(0, ruta_raiz)

from config import settings
from scheduler.autoScheduler import getTiempoEspera
from core.comm import enviar_alerta
from database import dbConnection, dbManager
from data.dataLoader import getParametros, nombre_key

import logging
logger = logging.getLogger(__name__) 


def fetchTwelveData(symbol, isNewLoad, tiempo=4,intervalo="1day"):
    key, inter, nom, nVelas, esperaMin = getParametros()
    logger.info(f"Cargando datos de la cuenta 12Data ({nom})")
    
    # 1. Definir el endpoint base según la necesidad
    # 2. IMPORTANTE: Usamos el parámetro ?symbol= para que la API entienda la diagonal
    if isNewLoad:
        dateStart = (datetime.now() - timedelta(days=tiempo*365)).strftime('%Y-%m-%d')
        # URL ESTRUCTURADA: endpoint + ?symbol= + activo        
        apiUrl = f"https://api.twelvedata.com/time_series?apikey={key}&symbol={symbol}&interval={intervalo}&start_date={dateStart}"
        
    else:
        
        # URL ESTRUCTURADA: endpoint + ?symbol= + activo
        apiUrl = f"https://api.twelvedata.com/time_series?apikey={key}&symbol={symbol}&interval=1day"

    
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
    print(f"Sincronizando con NY. Hora actual: {nyTime.strftime('%H:%M:%S')}")
    tiempo = 20 # en años  

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
                print(f"⌛ Símbolo nuevo detectado. Cargando {tiempo} años para: {currentSymbol}")
                historicalResponse = fetchTwelveData(currentSymbol, True, tiempo)
                
                if "values" in historicalResponse:
                    for dataPoint in historicalResponse["values"]:
                        insertQuery = """
                            INSERT IGNORE INTO StockPrices (symbol, priceDate, closePrice, volume)
                            VALUES (%s, %s, %s, %s)
                        """
                        dbCursor.execute(insertQuery, (currentSymbol, dataPoint['datetime'], dataPoint['close'], dataPoint.get('volume', 0)))
                print(f"✅ Historial finalizado para {currentSymbol}")
            
            else:
                print(f"🔄 Actualizando cierre diario para: {currentSymbol}")
                dailyResponse = fetchTwelveData(currentSymbol, False)
                
                if "close" in dailyResponse:
                    closeVal = dailyResponse['close']
                    dateVal = dailyResponse['datetime'].split(" ")[0] # Asegurar solo fecha
                    volVal = dailyResponse.get('volume', 0)

                    upsertQuery = """
                        INSERT INTO StockPrices (symbol, priceDate, closePrice, volume)
                        VALUES (%s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE 
                            closePrice = VALUES(closePrice), 
                            volume = VALUES(volume)
                    """
                    dbCursor.execute(upsertQuery, (currentSymbol, dateVal, closeVal, volVal))
            dbConn.commit()
            await asyncio.sleep(9)
        dbCursor.close()
        dbConn.close()
        print("🚀 Bot ejecutado correctamente.")

    except Exception as error:
        print(f"❌ Error en la ejecución: {error}")

if __name__ == "__main__":
    # Usa asyncio.run() para ejecutar funciones asíncronas
    try:
        asyncio.run(priceUpdateBot())
    except KeyboardInterrupt:
        pass