import os
import sys
import logging
import asyncio
from datetime import datetime, timedelta
import pandas as pd
import pytz

# Configuración de rutas
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from middleware.utils.loggerConfig import setupLogging
setupLogging(logPara="dataSymbolDaily", projectDir=os.path.dirname(os.path.abspath(__file__)), enableConsole=True)

logger = logging.getLogger("dataSymbolDaily")

from middleware.database import dbManager
from middleware.config.constants import TIMEZONE, DATA_SOURCE

async def syncDailyStockPrices():
    logger.info("Iniciando sincronización de StockPrices diarios (D1)...")
    
    # 1. Obtener todos los símbolos activos
    symbolsData = dbManager.getSymbols()
    if not symbolsData:
        logger.warning("No se encontraron símbolos activos en la base de datos.")
        return

    tzLocal = pytz.timezone(TIMEZONE)
    nowLocal = datetime.now(tzLocal)
    
    from middleware.api import forex, twelvedata
    from middleware.config.constants import API_KEYS
    import random

    totalInserted = 0
    for symbolInfo in symbolsData:
        symbol = str(symbolInfo['symbol'])
        logger.info(f"Procesando {symbol}...")

        # Obtener última fecha en StockPrices
        lastDate = dbManager.getLastStockPriceDate(symbol)
        
        if lastDate:
            # Empezamos desde la hora siguiente a la última fecha guardada
            startDate = tzLocal.localize(lastDate.replace(tzinfo=None) + timedelta(hours=1))
        else:
            # Si no hay datos, traer un buen historial (por ejemplo, desde 2020)
            startDateRaw = symbolInfo.get('startDate')
            if isinstance(startDateRaw, str):
                startDate = tzLocal.localize(datetime.strptime(startDateRaw, '%Y-%m-%d'))
            elif startDateRaw:
                startDate = tzLocal.localize(datetime.combine(startDateRaw, datetime.min.time()))
            else:
                startDate = tzLocal.localize(datetime(2020, 1, 1))

        current_hour_localized = nowLocal.replace(minute=0, second=0, microsecond=0)
        
        # Asegurar que start_date no sea la hora actual o futuro
        if startDate >= current_hour_localized:
            logger.info(f"[{symbol}] Ya está actualizado hasta la última hora cerrada.")
            continue
            
        endDate = current_hour_localized

        params = {
            "symbol": symbol,
            "interval": "1h",
            "start_date": startDate,
            "end_date": endDate
        }

        try:
            # Llamar al API (MT5 o TwelveData)
            df = None
            if DATA_SOURCE == "forex":
                df = await forex.getTimeSeries(params)
            
            # Fallback a TwelveData si MT5 no está disponible o falla
            if (df is None or df.empty) and API_KEYS:
                params["apikey"] = random.choice(API_KEYS)
                df = await twelvedata._callTimeSeriesApi(params)
            
            if df is not None and not df.empty:
                # Filtrar la vela actual (incompleta)
                df['datetimeOnly'] = df['datetime'].apply(lambda x: x if isinstance(x, pd.Timestamp) else datetime.strptime(str(x)[:19], '%Y-%m-%d %H:%M:%S'))
                current_hour_naive = current_hour_localized.replace(tzinfo=None)
                dfClosed = df[df['datetimeOnly'] < current_hour_naive].copy()
                
                if not dfClosed.empty:
                    inserted = dbManager.saveStockPrices(dfClosed, symbol)
                    logger.info(f"[{symbol}] Se insertaron/actualizaron {inserted} registros de precios (1h).")
                    totalInserted += inserted
                else:
                    logger.info(f"[{symbol}] No hay velas de 1h nuevas y cerradas para procesar.")
            else:
                logger.info(f"[{symbol}] Sin datos nuevos en la API.")

            # Respetar rate limits o no saturar MT5
            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error procesando {symbol}: {e}")
            await asyncio.sleep(2)

    logger.info(f"Sincronización terminada. Total de registros afectados: {totalInserted}")

if __name__ == "__main__":
    try:
        asyncio.run(syncDailyStockPrices())
    except KeyboardInterrupt:
        logger.info("Proceso detenido manualmente.")
