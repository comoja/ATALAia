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
    
    # 1. Obtener todos los símbolos activos de dataSymbol (Ratio + Sentinel)
    symbolsData = dbManager.getDataSymbols()
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
            # Obtener datos desde la tabla 'candles' mediante ConnectionPool microservicio
            df = await dbManager.getCandlesFromDb(symbol, timeframe="5min", limit=5000)
            if df is not None and not df.empty:
                df = df.reset_index()
                if 'timestamp' in df.columns:
                    df.rename(columns={'timestamp': 'datetime'}, inplace=True)
                start_naive = startDate.replace(tzinfo=None)
                end_naive = endDate.replace(tzinfo=None)
                df['dt_naive'] = pd.to_datetime(df['datetime']).dt.tz_localize(None)
                df = df[(df['dt_naive'] >= start_naive) & (df['dt_naive'] < end_naive)][['datetime', 'close']]
            else:
                df = pd.DataFrame()
            
            if not df.empty:
                # Resample de 5min a 1h
                import warnings
                with warnings.catch_warnings():
                    warnings.simplefilter('ignore')
                    df['datetime'] = pd.to_datetime(df['datetime'])
                    df.set_index('datetime', inplace=True)
                    df_1h = df.resample('1h').last().dropna()
                    df = df_1h.reset_index()
            
            if df is not None and not df.empty:
                # Filtrar la vela actual (incompleta)
                df['datetimeOnly'] = pd.to_datetime(df['datetime']).apply(lambda x: x.replace(tzinfo=None))
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
