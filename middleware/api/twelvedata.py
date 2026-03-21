"""
Asynchronous API client for twelvedata.com using httpx.
"""
import sys
import os
import logging
import httpx
import pandas as pd
import pytz
import asyncio

from middleware.config.constants import (
    TWELVE_DATA_API_URL, 
    TWELVE_DATA_CREDIT_LIMIT, 
    TWELVE_DATA_CREDIT_EMERGENCY_THRESHOLD
)
from middleware.database import dbManager, dbConnection


logger = logging.getLogger(__name__)

async def checkApiCredits(apiKey: str, accountName: str):
    """
    Checks the current usage of a Twelve Data API key and sends an alert if credits are low.
    """
    url = f"{TWELVE_DATA_API_URL}?apikey={apiKey}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            
            data = response.json()
            usedCredits = data.get('currentUsage', 0)
            remainingCredits = TWELVE_DATA_CREDIT_LIMIT - usedCredits

            if remainingCredits < TWELVE_DATA_CREDIT_EMERGENCY_THRESHOLD:
                message = (
                    f"⚠️ *EMERGENCIA:* Cuenta de API **{accountName}** casi sin créditos. "
                    f"Quedan solo {remainingCredits}."
                )
                # This needs a proper implementation of the alert system
                # await sendAlert(message) 
                logger.critical(f"API Key credits running low for {accountName}! Remaining: {remainingCredits}")

    except httpx.RequestError as e:
        logger.error(f"Error de red al verificar créditos de API para {accountName}: {e}")
    except Exception as e:
        logger.error(f"Error inesperado al verificar créditos de API para {accountName}: {e}")


async def getTimeSeries(symbol: str, interval: str, apiKey: str, nVelas: int = 200, accountName: str = None) -> pd.DataFrame | None:
    """
    Downloads time series data for a given symbol.
    """
    accountInfo = f" [{accountName}]" if accountName else ""
    logger.info(f"Descargando {nVelas} velas para {symbol} en intervalo {interval}{accountInfo}")
    url = f"{TWELVE_DATA_API_URL}/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": nVelas,
        "apikey": apiKey,
        "timezone": "America/Mexico_City"
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=20.0)
            response.raise_for_status()
            
            data = response.json()

        if "values" not in data:
            logger.warning(f"Respuesta de API sin 'values' para {symbol}: {data.get('message', 'No message')}")
            return None

        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").set_index("datetime")
        
        # Coerce columns to numeric, handling potential errors
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Ensure volume column exists and is numeric
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        else:
            df["volume"] = 0
            
        return df.dropna(subset=["close"])

    except httpx.RequestError as e:
        logger.error(f"Error de red en descarga de time series para {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error crítico en descarga de time series para {symbol}: {e}", exc_info=True)
        return None

async def getTimeSeriesSymbolWithDB(symbol: str,
                                    interval: str,
                                    apiKey: str,
                                    nVelas: int = 200,
                                    accountName: str = None
                                    ) -> pd.DataFrame | None:
    """
    Descarga velas de un símbolo y guarda automáticamente en la DB todas las temporalidades:
    - 5min (original)
    - 15min (resample)
    - 1h (resample)
    
    Ajusta automáticamente el número de velas de 5min necesarias para cubrir SMA20 de la temporalidad más grande.
    Retorna únicamente las velas de la temporalidad solicitada (interval).
    """
    import pytz

    accountInfo = f" [{accountName}]" if accountName else ""
    
    # --- Calcular velas mínimas para temporalidades mayores ---
    # Ejemplo: SMA20 en 1h -> 20 velas de 1h = 20*12 velas de 5min
    velas_minimas = 200  # default si no hay otra consideración
    
    if interval == "15min":
        velas_minimas = (nVelas * 3)  # SMA20 * 3 velas de 5min por cada 15min
    elif interval == "1h":
        velas_minimas = (nVelas * 12)  # SMA20 * 12 velas de 5min por cada 1h
    else:  # 5min
        velas_minimas = 5000
    sonMuchas= (velas_minimas > 5000)
    logger.info(f"Descargando {velas_minimas if not sonMuchas else nVelas} velas de {'5min' if not sonMuchas else interval } para {symbol} para luego resamplear a {interval} {accountInfo}")

    url = f"{TWELVE_DATA_API_URL}/time_series"
    params = {
        "symbol": symbol,
        "interval": "5min" if not sonMuchas else interval,  # siempre pedimos 5min
        "outputsize": velas_minimas if not sonMuchas else nVelas,
        "apikey": apiKey,
        "format": "JSON"
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=30.0)
            response.raise_for_status()
            data = response.json()

        if "code" in data:
            logger.error(f"API error: {data}")
            return None

        df_symbol = pd.DataFrame(data["values"])
        df_symbol['symbol'] = symbol
        df_symbol['datetime'] = pd.to_datetime(df_symbol['datetime'], utc=True)
        df_symbol = df_symbol.sort_values("datetime").reset_index(drop=True)

        # --- Asegurarnos de que todas las columnas sean float ---
        for col in ['open','high','low','close','volume']:
            if col not in df_symbol.columns:
                df_symbol[col] = 0.0
            df_symbol[col] = pd.to_numeric(df_symbol[col], errors='coerce')
        df_symbol = df_symbol.dropna(subset=['close'])

        # --- Guardar 5min ---
        last_dt_5min = await dbManager.getLastCandleDatetime(symbol, "5min" if not sonMuchas else interval)
        df_5min_new = df_symbol[df_symbol['datetime'] > last_dt_5min] if last_dt_5min is not None else df_symbol
        if not df_5min_new.empty:
            inserted_5min = await dbManager.insertNewCandlesToDb(df_5min_new, "5min" if not sonMuchas else interval)
        if not sonMuchas:
            # --- Resample a 15min ---
            df_15min = df_symbol.set_index('datetime').resample('15min', label='right', closed='right').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna().reset_index()
            df_15min['symbol'] = symbol
            last_dt_15min = await dbManager.getLastCandleDatetime(symbol, "15min")
            df_15min_new = df_15min[df_15min['datetime'] > last_dt_15min] if last_dt_15min is not None else df_15min
            if not df_15min_new.empty:
                inserted_15min = await dbManager.insertNewCandlesToDb(df_15min_new, "15min")

            # --- Resample a 1h ---
            df_1h = df_symbol.set_index('datetime').resample('1h', label='right', closed='right').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna().reset_index()
            df_1h['symbol'] = symbol
            last_dt_1h = await dbManager.getLastCandleDatetime(symbol, "1h")
            df_1h_new = df_1h[df_1h['datetime'] > last_dt_1h] if last_dt_1h is not None else df_1h
            if not df_1h_new.empty:
                inserted_1h = await dbManager.insertNewCandlesToDb(df_1h_new, "1h")
        else:
            return df_5min_new 

        # --- Devolver solo la temporalidad solicitada ---
        if interval == "5min":
            return df_5min_new
        elif interval == "15min":
            return df_15min_new
        elif interval == "1h":
            return df_1h_new
        else:
            logger.warning(f"Temporalidad {interval} no soportada. Se devuelve 5min por defecto.")
            return df_5min_new

    except Exception as e:
        logger.error(f"Error crítico en descarga/guardado/resample de time series: {e}", exc_info=True)
        return None

async def getComplexData(symbol: str, interval: str, apiKey: str) -> pd.DataFrame | None:
    """
    Fetches complex data (price, rsi, cci, macd) in a single API call.
    """
    logger.info(f"Descargando datos complejos para {symbol} en {interval}")
    url = f"{TWELVE_DATA_API_URL}/complex_data"
    payload = {
        "symbols": [symbol],
        "intervals": [interval],
        "outputsize": 30,
        "apikey": apiKey,
        "methods": [
            {"name": "price"},
            {"name": "rsi", "period": 14},
            {"name": "cci", "period": 20},
            {"name": "macd", "fastPeriod": 12, "slowPeriod": 26, "signalPeriod": 9}
        ]
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, timeout=20.0)
            response.raise_for_status()
            data = response.json()

        if not data.get('data'):
            logger.warning(f"Respuesta de API sin 'data' en complexData para {symbol}")
            return None

        apiData = data['data'][0]
        df = pd.DataFrame({
            'precio': [float(x['close']) for x in apiData[0]['values']],
            'rsi': [float(x['rsi']) for x in apiData[1]['values']],
            'cci': [float(x['cci']) for x in apiData[2]['values']],
            'macd': [float(x['macd']) for x in apiData[3]['values']]
        })
        # Invertir para que lo más nuevo esté al final
        return df.iloc[::-1].reset_index(drop=True)

    except httpx.RequestError as e:
        logger.error(f"Error de red en descarga de complexData para {symbol}: {e}")
        return None
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Error parseando la respuesta de complexData para {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error crítico en descarga de complexData para {symbol}: {e}", exc_info=True)
        return None


async def updateCandles5min(apiKey: str, accountName: str = None):
    """
    Descarga velas de 5 min para todos los símbolos activos y las guarda en la tabla 'candles'.
    Solo inserta velas nuevas.
    """
    # --- Traer símbolos ---
    symbols_raw = dbManager.getSymbols()  # lista de dicts
    symbols = [s['symbol'] for s in symbols_raw]
    if not symbols:
        logger.warning("No hay símbolos activos en la base de datos.")
        return

    accountInfo = f" [{accountName}]" if accountName else ""
    logger.info(f"Descargando velas 5min de {len(symbols)} símbolos{accountInfo}")

    # --- Descargar data multi-symbol ---
    url = f"{TWELVE_DATA_API_URL}/time_series"
    params = {
        "symbol": ",".join(symbols),
        "interval": "5min",
        "outputsize": 500,  # máximo que quieras traer
        "apikey": apiKey,
        "format": "JSON"
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=20.0)
            response.raise_for_status()
            data = response.json()

        if "code" in data:
            logger.error(f"API error: {data}")
            return

        # --- Preparar velas ---
        df_list = []
        for symbol in symbols:
            if symbol not in data or "values" not in data[symbol]:
                logger.warning(f"No hay datos para {symbol}")
                continue

            df_symbol = pd.DataFrame(data[symbol]["values"])
            df_symbol['symbol'] = symbol
            df_symbol['datetime'] = pd.to_datetime(df_symbol['datetime'])
            df_symbol['timeframe'] = "5min"
            df_symbol = df_symbol.rename(columns={
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "volume"
            })
            df_list.append(df_symbol)

        if not df_list:
            logger.warning("No se obtuvieron velas de ningún símbolo.")
            return

        df_all = pd.concat(df_list, ignore_index=True)
        df_all = df_all[['symbol','timeframe','datetime','open','high','low','close','volume']]

        # --- Guardar en DB ---
        conn = dbConnection.getConnection()
        cursor = conn.cursor()

        insert_sql = """
        INSERT IGNORE INTO candles (symbol, timeframe, datetime, open, high, low, close, volume)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        values = df_all.to_records(index=False)
        cursor.executemany(insert_sql, values)
        conn.commit()
        logger.info(f"Velas insertadas/ignorar duplicados: {cursor.rowcount}")
        conn.close()

    except Exception as e:
        logger.error(f"Error al actualizar velas 5min: {e}", exc_info=True)


def resample_candles(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Resample de velas agregadas (rule='15T' para 15min, '1H' para 1 hora)
    """
    df_resampled = df.set_index('datetime').resample(rule).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna().reset_index()
    df_resampled['symbol'] = df['symbol'].iloc[0]
    return df_resampled
