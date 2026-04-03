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
from datetime import datetime

from middleware.config.constants import (
    TWELVE_DATA_API_URL, 
    TWELVE_DATA_CREDIT_LIMIT, 
    TWELVE_DATA_CREDIT_EMERGENCY_THRESHOLD,
    TIMEZONE
)
from middleware.database import dbManager, dbConnection

try:
    from middleware.database.dbManager import getCandlesFromDb
except ImportError:
    getCandlesFromDb = None

try:
    from middleware.config.constants import DATA_SOURCE
except ImportError:
    DATA_SOURCE = "db"


logger = logging.getLogger(__name__)

DEFAULT_TIMEZONE = TIMEZONE

# 🔥 cliente reutilizable (mejor performance)
client = httpx.AsyncClient(timeout=30.0)


async def _callTimeSeriesApi(params: dict) -> pd.DataFrame | None:
    try:
        required = ["symbol", "interval", "apikey"]
        for key in required:
            if key not in params:
                logger.error(f"Falta parámetro requerido: {key}")
                return None
        
        api_params = {
            "symbol": params["symbol"],
            "interval": params["interval"],
            "apikey": params["apikey"],
            "format": "JSON",
            "timezone": TIMEZONE
        }
        
        if "outputSize" in params:
            api_params["outputsize"] = params["outputSize"]
        elif "outputsize" in params:
            api_params["outputsize"] = params["outputsize"]
        
        if "start_date" in params and params["start_date"]:
            start = params["start_date"]
            if hasattr(start, 'strftime'):
                api_params["start_date"] = start.strftime('%Y-%m-%d %H:%M:%S')
            else:
                api_params["start_date"] = str(start)
        
        if "end_date" in params and params["end_date"]:
            end = params["end_date"]
            if hasattr(end, 'strftime'):
                api_params["end_date"] = end.strftime('%Y-%m-%d %H:%M:%S')
            else:
                api_params["end_date"] = str(end)
        # -------------------------
        #  Retry robusto
        # -------------------------

        data = None
        for attempt in range(3):
            try:
                #logger.info(api_params  )
                response = await client.get(
                    f"{TWELVE_DATA_API_URL}/time_series",
                    params=api_params
                )

                response.raise_for_status()
                data = response.json()
                
                logger.info(f"[TwelveData] Response keys: {data.keys()}")
                if "values" in data:
                    logger.info(f"[TwelveData] Cantidad de valores: {len(data.get('values', []))}")
                else:
                    logger.warning(f"[TwelveData] Sin 'values', response: {data}")

                break
            
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                logger.error(f"HTTP error {status}: {e}")
                return None
            
            """
                if status in [429, 500, 502, 503, 504, 520] and attempt < 2:
                    wait = 1.5 * (attempt + 1)
                    logger.warning(f"Retry {attempt+1}/3 en {wait}s (status {status})")

                    await asyncio.sleep(wait)
                    continue

                logger.error(f"HTTP error {status}: {e}")
                return None

            except httpx.RequestError as e:
                if attempt < 2:
                    wait = 1.5 * (attempt + 1)
                    logger.warning(f"Retry network {attempt+1}/3 en {wait}s")
                    await asyncio.sleep(wait)
                    continue

                logger.error(f"Error de red: {e}")
                return None
        """
        if not data:
            return None
        
        count = data.get('count')
        if count:
            print(f"[getTimeSeries] Uso hoy: {count}/750")
        
        if "code" in data:
            logger.error(f"API error: {data}")
            return None
        
        if "values" not in data:
            logger.warning(f"No hay 'values'")
            return None
        
        df = pd.DataFrame(data["values"])

        df["datetime"] = pd.to_datetime(df["datetime"], utc=True)

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        else:
            df["volume"] = 0

        df = df.sort_values("datetime").reset_index(drop=True)

        # -------------------------
        #  Log uso
        # -------------------------
        if "count" in data:
            logger.info(f"[TwelveData] Uso: {data['count']}/750")

        
        return adjustDataframeInplace(df.dropna(subset=["close"]))
    
    except httpx.RequestError as e:
        logger.error(f"Error de red: {e}") 
        return None
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return None
    
def adjustDataframeInplace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["range"] = df["high"] - df["low"]
    df["spread"] = df["range"] * 0.2

    spread_high = df["spread"] * 0.3
    spread_low  = df["spread"] * 0.7

    df["high"] = df["high"] + spread_high
    df["low"]  = df["low"]  - spread_low

    adjustment = (spread_high - spread_low) / 2

    df["open"]  = df["open"]  + adjustment
    df["close"] = df["close"] + adjustment

    return df

async def getTimeSeries(params: dict) -> pd.DataFrame | None:
    
    if  DATA_SOURCE == "db":
        return adjustDataframeInplace(await getCandlesFromDb(params.get("symbol"), params.get("interval"), params.get("outputSize", 500)))
    else:
        return await _callTimeSeriesApi(params)


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
    df_resampled = df.set_index('datetime').resample(rule, closed='right', label='right').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna().reset_index()
    df_resampled['symbol'] = df['symbol'].iloc[0]
    return df_resampled

