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

        df["datetime"] = pd.to_datetime(df["datetime"]).dt.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward').dt.tz_convert(TIMEZONE)

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
    if df is None or df.empty:
        return df

    # Control preventivo: Evitar doble calibración y desfase acumulativo
    if hasattr(df, "attrs") and df.attrs.get("_calibrated", False):
        return df

    df = df.copy()

    # Intentar obtener el símbolo para aplicar el offset correcto
    symbol = "UNKNOWN"
    if "symbol" in df.columns:
        symbol = str(df["symbol"].iloc[0]).upper()
        
    # Intentar obtener el offset dinámicamente de la base de datos MySQL (SentinelSymbol)
    offset = 0.0
    has_db_offset = False
    if symbol != "UNKNOWN":
        try:
            symbol_data = dbManager.getSymbol(symbol)
            if symbol_data and "priceOffset" in symbol_data and symbol_data["priceOffset"] is not None and float(symbol_data["priceOffset"]) != 0.0:
                offset = float(symbol_data["priceOffset"])
                has_db_offset = True
        except Exception as e:
            logger.warning(f"Error al obtener priceOffset para {symbol} de la DB: {e}.")
            
    if has_db_offset:
        df["open"]  = df["open"]  + offset
        df["high"]  = df["high"]  + offset
        df["low"]   = df["low"]   + offset
        df["close"] = df["close"] + offset
        logger.info(f"[{symbol}] Capa de Calibración: Aplicado priceOffset de {offset:.5f} desde MySQL en RAM.")
    else:
        # Si no hay offset en la DB, reactivamos tu fórmula original de spread dinámico por volatilidad (Cualquier símbolo)
        # Esto calcula el spread de forma 100% proporcional al rango de la propia vela en tiempo real.
        df["range"] = df["high"] - df["low"]
        df["spread"] = df["range"] * 0.2

        spread_high = df["spread"] * 0.3
        spread_low  = df["spread"] * 0.7

        df["high"] = df["high"] + spread_high
        df["low"]  = df["low"]  - spread_low

        adjustment = (spread_high - spread_low) / 2

        df["open"]  = df["open"]  + adjustment
        df["close"] = df["close"] + adjustment
        logger.info(f"[{symbol}] Capa de Calibración: Aplicado Spread Dinámico del Bróker (20% del Rango) en RAM por defecto.")

    # Marcar como calibrado para prevenir doble desfase
    if hasattr(df, "attrs"):
        df.attrs["_calibrated"] = True

    return df

async def getTimeSeries(params: dict, forceDb: bool = False) -> pd.DataFrame | None:
    
    if forceDb:# DATA_SOURCE == "db":
        return adjustDataframeInplace(await getCandlesFromDb(params.get("symbol"), params.get("interval"), params.get("outputSize", 500)))
    else:
        return await _callTimeSeriesApi(params)


def resample_candles(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Resample de velas agregadas (rule='15T' para 15min, '1h' para 1 hora)
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

