import asyncio
import httpx
import pandas as pd
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

TWELVE_DATA_API_URL = "https://api.twelvedata.com"


async def getTimeSeriesWithStartDate(
    symbol: str,
    interval: str,
    apiKey: str,
    startDate: datetime,
    outputsize: int = 5000,
    endDate: Optional[datetime] = None
) -> pd.DataFrame | None:
    logger.info(f"Descargando {symbol} {interval} desde {startDate}" + (f" hasta {endDate}" if endDate else ""))
    
    url = f"{TWELVE_DATA_API_URL}/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "start_date": startDate.strftime('%Y-%m-%d %H:%M:%S'),
        "outputsize": outputsize,
        "apikey": apiKey,
        "format": "JSON",
        "timezone": "America/Mexico_City"
    }
    
    if endDate:
        params["end_date"] = endDate.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=30.0)
            response.raise_for_status()
            data = response.json()
        
        if "code" in data:
            logger.error(f"API error: {data}")
            return None
        
        if "values" not in data:
            logger.warning(f"No hay 'values' para {symbol}")
            return None
        
        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").reset_index(drop=True)
        
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        else:
            df["volume"] = 0
        
        return df.dropna(subset=["close"])
    
    except httpx.RequestError as e:
        logger.error(f"Error de red: {e}")
        return None
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        return None
