"""
API client for Finnhub forex data.
"""
import sys
import os
import logging
import finnhub
import pandas as pd
import pytz
from datetime import datetime

from middleware.config.constants import FINNHUB_API_KEY

logger = logging.getLogger(__name__)


async def getFinnhubForex(symbol: str, interval: str, nVelas: int = 200) -> pd.DataFrame | None:
    """
    Downloads forex candlestick data from Finnhub.
    
    Symbol format for Finnhub: "OANDA:EUR_USD" (OANDA prefix for forex)
    Interval: "1", "5", "15", "30", "60", "D", "W", "M"
    """
    
    if FINNHUB_API_KEY is None or FINNHUB_API_KEY == "":
        logger.error("FINNHUB_API_KEY no está configurada en configConstants")
        return None
    
    interval_map = {
        "5min": "5",
        "15min": "15",
        "30min": "30",
        "1h": "60",
        "1day": "D"
    }
    
    finnhub_interval = interval_map.get(interval, "15")
    
    finnhub_symbol = f"OANDA:{symbol.replace('/', '_')}"
    
    logger.info(f"Descargando {nVelas} velas de Finnhub para {finnhub_symbol} ({interval})")
    
    try:
        client = finnhub.Client(FINNHUB_API_KEY)
        
        now = int(datetime.now(pytz.UTC).timestamp())
        
        resolution_map = {
            "5": 300,
            "15": 900,
            "30": 1800,
            "60": 3600,
            "D": 86400
        }
        seconds_per_candle = resolution_map.get(finnhub_interval, 900)
        start_time = now - (nVelas * seconds_per_candle)
        
        data = client.crypto_candles(finnhub_symbol, finnhub_interval, start_time, now)
        
        if data.get('s') != 'ok':
            logger.error(f"Finnhub error: {data}")
            return None
        
        df = pd.DataFrame({
            'datetime': pd.to_datetime(data['t'], unit='s'),
            'open': data['o'],
            'high': data['h'],
            'low': data['l'],
            'close': data['c'],
            'volume': data['v']
        })
        
        df = df.sort_values('datetime').reset_index(drop=True)
        df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(pytz.UTC)
        
        logger.info(f"Finnhub: {len(df)} velas obtenidas")
        
        return df.set_index('datetime')
        
    except Exception as e:
        logger.error(f"Error al obtener datos de Finnhub: {e}")
        return None


def getFinnhubClient():
    """Get a Finnhub client instance."""
    if FINNHUB_API_KEY is None or FINNHUB_API_KEY == "":
        return None
    return finnhub.Client(FINNHUB_API_KEY)
