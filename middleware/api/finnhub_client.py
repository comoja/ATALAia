"""
API client for Finnhub forex data.
"""
import sys
import os
import logging
import finnhub
import pandas as pd
import pytz
import httpx
import asyncio
from datetime import datetime, timedelta

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
    return finnhub.Client(api_key=FINNHUB_API_KEY)

async def getLatestMarketNews(category: str = "general") -> list:
    """
    Obtiene los titulares de las últimas noticias del mercado usando httpx con timeout.
    """
    if not FINNHUB_API_KEY:
        return []
    
    url = f"https://finnhub.io/api/v1/news?category={category}&token={FINNHUB_API_KEY}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            news = response.json()
            return [item.get('headline') for item in news[:10]]
    except Exception as e:
        logger.error(f"Error obteniendo noticias de Finnhub (httpx): {e}")
        return []

async def getHighImpactEvents() -> list:
    """
    Obtiene eventos económicos de alto impacto usando httpx con timeout.
    """
    if not FINNHUB_API_KEY:
        return []
    
    now = datetime.now()
    start_date = now.strftime('%Y-%m-%d')
    end_date = (now + timedelta(days=1)).strftime('%Y-%m-%d')
    
    url = f"https://finnhub.io/api/v1/calendar/economic?from={start_date}&to={end_date}&token={FINNHUB_API_KEY}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()
            data = response.json()
            events = data.get('economicCalendar', [])
            return [e for e in events if e.get('impact') == 'high']
    except Exception as e:
        logger.error(f"Error obteniendo calendario de Finnhub (httpx): {e}")
        return []
