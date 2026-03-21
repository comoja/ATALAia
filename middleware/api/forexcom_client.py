"""
API client for Forex.com real-time data.
"""
import sys
import os
import logging
import pandas as pd
import pytz
from datetime import datetime

from middleware.config.constants import FOREXCOM_USERNAME, FOREXCOM_PASSWORD, FOREXCOM_APP_KEY

logger = logging.getLogger(__name__)


async def getForexComCandles(symbol: str, interval: str, nVelas: int = 200) -> pd.DataFrame | None:
    """
    Downloads candlestick data from Forex.com API.
    
    Symbol format: "EUR/USD"
    Interval: "M1", "M5", "M15", "M30", "H1", "H2", "H4", "D", "W"
    """
    
    if not FOREXCOM_USERNAME or not FOREXCOM_PASSWORD or not FOREXCOM_APP_KEY:
        logger.error("Credenciales de Forex.com no configuradas en configConstants")
        return None
    
    interval_map = {
        "5min": "M5",
        "15min": "M15",
        "30min": "M30",
        "1h": "H1",
        "4h": "H4",
        "1day": "D"
    }
    
    forexcom_interval = interval_map.get(interval, "M15")
    
    try:
        from forexcom import ForexCom
        from forexcom.models import CandleData, Offer
        
        client = ForexCom(
            username=FOREXCOM_USERNAME,
            password=FOREXCOM_PASSWORD,
            app_key=FOREXCOM_APP_KEY
        )
        
        if not client.is_connected():
            client.connect()
            if not client.is_connected():
                logger.error("No se pudo conectar a Forex.com")
                return None
        
        candles = client.get_candles(
            symbol=symbol,
            period=forexcom_interval,
            number=nVelas
        )
        
        if not candles:
            logger.error(f"No se obtuvieron velas de Forex.com para {symbol}")
            return None
        
        df = pd.DataFrame([
            {
                'datetime': c.dt,
                'open': c.o,
                'high': c.h,
                'low': c.l,
                'close': c.c,
                'volume': 0
            }
            for c in candles
        ])
        
        cdmx_tz = pytz.timezone('America/Mexico_City')
        df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(pytz.UTC).dt.tz_convert(cdmx_tz)
        
        df = df.sort_values('datetime').reset_index(drop=True)
        
        logger.info(f"Forex.com: {len(df)} velas obtenidas")
        
        return df.set_index('datetime')
        
    except Exception as e:
        logger.error(f"Error al obtener datos de Forex.com: {e}")
        return None
