"""
API client for Tradermade forex data.
"""
import sys
import os
import logging
import pandas as pd
import pytz
from datetime import datetime, timedelta

from middleware.config.constants import TRADERMADE_API_KEY

logger = logging.getLogger(__name__)


async def getTradermadeForex(symbol: str, interval: str, nVelas: int = 200) -> pd.DataFrame | None:
    """
    Downloads forex candlestick data from Tradermade.
    
    Symbol format: "EURUSD" (no slash)
    Interval: "5min", "15min", "30min", "1h", "1day"
    """
    
    if TRADERMADE_API_KEY is None or TRADERMADE_API_KEY == "":
        logger.error("TRADERMADE_API_KEY no está configurada en configConstants")
        return None
    
    interval_map = {
        "5min": "PT5M",
        "15min": "PT15M",
        "30min": "PT30M",
        "1h": "PT1H",
        "1day": "P1D"
    }
    
    period = interval_map.get(interval, "PT15M")
    
    tradermade_symbol = symbol.replace("/", "")
    
    logger.info(f"Descargando {nVelas} velas de Tradermade para {tradermade_symbol} ({interval})")
    
    try:
        import tradermade as tm
        
        tm.set_rest_api_key(TRADERMADE_API_KEY)
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        
        df = tm.get_historical(
            tradermade_symbol,
            period=period,
            start_date=start_date.strftime("%Y-%m-%d-%H:%M"),
            end_date=end_date.strftime("%Y-%m-%d-%H:%M"),
            interval=period,
            format="records"
        )
        
        if df is None or df.empty:
            logger.error(f"Tradermade no devolvió datos para {tradermade_symbol}")
            return None
        
        df = pd.DataFrame(df)
        df['datetime'] = pd.to_datetime(df['datetime'])
        
        df = df.sort_values('datetime').reset_index(drop=True)
        
        cdmx_tz = pytz.timezone('America/Mexico_City')
        df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(pytz.UTC).dt.tz_convert(cdmx_tz)
        
        logger.info(f"Tradermade: {len(df)} velas obtenidas")
        
        return df.set_index('datetime')
        
    except Exception as e:
        logger.error(f"Error al obtener datos de Tradermade: {e}")
        return None
