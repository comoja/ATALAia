"""
API client for Alpha Vantage forex data.
"""
import sys
import os
import logging
import pandas as pd
import pytz
from datetime import datetime

logger = logging.getLogger(__name__)

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from alpha_vantage.foreignexchange import ForeignExchange
from middlend.configConstants import ALPHA_VANTAGE_API_KEY


async def getAlphaVantageForex(symbol: str, interval: str, nVelas: int = 200) -> pd.DataFrame | None:
    """
    Downloads forex candlestick data from Alpha Vantage.
    
    Symbol format: "EUR/USD" -> "EURUSD"
    Alpha Vantage solo tiene datos diarios para forex en el plan gratuito.
    """
    
    if ALPHA_VANTAGE_API_KEY is None or ALPHA_VANTAGE_API_KEY == "":
        logger.error("ALPHA_VANTAGE_API_KEY no está configurada en configConstants")
        return None
    
    av_symbol = symbol.replace("/", "")
    
    logger.info(f"Descargando datos de Alpha Vantage para {av_symbol}")
    
    try:
        fx = ForeignExchange(key=ALPHA_VANTAGE_API_KEY)
        
        data, meta_data = fx.get_currency_exchange_daily(
            from_symbol=av_symbol[:3],
            to_symbol=av_symbol[3:],
            outputsize="compact"
        )
        
        if "Error Message" in data or "Note" in data:
            logger.error(f"Alpha Vantage error: {data}")
            return None
        
        df = pd.DataFrame.from_dict(data, orient='index')
        df.index = pd.to_datetime(df.index)
        df.columns = ['open', 'low', 'high', 'close']
        
        df = df.sort_index().reset_index()
        df.columns = ['datetime', 'open', 'low', 'high', 'close']
        
        for col in ['open', 'low', 'high', 'close']:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        cdmx_tz = pytz.timezone('America/Mexico_City')
        df['datetime'] = pd.to_datetime(df['datetime']).dt.tz_localize(pytz.UTC).dt.tz_convert(cdmx_tz)
        
        if len(df) > nVelas:
            df = df.tail(nVelas)
        
        logger.info(f"Alpha Vantage: {len(df)} velas diarias obtenidas")
        
        df['volume'] = 0
        
        return df.set_index('datetime')
        
    except Exception as e:
        logger.error(f"Error al obtener datos de Alpha Vantage: {e}")
        return None
