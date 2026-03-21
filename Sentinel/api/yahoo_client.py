"""
API client for Yahoo Finance forex data.
"""
import sys
import os
import logging
import pandas as pd
import pytz
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)


async def getYahooFinanceForex(symbol: str, interval: str, nVelas: int = 200) -> pd.DataFrame | None:
    """
    Downloads forex candlestick data from Yahoo Finance.
    
    Symbol format: "EUR/USD" -> "EURUSD=X"
    Interval: "5min", "15min", "30min", "1h", "1day"
    """
    
    interval_map = {
        "5min": "5m",
        "15min": "15m",
        "30min": "30m",
        "1h": "1h",
        "1day": "1d"
    }
    
    yf_interval = interval_map.get(interval, "15m")
    
    yf_symbol = symbol.replace("/", "") + "=X"
    
    logger.info(f"Descargando {nVelas} velas de Yahoo Finance para {yf_symbol} ({interval})")
    
    try:
        import yfinance as yf
        
        ticker = yf.Ticker(yf_symbol)
        
        now = datetime.now()
        start = now - timedelta(days=7)
        
        df = ticker.history(start=start, end=now, interval=yf_interval, auto_adjust=True)
        
        if df is None or df.empty:
            logger.error(f"Yahoo Finance no devolvió datos para {yf_symbol}")
            return None
        
        df.index = pd.to_datetime(df.index)
        
        cdmx_tz = pytz.timezone('America/Mexico_City')
        
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(pytz.UTC)
        
        df.index = df.index.tz_convert(cdmx_tz)
        
        if len(df) > nVelas:
            df = df.tail(nVelas)
        
        df = df.reset_index()
        df = df.rename(columns={'Datetime': 'datetime'})
        df = df[['datetime', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume']
        df = df.dropna()
        
        logger.info(f"Yahoo Finance: {len(df)} velas obtenidas")
        
        return df.set_index('datetime')
        
    except Exception as e:
        logger.error(f"Error al obtener datos de Yahoo Finance: {e}")
        return None
