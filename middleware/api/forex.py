"""
Asynchronous API client for MetaTrader 5 (MT5) to retrieve Forex rates, mimicking TwelveData interface.
Supports both native Windows MT5 and Wine MT5 Bridge on Linux.
"""
import os
import sys
import logging
import asyncio
import httpx
import requests
import pandas as pd
import pytz
from datetime import datetime, timedelta

from middleware.config.constants import TIMEZONE, mt5Login, mt5Password, mt5Server
from middleware.database import dbManager, dbConnection

# Intentar importar MetaTrader5 nativo (Windows)
try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

logger = logging.getLogger(__name__)

MT5_BRIDGE_URL = os.getenv("MT5_BRIDGE_URL", "http://127.0.0.1:8005")

# Mapeo de timeframes de TwelveData a constantes de MetaTrader 5
TIMEFRAME_MAP = {}
if mt5 is not None:
    TIMEFRAME_MAP = {
        "1min": mt5.TIMEFRAME_M1,
        "5min": mt5.TIMEFRAME_M5,
        "15min": mt5.TIMEFRAME_M15,
        "30min": mt5.TIMEFRAME_M30,
        "1h": mt5.TIMEFRAME_H1,
        "4h": mt5.TIMEFRAME_H4,
        "1day": mt5.TIMEFRAME_D1,
        "1d": mt5.TIMEFRAME_D1,
        "1week": mt5.TIMEFRAME_W1,
        "1month": mt5.TIMEFRAME_MN1,
    }

def _is_bridge_online() -> bool:
    """Verifica si el servidor MT5 Bridge de Wine está activo."""
    try:
        res = requests.get(f"{MT5_BRIDGE_URL}/health", timeout=1)
        if res.status_code == 200 and res.json().get("status") == "online":
            return True
    except Exception:
        pass
    return False

async def _ensureMt5Initialized() -> bool:
    """
    Asegura que el terminal de MT5 esté inicializado y conectado (nativo o vía Bridge).
    """
    if mt5 is not None:
        try:
            terminalInfo = mt5.terminal_info()
            if terminalInfo is not None:
                return True
            isInitialized = await asyncio.to_thread(mt5.initialize)
            return isInitialized
        except Exception as e:
            logger.error(f"[ForexAPI] Error inicializando MT5 nativo: {e}")
            return False

    # Verificar bridge de Wine
    return await asyncio.to_thread(_is_bridge_online)

def adjustDataframeInplace(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df

    if hasattr(df, "attrs") and df.attrs.get("_calibrated", False):
        return df

    df = df.copy()

    symbol = "UNKNOWN"
    if "symbol" in df.columns:
        symbol = str(df["symbol"].iloc[0]).upper()
        
    offset = 0.0
    hasDbOffset = False
    if symbol != "UNKNOWN":
        try:
            symbolData = dbManager.getSymbol(symbol)
            if symbolData and "priceOffset" in symbolData and symbolData["priceOffset"] is not None and float(symbolData["priceOffset"]) != 0.0:
                offset = float(symbolData["priceOffset"])
                hasDbOffset = True
        except Exception as e:
            logger.warning(f"Error al obtener priceOffset para {symbol} de la DB: {e}.")
            
    if hasDbOffset:
        df["open"]  = df["open"]  + offset
        df["high"]  = df["high"]  + offset
        df["low"]   = df["low"]   + offset
        df["close"] = df["close"] + offset
        logger.info(f"[{symbol}] Capa de Calibración MT5: Aplicado priceOffset de {offset:.5f} desde MySQL en RAM.")
    else:
        df["range"] = df["high"] - df["low"]
        df["spread"] = df["range"] * 0.2

        spreadHigh = df["spread"] * 0.3
        spreadLow  = df["spread"] * 0.7

        df["high"] = df["high"] + spreadHigh
        df["low"]  = df["low"]  - spreadLow

        adjustment = (spreadHigh - spreadLow) / 2

        df["open"]  = df["open"]  + adjustment
        df["close"] = df["close"] + adjustment
        logger.info(f"[{symbol}] Capa de Calibración MT5: Aplicado Spread Dinámico del Bróker (20% del Rango) en RAM por defecto.")

    if hasattr(df, "attrs"):
        df.attrs["_calibrated"] = True

    return df

async def _callForexMt5Api(params: dict) -> pd.DataFrame | None:
    """
    Consulta los datos de velas directamente desde MT5 (nativo o vía Bridge Wine) de forma asíncrona.
    """
    if not await _ensureMt5Initialized():
        logger.warning("[ForexAPI] MT5 no está inicializado ni el Bridge está disponible.")
        return None

    symbolRaw = params.get("symbol")
    interval = params.get("interval", "5min")
    outputSize = int(params.get("outputSize", params.get("outputsize", 5000)))
    startDate = params.get("start_date")
    endDate = params.get("end_date")

    if not symbolRaw:
        logger.error("[ForexAPI] Símbolo no proporcionado.")
        return None 

    # Obtener el símbolo correspondiente para MT5 desde la tabla máster `symbols` en la BD
    symbolData = dbManager.getSymbol(symbolRaw)
    if symbolData and symbolData.get('MT5'):
        symbol = symbolData['MT5']
    else:
        symbol = symbolRaw.replace("/", "").upper()

    tzCdmx = pytz.timezone(TIMEZONE)
    tsFrom = None
    tsTo = None
    dtTo = None

    if startDate:
        dtFrom = startDate
        if isinstance(dtFrom, str):
            try:
                dtFrom = datetime.strptime(dtFrom, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                dtFrom = datetime.strptime(dtFrom, '%Y-%m-%d')
        if hasattr(dtFrom, "to_pydatetime"):
            dtFrom = dtFrom.to_pydatetime()
        if dtFrom.tzinfo is None:
            dtFrom = tzCdmx.localize(dtFrom)
        else:
            dtFrom = dtFrom.astimezone(tzCdmx)
        tsFrom = int(dtFrom.timestamp())

    if endDate:
        dtTo = endDate
        if isinstance(dtTo, str):
            try:
                dtTo = datetime.strptime(dtTo, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                dtTo = datetime.strptime(dtTo, '%Y-%m-%d')
        if hasattr(dtTo, "to_pydatetime"):
            dtTo = dtTo.to_pydatetime()
        if dtTo.tzinfo is None:
            dtTo = tzCdmx.localize(dtTo)
        else:
            dtTo = dtTo.astimezone(tzCdmx)
        tsTo = int(dtTo.timestamp())

    try:
        rates = None
        
        # 1. Si MT5 nativo está disponible (Windows)
        if mt5 is not None:
            mt5Timeframe = TIMEFRAME_MAP.get(interval, mt5.TIMEFRAME_M5)
            symbolInfo = await asyncio.to_thread(mt5.symbol_info, symbol)
            if symbolInfo is None:
                selected = await asyncio.to_thread(mt5.symbol_select, symbol, True)
                if not selected:
                    logger.error(f"[ForexAPI] Símbolo {symbol} no encontrado en MT5.")
                    return None

            if tsFrom and tsTo:
                rates = await asyncio.to_thread(mt5.copy_rates_range, symbol, mt5Timeframe, tsFrom, tsTo)
            elif tsFrom:
                rates = await asyncio.to_thread(mt5.copy_rates_from, symbol, mt5Timeframe, tsFrom, min(outputSize, 3000))
            else:
                rates = await asyncio.to_thread(mt5.copy_rates_from_pos, symbol, mt5Timeframe, 1, outputSize)
            
            if rates is not None and len(rates) > 0:
                df = pd.DataFrame(rates)
            else:
                df = pd.DataFrame()
        else:
            # 2. Consultar a través del Bridge de Wine
            async with httpx.AsyncClient(timeout=10) as client:
                body = {
                    "symbol": symbol,
                    "interval": interval,
                    "output_size": outputSize,
                    "ts_from": tsFrom,
                    "ts_to": tsTo
                }
                res = await client.post(f"{MT5_BRIDGE_URL}/rates", json=body)
                if res.status_code == 200:
                    data = res.json()
                    rates_list = data.get("rates", [])
                    df = pd.DataFrame(rates_list) if rates_list else pd.DataFrame()
                else:
                    logger.error(f"[ForexAPI] Error en Bridge MT5: {res.text}")
                    df = pd.DataFrame()

        if df.empty:
            logger.warning(f"[ForexAPI] No se obtuvieron velas para {symbol} desde MT5.")
            return None

        # Convertir timestamps Unix UTC a timezone local
        df['datetime'] = pd.to_datetime(df['time'], unit='s', utc=True).dt.tz_convert(tzCdmx)
        
        # Mapear columnas
        if 'tick_volume' in df.columns:
            df = df.rename(columns={'tick_volume': 'volume'})
        elif 'volume' not in df.columns:
            df['volume'] = 0
            
        df['symbol'] = symbolRaw

        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)

        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'symbol']]
        df = df.sort_values("datetime").reset_index(drop=True)

        if dtTo:
            dfFiltered = df[df['datetime'] <= dtTo].reset_index(drop=True)
            if not dfFiltered.empty:
                df = dfFiltered

        return adjustDataframeInplace(df.dropna(subset=["close"]))

    except Exception as e:
        logger.error(f"[ForexAPI] Error al obtener velas desde MT5 para {symbol}: {e}", exc_info=True)
        return None

async def getTimeSeries(params: dict) -> pd.DataFrame | None:
    """
    Función pública principal idéntica a twelvedata.py
    """
    return await _callForexMt5Api(params)

def resample_candles(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    """
    Agrega velas utilizando resampling (ej. rule='15T' para 15min, '1h' para 1 hora)
    """
    if df is None or df.empty:
        return df
    dfResampled = df.set_index('datetime').resample(rule, closed='right', label='right').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna().reset_index()
    dfResampled['symbol'] = df['symbol'].iloc[0]
    return dfResampled
