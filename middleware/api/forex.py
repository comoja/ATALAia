"""
Asynchronous API client for MetaTrader 5 (MT5) to retrieve Forex rates, mimicking TwelveData interface.
"""
import os
import sys
import logging
import asyncio
import pandas as pd
import pytz
from datetime import datetime, timedelta

from middleware.config.constants import TIMEZONE, mt5Login, mt5Password, mt5Server
from middleware.database import dbManager, dbConnection

# Intentar importar MetaTrader5 con bypass para entornos macOS/desarrollo
try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

logger = logging.getLogger(__name__)

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

async def _ensureMt5Initialized() -> bool:
    """
    Asegura que el terminal de MT5 esté inicializado y conectado.
    Retorna True si la conexión es exitosa, False en caso contrario.
    """
    if mt5 is None:
        logger.warning("[ForexAPI] MetaTrader5 no está disponible o no es compatible en esta plataforma.")
        return False
        
    try:
        # Verificar si ya está inicializado y conectado a la cuenta correcta
        terminalInfo = mt5.terminal_info()
        if terminalInfo is not None:
            if mt5Login > 0:
                accountInfo = mt5.account_info()
                if accountInfo is not None and accountInfo.login == mt5Login and accountInfo.server == mt5Server:
                    logger.debug(f"[ForexAPI] MT5 ya está inicializado y conectado a la cuenta {mt5Login}.")
                    return True
            else:
                logger.debug("[ForexAPI] MT5 ya está inicializado con sesión activa.")
                return True

        logger.info("[ForexAPI] Intentando conectar con el terminal de MetaTrader 5...")
        
        # Inicializar MT5 sin credenciales directamente en initialize para evitar IPC timeout
        isInitialized = await asyncio.to_thread(mt5.initialize)
        if not isInitialized:
            errorCode = mt5.last_error()
            logger.error(f"[ForexAPI] Falló inicialización de MetaTrader5. Código de error: {errorCode}")
            return False

        # Si tenemos credenciales parametrizadas, realizamos el login por separado
        if mt5Login > 0:
            logger.info(f"[ForexAPI] Realizando login programático a la cuenta {mt5Login}...")
            isLogged = await asyncio.to_thread(
                mt5.login,
                mt5Login,
                password=mt5Password,
                server=mt5Server
            )
            if not isLogged:
                errorCode = mt5.last_error()
                logger.error(f"[ForexAPI] Falló login en MetaTrader5. Código de error: {errorCode}")
                await asyncio.to_thread(mt5.shutdown)
                return False
            
        logger.info("[ForexAPI] Conexión e inicialización exitosa con MetaTrader 5.")
        return True
    except Exception as e:
        logger.error(f"[ForexAPI] Excepción al intentar inicializar MT5: {e}")
        return False

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
    Consulta los datos de velas directamente desde MT5 de forma asíncrona.
    """
    if not await _ensureMt5Initialized():
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

    # Mapear el intervalo
    mt5Timeframe = TIMEFRAME_MAP.get(interval)
    if mt5Timeframe is None:
        logger.error(f"[ForexAPI] Intervalo no soportado por MT5: {interval}")
        return None

    try:
        # Verificar que el símbolo esté disponible en el MarketWatch de MT5
        symbolInfo = await asyncio.to_thread(mt5.symbol_info, symbol)
        if symbolInfo is None:
            # Intentar seleccionarlo en MarketWatch
            selected = await asyncio.to_thread(mt5.symbol_select, symbol, True)
            if not selected:
                logger.error(f"[ForexAPI] Símbolo {symbol} no encontrado o no se pudo seleccionar en MT5.")
                return None

        rates = None
        # Obtener los rates según el rango de fechas o el tamaño de salida
        if startDate:
            tzCdmx = pytz.timezone(TIMEZONE)
            
            # Asegurar datetime tz-aware en CDMX
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
            else:
                tsTo = None

            # Determinar el intervalo en minutos
            intervalMinutes = 5
            if interval == "1min":
                intervalMinutes = 1
            elif interval == "5min":
                intervalMinutes = 5
            elif interval == "15min":
                intervalMinutes = 15
            elif interval == "30min":
                intervalMinutes = 30
            elif interval == "1h":
                intervalMinutes = 60
            elif interval == "4h":
                intervalMinutes = 240
            elif interval == "1day" or interval == "1d":
                intervalMinutes = 1440
            elif interval == "1week":
                intervalMinutes = 10080
            elif interval == "1month":
                intervalMinutes = 43200

            # Solo validar la vela más antigua si el inicio solicitado es anterior a hace 30 días (para evitar consultas históricas innecesarias)
            limitTs = int((datetime.now(pytz.timezone(TIMEZONE)) - timedelta(days=30)).timestamp())
            if tsFrom < limitTs:
                oldestRates = await asyncio.to_thread(mt5.copy_rates_from, symbol, mt5Timeframe, datetime(1990, 1, 1), 1)
                if oldestRates is not None and len(oldestRates) > 0:
                    oldestTs = int(oldestRates[0]['time'])
                    if tsFrom < oldestTs:
                        logger.info(f"[ForexAPI] tsFrom ({dtFrom}) es anterior al historial disponible en MT5 ({datetime.fromtimestamp(oldestTs, tz=tzCdmx)}). Ajustando rango al inicio de la historia.")
                        tsFrom = oldestTs
                        dtFrom = datetime.fromtimestamp(tsFrom, tz=tzCdmx)
                        if tsTo is not None and tsTo < tsFrom:
                            tsTo = tsFrom + (3000 * intervalMinutes * 60)
                            dtTo = datetime.fromtimestamp(tsTo, tz=tzCdmx)

            if tsTo is not None:
                # Recorte de seguridad para no pasarse de 3000 velas en la consulta
                maxCandles = 3000
                maxSeconds = maxCandles * intervalMinutes * 60
                if tsTo - tsFrom > maxSeconds:
                    tsTo = tsFrom + maxSeconds
                    dtTo = datetime.fromtimestamp(tsTo, tz=tzCdmx)

                logger.info(f"[ForexAPI] Solicitando rango desde {dtFrom} hasta {dtTo} (intervalo {interval})")
                rates = await asyncio.to_thread(mt5.copy_rates_range, symbol, mt5Timeframe, tsFrom, tsTo)
            else:
                rates = await asyncio.to_thread(mt5.copy_rates_from, symbol, mt5Timeframe, tsFrom, min(outputSize, 3000))
        else:
            # Por defecto obtener las últimas outputSize velas terminadas
            rates = await asyncio.to_thread(mt5.copy_rates_from_pos, symbol, mt5Timeframe, 1, outputSize)

        if rates is None or len(rates) == 0:
            logger.warning(f"[ForexAPI] No se pudieron obtener velas para {symbol} desde MT5.")
            return None

        # Convertir a DataFrame
        df = pd.DataFrame(rates)
        
        # Excluir la vela en desarrollo actual (vela 0) para asegurar solo velas terminadas
        currentBar = await asyncio.to_thread(mt5.copy_rates_from_pos, symbol, mt5Timeframe, 0, 1)
        if currentBar is not None and len(currentBar) > 0:
            currentBarTime = int(currentBar[0]['time'])
            df = df[df['time'] < currentBarTime]

        if df.empty:
            logger.warning(f"[ForexAPI] No quedaron velas terminadas para {symbol} después de filtrar la vela actual.")
            return None
        
        # MT5 retorna time en segundos Unix (UTC). Convertimos a datetime con timezone local (TIMEZONE)
        df['datetime'] = pd.to_datetime(df['time'], unit='s', utc=True).dt.tz_convert(pytz.timezone(TIMEZONE))
        
        # Mapear nombres de columnas
        df = df.rename(columns={'tick_volume': 'volume'})
        df['symbol'] = symbolRaw # Mantener el símbolo original con barra si venía así
        
        # Asegurar tipos correctos
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)

        # Seleccionar y ordenar columnas
        df = df[['datetime', 'open', 'high', 'low', 'close', 'volume', 'symbol']]
        df = df.sort_values("datetime").reset_index(drop=True)

        if endDate:
            dfFiltered = df[df['datetime'] <= dtTo].reset_index(drop=True)
            if dfFiltered.empty and not df.empty:
                logger.info(f"[ForexAPI] Rango solicitado {dtFrom} a {dtTo} no tiene datos. Devolviendo la primera vela disponible en MT5 ({df['datetime'].iloc[0]}) para avanzar.")
                dfFiltered = df.head(1).reset_index(drop=True)
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
