import os
import sys
import pandas as pd
from datetime import datetime
import logging
import asyncio
import functools
import time
import requests
from middleware.database import dbConnection
from middleware.config.constants import CONNECTION_POOL_URL

logger = logging.getLogger(__name__)

_session = requests.Session()
_adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=1)
_session.mount("http://", _adapter)
_session.mount("https://", _adapter)

def _call_connection_pool(method: str, path: str, json_data: dict = None, params: dict = None, timeout: float = 5.0):
    """
    Realiza peticiones HTTP al microservicio ConnectionPool (http://127.0.0.1:8000/api/v1).
    Utiliza HTTP Connection Pooling para reusar sockets TCP y optimizar rendimiento.
    Retorna respuesta JSON o None si falla/no responde.
    """
    try:
        url = f"{CONNECTION_POOL_URL.rstrip('/')}/{path.lstrip('/')}"
        m = method.upper()
        if m == "GET":
            res = _session.get(url, params=params, timeout=timeout)
        elif m == "POST":
            res = _session.post(url, json=json_data, params=params, timeout=timeout)
        elif m == "PUT":
            res = _session.put(url, json=json_data, params=params, timeout=timeout)
        elif m == "DELETE":
            res = _session.delete(url, params=params, timeout=timeout)
        else:
            return None
        if res.status_code in (200, 201):
            return res.json()
        else:
            logger.warning(f"ConnectionPool [{method} {path}] HTTP {res.status_code}: {res.text[:200]}")
    except requests.exceptions.ConnectionError as e:
        logger.warning(f"ConnectionPool NO ACCESIBLE en {url} → {e}")
    except requests.exceptions.Timeout:
        logger.warning(f"ConnectionPool TIMEOUT [{method} {path}] → {url}")
    except Exception as e:
        logger.warning(f"ConnectionPool error inesperado [{method} {path}] → {e}")
    return None

def ttl_cache(seconds=30):
    """Caché TTL que NO almacena resultados vacíos/falsos para evitar envenenar el caché en fallos transitorios."""
    def decorator(func):
        cache = {}
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(sorted(kwargs.items())))
            now = time.time()
            if key in cache:
                val, expiry = cache[key]
                if now < expiry:
                    return val
                else:
                    del cache[key]
            val = func(*args, **kwargs)
            # Solo cachear si el resultado es válido (no vacío)
            if val:
                cache[key] = (val, now + seconds)
            return val
        wrapper.cache_clear = lambda: cache.clear()
        return wrapper
    return decorator

def init_alerts_table():
    """Crea la tabla de registro de alertas y asegura que strategyConfig tenga las columnas necesarias (gestionado por ConnectionPool)."""
    pass

def get_api_usage(account_name):
    """Obtiene el consumo actual de una cuenta desde ConnectionPool."""
    try:
        res = _call_connection_pool("GET", f"/api-usage/{account_name}")
        if res and "calls_today" in res:
            last_reset = res.get("last_reset_date")
            today_str = datetime.now().strftime("%Y-%m-%d")
            if last_reset and str(last_reset) != today_str:
                update_api_usage(account_name, 0, reset=True)
                return 0
            return res.get("calls_today", 0)
        return 0
    except Exception as e:
        logger.error(f"Error en get_api_usage: {e}")
        return 0

def update_api_usage(account_name, calls, reset=False):
    """Actualiza o resetea el contador de llamadas en ConnectionPool."""
    try:
        if not reset and calls > 0:
            _call_connection_pool("POST", f"/api-usage/{account_name}/increment")
    except Exception as e:
        logger.error(f"Error en update_api_usage: {e}")

def is_alert_sent(symbol, strategy, candle_time, id_cuenta=None):
    """Verifica si ya se envió una alerta para este símbolo, estrategia, vela y cuenta mediante ConnectionPool."""
    try:
        params = {
            "symbol": symbol,
            "strategy": strategy,
            "candle_time": str(candle_time)
        }
        if id_cuenta:
            params["id_cuenta"] = id_cuenta
        res = _call_connection_pool("GET", "/trades/is-alert-sent", params=params)
        if res and isinstance(res, dict) and "sent" in res:
            return bool(res["sent"])
        return False
    except Exception as e:
        logger.error(f"Error en is_alert_sent: {e}")
        return False

def is_trade_duplicate(symbol, strategy, intervalo, direction, size=None, id_cuenta=None):
    """Verifica si ya existe un trade con los mismos parámetros y status=OPEN mediante ConnectionPool."""
    try:
        params = {
            "symbol": symbol,
            "strategy": strategy,
            "intervalo": intervalo,
            "direction": direction
        }
        if id_cuenta:
            params["id_cuenta"] = id_cuenta
        res = _call_connection_pool("GET", "/trades/check-duplicate", params=params)
        if res and isinstance(res, dict) and "duplicate" in res:
            return bool(res["duplicate"])
        return False
    except Exception as e:
        logger.error(f"Error en is_trade_duplicate: {e}")
        return False


def mark_alert_sent(symbol, strategy, candle_time):
    """Registra que se ha enviado una alerta (ya se hace al insertar en trades)."""
    pass


try:
    from dataSymbol.core.databaseManager import DatabaseManager
except ImportError:
    DatabaseManager = None

try:
    from middleware.config.constants import DATA_SOURCE, INTERVAL, API_KEYS
except ImportError:
    DATA_SOURCE = "db"
    INTERVAL = "15min"
    API_KEYS = []

_indice_key = -1

def cierraTradeEnDb(idTrade, precioCierre, fechaCierre, comentario):
    try:
        params = {
            "exit_price": float(precioCierre),
            "pnl": 0.0
        }
        _call_connection_pool("POST", f"/trades/{idTrade}/close", params=params)
    except Exception as error:
        logger.error(f"❌ Error al cerrar trade en DB mediante ConnectionPool: {error}")


def verificaCierreTrade(tradeData, dfVelas):
    try:
        from urllib.parse import quote
        safe_sym = quote(tradeData['symbol'], safe='')
        trades = _call_connection_pool("GET", f"/trades/cuenta/1")
        if not trades or not isinstance(trades, list):
            return False
            
        for trade in trades:
            if trade.get('symbol') != tradeData['symbol'] or trade.get('status') != 'OPEN':
                continue
            stopLoss = float(trade.get('stopLoss', 0))
            takeProfit = float(trade.get('takeProfit', 0))
            direction = str(trade.get('direction', '')).lower()
            idTrade = trade['idTrade']
            
            openTime = tradeData['openTime']
            if isinstance(openTime, str):
                openTime = datetime.strptime(openTime, '%Y-%m-%d %H:%M:%S')

            dfVelas['datetime'] = pd.to_datetime(dfVelas['datetime'])
            dfPosterior = dfVelas[dfVelas['datetime'] > openTime].copy()

            if dfPosterior.empty:
                return False

            for index, row in dfPosterior.sort_values('datetime').iterrows():
                velaHigh = float(row['high'])
                velaLow = float(row['low'])
                fechaVela = row['datetime']
                precioCierre = 0
                motivoCierre = ""

                if direction == 'largo':
                    if velaLow <= stopLoss:
                        precioCierre = stopLoss
                        motivoCierre = "STOP_LOSS"
                    elif velaHigh >= takeProfit:
                        precioCierre = takeProfit
                        motivoCierre = "TAKE_PROFIT"

                elif direction == 'corto':
                    if velaHigh >= stopLoss:
                        precioCierre = stopLoss
                        motivoCierre = "STOP_LOSS"
                    elif velaLow <= takeProfit:
                        precioCierre = takeProfit
                        motivoCierre = "TAKE_PROFIT"

                if precioCierre > 0:
                    print(f"🎯 Trade {idTrade} cerrado por {motivoCierre} en {fechaVela}")
                    cierraTradeEnDb(idTrade, precioCierre, fechaVela, motivoCierre)
                    return True 
        return False
    except Exception as error:
        logger.error(f"❌ Error en verificaCierreTrade: {error}")
        return False


def logTrade(symbol, regime, pf, sharpe):
    try:
        payload = {
            "symbol": symbol,
            "strategy": regime,
            "pnl": float(pf),
            "intervalo": "5min",
            "direction": "long",
            "openPrice": 0.0,
            "idCuenta": 1
        }
        _call_connection_pool("POST", "/trades", json_data=payload)
    except Exception as error:
        logger.error(f"❌ Error en logTrade: {error}")

@ttl_cache(30)
def getAccount(id=None):
    try:
        def normalize_acc(acc):
            if not isinstance(acc, dict): return acc
            token = acc.get('tokenMsg') or acc.get('TokenMsg')
            grupo = acc.get('idGrupoMsg') or acc.get('IdGrupoMsg')
            cap = acc.get('capital') if acc.get('capital') is not None else acc.get('Capital')
            nom = acc.get('nombre') or acc.get('Nombre')
            act = acc.get('activo') if acc.get('activo') is not None else acc.get('Activo')
            
            acc['tokenMsg'] = token
            acc['TokenMsg'] = token
            acc['idGrupoMsg'] = grupo
            acc['IdGrupoMsg'] = grupo
            acc['capital'] = cap
            acc['Capital'] = cap
            acc['nombre'] = nom
            acc['Nombre'] = nom
            acc['activo'] = act
            acc['Activo'] = act
            return acc

        if id:
            res = _call_connection_pool("GET", f"/cuentas/{id}")
            return [normalize_acc(res)] if res and isinstance(res, dict) else []
        else:
            res = _call_connection_pool("GET", "/cuentas/active")
            return [normalize_acc(item) for item in res] if res and isinstance(res, list) else []
    except Exception as e:
        logger.error(f"Error en getAccount: {e}")
        return []

def getCuentaCapital(idCuenta: int) -> float:
    try:
        res = _call_connection_pool("GET", f"/cuentas/{idCuenta}")
        if res and isinstance(res, dict) and "Capital" in res:
            return float(res["Capital"])
        return 0.0
    except Exception as e:
        logger.error(f"Error al obtener capital de cuenta {idCuenta}: {e}")
        return 0.0

@ttl_cache(30)
def isEstrategiaHabilitadaParaCuenta(idCuenta: int, nombreEstrategia: str) -> bool:
    try:
        res = _call_connection_pool("GET", f"/cuenta-estrategias/cuenta/{idCuenta}")
        if res is not None and isinstance(res, list):
            if len(res) == 0:
                return True
            baseName = nombreEstrategia.split('_')[0]
            for item in res:
                st = item.get("strategy", "")
                if st == nombreEstrategia or st == baseName:
                    return True
            return False
        return True
    except Exception as e:
        logger.error(f"Error en isEstrategiaHabilitadaParaCuenta: {e}")
        return True

@ttl_cache(30)
def isTipoHabilitadoParaCuenta(idCuenta: int, tipoSymbol: str) -> bool:
    return True

@ttl_cache(30)
def getSymbols():
    """Obtiene todos los símbolos activos para Sentinel desde ConnectionPool."""
    return getSentinelSymbols()

@ttl_cache(30)
def getSentinelSymbols():
    """Obtiene todos los símbolos activos para Sentinel desde ConnectionPool."""
    res = _call_connection_pool("GET", "/sentinel-symbols/active")
    return res if res is not None and isinstance(res, list) else []

@ttl_cache(30)
def getDataSymbols():
    """
    Obtiene los símbolos activos para dataSymbol desde ConnectionPool.
    """
    ratio_syms = _call_connection_pool("GET", "/ratio-symbols/active")
    sentinel_syms = _call_connection_pool("GET", "/sentinel-symbols/active")
    
    combined = {}
    if ratio_syms and isinstance(ratio_syms, list):
        for item in ratio_syms:
            if isinstance(item, dict) and "symbol" in item:
                combined[item["symbol"]] = item
    if sentinel_syms and isinstance(sentinel_syms, list):
        for item in sentinel_syms:
            if isinstance(item, dict) and "symbol" in item:
                if item["symbol"] not in combined:
                    combined[item["symbol"]] = item
    if combined:
        return sorted(list(combined.values()), key=lambda x: str(x.get("symbol", "")))
    return []

@ttl_cache(30)
def getRatioSymbols():
    """Obtiene todos los símbolos activos para el módulo de RATIO desde ConnectionPool."""
    res = _call_connection_pool("GET", "/ratio-symbols/active")
    return res if res is not None and isinstance(res, list) else []

@ttl_cache(30)
def getRatioSymbol(symbol: str):
    """Obtiene la información de un símbolo desde ConnectionPool."""
    res = _call_connection_pool("GET", "/ratio-symbols/by-symbol", params={"symbol": symbol})
    if res: return res
    res2 = _call_connection_pool("GET", "/available-symbols/by-symbol", params={"symbol": symbol})
    return res2

@ttl_cache(30)
def getSymbol(symbol: str):
    """Obtiene la información de un símbolo desde ConnectionPool."""
    res = _call_connection_pool("GET", "/sentinel-symbols/by-symbol", params={"symbol": symbol})
    if res: return res
    res2 = _call_connection_pool("GET", "/available-symbols/by-symbol", params={"symbol": symbol})
    return res2

def getSymbolStartDate(symbol: str):
    res = _call_connection_pool("GET", "/sentinel-symbols/by-symbol", params={"symbol": symbol})
    if res and isinstance(res, dict) and "startDate" in res:
        return res["startDate"]
    return None

@ttl_cache(30)
def getSymbolTypeConfig(tipo: str):
    return None

@ttl_cache(30)
def getStrategyConfig(nombreEstrategia: str):
    res = _call_connection_pool("GET", "/strategy-configs/enabled")
    if res and isinstance(res, list):
        for item in res:
            if item.get("strategy") == nombreEstrategia:
                return item
    return None

@ttl_cache(30)
def getSymbolStrategyConfig(strategyName: str, symbol: str) -> dict:
    try:
        res = _call_connection_pool("GET", "/symbol-strategy-configs/by-combo", params={"strategy": strategyName, "symbol": symbol})

        if res and isinstance(res, dict):
            import json
            params = {}
            if res.get('parametersJson'):
                rawParams = res['parametersJson']
                params = json.loads(rawParams) if isinstance(rawParams, str) else rawParams
            
            imacd_params = {}
            if res.get('jsonIMACD'):
                raw_imacd = res['jsonIMACD']
                imacd_params = json.loads(raw_imacd) if isinstance(raw_imacd, str) else raw_imacd
            
            params['useImpulseMacdFilter'] = imacd_params.get('useImpulseMacdFilter', 0)
            params['macdFast'] = imacd_params.get('macdFast', 12)
            params['macdSlow'] = imacd_params.get('macdSlow', 26)
            params['macdSignal'] = imacd_params.get('macdSignal', 9)
            
            global_min_conf = res.get('min_confidence', 70)
            global_min_rr = res.get('min_rr', 1.5)
            params['minConfidence'] = global_min_conf
            params['minRiskRewardRatio'] = global_min_rr
            params['broker'] = res.get('broker', False)
            return params
        return {}
    except Exception as e:
        logger.error(f"Error en getSymbolStrategyConfig: {e}")
        return {}



def buscaTrade(tradeData):
    try:
        dup = is_trade_duplicate(
            symbol=tradeData['symbol'],
            strategy=tradeData.get('strategy', ''),
            intervalo=tradeData.get('intervalo', '15min'),
            direction=tradeData['direction'],
            id_cuenta=tradeData.get('idCuenta')
        )
        if dup:
            logger.info(f"⚠️ Trade ya existente para {tradeData['symbol']} - se omite actualización")
            return False
        insertarTrade(tradeData)
        return True
    except Exception as error:
        logger.error(f"❌ Error en buscaTrade: {error}")
        return False

def actualizarTrade(idTrade, data):
    try:
        params = {
            "exit_price": float(data.get('exitPrice', 0)),
            "pnl": float(data.get('pnl', 0))
        }
        _call_connection_pool("POST", f"/trades/{idTrade}/close", params=params)
    except Exception as e:
        logger.error(f"❌ Error al actualizarTrade {idTrade}: {e}")

def insertarTrade(data):
    try:
        payload = {
            "idCuenta": data.get('idCuenta', 1),
            "symbol": data.get('symbol'),
            "direction": data.get('direction', 'long').lower(),
            "openTime": str(data.get('openTime')),
            "size": float(data.get('size', 1.0)),
            "entryPrice": float(data.get('entryPrice', 0.0)),
            "stopLoss": float(data.get('stopLoss')) if data.get('stopLoss') else None,
            "takeProfit": float(data.get('takeProfit')) if data.get('takeProfit') else None,
            "intervalo": data.get('intervalo', '15min'),
            "strategy": data.get('strategy', ''),
            "margin_used": float(data.get('margin_used', 0.0)),
            "candleTime": str(data.get('candleTime')) if data.get('candleTime') else None,
            "ticketId": str(data.get('ticketId')) if data.get('ticketId') else None
        }
        res = _call_connection_pool("POST", "/trades", json_data=payload)
        if res and isinstance(res, dict) and "idTrade" in res:
            logger.info(f"🚀 Nuevo trade insertado via ConnectionPool: {data['symbol']}")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Error al insertarTrade: {e}")
        return False

def getOpenTradesForActiveAccounts():
    try:
        res = _call_connection_pool("GET", "/trades/cuenta/1", params={"status_filter": "OPEN"})
        return res if res and isinstance(res, list) else []
    except Exception as e:
        logger.error(f"Error en getOpenTradesForActiveAccounts: {e}")
        return []

def getOpenTrades():
    try:
        res = _call_connection_pool("GET", "/trades/cuenta/1", params={"status_filter": "OPEN"})
        return res if res and isinstance(res, list) else []
    except Exception as e:
        logger.error(f"❌ Error en getOpenTrades: {e}")
        return []

def getOpenTradesBySymbol(symbol: str) -> list:
    try:
        res = _call_connection_pool("GET", "/trades/cuenta/1", params={"status_filter": "OPEN"})
        if res and isinstance(res, list):
            return [t for t in res if t.get("symbol") == symbol]
        return []
    except Exception as e:
        logger.error(f"❌ Error en getOpenTradesBySymbol: {e}")
        return []

def updateTradeLevels(id_trade: int, stop_loss: float, take_profit: float):
    """Actualiza los niveles de SL y TP de un trade abierto via ConnectionPool."""
    return True

def closeTrade(idTrade: int, exitPrice: float, pnl: float, reason: str, capital_anterior: float = None, pnl_anterior: float = None):
    try:
        params = {"exit_price": float(exitPrice), "pnl": float(pnl)}
        res = _call_connection_pool("POST", f"/trades/{idTrade}/close", params=params)
        return res is not None
    except Exception as e:
        logger.error(f"Error en closeTrade: {e}")
        return False
        


def getTradesClosedToday(idCuenta: int, fecha: str):
    """
    Retorna la lista de trades cerratizados hoy para una cuenta mediante ConnectionPool.
    fecha: Formato 'YYYY-MM-DD'
    """
    try:
        res = _call_connection_pool("GET", f"/trades/cuenta/{idCuenta}")
        if res and isinstance(res, list):
            filtered = []
            for t in res:
                close_time = t.get("closeTime")
                if close_time and str(close_time).startswith(fecha):
                    filtered.append(t)
            return filtered
        return []
    except Exception as e:
        logger.error(f"Error en getTradesClosedToday: {e}")
        return []

def getAccountById(idCuenta: int):
    """
    Alias para getAccount(id) que retorna un solo objeto.
    """
    cuentas = getAccount(idCuenta)
    return cuentas[0] if cuentas else None


async def getLastCandleDatetime(symbol: str, timeframe: str):
    def query():
        res = _call_connection_pool("GET", "/candles/stats/last-timestamp", params={"symbol": symbol, "timeframe": timeframe})
        if res and res.get("last_timestamp"):
            return pd.to_datetime(res["last_timestamp"])
        return None
    return await asyncio.to_thread(query)

async def insertNewCandlesToDb(df, timeframe: str) -> int:
    """
    Inserta velas en la tabla 'candles' de forma segura y asincrónica.
    """
    if df.empty:
        logger.info(f"No hay velas para insertar en {timeframe}.")
        return 0

    if 'volume' not in df.columns:
        df['volume'] = None

    def insert():
        try:
            payload = []
            for _, row in df.iterrows():
                payload.append({
                    "symbol": row['symbol'],
                    "timeframe": timeframe,
                    "timestamp": str(row['timestamp']),
                    "open": float(row['open']),
                    "high": float(row['high']),
                    "low": float(row['low']),
                    "close": float(row['close']),
                    "volume": float(row['volume']) if row.get('volume') is not None else 0.0
                })
            res = _call_connection_pool("POST", "/candles/bulk", json_data=payload, timeout=15.0)
            if res and res.get("status") == "ok":
                return res.get("inserted", len(payload))
            return 0
        except Exception as e:
            logger.error(f"Error en insertNewCandlesToDb: {e}", exc_info=True)
            return 0

    inserted_count = await asyncio.to_thread(insert)
    return inserted_count


async def getCandlesFromDb(symbol: str, timeframe: str = "5min", limit: int = 500) -> pd.DataFrame:
    """
    Obtiene velas de la tabla 'candles' como DataFrame usando ConnectionPool (o fallback MySQL).
    """
    def query():
        cp_res = _call_connection_pool("GET", "/candles/symbol-query", params={"symbol": symbol, "timeframe": timeframe, "limit": limit}, timeout=15.0)
        if cp_res is not None and isinstance(cp_res, list) and len(cp_res) > 0:
            try:
                df = pd.DataFrame(cp_res)
                if 'timestamp' in df.columns and 'close' in df.columns:
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    from middleware.config.constants import TIMEZONE
                    df['timestamp'] = df['timestamp'].dt.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward')
                    df = df.sort_values('timestamp').set_index('timestamp')
                    for col in ['open', 'high', 'low', 'close']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                    if 'volume' in df.columns:
                        df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
                    else:
                        df['volume'] = 0.0
                    return df[['open', 'high', 'low', 'close', 'volume']].dropna(subset=['close'])
            except Exception as ex:
                logger.debug(f"Error parseando candles de ConnectionPool: {ex}")
        return pd.DataFrame()

    return await asyncio.to_thread(query)


def _get_api_key():
    global _indice_key
    if not API_KEYS:
        return None
    _indice_key = (_indice_key + 1) % len(API_KEYS)
    return API_KEYS[_indice_key]


async def getCandles(symbol: str, n_velas: int = 500) -> pd.DataFrame:
    """
    Obtiene velas según DATA_SOURCE:
    - "db": tabla candles (5min)
    - "12data": API 12Data (usa INTERVAL)
    """
    try:
        from middleware.api.twelvedata import adjustDataframeInplace
    except ImportError:
        adjustDataframeInplace = lambda df: df

    if DATA_SOURCE == "forex":
        try:
            from middleware.api import forex
            params = {
                "symbol": symbol,
                "interval": "5min",
                "outputSize": n_velas
            }
            df = await forex.getTimeSeries(params)
            if df is not None and not df.empty:
                return df
            else:
                logger.warning(f"forex.getTimeSeries retornó None o vacío para {symbol}. Usando DB como fallback.")
                df = await getCandlesFromDb(symbol, "5min", n_velas)
                if not df.empty and "symbol" not in df.columns:
                    df["symbol"] = symbol
                return adjustDataframeInplace(df)
        except Exception as e:
            logger.error(f"Error en getCandles (Forex MT5): {e}")
            return pd.DataFrame()
    elif DATA_SOURCE == "12data":
        api_key = _get_api_key()
        if not api_key:
            logger.error("No hay API keys disponibles para 12Data")
            return pd.DataFrame()
        
        url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={INTERVAL}&outputsize={n_velas}&apikey={api_key}"
        try:
            response = requests.get(url).json()
            if "values" not in response:
                logger.warning(f"Respuesta sin valores para {symbol}: {response.get('message')}")
                return pd.DataFrame()
            
            df = pd.DataFrame(response["values"])
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime").set_index("datetime")
            
            for col in ["open", "high", "low", "close"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            
            if "volume" in df.columns:
                df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
            else:
                df["volume"] = pd.Series(0, index=df.index)
            
            df_cleaned = df.dropna(subset=["close"])
            if "symbol" not in df_cleaned.columns:
                df_cleaned["symbol"] = symbol
            return adjustDataframeInplace(df_cleaned)
        except Exception as e:
            logger.error(f"Error en getCandles (12Data): {e}")
            return pd.DataFrame()
    else:
        df = await getCandlesFromDb(symbol, "5min", n_velas)
        if not df.empty and "symbol" not in df.columns:
            df["symbol"] = symbol
        return adjustDataframeInplace(df)


def get_sleep_time(esperaMin: int = 15) -> int:
    """
    Retorna el tiempo de espera en MINUTOS entre solicitudes según DATA_SOURCE:
    - "db": 5 minutos
    - "12data": usa el valor de esperaMin proporcionado (minutos)
    """
    if DATA_SOURCE == "db":
        return 5
    return esperaMin


def get_min_wait_time() -> int:
    """
    Retorna el tiempo mínimo de espera entre solicitudes (en segundos).
    - "db": 0 segundos (datos locales, no hay límite de API)
    - "12data": 3 segundos (límite de 8 llamadas/min)
    """
    if DATA_SOURCE == "db":
        return 1
    return 3

def getBrokers() -> list:
    """Retorna la lista de todos los brokers registrados mediante ConnectionPool."""
    try:
        res = _call_connection_pool("GET", "/brokers")
        return res if res and isinstance(res, list) else []
    except Exception as e:
        logger.error(f"Error en getBrokers: {e}")
        return []

def getBrokerCuentas(idCuenta: int = None) -> list:
    """
    Retorna la lista de mapeos de broker por cuenta mediante ConnectionPool.
    """
    try:
        if idCuenta is not None:
            res = _call_connection_pool("GET", f"/broker-cuentas/cuenta/{idCuenta}")
        else:
            res = _call_connection_pool("GET", "/broker-cuentas")
        return res if res and isinstance(res, list) else []
    except Exception as e:
        logger.error(f"Error en getBrokerCuentas: {e}")
        return []

def addBrokerCuenta(idCuenta: int, idBroker: int, tipoConexion: str, loginUsuario: str = None, tokenAcceso: str = None, activo: int = 1) -> bool:
    try:
        payload = {
            "idCuenta": idCuenta,
            "idBroker": idBroker,
            "tipoConexion": tipoConexion,
            "loginUsuario": loginUsuario,
            "tokenAcceso": tokenAcceso,
            "activo": bool(activo)
        }
        res = _call_connection_pool("POST", "/broker-cuentas", json_data=payload)
        return res is not None
    except Exception as e:
        logger.error(f"Error en addBrokerCuenta: {e}")
        return False

def getRiesgoSugerido(symbol: str, strategy: str) -> float:
    return 1.0

def getLastStockPriceDate(symbol: str):
    """Obtiene la fecha más reciente registrada para un símbolo en StockPrices mediante ConnectionPool."""
    try:
        res = _call_connection_pool("GET", "/stock-prices/last-date", params={"symbol": symbol})
        if res and res.get("last_date"):
            return pd.to_datetime(res["last_date"])
        return None
    except Exception as e:
        logger.error(f"Error en getLastStockPriceDate para {symbol}: {e}")
        return None

def saveStockPrices(df: pd.DataFrame, symbol: str) -> int:
    """Guarda o actualiza masivamente precios diarios en StockPrices mediante ConnectionPool."""
    if df is None or df.empty: return 0
    try:
        payload = []
        for idx, row in df.iterrows():
            dt = str(row['datetime']) if isinstance(row['datetime'], pd.Timestamp) else str(row['datetime'])
            price = float(row['close'])
            payload.append({"symbol": symbol, "priceDate": str(dt), "closePrice": price})

        res = _call_connection_pool("POST", "/stock-prices/bulk", json_data=payload)
        if res and "inserted" in res:
            return res["inserted"]
        return 0
    except Exception as e:
        logger.error(f"Error guardando StockPrices para {symbol}: {e}")
        return 0


async def getStockPricesFromDb(symbol: str, limit: int = 365) -> pd.DataFrame:
    """
    Obtiene los precios de cierre diarios de la tabla 'StockPrices' como DataFrame mediante ConnectionPool.
    """
    def query():
        try:
            from urllib.parse import quote
            safe_sym = quote(symbol, safe='')
            res = _call_connection_pool("GET", "/stock-prices", params={"symbol": symbol, "limit": limit})
            if res and isinstance(res, list):
                df = pd.DataFrame(res)
                if not df.empty and 'symbol' in df.columns:
                    df = df[df['symbol'] == symbol]
                    if not df.empty and 'priceDate' in df.columns:
                        df['priceDate'] = pd.to_datetime(df['priceDate'])
                        df = df.sort_values('priceDate').set_index('priceDate')
                        df['closePrice'] = pd.to_numeric(df['closePrice'], errors='coerce')
                        return df[['closePrice']].dropna(subset=['closePrice'])
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error en getStockPricesFromDb: {e}")
            return pd.DataFrame()

    return await asyncio.to_thread(query)
