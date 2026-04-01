import os
import sys
import mysql.connector
import pandas as pd
from datetime import datetime
import logging
import asyncio
import requests

logger = logging.getLogger(__name__)

from middleware.database import dbConnection

try:
    from middleware.config.constants import DATA_SOURCE, INTERVAL, API_KEYS
except ImportError:
    DATA_SOURCE = "db"
    INTERVAL = "15min"
    API_KEYS = []

_indice_key = -1

def cierraTradeEnDb(idTrade, precioCierre, fechaCierre, comentario):
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()

        sqlClose = """
            UPDATE trades 
            SET exitPrice = %s, 
                closeTime = %s
            WHERE idTrade = %s
        """
        dbCursor.execute(sqlClose, (precioCierre, fechaCierre, comentario, idTrade))
        dbConn.commit()
        
    except Exception as error:
        logger.error(f"❌ Error al cerrar trade en DB: {error}")
    finally:
        dbCursor.close()
        dbConn.close()


def verificaCierreTrade(tradeData, dfVelas):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM trades WHERE closeTime is null and symbol = %s", (tradeData['symbol'],))
        
        trades = cursor.fetchall()    
        for trade in trades:
            stopLoss = float(trade['stopLoss'])
            takeProfit = float(trade['takeProfit'])
            direction = trade['direction'].lower()
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

                if direction == 'buy':
                    if velaLow <= stopLoss:
                        precioCierre = stopLoss
                        motivoCierre = "STOP_LOSS"
                    elif velaHigh >= takeProfit:
                        precioCierre = takeProfit
                        motivoCierre = "TAKE_PROFIT"

                elif direction == 'sell':
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
        conn.close()        
    except Exception as error:
        logger.error(f"❌ Error en verificaCierreTrade: {error}")
        return False


def logTrade(symbol, regime, pf, sharpe):
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO trades (symbol, regime, pf, sharpe)
        VALUES (%s, %s, %s, %s)
    """, (symbol, regime, pf, sharpe))

    conn.commit()
    conn.close()

def getAccount(id=None):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        if id:
            cursor.execute("SELECT * FROM CUENTA WHERE idCuenta = %s ", (id,))
        else:
            cursor.execute("SELECT * FROM CUENTA WHERE Activo=1")
        
        cuentas = cursor.fetchall()
        
        conn.close()
        if cuentas:
            return cuentas
        return []
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return []

def isEstrategiaHabilitadaParaCuenta(idCuenta: int, nombreEstrategia: str) -> bool:
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT estrategias FROM CUENTA WHERE idCuenta = %s", (idCuenta,))
        
        result = cursor.fetchone()
        conn.close()
        
        if not result or not result.get('estrategias'):
            return True
        
        estrategias_str = result['estrategias']
        estrategias = [e.strip() for e in estrategias_str.split(',')]
        return nombreEstrategia in estrategias
    except Exception as e:
        logger.error(f"Error en isEstrategiaHabilitadaParaCuenta: {e}", exc_info=True)
        return True
    
def getSymbols():
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM SentinelSymbol WHERE Activo=1")
        
        symbols = cursor.fetchall()
        
        conn.close()
        if symbols:
            return symbols
        return []
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return []

def getSymbol(symbol: str):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM SentinelSymbol WHERE symbol = %s", (symbol,))
        
        result = cursor.fetchone()
        
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return None

def getSymbolStartDate(symbol: str):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT startDate FROM SentinelSymbol WHERE symbol = %s", (symbol,))
        result = cursor.fetchone()
        conn.close()
        return result['startDate'] if result else None
    except Exception as e:
        logger.error(f"Error en getSymbolStartDate: {e}", exc_info=True)
        return None

def getSymbolTypeConfig(tipo: str):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM SymbolTypeConfig WHERE tipo = %s", (tipo,))
        
        result = cursor.fetchone()
        
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return None

def getStrategyConfig(nombre: str):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM strategyConfig WHERE nombre = %s AND enabled = TRUE", (nombre,))
        
        result = cursor.fetchone()
        
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error en getStrategyConfig: {e}", exc_info=True)
        return None

def buscaTrade(tradeData):
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)

        strategy = tradeData.get('strategy', '')
        fvgNum = tradeData.get('fvgNum', 0)
        
        if strategy in ['ImbalanceLDN', 'ImbalanceNY'] and fvgNum > 0:
            sqlCheck = """
                SELECT idTrade FROM trades 
                WHERE idCuenta = %s 
                    AND symbol = %s 
                    AND direction = %s 
                    AND strategy = %s
                    AND closeTime IS NULL 
                    AND DATE(openTime) = CURDATE()
                LIMIT 1
            """
            paramsCheck = (
                tradeData['idCuenta'], 
                tradeData['symbol'], 
                tradeData['direction'],
                strategy
            )
        else:
            sqlCheck = """
                SELECT idTrade FROM trades 
                WHERE idCuenta = %s 
                    AND symbol = %s 
                    AND direction = %s 
                    AND strategy = %s
                    AND closeTime IS NULL 
                LIMIT 1
            """
            paramsCheck = (
                tradeData['idCuenta'], 
                tradeData['symbol'], 
                tradeData['direction'],
                strategy
            )
        
        dbCursor.execute(sqlCheck, paramsCheck)
        tradeExistente = dbCursor.fetchone()

        if tradeExistente:
            logger.info(f"⚠️ Trade ya existente {tradeExistente['idTrade']} para {tradeData['symbol']} - se omite actualización")
        else:
            if strategy in ['ImbalanceLDN', 'ImbalanceNY']:
                sqlCount = """
                    SELECT COUNT(*) as total FROM trades 
                    WHERE idCuenta = %s 
                        AND symbol = %s 
                        AND strategy = %s
                        AND closeTime IS NULL 
                        AND DATE(openTime) = CURDATE()
                """
                dbCursor.execute(sqlCount, (tradeData['idCuenta'], tradeData['symbol'], strategy))
                result = dbCursor.fetchone()
                if result and result['total'] >= 2:
                    logger.info(f"⚠️ Límite de 2 trades alcanzado para {strategy} en {tradeData['symbol']} - se omite")
                    return
            
            insertarTrade(tradeData)
            logger.info(f"🆕 Nuevo trade insertado para {tradeData['symbol']}")

    except Exception as error:
        logger.error(f"❌ Error en buscaTrade: {error}")
    finally:
        if 'dbCursor' in locals(): 
            dbCursor.close()

def actualizarTrade(idTrade, data):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()
        
        sqlUpdate = """
            UPDATE trades 
            SET closeTime = %s, exitPrice = %s, pnl = %s, slippage = %s, commission = %s, openTime = %s
            WHERE idTrade = %s
        """
        valores = (
            data['closeTime'], data['exitPrice'], data['pnl'], 
            data.get('slippage', 0), data.get('commission', 0), data['openTime'], idTrade
        )
        
        cursor.execute(sqlUpdate, valores)
        conn.commit()
        logger.info(f"✅ Trade {idTrade} actualizado.")

    except Exception as e:
        logger.error(f"❌ Error al actualizarTrade {idTrade}: {e}")
        if 'conn' in locals(): conn.rollback()

def insertarTrade(data):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()

        margin_used = float(data.get('margin_used', 0))
        
        sqlInsert = """
            INSERT INTO trades (idCuenta, symbol, direction, openTime, size, entryPrice, stopLoss, takeProfit,intervalo, strategy, margin_used)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s)
        """
        valores = (
            data['idCuenta'], data['symbol'], data['direction'], 
            data['openTime'], data['size'], data['entryPrice'], 
            data.get('stopLoss'), data.get('takeProfit'),
            data.get('intervalo', '15min'),
            data.get('strategy', ''),
            margin_used
        )

        cursor.execute(sqlInsert, valores)
        
        if margin_used > 0:
            cursor.execute("UPDATE Cuenta SET Capital = Capital - %s WHERE idCuenta = %s", 
                         (margin_used, data['idCuenta']))
        
        conn.commit()
        logger.info(f"🚀 Nuevo trade insertado: {data['symbol']} | Margen reservado: {margin_used}")

    except Exception as e:
        logger.error(f"❌ Error al insertarTrade: {e}")
        if 'conn' in locals(): conn.rollback()

def getOpenTrades():
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM trades WHERE closeTime IS NULL")
        trades = cursor.fetchall()
        conn.close()
        return trades
    except Exception as e:
        logger.error(f"❌ Error en getOpenTrades: {e}")
        return []

def closeTrade(idTrade: int, exitPrice: float, pnl: float, reason: str):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT idCuenta, margin_used FROM trades WHERE idTrade = %s", (idTrade,))
        trade = cursor.fetchone()
        if not trade:
            logger.warning(f"Trade {idTrade} no encontrado")
            return False
        
        idCuenta = trade['idCuenta']
        margin_used = float(trade['margin_used']) if trade['margin_used'] else 0
        closeTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        cursor.execute("""
            UPDATE trades 
            SET closeTime = %s, exitPrice = %s, pnl = %s 
            WHERE idTrade = %s
        """, (closeTime, exitPrice, pnl, idTrade))
        
        capital_change = pnl + margin_used
        cursor.execute("""
            UPDATE Cuenta SET Capital = Capital + %s WHERE idCuenta = %s
        """, (capital_change, idCuenta))
        
        conn.commit()
        conn.close()
        logger.info(f"✅ Trade {idTrade} cerrado: {reason} | PnL: {pnl:.2f} | Margen devuelto: {margin_used:.2f} | Capital actualizado: {capital_change:.2f}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al cerrar trade {idTrade}: {e}")
        if 'conn' in locals(): conn.rollback()
        return False


async def getLastCandleDatetime(symbol: str, timeframe: str):
    def query():
        try:
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT MAX(timestamp) FROM candles WHERE symbol=%s AND timeframe=%s",
                (symbol, timeframe)
            )
            result = cursor.fetchone()
            conn.close()
            return result[0] if result[0] else None
        except Exception as e:
            logger.error(f"Error en getLastCandleDatetime: {e}", exc_info=True)
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
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            insert_query = """
                INSERT IGNORE INTO candles 
                (symbol, timeframe, timestamp, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = [
                (
                    row['symbol'], 
                    timeframe, 
                    row['timestamp'], 
                    row['open'], 
                    row['high'], 
                    row['low'], 
                    row['close'], 
                    row['volume']
                )
                for _, row in df.iterrows()
            ]

            cursor.executemany(insert_query, values)
            conn.commit()
            inserted = cursor.rowcount
            conn.close()
            return inserted
        except Exception as e:
            logger.error(f"Error en insertNewCandlesToDb: {e}", exc_info=True)
            return 0

    inserted_count = await asyncio.to_thread(insert)
    return inserted_count


async def getCandlesFromDb(symbol: str, timeframe: str = "5min", limit: int = 500) -> pd.DataFrame:
    """
    Obtiene velas de la tabla 'candles' como DataFrame.
    """
    def query():
        try:
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT timestamp, open, high, low, close, volume FROM candles "
                "WHERE symbol=%s AND timeframe=%s ORDER BY timestamp DESC LIMIT %s",
                (symbol, timeframe, limit)
            )
            rows = cursor.fetchall()
            conn.close()
            
            if not rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(rows, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.sort_values('timestamp').set_index('timestamp')
            
            for col in ['open', 'high', 'low', 'close']:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0)
            
            return df.dropna(subset=['close'])
        except Exception as e:
            logger.error(f"Error en getCandlesFromDb: {e}", exc_info=True)
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
    if DATA_SOURCE == "12data":
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
            
            return df.dropna(subset=["close"])
        except Exception as e:
            logger.error(f"Error en getCandles (12Data): {e}")
            return pd.DataFrame()
    else:
        return await getCandlesFromDb(symbol, "5min", n_velas)


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
        return 2
    return 4