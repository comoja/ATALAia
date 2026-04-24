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

def init_alerts_table():
    """Crea la tabla de registro de alertas y asegura que strategyConfig tenga las columnas necesarias."""
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()
      
        
        # Tabla de Configuración de Estrategias (Asegurar columnas)
        dbCursor.execute("SHOW TABLES LIKE 'strategyConfig'")
        if not dbCursor.fetchone():
            sql_config = """
                CREATE TABLE strategyConfig (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    strategy VARCHAR(10) NOT NULL UNIQUE,
                    enabled BOOLEAN DEFAULT TRUE,
                    max_minutos_fvg INT DEFAULT 40,
                    max_minutos_signal INT DEFAULT 40,
                    min_rr DOUBLE DEFAULT 1.5,
                    min_confidence INT DEFAULT 70,
                    max_drawdown_percent DOUBLE DEFAULT 5.0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """
            dbCursor.execute(sql_config)
            
            # Insertar defaults
            strategies = [
                ('EMA20200', 1.5, 70), ('Sniper', 2.0, 80), ('SMA20_200', 1.5, 70),
                ('ImbalanceNY', 1.5, 75), ('ImbalanceLDN', 1.5, 75), ('Patron4h', 1.5, 70),
                ('SesgoBiasHTF', 1.5, 70), ('SilverBullet', 1.5, 75), ('GenericFVG', 0.5, 60)
            ]
            for name, rr, conf in strategies:
                dbCursor.execute(
                    "INSERT IGNORE INTO strategyConfig (strategy, min_rr, min_confidence) VALUES (%s, %s, %s)",
                    (name, rr, conf)
                )
        else:
            # Asegurar que existan las columnas nuevas (alter table if not exists pattern)
            cols = {
                "min_rr": "DOUBLE DEFAULT 1.5",
                "min_confidence": "INT DEFAULT 70",
                "max_drawdown_percent": "DOUBLE DEFAULT 5.0"
            }
            for col, definition in cols.items():
                try:
                    dbCursor.execute(f"ALTER TABLE strategyConfig ADD COLUMN {col} {definition}")
                except:
                    pass # Ya existe
        
        # Tabla de Uso de API
        sql_api = """
            CREATE TABLE IF NOT EXISTS api_usage (
                account_name VARCHAR(50) PRIMARY KEY,
                api_key VARCHAR(100),
                calls_today INT DEFAULT 0,
                last_call TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_reset_date DATE
            )
        """
        dbCursor.execute(sql_api)
        
        dbConn.commit()
    except Exception as e:
        logger.error(f"❌ Error al inicializar tablas: {e}")
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def get_api_usage(account_name):
    """Obtiene el consumo actual de una cuenta desde la DB."""
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)
        sql = "SELECT calls_today, last_reset_date FROM api_usage WHERE account_name = %s"
        dbCursor.execute(sql, (account_name,))
        result = dbCursor.fetchone()
        
        today = datetime.now().date()
        if result:
            if result['last_reset_date'] != today:
                # Si es un nuevo día, reseteamos en DB
                update_api_usage(account_name, 0, reset=True)
                return 0
            return result['calls_today']
        return 0
    except Exception as e:
        logger.error(f"Error en get_api_usage: {e}")
        return 0
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def update_api_usage(account_name, calls, reset=False):
    """Actualiza o resetea el contador de llamadas en la DB."""
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()
        today = datetime.now().date()
        if reset:
            sql = "INSERT INTO api_usage (account_name, calls_today, last_reset_date) VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE calls_today=%s, last_reset_date=%s"
            dbCursor.execute(sql, (account_name, 0, today, 0, today))
        else:
            sql = "UPDATE api_usage SET calls_today = %s, last_call = CURRENT_TIMESTAMP WHERE account_name = %s"
            dbCursor.execute(sql, (calls, account_name))
        dbConn.commit()
    except Exception as e:
        logger.error(f"Error en update_api_usage: {e}")
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def is_alert_sent(symbol, strategy, candle_time, id_cuenta=None):
    """Verifica si ya se envió una alerta para este símbolo, estrategia, vela y cuenta (usando trades)."""
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()
        
        if id_cuenta:
            sql = "SELECT idTrade FROM trades WHERE symbol=%s AND strategy=%s AND candleTime=%s AND idCuenta=%s AND sentAt IS NOT NULL"
            dbCursor.execute(sql, (symbol, strategy, candle_time, id_cuenta))
        else:
            sql = "SELECT idTrade FROM trades WHERE symbol=%s AND strategy=%s AND candleTime=%s AND sentAt IS NOT NULL"
            dbCursor.execute(sql, (symbol, strategy, candle_time))
        result = dbCursor.fetchone()
        return result is not None
    except Exception as e:
        logger.error(f"❌ Error en is_alert_sent: {e}")
        return False
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def is_trade_duplicate(symbol, strategy, intervalo, direction, size, id_cuenta=None):
    """Verifica si ya existe un trade con los mismos parámetros y status=OPEN."""
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)
        
        sql = """
            SELECT idTrade FROM trades 
            WHERE symbol = %s 
              AND strategy = %s 
              AND intervalo = %s 
              AND direction = %s 
              AND size = %s 
              AND status = 'OPEN'
        """
        params = (symbol, strategy, intervalo, direction, size)
        
        if id_cuenta:
            sql += " AND idCuenta = %s"
            params = (symbol, strategy, intervalo, direction, size, id_cuenta)
        
        sql += " LIMIT 1"
        
        dbCursor.execute(sql, params)
        result = dbCursor.fetchone()
        return result is not None
    except Exception as e:
        logger.error(f"❌ Error en is_trade_duplicate: {e}")
        return False
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

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
            cursor.execute("SELECT * FROM CUENTA WHERE idCuenta = %s AND Activo=1", (id,))
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

def getCuentaCapital(idCuenta: int) -> float:
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT Capital FROM CUENTA WHERE idCuenta = %s", (idCuenta,))
        result = cursor.fetchone()
        conn.close()
        if result:
            return float(result['Capital'])
        return 0.0
    except Exception as e:
        logger.error(f"Error al obtener capital de cuenta {idCuenta}: {e}")
        return 0.0

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
        
        # Validación flexible: permite coincidencia exacta o base (ej. Patron4h_TP1 -> Patron4h)
        base_name = nombreEstrategia.split('_')[0]
        return (nombreEstrategia in estrategias) or (base_name in estrategias)
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

def getStrategyConfig(nombreEstrategia: str):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM strategyConfig WHERE strategy = %s AND enabled = TRUE", (nombreEstrategia,))
        
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
        cursor = conn.cursor(dictionary=True)

        symbol = data.get('symbol')
        strategy = data.get('strategy', '')
        intervalo = data.get('intervalo', '15min')
        direction = data.get('direction')
        size = data.get('size')
        idCuenta = data.get('idCuenta')

        cursor.execute("""
            SELECT idTrade FROM trades 
            WHERE idCuenta = %s 
              AND symbol = %s 
              AND strategy = %s 
              AND intervalo = %s 
              AND direction = %s 
              AND size = %s 
              AND status = 'OPEN'
            LIMIT 1
        """, (idCuenta, symbol, strategy, intervalo, direction, size))
        existing = cursor.fetchone()

        if existing:
            conn.close()
            logger.warning(f"⚠️ Trade duplicado omitido: {symbol} | {strategy} | {intervalo} | {direction} | size={size}")
            return None

        cursor = conn.cursor()
        margin_used = float(data.get('margin_used', 0))
        candle_time = data.get('candleTime')
        
        sqlInsert = """
            INSERT INTO trades (idCuenta, symbol, direction, openTime, size, entryPrice, stopLoss, takeProfit, intervalo, strategy, margin_used, candleTime, sentAt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        valores = (
            data['idCuenta'], data['symbol'], data['direction'], 
            data['openTime'], data['size'], data['entryPrice'], 
            data.get('stopLoss'), data.get('takeProfit'),
            data.get('intervalo', '15min'),
            data.get('strategy', ''),
            margin_used,
            candle_time
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

def getOpenTradesForActiveAccounts():
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT t.* FROM trades t
            JOIN CUENTA c ON t.idCuenta = c.idCuenta
            WHERE t.status = 'OPEN' AND c.Activo = 1
        """)
        trades = cursor.fetchall()
        conn.close()
        return trades
    except Exception as e:
        logger.error(f"Error en getOpenTradesForActiveAccounts: {e}")
        return []

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

def getOpenTradesBySymbol(symbol: str) -> list:
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM trades WHERE symbol = %s AND status = 'OPEN'",
            (symbol,)
        )
        trades = cursor.fetchall()
        conn.close()
        return trades if trades else []
    except Exception as e:
        logger.error(f"❌ Error en getOpenTradesBySymbol: {e}")
        return []

def closeTrade(idTrade: int, exitPrice: float, pnl: float, reason: str, capital_anterior: float = None, pnl_anterior: float = None):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        
        cursor.execute("SELECT idCuenta, margin_used, symbol, closeTime FROM trades WHERE idTrade = %s", (idTrade,))
        trade = cursor.fetchone()
        if not trade:
            logger.warning(f"Trade {idTrade} no encontrado")
            return False
        
        if trade['closeTime'] is not None:
            logger.warning(f"Trade {idTrade} ya está cerrado. Omitiendo.")
            return False
        
        idCuenta = trade['idCuenta']
        margin_used = float(trade['margin_used']) if trade['margin_used'] else 0
        closeTime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        nuevo_pnl = pnl_anterior + pnl if pnl_anterior is not None else pnl
        
        cursor.execute("""
            UPDATE trades 
            SET closeTime = %s, exitPrice = %s, pnl = %s, status = 'CLOSED' 
            WHERE idTrade = %s
        """, (closeTime, exitPrice, nuevo_pnl, idTrade))
        
        if capital_anterior is not None:
            capital_nuevo = capital_anterior + pnl + margin_used
            cursor.execute("UPDATE Cuenta SET Capital = %s WHERE idCuenta = %s", (capital_nuevo, idCuenta))
        else:
            capital_change = pnl + margin_used
            cursor.execute("UPDATE Cuenta SET Capital = Capital + %s WHERE idCuenta = %s", (capital_change, idCuenta))
        
        conn.commit()
        conn.close()
        color_tag = "✅" if pnl >= 0 else "❌"
        logger.info(f"{color_tag} Trade {idTrade} Symbol: {trade['symbol']} | Cerrado: {reason} | PnL: {nuevo_pnl:.2f} | Margen devuelto: {margin_used:.2f} | Capital actualizado: {capital_nuevo if capital_anterior is not None else capital_change:.2f}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al cerrar trade {idTrade}: {e}")
        if 'conn' in locals() and conn is not None: 
            try: conn.rollback() 
            except: pass
        return False

def getTradesClosedToday(idCuenta: int, fecha: str):
    """
    Retorna la lista de trades cerratizados hoy para una cuenta.
    fecha: Formato 'YYYY-MM-DD'
    """
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT pnl FROM trades WHERE idCuenta = %s AND DATE(closeTime) = %s AND closeTime IS NOT NULL"
        cursor.execute(sql, (idCuenta, fecha))
        results = cursor.fetchall()
        conn.close()
        return results
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
            
            # Localizar a la zona horaria del sistema
            from middleware.config.constants import TIMEZONE
            import pytz
            df['timestamp'] = df['timestamp'].dt.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward')
            
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
        return 1
    return 3


