import os
import sys
import mysql.connector
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

def _call_connection_pool(method: str, path: str, json_data: dict = None, params: dict = None, timeout: float = 3.0):
    """
    Realiza peticiones HTTP al microservicio ConnectionPool (http://127.0.0.1:8000/api/v1).
    Retorna respuesta JSON o None si falla/no responde.
    """
    try:
        url = f"{CONNECTION_POOL_URL.rstrip('/')}/{path.lstrip('/')}"
        m = method.upper()
        if m == "GET":
            res = requests.get(url, params=params, timeout=timeout)
        elif m == "POST":
            res = requests.post(url, json=json_data, params=params, timeout=timeout)
        elif m == "PUT":
            res = requests.put(url, json=json_data, params=params, timeout=timeout)
        elif m == "DELETE":
            res = requests.delete(url, params=params, timeout=timeout)
        else:
            return None
        if res.status_code in (200, 201):
            return res.json()
    except Exception as e:
        logger.debug(f"ConnectionPool helper call to {path} failed: {e}")
    return None

def ttl_cache(seconds=30):
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
            val = func(*args, **kwargs)
            cache[key] = (val, now + seconds)
            return val
        wrapper.cache_clear = lambda: cache.clear()
        return wrapper
    return decorator

def init_alerts_table():
    """Crea la tabla de registro de alertas y asegura que strategyConfig tenga las columnas necesarias."""
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()
      
        
        # Tabla de Configuración de Estrategias (Asegurar columnas)
        strategies = [
            ('CruceEMA', 1.5, 70), ('Sniper', 2.0, 80), 
            ('ImbalanceNY', 1.5, 75), ('ImbalanceLDN', 1.5, 75), ('Patron4h', 1.5, 70),
            ('SesgoBiasHTF', 1.5, 70), ('SilverBullet', 1.5, 75), ('GenericFVG', 0.5, 60),
            ('FVGDiario', 2.0, 70), ('SpeedBot', 1.5, 70), ('ImbalancePMNY', 1.5, 75),
            ('BreakoutNY', 1.0, 75), ('ReversionMedia', 2.5, 70), ('QTrend', 1.5, 70),
            ('BreakoutProbability', 1.5, 60)
        ]

        dbCursor.execute("SHOW TABLES LIKE 'strategyConfig'")
        if not dbCursor.fetchone():
            sql_config = """
                CREATE TABLE strategyConfig (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    strategy VARCHAR(50) NOT NULL UNIQUE,
                    enabled BOOLEAN DEFAULT TRUE,
                    max_minutos_fvg INT DEFAULT 40,
                    max_minutos_signal INT DEFAULT 40,
                    min_rr DOUBLE DEFAULT 1.5,
                    max_rr DOUBLE DEFAULT 1.5,
                    min_confidence INT DEFAULT 70,
                    start_hour INT DEFAULT 16,
                    start_minute INT DEFAULT 30,
                    proba_threshold_long DOUBLE DEFAULT 0.55,
                    proba_threshold_short DOUBLE DEFAULT 0.45,
                    jpy_threshold_adjust_pct DOUBLE DEFAULT 20.0,
                    jpy_min_confidence_adjust_pct DOUBLE DEFAULT 10.0,
                    jpy_extra_confirmations INT DEFAULT 1,
                    max_drawdown_percent DOUBLE DEFAULT 5.0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
            """
            dbCursor.execute(sql_config)
        else:
            try:
                dbCursor.execute("ALTER TABLE strategyConfig MODIFY COLUMN strategy VARCHAR(50) NOT NULL")
            except:
                pass

            # Asegurar que existan las columnas nuevas (alter table if not exists pattern)
            cols = {
                "enabled": "BOOLEAN DEFAULT TRUE",
                "min_rr": "DOUBLE DEFAULT 1.5",
                "max_rr": "DOUBLE DEFAULT 1.5",
                "min_confidence": "INT DEFAULT 70",
                "start_hour": "INT DEFAULT 16",
                "start_minute": "INT DEFAULT 30",
                "proba_threshold_long": "DOUBLE DEFAULT 0.55",
                "proba_threshold_short": "DOUBLE DEFAULT 0.45",
                "jpy_threshold_adjust_pct": "DOUBLE DEFAULT 20.0",
                "jpy_min_confidence_adjust_pct": "DOUBLE DEFAULT 10.0",
                "jpy_extra_confirmations": "INT DEFAULT 1",
                "max_drawdown_percent": "DOUBLE DEFAULT 5.0"
            }
            for col, definition in cols.items():
                try:
                    dbCursor.execute(f"ALTER TABLE strategyConfig ADD COLUMN {col} {definition}")
                except:
                    pass # Ya existe

        for name, rr, conf in strategies:
            dbCursor.execute(
                "INSERT IGNORE INTO strategyConfig (strategy, min_rr, min_confidence) VALUES (%s, %s, %s)",
                (name, rr, conf)
            )

        dbCursor.execute("""
            CREATE TABLE IF NOT EXISTS CuentaEstrategia (
                idCuenta INT NOT NULL,
                strategy VARCHAR(50) NOT NULL,
                PRIMARY KEY (idCuenta, strategy),
                FOREIGN KEY (idCuenta) REFERENCES Cuenta(idCuenta) ON DELETE CASCADE,
                FOREIGN KEY (strategy) REFERENCES strategyConfig(strategy) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)

        # Asegurar que todas las cuentas activas tengan vinculadas BreakoutNY y QTrend
        dbCursor.execute("SELECT idCuenta FROM Cuenta WHERE Activo = 1")
        cuentasActivas = dbCursor.fetchall()
        for c in cuentasActivas:
            idCta = c[0]
            for est in ['BreakoutNY', 'QTrend', 'BreakoutProbability']:
                dbCursor.execute(
                    "INSERT IGNORE INTO CuentaEstrategia (idCuenta, strategy) VALUES (%s, %s)",
                    (idCta, est)
                )

        symbol_cols = {
            "sniper_threshold_adjust_pct": "DOUBLE NULL",
            "sniper_min_confidence_adjust_pct": "DOUBLE NULL",
            "sniper_extra_confirmations": "INT NULL",
            "sniper_max_rr": "DOUBLE NULL",
            "priceOffset": "DOUBLE DEFAULT 0.0"
        }
        for col, definition in symbol_cols.items():
            try:
                dbCursor.execute(f"ALTER TABLE SentinelSymbol ADD COLUMN {col} {definition}")
            except:
                pass
        
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
        
        # Crear tabla broker si no existe
        dbCursor.execute("""
            CREATE TABLE IF NOT EXISTS broker (
                idBroker INT AUTO_INCREMENT PRIMARY KEY,
                nombre VARCHAR(100) NOT NULL UNIQUE,
                activo TINYINT(1) NOT NULL DEFAULT 1,
                createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """)

        # Crear tabla BrokerCuenta si no existe
        dbCursor.execute("""
            CREATE TABLE IF NOT EXISTS BrokerCuenta (
                idBrokerCuenta INT AUTO_INCREMENT PRIMARY KEY,
                idCuenta INT NOT NULL,
                idBroker INT NOT NULL,
                tipoConexion ENUM('PRIMARIA', 'ESPEJO', 'PUENTE') NOT NULL DEFAULT 'PRIMARIA',
                loginUsuario VARCHAR(150) DEFAULT NULL,
                tokenAcceso VARCHAR(255) DEFAULT NULL,
                activo TINYINT(1) NOT NULL DEFAULT 1,
                createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (idCuenta) REFERENCES Cuenta(idCuenta) ON DELETE CASCADE,
                FOREIGN KEY (idBroker) REFERENCES broker(idBroker) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
        """)

        # Sembrar brokers iniciales
        brokersSeed = [
            ("Oanda", 1),
            ("Forex.com", 1),
            ("MetaTrader 5", 1)
        ]
        dbCursor.executemany("""
            INSERT INTO broker (nombre, activo) 
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE activo = VALUES(activo);
        """, brokersSeed)
        
        # Crear tabla symbolStrategyConfig si no existe
        dbCursor.execute("""
            CREATE TABLE IF NOT EXISTS symbolStrategyConfig (
                strategy VARCHAR(50) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                enabled BOOLEAN DEFAULT TRUE,
                broker TINYINT(1) NOT NULL DEFAULT 0,
                parametersJson JSON DEFAULT NULL,
                jsonIMACD JSON DEFAULT NULL,
                updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (strategy, symbol),
                FOREIGN KEY (strategy) REFERENCES strategyConfig(strategy) ON DELETE CASCADE,
                FOREIGN KEY (symbol) REFERENCES SentinelSymbol(symbol) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        
        try:
            dbCursor.execute("ALTER TABLE symbolStrategyConfig ADD COLUMN jsonIMACD JSON DEFAULT NULL")
        except:
            pass
            
        try:
            dbCursor.execute("ALTER TABLE symbolStrategyConfig ADD COLUMN broker TINYINT(1) NOT NULL DEFAULT 0")
        except:
            pass
            
        dbConn.commit()
    except Exception as e:
        logger.error(f"❌ Error al inicializar tablas: {e}")
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def get_api_usage(account_name):
    """Obtiene el consumo actual de una cuenta desde ConnectionPool (o fallback DB)."""
    try:
        res = _call_connection_pool("GET", f"/api-usage/{account_name}")
        if res and "calls_today" in res:
            last_reset = res.get("last_reset_date")
            today_str = datetime.now().strftime("%Y-%m-%d")
            if last_reset and str(last_reset) != today_str:
                update_api_usage(account_name, 0, reset=True)
                return 0
            return res.get("calls_today", 0)

        dbConn = dbConnection.getConnection()
        if not dbConn:
            return 0
        dbCursor = dbConn.cursor(dictionary=True)
        sql = "SELECT calls_today, last_reset_date FROM api_usage WHERE account_name = %s"
        dbCursor.execute(sql, (account_name,))
        result = dbCursor.fetchone()
        
        today = datetime.now().date()
        if result:
            if result['last_reset_date'] != today:
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
    """Actualiza o resetea el contador de llamadas en ConnectionPool (o fallback DB)."""
    try:
        if not reset and calls > 0:
            res = _call_connection_pool("POST", f"/api-usage/{account_name}/increment")
            if res and "calls_today" in res:
                return

        dbConn = dbConnection.getConnection()
        if not dbConn:
            return
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

def is_trade_duplicate(symbol, strategy, intervalo, direction, size=None, id_cuenta=None):
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
              AND status = 'OPEN'
        """
        params = [symbol, strategy, intervalo, direction]
        
        if id_cuenta:
            sql += " AND idCuenta = %s"
            params.append(id_cuenta)
        
        sql += " LIMIT 1"
        
        dbCursor.execute(sql, tuple(params))
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
    dbConn = None
    dbCursor = None
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()

        sqlClose = """
            UPDATE trades 
            SET exitPrice = %s, 
                closeTime = %s,
                status = 'CLOSED'
            WHERE idTrade = %s
        """
        # Se remueve comentario de los parámetros para evitar error de mismatch
        dbCursor.execute(sqlClose, (precioCierre, fechaCierre, idTrade))
        dbConn.commit()
        
    except Exception as error:
        logger.error(f"❌ Error al cerrar trade en DB: {error}")
    finally:
        if dbCursor:
            dbCursor.close()
        if dbConn:
            dbConn.close()


def verificaCierreTrade(tradeData, dfVelas):
    conn = None
    cursor = None
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
        return False
    except Exception as error:
        logger.error(f"❌ Error en verificaCierreTrade: {error}")
        return False
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def logTrade(symbol, regime, pf, sharpe):
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO trades (symbol, regime, pf, sharpe)
            VALUES (%s, %s, %s, %s)
        """, (symbol, regime, pf, sharpe))

        conn.commit()
    except Exception as error:
        logger.error(f"❌ Error en logTrade: {error}")
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

@ttl_cache(30)
def getAccount(id=None):
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        if id:
            cursor.execute("SELECT * FROM CUENTA WHERE idCuenta = %s AND Activo=1", (id,))
        else:
            cursor.execute("SELECT * FROM CUENTA WHERE Activo=1")
        
        cuentas = cursor.fetchall()
        if cuentas:
            return cuentas
        return []
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return []
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

def getCuentaCapital(idCuenta: int) -> float:
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT Capital FROM CUENTA WHERE idCuenta = %s", (idCuenta,))
        result = cursor.fetchone()
        if result:
            return float(result['Capital'])
        return 0.0
    except Exception as e:
        logger.error(f"Error al obtener capital de cuenta {idCuenta}: {e}")
        return 0.0
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def isEstrategiaHabilitadaParaCuenta(idCuenta: int, nombreEstrategia: str) -> bool:
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        
        # Si la cuenta no tiene configurada ninguna estrategia, por defecto permitimos operar (lógica defensiva)
        cursor.execute("SELECT COUNT(*) as total FROM CuentaEstrategia WHERE idCuenta = %s", (idCuenta,))
        cnt = cursor.fetchone()
        if cnt['total'] == 0:
            return True
            
        baseName = nombreEstrategia.split('_')[0]
        cursor.execute("""
            SELECT idCuenta FROM CuentaEstrategia 
            WHERE idCuenta = %s AND (strategy = %s OR strategy = %s)
            LIMIT 1
        """, (idCuenta, nombreEstrategia, baseName))
        
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        logger.error(f"Error en isEstrategiaHabilitadaParaCuenta: {e}", exc_info=True)
        return True
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def isTipoHabilitadoParaCuenta(idCuenta: int, tipoSymbol: str) -> bool:
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT tipo FROM cuentaOpera WHERE idCuenta = %s", (idCuenta,))
        
        results = cursor.fetchall()
        if not results:
            # Si no hay registros en cuentaOpera para esta cuenta, no hay restricción por tipo.
            return True
        
        tiposHabilitados = [r['tipo'].upper() for r in results]
        return tipoSymbol.upper() in tiposHabilitados
    except Exception as e:
        logger.error(f"Error en isTipoHabilitadoParaCuenta: {e}", exc_info=True)
        return True
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def getSymbols():
    """Obtiene todos los símbolos activos para Sentinel desde la tabla máster `symbols`."""
    return getSentinelSymbols()

@ttl_cache(30)
def getSentinelSymbols():
    """Obtiene todos los símbolos activos para Sentinel desde ConnectionPool (o fallback MySQL)."""
    res = _call_connection_pool("GET", "/sentinel-symbols/active")
    if res is not None and isinstance(res, list):
        return res

    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT *, activoSentinel AS Activo FROM symbols WHERE activoSentinel = 1")
        symbols = cursor.fetchall()
        return symbols if symbols else []
    except Exception as e:
        logger.error(f"Error en getSentinelSymbols: {e}", exc_info=True)
        return []
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def getDataSymbols():
    """
    Obtiene los símbolos activos para dataSymbol (activoRatio = 1 OR activoSentinel = 1) desde ConnectionPool (o fallback MySQL).
    """
    ratio_syms = _call_connection_pool("GET", "/ratio-symbols/active")
    sentinel_syms = _call_connection_pool("GET", "/sentinel-symbols/active")
    
    if ratio_syms is not None or sentinel_syms is not None:
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

    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM symbols WHERE activoRatio = 1 OR activoSentinel = 1 ORDER BY symbol")
        symbols = cursor.fetchall()
        return symbols if symbols else []
    except Exception as e:
        logger.error(f"Error en getDataSymbols: {e}", exc_info=True)
        return []
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def getRatioSymbols():
    """Obtiene todos los símbolos activos para el módulo de RATIO (ATALAia) desde ConnectionPool (o fallback MySQL)."""
    res = _call_connection_pool("GET", "/ratio-symbols/active")
    if res is not None and isinstance(res, list):
        return res

    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT *, activoRatio AS Activo FROM symbols WHERE activoRatio = 1 ORDER BY symbol")
        symbols = cursor.fetchall()
        return symbols if symbols else []
    except Exception as e:
        logger.error(f"Error en getRatioSymbols: {e}", exc_info=True)
        return []
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def getRatioSymbol(symbol: str):
    """Obtiene la información de un símbolo desde `symbols`."""
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT *, activoRatio AS Activo FROM symbols WHERE symbol = %s", (symbol,))
        return cursor.fetchone()
    except Exception as e:
        logger.error(f"Error en getRatioSymbol: {e}", exc_info=True)
        return None
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def getSymbol(symbol: str):
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT *, activoSentinel AS Activo FROM symbols WHERE symbol = %s OR MT5 = %s OR FOREX = %s OR TradingView = %s", (symbol, symbol, symbol, symbol))
        
        result = cursor.fetchone()
        return result
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return None
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

def getSymbolStartDate(symbol: str):
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT startDate FROM SentinelSymbol WHERE symbol = %s", (symbol,))
        result = cursor.fetchone()
        return result['startDate'] if result else None
    except Exception as e:
        logger.error(f"Error en getSymbolStartDate: {e}", exc_info=True)
        return None
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def getSymbolTypeConfig(tipo: str):
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM SymbolTypeConfig WHERE tipo = %s", (tipo,))
        
        result = cursor.fetchone()
        return result
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return None
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def getStrategyConfig(nombreEstrategia: str):
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM strategyConfig WHERE strategy = %s AND enabled = TRUE", (nombreEstrategia,))
        
        result = cursor.fetchone()
        return result
    except Exception as e:
        logger.error(f"Error en getStrategyConfig: {e}", exc_info=True)
        return None
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

@ttl_cache(30)
def getSymbolStrategyConfig(strategyName: str, symbol: str) -> dict:
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        # 1. Intentar obtener la configuración específica del símbolo y combinarla con la configuración global de la estrategia
        cursor.execute("""
            SELECT ssc.parametersJson, ssc.jsonIMACD, ssc.broker, sc.min_confidence, sc.min_rr
            FROM symbolStrategyConfig ssc
            JOIN strategyConfig sc ON ssc.strategy = sc.strategy
            WHERE ssc.strategy = %s AND ssc.symbol = %s AND ssc.enabled = TRUE
        """, (strategyName, symbol))
        result = cursor.fetchone()
        
        # 2. Si existe el registro, decodificar JSON y retornar
        if result:
            import json
            params = {}
            if result.get('parametersJson'):
                rawParams = result['parametersJson']
                params = json.loads(rawParams) if isinstance(rawParams, str) else rawParams
            
            # Decodificar jsonIMACD
            imacd_params = {}
            if result.get('jsonIMACD'):
                raw_imacd = result['jsonIMACD']
                imacd_params = json.loads(raw_imacd) if isinstance(raw_imacd, str) else raw_imacd
            
            # Integrar los parámetros de IMACD
            params['useImpulseMacdFilter'] = imacd_params.get('useImpulseMacdFilter', 0)
            params['macdFast'] = imacd_params.get('macdFast', 12)
            params['macdSlow'] = imacd_params.get('macdSlow', 26)
            params['macdSignal'] = imacd_params.get('macdSignal', 9)
            
            # Unificar min_confidence y min_rr con los de la tabla global (si no vienen en params)
            global_min_conf = result.get('min_confidence', 70)
            global_min_rr = result.get('min_rr', 1.5)
            
            # El minConfidence toma el valor global de strategyConfig.min_confidence para evitar redundancias
            params['minConfidence'] = global_min_conf
            params['min_confidence'] = global_min_conf
            
            if 'minRr' not in params:
                params['minRr'] = global_min_rr
            if 'min_rr' not in params:
                params['min_rr'] = global_min_rr
                
            # Agregar el campo broker
            params['broker'] = result.get('broker', 0)
            
            return params
            
        # 3. Fallback: Obtener la configuración global de la estrategia
        cursor.execute("SELECT * FROM strategyConfig WHERE strategy = %s AND enabled = TRUE", (strategyName,))
        globalConfig = cursor.fetchone()
        if globalConfig:
            globalConfig['useImpulseMacdFilter'] = 0 # Default global
            globalConfig['minConfidence'] = globalConfig.get('min_confidence', 70)
            globalConfig['broker'] = 0 # Default para broker
            return globalConfig
        return {}
    except Exception as e:
        logger.error(f"Error en getSymbolStrategyConfig para {strategyName} - {symbol}: {e}", exc_info=True)
        return {}
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

def buscaTrade(tradeData):
    dbConn = None
    dbCursor = None
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
            return False
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
                    return False
            
            insertarTrade(tradeData)
            logger.info(f"🆕 Nuevo trade insertado para {tradeData['symbol']}")
            return True

    except Exception as error:
        logger.error(f"❌ Error en buscaTrade: {error}")
        raise error
    finally:
        if dbCursor:
            dbCursor.close()
        if dbConn:
            dbConn.close()

def actualizarTrade(idTrade, data):
    conn = None
    cursor = None
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
        if conn: conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def insertarTrade(data):
    conn = None
    cursor = None
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
            logger.warning(f"⚠️ Trade duplicado omitido: {symbol} | {strategy} | {intervalo} | {direction} | size={size}")
            return False

        cursor = conn.cursor()
        marginUsedVal = float(data.get('margin_used', 0))
        candleTimeVal = data.get('candleTime')
        ticketIdVal = data.get('ticketId')
        
        sqlInsert = """
            INSERT INTO trades (idCuenta, symbol, direction, openTime, size, entryPrice, stopLoss, takeProfit, intervalo, strategy, setup, margin_used, candleTime, ticketId, sentAt)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        """
        valores = (
            data['idCuenta'], data['symbol'], data['direction'], 
            data['openTime'], data['size'], data['entryPrice'], 
            data.get('stopLoss'), data.get('takeProfit'),
            data.get('intervalo', '15min'),
            data.get('strategy', ''),
            data.get('setup'),  # Requerido para detección de señales de ajuste
            marginUsedVal,
            candleTimeVal,
            ticketIdVal
        )

        cursor.execute(sqlInsert, valores)
        
        if marginUsedVal > 0:
            cursor.execute("UPDATE Cuenta SET Capital = Capital - %s WHERE idCuenta = %s", 
                         (marginUsedVal, data['idCuenta']))
        
        conn.commit()
        logger.info(f"🚀 Nuevo trade insertado: {data['symbol']} | Margen reservado: {marginUsedVal}")
        return True

    except Exception as e:
        logger.error(f"❌ Error al insertarTrade: {e}")
        if conn: conn.rollback()
        raise e
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def getOpenTradesForActiveAccounts():
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT t.* FROM trades t
            JOIN CUENTA c ON t.idCuenta = c.idCuenta
            WHERE t.status = 'OPEN' AND c.Activo = 1
        """)
        trades = cursor.fetchall()
        return trades
    except Exception as e:
        logger.error(f"Error en getOpenTradesForActiveAccounts: {e}")
        return []
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def getOpenTrades():
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM trades WHERE closeTime IS NULL")
        trades = cursor.fetchall()
        return trades
    except Exception as e:
        logger.error(f"❌ Error en getOpenTrades: {e}")
        return []
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def getOpenTradesBySymbol(symbol: str) -> list:
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(
            "SELECT * FROM trades WHERE symbol = %s AND status = 'OPEN'",
            (symbol,)
        )
        trades = cursor.fetchall()
        return trades if trades else []
    except Exception as e:
        logger.error(f"❌ Error en getOpenTradesBySymbol: {e}")
        return []
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def updateTradeLevels(id_trade: int, stop_loss: float, take_profit: float):
    """Actualiza los niveles de SL y TP de un trade abierto."""
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()
        sql = "UPDATE trades SET stopLoss = %s, takeProfit = %s WHERE idTrade = %s"
        cursor.execute(sql, (stop_loss, take_profit, id_trade))
        conn.commit()
        logger.info(f"✅ Trade {id_trade} actualizado: SL={stop_loss}, TP={take_profit}")
        return True
    except Exception as e:
        logger.error(f"❌ Error al actualizar niveles del trade {id_trade}: {e}")
        return False
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def closeTrade(idTrade: int, exitPrice: float, pnl: float, reason: str, capital_anterior: float = None, pnl_anterior: float = None):
    conn = None
    cursor = None
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
        
        capital_nuevo = 0.0
        capital_change = 0.0
        if capital_anterior is not None:
            capital_nuevo = capital_anterior + pnl + margin_used
            cursor.execute("UPDATE Cuenta SET Capital = %s WHERE idCuenta = %s", (capital_nuevo, idCuenta))
        else:
            capital_change = pnl + margin_used
            cursor.execute("UPDATE Cuenta SET Capital = Capital + %s WHERE idCuenta = %s", (capital_change, idCuenta))
        
        conn.commit()
        color_tag = "✅" if pnl >= 0 else "❌"
        logger.info(f"{color_tag} Trade {idTrade} Symbol: {trade['symbol']} | Cerrado: {reason} | PnL: {nuevo_pnl:.2f} | Margen devuelto: {margin_used:.2f} | Capital actualizado: {capital_nuevo if capital_anterior is not None else capital_change:.2f}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error al cerrar trade {idTrade}: {e}")
        if conn: 
            try: conn.rollback() 
            except: pass
        return False
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def getTradesClosedToday(idCuenta: int, fecha: str):
    """
    Retorna la lista de trades cerratizados hoy para una cuenta.
    fecha: Formato 'YYYY-MM-DD'
    """
    conn = None
    cursor = None
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        sql = "SELECT pnl FROM trades WHERE idCuenta = %s AND DATE(closeTime) = %s AND closeTime IS NOT NULL"
        cursor.execute(sql, (idCuenta, fecha))
        results = cursor.fetchall()
        return results
    except Exception as e:
        logger.error(f"Error en getTradesClosedToday: {e}")
        return []
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

def getAccountById(idCuenta: int):
    """
    Alias para getAccount(id) que retorna un solo objeto.
    """
    cuentas = getAccount(idCuenta)
    return cuentas[0] if cuentas else None


async def getLastCandleDatetime(symbol: str, timeframe: str):
    def query():
        conn = None
        cursor = None
        try:
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT MAX(timestamp) FROM candles WHERE symbol=%s AND timeframe=%s",
                (symbol, timeframe)
            )
            result = cursor.fetchone()
            return result[0] if result[0] else None
        except Exception as e:
            logger.error(f"Error en getLastCandleDatetime: {e}", exc_info=True)
            return None
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

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
        conn = None
        cursor = None
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
            return inserted
        except Exception as e:
            logger.error(f"Error en insertNewCandlesToDb: {e}", exc_info=True)
            return 0
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    inserted_count = await asyncio.to_thread(insert)
    return inserted_count


async def getCandlesFromDb(symbol: str, timeframe: str = "5min", limit: int = 500) -> pd.DataFrame:
    """
    Obtiene velas de la tabla 'candles' como DataFrame usando ConnectionPool (o fallback MySQL).
    """
    def query():
        from urllib.parse import quote
        safe_sym = quote(symbol, safe='')
        safe_tf = quote(timeframe, safe='')
        cp_res = _call_connection_pool("GET", f"/candles/symbol/{safe_sym}/timeframe/{safe_tf}", params={"limit": limit})
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

        conn = None
        cursor = None
        try:
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT timestamp, open, high, low, close, volume FROM candles "
                "WHERE symbol=%s AND timeframe=%s ORDER BY timestamp DESC LIMIT %s",
                (symbol, timeframe, limit)
            )
            rows = cursor.fetchall()
            
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
        finally:
            if cursor:
                try: cursor.close()
                except: pass
            if conn:
                try: conn.close()
                except: pass

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
    """Retorna la lista de todos los brokers registrados."""
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)
        dbCursor.execute("SELECT idBroker, nombre, activo FROM broker ORDER BY nombre")
        return dbCursor.fetchall()
    except Exception as e:
        logger.error(f"Error en getBrokers: {e}")
        return []
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def getBrokerCuentas(idCuenta: int = None) -> list:
    """
    Retorna la lista de mapeos de broker por cuenta.
    Opcionalmente filtra por idCuenta.
    """
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)
        
        sql = """
            SELECT bc.idBrokerCuenta, bc.idCuenta, bc.idBroker, b.nombre AS nombreBroker, 
                   bc.tipoConexion, bc.loginUsuario, bc.tokenAcceso, bc.activo, bc.createdAt,
                   bc.Apikey, bc.Servidor
            FROM BrokerCuenta bc
            JOIN broker b ON bc.idBroker = b.idBroker
        """
        if idCuenta is not None:
            sql += " WHERE bc.idCuenta = %s"
            dbCursor.execute(sql, (idCuenta,))
        else:
            dbCursor.execute(sql)
            
        return dbCursor.fetchall()
    except Exception as e:
        logger.error(f"Error en getBrokerCuentas: {e}")
        return []
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def addBrokerCuenta(idCuenta: int, idBroker: int, tipoConexion: str, loginUsuario: str = None, tokenAcceso: str = None, activo: int = 1) -> bool:
    """
    Agrega o actualiza una relación de broker por cuenta para soporte de Mirroring.
    """
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()
        
        sql = """
            INSERT INTO BrokerCuenta (idCuenta, idBroker, tipoConexion, loginUsuario, tokenAcceso, activo)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                tipoConexion = VALUES(tipoConexion),
                loginUsuario = VALUES(loginUsuario),
                tokenAcceso = VALUES(tokenAcceso),
                activo = VALUES(activo)
        """
        dbCursor.execute(sql, (idCuenta, idBroker, tipoConexion, loginUsuario, tokenAcceso, activo))
        dbConn.commit()
        return True
    except Exception as e:
        logger.error(f"Error en addBrokerCuenta: {e}")
        return False
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()


def getRiesgoSugerido(symbol: str, strategy: str) -> float:
    """
    Obtiene el riesgo sugerido para una combinación de símbolo y estrategia.
    Si no existe, retorna 1.0 por defecto.
    """
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)
        sql = "SELECT riesgoSugerido FROM EstrategiaSymbol WHERE symbol = %s AND strategy = %s LIMIT 1"
        dbCursor.execute(sql, (symbol, strategy))
        result = dbCursor.fetchone()
        if result and result.get('riesgoSugerido') is not None:
            return float(result['riesgoSugerido'])
        return 1.0
    except Exception as e:
        logger.error(f"Error en getRiesgoSugerido: {e}")
        return 1.0
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def getLastStockPriceDate(symbol: str):
    """Obtiene la fecha más reciente registrada para un símbolo en StockPrices mediante ConnectionPool (o fallback DB)."""
    try:
        from urllib.parse import quote
        safe_sym = quote(symbol, safe='')
        res = _call_connection_pool("GET", f"/stock-prices/symbol/{safe_sym}/last-date")
        if res and res.get("last_date"):
            return pd.to_datetime(res["last_date"])

        dbConn = dbConnection.getConnection()
        if not dbConn: return None
        dbCursor = dbConn.cursor()
        dbCursor.execute("SELECT MAX(priceDate) FROM StockPrices WHERE symbol = %s", (symbol,))
        res = dbCursor.fetchone()
        if res and res[0]:
            return pd.to_datetime(res[0])
        return None
    except Exception as e:
        logger.error(f"Error en getLastStockPriceDate para {symbol}: {e}")
        return None
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()

def saveStockPrices(df: pd.DataFrame, symbol: str) -> int:
    """Guarda o actualiza masivamente precios diarios en StockPrices mediante ConnectionPool (o fallback DB)."""
    if df is None or df.empty: return 0
    try:
        payload = []
        for idx, row in df.iterrows():
            dt = row['datetime'].date() if isinstance(row['datetime'], pd.Timestamp) else row['datetime']
            price = float(row['close'])
            payload.append({"symbol": symbol, "priceDate": str(dt), "closePrice": price})

        res = _call_connection_pool("POST", "/stock-prices/bulk", json_data=payload)
        if res and "inserted" in res:
            return res["inserted"]

        dbConn = dbConnection.getConnection()
        if not dbConn: return 0
        dbCursor = dbConn.cursor()
        sql = """
            INSERT INTO StockPrices (symbol, priceDate, closePrice)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                closePrice = VALUES(closePrice)
        """
        data_to_insert = [(item['symbol'], item['priceDate'], item['closePrice']) for item in payload]
        
        dbCursor.executemany(sql, data_to_insert)
        dbConn.commit()
        return dbCursor.rowcount
    except Exception as e:
        logger.error(f"Error guardando StockPrices para {symbol}: {e}")
        return 0
    finally:
        if 'dbCursor' in locals(): dbCursor.close()
        if 'dbConn' in locals(): dbConn.close()


async def getStockPricesFromDb(symbol: str, limit: int = 365) -> pd.DataFrame:
    """
    Obtiene los precios de cierre diarios de la tabla 'StockPrices' como DataFrame.
    """
    def query():
        conn = None
        cursor = None
        try:
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT priceDate, closePrice FROM StockPrices "
                "WHERE symbol=%s ORDER BY priceDate DESC LIMIT %s",
                (symbol, limit)
            )
            rows = cursor.fetchall()
            
            if not rows:
                return pd.DataFrame()
            
            df = pd.DataFrame(rows, columns=['priceDate', 'closePrice'])
            df['priceDate'] = pd.to_datetime(df['priceDate'])
            df = df.sort_values('priceDate').set_index('priceDate')
            
            df['closePrice'] = pd.to_numeric(df['closePrice'], errors='coerce')
            
            return df.dropna(subset=['closePrice'])
        except Exception as e:
            logger.error(f"Error en getStockPricesFromDb: {e}", exc_info=True)
            return pd.DataFrame()
        finally:
            if cursor: cursor.close()
            if conn: conn.close()

    import asyncio
    return await asyncio.to_thread(query)
