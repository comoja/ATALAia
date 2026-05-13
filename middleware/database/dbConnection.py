import mysql.connector
from mysql.connector import pooling, Error as MySQLError
from middleware.config.constants import dbConfig
import logging
import time
import threading

logger = logging.getLogger(__name__)

class DBConnectionPool:
    _instance = None
    _pool = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._init_pool()
        return cls._instance

    def _init_pool(self):
        try:
            # Inyectar timeouts de seguridad
            pool_config = dbConfig.copy()
            if 'connect_timeout' not in pool_config:
                pool_config['connect_timeout'] = 10
            
            self._pool = pooling.MySQLConnectionPool(
                pool_name="atalaia_pool",
                pool_size=10,
                pool_reset_session=True,
                **pool_config
            )
            logger.info("Pool de conexiones MySQL inicializado (size=10)")
        except MySQLError as e:
            logger.error(f"Error al crear pool de conexiones: {e}")
            self._pool = None

    def get_connection(self, retries=3, wait=0.5):
        if self._pool is None:
            self._recreate_pool()
        
        for attempt in range(retries):
            try:
                conn = self._pool.get_connection()
                if conn and not conn.is_connected():
                    conn.reconnect()
                return conn
            except MySQLError as e:
                if e.errno == 1040 and attempt < retries - 1:
                    logger.warning(f"Pool exhausted, esperando {wait}s (intento {attempt+1}/{retries})")
                    time.sleep(wait)
                    continue
                logger.warning(f"Error al obtener conexión del pool: {e}")
                self._recreate_pool()
                if attempt < retries - 1:
                    continue
                return self._direct_connect()
        
        return self._direct_connect()

    def _recreate_pool(self):
        try:
            self._pool = None
            self._init_pool()
        except Exception as e:
            logger.error(f"Error al recrear pool: {e}")

    def _direct_connect(self):
        try:
            return mysql.connector.connect(**dbConfig)
        except MySQLError as e:
            logger.error(f"Error al conectar directamente: {e}")
            return None

    def health_check(self):
        try:
            conn = self.get_connection()
            if conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                cursor.fetchone()
                cursor.close()
                conn.close()
                return True
            return False
        except Exception as e:
            logger.warning(f"Health check falló: {e}")
            return False

_pool_instance = DBConnectionPool()

def getConnection():
    try:
        conn = _pool_instance.get_connection()
        if conn is None:
            return None
        
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
        except MySQLError as e:
            if e.errno in (2006, 2013, 1040, 1042):
                logger.warning(f"Conexión MySQL perdida, intentando reconectar: {e}")
                try: conn.close()
                except: pass
                conn = _pool_instance.get_connection()
                if conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
                    cursor.close()
            else:
                raise
        
        return conn
    except MySQLError as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        return None
