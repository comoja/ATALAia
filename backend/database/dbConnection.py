import mysql.connector
from middleware.config.constants import dbConfig
import logging
logger = logging.getLogger(__name__) 

def getConnection():
    try:
        return mysql.connector.connect(**dbConfig)
    except mysql.connector.Error as e:
        logger.error(f"Error al conectar a la base de datos: {e}")
        return None 