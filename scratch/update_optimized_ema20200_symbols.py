import logging
import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updateEMA20200Symbols")

def updateEMA20200Symbols():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    try:
        # 1. Habilitar la estrategia EMA20200 a nivel global
        logger.info("Habilitando EMA20200 en strategyConfig...")
        cursor.execute("""
            UPDATE strategyConfig 
            SET enabled = 1, updated_at = NOW() 
            WHERE strategy = 'EMA20200'
        """)
        
        # 2. Agregar todos los símbolos a symbolNotStrategia ya que ninguno fue viable
        symbolsToExclude = [
            'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
            'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
            'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
        ]

        logger.info("Registrando exclusiones en symbolNotStrategia para EMA20200...")
        excludeSql = """
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, 'EMA20200', 'Filtro Aut.: Sin combinaciones viables o trades suficientes (>3) en optimización Grid Search.')
        """
        for symbol in symbolsToExclude:
            cursor.execute(excludeSql, (symbol,))
            logger.info(f"   🚫 Excluido {symbol} de EMA20200.")

        conn.commit()
        logger.info("¡Base de datos actualizada con éxito para EMA20200!")

    except Exception as e:
        logger.error(f"Error al actualizar la base de datos: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    updateEMA20200Symbols()
