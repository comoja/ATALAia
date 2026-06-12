import logging
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scratch")

EXCLUDED_SYMBOLS = [
    'USD/MXN', 'AUD/USD', 'USD/JPY', 'XAU/USD',
    'EUR/USD', 'GBP/CAD', 'EUR/GBP', 'GBP/JPY', 'USD/CHF'
]

def insertExclusions():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo establecer conexión con la base de datos")
        return

    cursor = conn.cursor()
    try:
        # Verificar que la tabla exista
        cursor.execute("SHOW TABLES LIKE 'symbolNotStrategia'")
        if not cursor.fetchone():
            logger.info("Creando la tabla symbolNotStrategia...")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS symbolNotStrategia (
                    symbol VARCHAR(20) NOT NULL,
                    strategy VARCHAR(50) NOT NULL,
                    PRIMARY KEY (symbol, strategy)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
            """)
            conn.commit()

        # Insertar las exclusiones de forma segura
        logger.info("Insertando exclusiones en symbolNotStrategia...")
        sqlInsert = """
            INSERT INTO symbolNotStrategia (symbol, strategy)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE strategy = strategy
        """
        values = [(symbol, 'PremiumConfluence') for symbol in EXCLUDED_SYMBOLS]
        cursor.executemany(sqlInsert, values)
        conn.commit()
        logger.info(f"Se insertaron/actualizaron correctamente las {len(EXCLUDED_SYMBOLS)} exclusiones de activos para la estrategia PremiumConfluence.")

    except Exception as e:
        logger.error(f"Error operando en la base de datos: {e}", exc_info=True)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    insertExclusions()
