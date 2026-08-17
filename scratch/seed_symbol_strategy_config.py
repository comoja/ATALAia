import logging
import json
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("seed")

def seedSymbolStrategyConfigs():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    try:
        # Asegurar la creación de la tabla
        logger.info("Verificando existencia de la tabla symbolStrategyConfig...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS symbolStrategyConfig (
                strategy VARCHAR(50) NOT NULL,
                symbol VARCHAR(20) NOT NULL,
                enabled BOOLEAN DEFAULT TRUE,
                parametersJson JSON DEFAULT NULL,
                updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (strategy, symbol),
                FOREIGN KEY (strategy) REFERENCES strategyConfig(strategy) ON DELETE CASCADE,
                FOREIGN KEY (symbol) REFERENCES SentinelSymbol(symbol) ON DELETE CASCADE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """)
        conn.commit()

        # Parámetros optimizados para el Oro (XAU/USD)
        goldParams = {
            "supertrend_period": 10,
            "supertrend_mult": 2.5,
            "ha_period1": 12,
            "ha_period2": 12,
            "macd_length": 34,
            "macd_signal": 9,
            "min_rr": 2.0,
            "lookback": 15
        }

        # Parámetros optimizados para Bitcoin (BTC/USD)
        btcParams = {
            "supertrend_period": 10,
            "supertrend_mult": 2.5,
            "ha_period1": 12,
            "ha_period2": 12,
            "macd_length": 34,
            "macd_signal": 9,
            "min_rr": 2.0,
            "lookback": 15
        }

        # Parámetros optimizados para divisas tradicionales (Forex)
        forexParams = {
            "supertrend_period": 10,
            "supertrend_mult": 1.5,
            "ha_period1": 10,
            "ha_period2": 10,
            "macd_length": 20,
            "macd_signal": 9,
            "min_rr": 1.5,
            "lookback": 8
        }

        # Vinculaciones específicas por símbolo
        seeds = [
            ('PremiumConfluence', 'XAU/USD', goldParams),
            ('PremiumConfluence', 'BTC/USD', btcParams),
            ('PremiumConfluence', 'NZD/USD', forexParams),
            ('PremiumConfluence', 'USD/CAD', forexParams),
            ('PremiumConfluence', 'GBP/USD', forexParams)
        ]

        logger.info("Sembrando registros en symbolStrategyConfig...")
        sqlInsert = """
            INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                enabled = VALUES(enabled),
                parametersJson = VALUES(parametersJson)
        """
        
        values = [
            (strategy, symbol, True, json.dumps(params))
            for strategy, symbol, params in seeds
        ]
        
        cursor.executemany(sqlInsert, values)
        conn.commit()
        logger.info(f"Sembrado completado con éxito: {len(seeds)} registros creados/actualizados en symbolStrategyConfig.")

    except Exception as e:
        logger.error(f"Error al sembrar parámetros: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    seedSymbolStrategyConfigs()
