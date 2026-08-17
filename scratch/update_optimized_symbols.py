import logging
import json
import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updateOptimizedSymbols")

def updateOptimizedSymbols():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    try:
        # Definir configuraciones ganadoras
        mxnParams = {
            "supertrend_period": 10,
            "supertrend_mult": 1.5,
            "ha_period1": 10,
            "ha_period2": 10,
            "macd_length": 20,
            "macd_signal": 9,
            "min_rr": 1.5,
            "lookback": 15
        }

        audParams = {
            "supertrend_period": 10,
            "supertrend_mult": 2.0,
            "ha_period1": 10,
            "ha_period2": 10,
            "macd_length": 20,
            "macd_signal": 9,
            "min_rr": 1.5,
            "lookback": 15
        }

        jpyParams = {
            "supertrend_period": 10,
            "supertrend_mult": 2.0,
            "ha_period1": 10,
            "ha_period2": 10,
            "macd_length": 34,
            "macd_signal": 9,
            "min_rr": 1.5,
            "lookback": 15
        }

        optimizedSeeds = [
            ('PremiumConfluence', 'USD/MXN', mxnParams),
            ('PremiumConfluence', 'AUD/USD', audParams),
            ('PremiumConfluence', 'GBP/JPY', jpyParams)
        ]

        # 1. Eliminar de symbolNotStrategia para permitir la ejecución de PremiumConfluence en estos símbolos
        symbolsToRemove = ['USD/MXN', 'AUD/USD', 'GBP/JPY']
        logger.info("Eliminando símbolos optimizados de symbolNotStrategia...")
        deleteSql = """
            DELETE FROM symbolNotStrategia 
            WHERE strategy = 'PremiumConfluence' AND symbol = %s
        """
        for symbol in symbolsToRemove:
            cursor.execute(deleteSql, (symbol,))
            logger.info(f"   ✔️ Removido {symbol} de exclusions para PremiumConfluence")

        # 2. Insertar o actualizar parámetros en symbolStrategyConfig
        logger.info("Insertando/Actualizando configuraciones en symbolStrategyConfig...")
        insertSql = """
            INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                enabled = VALUES(enabled),
                parametersJson = VALUES(parametersJson)
        """
        
        insertValues = [
            (strategy, symbol, True, json.dumps(params))
            for strategy, symbol, params in optimizedSeeds
        ]
        
        cursor.executemany(insertSql, insertValues)
        logger.info("   ✔️ Nuevas parametrizaciones insertadas exitosamente.")

        conn.commit()
        logger.info("¡Base de datos actualizada con éxito!")

    except Exception as e:
        logger.error(f"Error al actualizar la base de datos: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    updateOptimizedSymbols()
