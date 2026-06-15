import logging
import json
import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updateIchimokuSymbols")

def updateIchimokuSymbols():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    try:
        # Definir configuraciones ganadoras
        nzdParams = {
            "tenkan_period": 7,
            "kijun_period": 22,
            "senkou_period": 44,
            "displacement": 22,
            "min_rr": 2.0
        }

        btcParams = {
            "tenkan_period": 10,
            "kijun_period": 30,
            "senkou_period": 60,
            "displacement": 30,
            "min_rr": 1.5
        }

        optimizedSeeds = [
            ('Ichimoku', 'NZD/USD', nzdParams),
            ('Ichimoku', 'BTC/USD', btcParams)
        ]

        # 1. Eliminar de symbolNotStrategia para permitir la ejecución de Ichimoku en estos símbolos
        symbolsToRemove = ['NZD/USD', 'BTC/USD']
        logger.info("Eliminando símbolos optimizados de symbolNotStrategia...")
        deleteSql = """
            DELETE FROM symbolNotStrategia 
            WHERE strategy = 'Ichimoku' AND symbol = %s
        """
        for symbol in symbolsToRemove:
            cursor.execute(deleteSql, (symbol,))
            logger.info(f"   ✔️ Removido {symbol} de exclusions para Ichimoku")

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
        logger.info("   ✔️ Nuevas parametrizaciones de Ichimoku insertadas exitosamente.")

        conn.commit()
        logger.info("¡Base de datos actualizada con éxito para Ichimoku!")

    except Exception as e:
        logger.error(f"Error al actualizar la base de datos: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    updateIchimokuSymbols()
