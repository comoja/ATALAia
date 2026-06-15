import logging
import json
import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updateBreakoutProbabilitySymbols")

def updateBreakoutProbabilitySymbols():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    try:
        # 1. Habilitar la estrategia BreakoutProbability a nivel global
        logger.info("Habilitando BreakoutProbability en strategyConfig...")
        cursor.execute("""
            INSERT INTO strategyConfig (strategy, enabled, min_rr, min_confidence, updated_at)
            VALUES ('BreakoutProbability', 1, 1.5, 50, NOW())
            ON DUPLICATE KEY UPDATE 
                enabled = 1,
                min_rr = 1.5,
                min_confidence = 50,
                updated_at = NOW()
        """)
        
        # 2. Definir configuraciones ganadoras (Profit Factor > 1.25 y Win Rate > 42%)
        winningConfigs = {
            'EUR/USD': {"channelLen": 15, "targetAtrMult": 1.2, "minProbThreshold": 50.0, "minRr": 1.5},
            'USD/CHF': {"channelLen": 15, "targetAtrMult": 1.2, "minProbThreshold": 50.0, "minRr": 2.0}
        }

        # 3. Eliminar símbolos ganadores de symbolNotStrategia
        logger.info("Removiendo símbolos ganadores de symbolNotStrategia...")
        deleteSql = """
            DELETE FROM symbolNotStrategia 
            WHERE strategy = 'BreakoutProbability' AND symbol = %s
        """
        for symbol in winningConfigs.keys():
            cursor.execute(deleteSql, (symbol,))
            logger.info(f"   ✔️ Removido {symbol} de exclusions para BreakoutProbability")

        # 4. Insertar parámetros ganadores en symbolStrategyConfig
        logger.info("Insertando parametrizaciones ganadoras en symbolStrategyConfig...")
        insertSql = """
            INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                enabled = VALUES(enabled),
                parametersJson = VALUES(parametersJson)
        """
        
        insertValues = [
            ('BreakoutProbability', symbol, True, json.dumps(params))
            for symbol, params in winningConfigs.items()
        ]
        cursor.executemany(insertSql, insertValues)
        logger.info("   ✔️ Parámetros insertados exitosamente.")

        # 5. Agregar símbolos perdedores / no rentables a symbolNotStrategia
        # (Para evitar que BreakoutProbability los opere cuando esté encendida globalmente)
        unprofitableSymbols = [
            ('GBP/USD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('AUD/USD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('NZD/USD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('USD/CAD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('EUR/GBP', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('GBP/CAD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('GBP/JPY', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('USD/JPY', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('USD/MXN', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('XAU/USD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('BTC/USD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.')
        ]

        logger.info("Registrando exclusiones de símbolos no rentables...")
        excludeSql = """
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, 'BreakoutProbability', %s)
        """
        for symbol, reason in unprofitableSymbols:
            cursor.execute(excludeSql, (symbol, reason))
            logger.info(f"   🚫 Excluido {symbol} de BreakoutProbability. Motivo: {reason}")

        conn.commit()
        logger.info("¡Base de datos actualizada con éxito para BreakoutProbability!")

    except Exception as e:
        logger.error(f"Error al actualizar la base de datos: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    updateBreakoutProbabilitySymbols()
