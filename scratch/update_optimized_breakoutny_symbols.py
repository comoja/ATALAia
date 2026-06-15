import logging
import json
import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updateBreakoutNYSymbols")

def updateBreakoutNYSymbols():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    try:
        # 1. Habilitar la estrategia BreakoutNY a nivel global
        logger.info("Habilitando BreakoutNY en strategyConfig...")
        cursor.execute("""
            UPDATE strategyConfig 
            SET enabled = 1, updated_at = NOW() 
            WHERE strategy = 'BreakoutNY'
        """)
        
        # 2. Definir configuraciones ganadoras (Profit Factor > 1.25 y Win Rate > 42%)
        winningConfigs = {
            'GBP/USD': {"range_duration": 15, "trading_window": 120, "min_rr": 2.0},
            'AUD/USD': {"range_duration": 30, "trading_window": 180, "min_rr": 1.5},
            'NZD/USD': {"range_duration": 15, "trading_window": 150, "min_rr": 2.0},
            'XAU/USD': {"range_duration": 30, "trading_window": 180, "min_rr": 1.5}
        }

        # 3. Eliminar símbolos ganadores de symbolNotStrategia
        logger.info("Removiendo símbolos ganadores de symbolNotStrategia...")
        deleteSql = """
            DELETE FROM symbolNotStrategia 
            WHERE strategy = 'BreakoutNY' AND symbol = %s
        """
        for symbol in winningConfigs.keys():
            cursor.execute(deleteSql, (symbol,))
            logger.info(f"   ✔️ Removido {symbol} de exclusions para BreakoutNY")

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
            ('BreakoutNY', symbol, True, json.dumps(params))
            for symbol, params in winningConfigs.items()
        ]
        cursor.executemany(insertSql, insertValues)
        logger.info("   ✔️ Parámetros insertados exitosamente.")

        # 5. Agregar símbolos perdedores / no rentables a symbolNotStrategia
        # (Para evitar que BreakoutNY los opere cuando esté encendida globalmente)
        unprofitableSymbols = [
            ('EUR/USD', 'Filtro Aut.: Win Rate (33.3%) y Profit Factor (1.0) insuficientes.'),
            ('USD/CAD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('USD/CHF', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('EUR/GBP', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('GBP/CAD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('GBP/JPY', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('USD/JPY', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('USD/MXN', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('BTC/USD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.')
        ]

        logger.info("Registrando exclusiones de símbolos no rentables...")
        excludeSql = """
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, 'BreakoutNY', %s)
        """
        for symbol, reason in unprofitableSymbols:
            cursor.execute(excludeSql, (symbol, reason))
            logger.info(f"   🚫 Excluido {symbol} de BreakoutNY. Motivo: {reason}")

        conn.commit()
        logger.info("¡Base de datos actualizada con éxito para BreakoutNY!")

    except Exception as e:
        logger.error(f"Error al actualizar la base de datos: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    updateBreakoutNYSymbols()
