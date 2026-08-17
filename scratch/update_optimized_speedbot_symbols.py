import logging
import json
import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updateSpeedBotSymbols")

def updateSpeedBotSymbols():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    try:
        # 1. Habilitar la estrategia SpeedBot a nivel global
        logger.info("Habilitando SpeedBot en strategyConfig...")
        cursor.execute("""
            UPDATE strategyConfig 
            SET enabled = 1, updated_at = NOW() 
            WHERE strategy = 'SpeedBot'
        """)
        
        # 2. Definir configuraciones ganadoras (Profit Factor > 1.25 y Win Rate > 42%)
        winningConfigs = {
            'GBP/USD': {"min_rr": 1.5, "atr_mult_trigger": 1.2, "body_ratio_threshold": 0.75, "confirm_ratio": 0.5},
            'AUD/USD': {"min_rr": 1.5, "atr_mult_trigger": 1.6, "body_ratio_threshold": 0.75, "confirm_ratio": 0.5},
            'NZD/USD': {"min_rr": 2.0, "atr_mult_trigger": 1.4, "body_ratio_threshold": 0.75, "confirm_ratio": 0.4},
            'USD/CHF': {"min_rr": 1.5, "atr_mult_trigger": 1.6, "body_ratio_threshold": 0.75, "confirm_ratio": 0.6},
            'GBP/CAD': {"min_rr": 2.0, "atr_mult_trigger": 1.2, "body_ratio_threshold": 0.75, "confirm_ratio": 0.4},
            'GBP/JPY': {"min_rr": 2.0, "atr_mult_trigger": 1.2, "body_ratio_threshold": 0.75, "confirm_ratio": 0.6},
            'USD/MXN': {"min_rr": 1.5, "atr_mult_trigger": 1.2, "body_ratio_threshold": 0.75, "confirm_ratio": 0.4},
            'XAU/USD': {"min_rr": 2.0, "atr_mult_trigger": 1.2, "body_ratio_threshold": 0.80, "confirm_ratio": 0.4}
        }

        # 3. Eliminar símbolos ganadores de symbolNotStrategia
        logger.info("Removiendo símbolos ganadores de symbolNotStrategia...")
        deleteSql = """
            DELETE FROM symbolNotStrategia 
            WHERE strategy = 'SpeedBot' AND symbol = %s
        """
        for symbol in winningConfigs.keys():
            cursor.execute(deleteSql, (symbol,))
            logger.info(f"   ✔️ Removido {symbol} de exclusions para SpeedBot")

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
            ('SpeedBot', symbol, True, json.dumps(params))
            for symbol, params in winningConfigs.items()
        ]
        cursor.executemany(insertSql, insertValues)
        logger.info("   ✔️ Parámetros insertados exitosamente.")

        # 5. Agregar símbolos perdedores / no rentables a symbolNotStrategia
        # (Para evitar que SpeedBot los opere cuando esté encendida globalmente)
        unprofitableSymbols = [
            ('EUR/USD', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('EUR/GBP', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('USD/JPY', 'Filtro Aut.: Sin combinaciones rentables en optimización Grid Search.'),
            ('USD/CAD', 'Filtro Aut.: Win Rate (40%) y Profit Factor (1.0) insuficientes.'),
            ('BTC/USD', 'Filtro Aut.: Win Rate (40%) y Profit Factor (1.33) insuficientes.')
        ]

        logger.info("Registrando exclusiones de símbolos no rentables...")
        excludeSql = """
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, 'SpeedBot', %s)
        """
        for symbol, reason in unprofitableSymbols:
            cursor.execute(excludeSql, (symbol, reason))
            logger.info(f"   🚫 Excluido {symbol} de SpeedBot. Motivo: {reason}")

        conn.commit()
        logger.info("¡Base de datos actualizada con éxito para SpeedBot!")

    except Exception as e:
        logger.error(f"Error al actualizar la base de datos: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    updateSpeedBotSymbols()
