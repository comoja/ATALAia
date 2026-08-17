import logging
import sys
import json
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("updateImbalanceSymbols")

def updateImbalanceSymbols():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo conectar a la base de datos")
        return

    cursor = conn.cursor()
    try:
        strategies = ["ImbalanceLDN", "ImbalanceNY", "ImbalancePMNY"]
        
        # 1. Habilitar las tres estrategias globalmente
        for strategy in strategies:
            logger.info(f"Habilitando {strategy} en strategyConfig...")
            cursor.execute("""
                UPDATE strategyConfig 
                SET enabled = 1, updated_at = NOW() 
                WHERE strategy = %s
            """, (strategy,))
            
        ALL_SYMBOLS = [
            'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
            'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
            'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
        ]
        
        # --- IMBALANCEL DN ---
        logger.info("Actualizando ImbalanceLDN...")
        cursor.execute("DELETE FROM symbolStrategyConfig WHERE strategy = 'ImbalanceLDN'")
        excludeSql = "REPLACE INTO symbolNotStrategia (symbol, strategy, reason) VALUES (%s, 'ImbalanceLDN', 'Filtro Aut.: Sin combinaciones viables o trades suficientes (>3) en optimización Grid Search.')"
        for symbol in ALL_SYMBOLS:
            cursor.execute(excludeSql, (symbol,))
            
        # --- IMBALANCE NY ---
        logger.info("Actualizando ImbalanceNY...")
        # USD/CAD es viable
        usdcad_config = {
            "minRr": 1.5,
            "maxMinutosFvg": 15,
            "minConfidence": 70.0,
            "minUsdProfit": 10.0
        }
        cursor.execute("""
            REPLACE INTO symbolStrategyConfig (symbol, strategy, parametersJson, updatedAt)
            VALUES ('USD/CAD', 'ImbalanceNY', %s, NOW())
        """, (json.dumps(usdcad_config),))

        logger.info("   ✅ Configuración de USD/CAD insertada en symbolStrategyConfig.")
        
        # Quitar USD/CAD de exclusiones
        cursor.execute("DELETE FROM symbolNotStrategia WHERE symbol = 'USD/CAD' AND strategy = 'ImbalanceNY'")
        
        # Excluir los demás para ImbalanceNY
        excludeNySql = "REPLACE INTO symbolNotStrategia (symbol, strategy, reason) VALUES (%s, 'ImbalanceNY', 'Filtro Aut.: Sin combinaciones viables o trades suficientes (>3) en optimización Grid Search.')"
        for symbol in ALL_SYMBOLS:
            if symbol != 'USD/CAD':
                cursor.execute(excludeNySql, (symbol,))
                
        # --- IMBALANCE PMNY ---
        logger.info("Actualizando ImbalancePMNY...")
        cursor.execute("DELETE FROM symbolStrategyConfig WHERE strategy = 'ImbalancePMNY'")
        excludePmnySql = "REPLACE INTO symbolNotStrategia (symbol, strategy, reason) VALUES (%s, 'ImbalancePMNY', 'Filtro Aut.: Sin combinaciones viables o trades suficientes (>3) en optimización Grid Search.')"
        for symbol in ALL_SYMBOLS:
            cursor.execute(excludePmnySql, (symbol,))
            
        conn.commit()
        logger.info("¡Base de datos actualizada con éxito para ImbalanceLDN, ImbalanceNY e ImbalancePMNY!")

    except Exception as e:
        logger.error(f"Error al actualizar la base de datos: {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    updateImbalanceSymbols()
