import sys
import os
import pandas as pd
import json
import logging

# Configurar el path del proyecto para poder importar middleware
sys.path.append("/Volumes/TimeMachine/ATALAia")

from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ALL_SYMBOLS = [
    'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
    'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
    'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
]

def updateOptimizedQTrendSymbols():
    csvPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/qtrend_grid_results_best.csv"
    strategyName = "QTrend"
    
    if not os.path.exists(csvPath):
        logger.error(f"No se encontró el archivo de mejores resultados en: {csvPath}")
        return
        
    dfBest = pd.read_csv(csvPath)
    viableSymbols = set()
    
    conn = None
    cursor = None

    try:
        conn = dbConnection.getConnection()
        if not conn:
            logger.error("No se pudo conectar a la base de datos MySQL.")
            return
            
        cursor = conn.cursor()

        # Asegurar que strategyConfig tenga insertada la estrategia QTrend y esté habilitada
        cursor.execute("""
            INSERT INTO strategyConfig (strategy, enabled, min_rr, min_confidence)
            VALUES (%s, TRUE, 1.5, 70)
            ON DUPLICATE KEY UPDATE enabled = TRUE
        """, (strategyName,))

        # 1. Procesar los símbolos viables desde el CSV
        for _, row in dfBest.iterrows():
            symbol = row['symbol']
            viableSymbols.add(symbol)
            
            # Construir el JSON de parámetros en camelCase
            params = {
                "supertrendPeriod": int(row['supertrendPeriod']),
                "supertrendMultiplier": float(row['supertrendMultiplier']),
                "qtrendFast": int(row['qtrendFast']),
                "qtrendSlow": int(row['qtrendSlow']),
                "tpPercent" if symbol in ['XAU/USD', 'BTC/USD'] else "minRr": float(row['tpParam']),
                "winRate": float(row['winRate']),
                "profitFactor": float(row['profitFactor'])
            }
            paramsJsonStr = json.dumps(params)

            # Insertar o actualizar en symbolStrategyConfig
            cursor.execute("""
                INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                VALUES (%s, %s, TRUE, %s)
                ON DUPLICATE KEY UPDATE enabled = TRUE, parametersJson = VALUES(parametersJson)
            """, (strategyName, symbol, paramsJsonStr))

            # Eliminar de la tabla de exclusiones si existía
            cursor.execute("""
                DELETE FROM symbolNotStrategia
                WHERE strategy = %s AND symbol = %s
            """, (strategyName, symbol))

            logger.info(f"✅ Símbolo viable sembrado: {symbol} para {strategyName} con parámetros: {paramsJsonStr}")

        # 2. Procesar los símbolos excluidos (los que no están en el CSV de viables)
        for symbol in ALL_SYMBOLS:
            if symbol not in viableSymbols:
                reason = "No viable en backtesting (PF < 1.25 o WR < 42% con datos reales)"
                
                # Insertar en la tabla de exclusiones
                cursor.execute("""
                    REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
                    VALUES (%s, %s, %s)
                """, (symbol, strategyName, reason))

                # Deshabilitar o eliminar de symbolStrategyConfig para evitar conflictos
                cursor.execute("""
                    UPDATE symbolStrategyConfig
                    SET enabled = FALSE
                    WHERE strategy = %s AND symbol = %s
                """, (strategyName, symbol))

                logger.info(f"🛡️ Símbolo excluido sembrado: {symbol} para {strategyName} debido a: {reason}")

        conn.commit()
        logger.info("🎉 Proceso de actualización de parámetros QTrend finalizado con éxito.")

    except Exception as e:
        logger.error(f"❌ Error durante la actualización de la base de datos: {e}", exc_info=True)
        if conn:
            try: conn.rollback()
            except: pass
    finally:
        if cursor:
            try: cursor.close()
            except: pass
        if conn:
            try: conn.close()
            except: pass

if __name__ == "__main__":
    updateOptimizedQTrendSymbols()
