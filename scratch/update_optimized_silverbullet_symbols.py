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

def updateOptimizedSilverBulletSymbols():
    csvPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/silverbullet_grid_results_best.csv"
    strategyName = "SilverBullet"
    
    if not os.path.exists(csvPath):
        logger.error(f"No se encontró el archivo de mejores resultados en: {csvPath}")
        return
        
    viableSymbols = set()
    dfBest = pd.DataFrame()
    try:
        dfBest = pd.read_csv(csvPath)
    except pd.errors.EmptyDataError:
        logger.info("El archivo de mejores resultados está vacío (ningún símbolo fue viable).")
    except Exception as e:
        logger.error(f"Error al leer el archivo de mejores resultados: {e}")
        
    conn = None
    cursor = None

    try:
        conn = dbConnection.getConnection()
        if not conn:
            logger.error("No se pudo conectar a la base de datos MySQL.")
            return
            
        cursor = conn.cursor()

        # Asegurar que strategyConfig tenga insertada la estrategia SilverBullet y esté habilitada
        cursor.execute("""
            INSERT INTO strategyConfig (strategy, enabled, min_rr, min_confidence)
            VALUES (%s, TRUE, 1.5, 70)
            ON DUPLICATE KEY UPDATE enabled = TRUE
        """, (strategyName,))

        # 1. Procesar los símbolos viables desde el CSV
        if not dfBest.empty:
            for _, row in dfBest.iterrows():
                symbol = row['symbol']
                viableSymbols.add(symbol)
                
                # Construir el JSON de parámetros en camelCase
                params = {
                    "fvgMinPct": float(row['fvgMinPct']),
                    "minRr": float(row['minRr']),
                    "minAdx": float(row['minAdx']),
                    "maxSignalAgeMin": 45,
                    "oteFibMin": 0.62,
                    "oteFibMax": 0.79,
                    "useOteFilter": True,
                    "minConfidence": 70,
                    "minUsdProfit": 10.0,
                    "filterByHtfTrend": False,
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
        logger.info("🎉 Proceso de actualización de parámetros SilverBullet finalizado con éxito.")

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
    updateOptimizedSilverBulletSymbols()
