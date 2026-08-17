import sys
import os
import json
import logging

# Configurar el path del proyecto para poder importar middleware
sys.path.append("/Volumes/TimeMachine/ATALAia")

from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

def updateOptimizedPatron4hSymbols():
    viableSymbols = [
        {"symbol": "GBP/USD", "parameters": {"minRR": 2.0, "winRate": 66.7, "profitFactor": 1.45}},
        {"symbol": "AUD/USD", "parameters": {"minRR": 1.5, "winRate": 100.0, "profitFactor": 999.0}},
        {"symbol": "NZD/USD", "parameters": {"minRR": 1.5, "winRate": 100.0, "profitFactor": 999.0}},
        {"symbol": "USD/CHF", "parameters": {"minRR": 1.5, "winRate": 100.0, "profitFactor": 999.0}},
        {"symbol": "GBP/CAD", "parameters": {"minRR": 2.0, "winRate": 75.0, "profitFactor": 2.31}}
    ]

    excludedSymbols = [
        {"symbol": "EUR/USD", "reason": "No viable en backtesting real (PF < 1.25 o sin trades)"},
        {"symbol": "USD/CAD", "reason": "No viable en backtesting real (PF < 1.25 o sin trades)"},
        {"symbol": "EUR/GBP", "reason": "No viable en backtesting real (PF < 1.25 o sin trades)"},
        {"symbol": "GBP/JPY", "reason": "No viable en backtesting real (PF < 1.25 o sin trades)"},
        {"symbol": "USD/JPY", "reason": "No viable en backtesting real (PF < 1.25 o sin trades)"},
        {"symbol": "USD/MXN", "reason": "No viable en backtesting real (PF < 1.25 o sin trades)"},
        {"symbol": "XAU/USD", "reason": "No viable en backtesting real (PF < 1.25 o sin trades)"},
        {"symbol": "BTC/USD", "reason": "No viable en backtesting real (PF < 1.25 o sin trades)"}
    ]

    strategyName = "Patron4h"
    conn = None
    cursor = None

    try:
        conn = dbConnection.getConnection()
        if not conn:
            logger.error("No se pudo conectar a la base de datos MySQL.")
            return
        
        cursor = conn.cursor()

        # Asegurar que strategyConfig tenga insertada la estrategia Patron4h y esté habilitada
        cursor.execute("""
            INSERT INTO strategyConfig (strategy, enabled, min_rr, min_confidence)
            VALUES (%s, TRUE, 1.5, 70)
            ON DUPLICATE KEY UPDATE enabled = TRUE
        """, (strategyName,))

        # 1. Procesar los símbolos viables
        for item in viableSymbols:
            symbol = item["symbol"]
            params = item["parameters"]
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

        # 2. Procesar los símbolos excluidos
        for item in excludedSymbols:
            symbol = item["symbol"]
            reason = item["reason"]

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
        logger.info("🎉 Proceso de actualización de parámetros Patron4h finalizado con éxito.")

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
    updateOptimizedPatron4hSymbols()
