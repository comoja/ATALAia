import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection
from middleware.database import dbManager

def reset_configs():
    print("🔄 Iniciando reseteo inicial de symbolStrategyConfig...")
    conn = dbConnection.getConnection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos.")
        return
        
    cursor = conn.cursor(dictionary=True)
    try:
        # 1. Obtener todos los símbolos activos
        cursor.execute("SELECT symbol FROM SentinelSymbol WHERE Activo = 1")
        active_symbols = [r['symbol'] for r in cursor.fetchall()]
        
        # 2. Obtener todas las estrategias
        cursor.execute("SELECT strategy FROM strategyConfig")
        strategies = [r['strategy'] for r in cursor.fetchall()]
        
        print(f"Símbolos activos ({len(active_symbols)}): {active_symbols}")
        print(f"Estrategias ({len(strategies)}): {strategies}")
        
        # 3. Actualizar registros existentes a enabled = 1 y broker = 0
        cursor.execute("UPDATE symbolStrategyConfig SET enabled = 1, broker = 0")
        conn.commit()
        print("✅ Todos los registros existentes en symbolStrategyConfig actualizados a enabled = 1 y broker = 0.")
        
        # 4. Insertar combinaciones faltantes
        inserted_count = 0
        for strategy in strategies:
            for symbol in active_symbols:
                # Comprobar si ya existe
                cursor.execute("""
                    SELECT 1 FROM symbolStrategyConfig 
                    WHERE strategy = %s AND symbol = %s
                """, (strategy, symbol))
                if not cursor.fetchone():
                    # Insertar con enabled = 1, broker = 0 y parametersJson vacío
                    cursor.execute("""
                        INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, broker, parametersJson)
                        VALUES (%s, %s, 1, 0, '{}')
                    """, (strategy, symbol))
                    inserted_count += 1
                    
        conn.commit()
        print(f"✅ Se insertaron {inserted_count} combinaciones faltantes con enabled = 1 y broker = 0.")
        
    except Exception as e:
        print(f"❌ Error al resetear configuraciones: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    reset_configs()
