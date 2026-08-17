import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def sync_strategies():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    
    # 1. Obtener estrategias activas
    cursor.execute("SELECT strategy FROM strategyConfig WHERE enabled = 1")
    active_strategies = [row['strategy'] for row in cursor.fetchall()]
    print(f"Estrategias activas ({len(active_strategies)}): {active_strategies}")
    
    # 2. Obtener símbolos activos
    cursor.execute("SELECT symbol FROM SentinelSymbol WHERE Activo = 1")
    active_symbols = [row['symbol'] for row in cursor.fetchall()]
    print(f"Símbolos activos ({len(active_symbols)}): {active_symbols}")
    
    # 3. Comprobar e insertar combinaciones faltantes
    added = 0
    for strat in active_strategies:
        for sym in active_symbols:
            cursor.execute("SELECT * FROM symbolStrategyConfig WHERE strategy = %s AND symbol = %s", (strat, sym))
            row = cursor.fetchone()
            if not row:
                print(f"Insertando combinación faltante: {strat} - {sym}")
                cursor.execute("""
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, broker, parametersJson)
                    VALUES (%s, %s, FALSE, FALSE, '{}')
                """, (strat, sym))
                added += 1
                
    # Opcional: ¿Desactivar las que ya no están activas en las tablas maestras?
    # El requerimiento solo menciona que no quede ninguna estrategia activa sin registrar
                
    conn.commit()
    cursor.close()
    conn.close()
    print(f"\nSincronización completada. Se añadieron {added} combinaciones.")

if __name__ == "__main__":
    sync_strategies()
