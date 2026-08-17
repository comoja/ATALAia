import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_config():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    
    print("📋 Resumen de symbolStrategyConfig:")
    cursor.execute("""
        SELECT strategy, symbol, enabled, broker 
        FROM symbolStrategyConfig 
        ORDER BY strategy, symbol
    """)
    rows = cursor.fetchall()
    
    print(f"Total registros: {len(rows)}")
    enabled_count = sum(1 for r in rows if r['enabled'])
    broker_count = sum(1 for r in rows if r['broker'])
    print(f"Habilitados (enabled=1): {enabled_count}")
    print(f"Broker (broker=1): {broker_count}")
    
    print("\nDetalle de registros:")
    for r in rows:
        print(f"  - {r['strategy']} / {r['symbol']}: enabled={r['enabled']}, broker={r['broker']}")
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_config()
