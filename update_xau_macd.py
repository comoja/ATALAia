import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from middleware.database import dbConnection

conn = dbConnection.getConnection()
cursor = conn.cursor()
try:
    cursor.execute("""
        UPDATE symbolStrategyConfig
        SET jsonIMACD = JSON_OBJECT(
            'useImpulseMacdFilter', 1,
            'macdFast', 12,
            'macdSlow', 40,
            'macdSignal', 9
        )
        WHERE symbol = 'XAU/USD'
    """)
    conn.commit()
    print(f"Filas actualizadas: {cursor.rowcount}")
except Exception as e:
    print(f"Error: {e}")
finally:
    cursor.close()
    conn.close()
