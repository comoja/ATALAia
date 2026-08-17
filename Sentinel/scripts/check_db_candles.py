import sys
import os
import pandas as pd

# Asegurar que el path del proyecto esté en el sistema
sys.path.append("/Volumes/TimeMachine/ATALAia")

from middleware.database import dbConnection

def check_candles():
    print("--- Inspeccionando velas en la Base de Datos ---")
    try:
        connection = dbConnection.getConnection()
        cursor = connection.cursor(dictionary=True)
        
        # Consultar qué símbolos y timeframes tienen velas en la base de datos
        query = """
            SELECT symbol, timeframe, COUNT(*) as total_candles, MIN(timestamp) as min_date, MAX(timestamp) as max_date
            FROM candles
            GROUP BY symbol, timeframe
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        if not rows:
            print("⚠️ No se encontraron velas en la tabla 'candles'.")
            return
            
        df = pd.DataFrame(rows)
        print("\n--- Resumen de Datos de Velas en la BD ---")
        print(df.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error al consultar la tabla 'candles': {e}")

if __name__ == '__main__':
    check_candles()
