import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check():
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    
    for table in ["EstrategiaSymbol", "symbolNotStrategia", "strategyConfig", "Cuenta"]:
        try:
            cursor.execute(f"DESCRIBE {table}")
            columns = cursor.fetchall()
            print(f"\n--- Estructura de {table} ---")
            for col in columns:
                print(f"Columna: {col[0]} | Tipo: {col[1]} | Null: {col[2]} | Key: {col[3]}")
        except Exception as e:
            print(f"Error describiendo {table}: {e}")
            
    conn.close()

if __name__ == "__main__":
    check()
