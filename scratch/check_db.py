import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check():
    print("Conectando a base de datos...")
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    
    # 1. Checar estructura de SentinelSymbol
    try:
        cursor.execute("DESCRIBE SentinelSymbol")
        columns = cursor.fetchall()
        print("\n--- Estructura de SentinelSymbol ---")
        for col in columns:
            print(f"Columna: {col[0]} | Tipo: {col[1]} | Null: {col[2]} | Key: {col[3]}")
    except Exception as e:
        print(f"Error describiendo SentinelSymbol: {e}")
        
    # 2. Checar estructura de RatioSymbol
    try:
        cursor.execute("DESCRIBE RatioSymbol")
        columns = cursor.fetchall()
        print("\n--- Estructura de RatioSymbol ---")
        for col in columns:
            print(f"Columna: {col[0]} | Tipo: {col[1]} | Null: {col[2]} | Key: {col[3]}")
    except Exception as e:
        print(f"Error describiendo RatioSymbol: {e}")
        
    conn.close()

if __name__ == "__main__":
    check()
