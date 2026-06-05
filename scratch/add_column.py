import sys
import os

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def addColumn():
    print("=== Añadiendo columna 'ticketId' a la tabla 'trades' ===")
    conn = dbConnection.getConnection()
    if conn is None:
        print("❌ ERROR: No se pudo obtener la conexión a la base de datos.")
        return

    try:
        cursor = conn.cursor()
        
        # Consultar si la columna ya existe para evitar errores
        cursor.execute("SHOW COLUMNS FROM trades LIKE 'ticketId'")
        exists = cursor.fetchone()
        
        if exists:
            print("ℹ️ La columna 'ticketId' ya existe en la tabla 'trades'.")
        else:
            print("Ejecutando ALTER TABLE...")
            cursor.execute("ALTER TABLE trades ADD COLUMN ticketId VARCHAR(50) DEFAULT NULL")
            conn.commit()
            print("✅ Columna 'ticketId' añadida con éxito a la tabla 'trades'.")
            
    except Exception as e:
        print(f"❌ ERROR al ejecutar ALTER TABLE: {e}")
        if conn: conn.rollback()
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    addColumn()
