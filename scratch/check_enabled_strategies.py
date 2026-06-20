import os
import sys

# Agregar ruta raíz al path para importar correctamente
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_strategies():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    
    print("📋 Listado de todas las estrategias registradas en strategyConfig:")
    cursor.execute("SELECT strategy, enabled FROM strategyConfig ORDER BY strategy")
    rows = cursor.fetchall()
    
    enabled_count = 0
    disabled_count = 0
    for r in rows:
        status = "🟢 HABILITADA" if r['enabled'] else "🔴 DESHABILITADA"
        print(f"   - {r['strategy']}: {status}")
        if r['enabled']:
            enabled_count += 1
        else:
            disabled_count += 1
            
    print(f"\n📊 Resumen: {enabled_count} estrategias habilitadas, {disabled_count} deshabilitadas (Total: {len(rows)})")
    
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_strategies()
