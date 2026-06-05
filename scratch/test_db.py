import sys
import os

# Set up paths
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection
from middleware.database import dbManager

def runTest():
    print("=== Diagnosticando Conexión a Base de Datos MySQL ===")
    
    conn = dbConnection.getConnection()
    if conn is None:
        print("❌ ERROR: No se pudo obtener la conexión a la base de datos (retornó None).")
        return
        
    print("✅ Conexión establecida correctamente.")
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # Test 1: Check if tables exist
        print("\nTest 1: Consultando últimas 5 filas en la tabla 'trades'...")
        cursor.execute("SELECT idTrade, symbol, strategy, direction, status, closeTime FROM trades ORDER BY idTrade DESC LIMIT 5")
        rows = cursor.fetchall()
        print(f"Número de filas obtenidas: {len(rows)}")
        for r in rows:
            print(f"  - ID: {r['idTrade']} | Símbolo: {r['symbol']} | Estrategia: {r['strategy']} | Dirección: {r['direction']} | Estado: {r['status']} | Cierre: {r['closeTime']}")
            
        # Test 2: Try to insert a dummy trade and roll it back
        print("\nTest 2: Intentando inserción de prueba (con Rollback)...")
        dummyTrade = {
            "idCuenta": 2,
            "symbol": "USD/CAD",
            "direction": "LARGO",
            "openTime": "2026-06-05 08:00:00",
            "size": 1000.0,
            "entryPrice": 1.39000,
            "stopLoss": 1.38000,
            "takeProfit": 1.41000,
            "intervalo": "15min",
            "strategy": "GenericFVG",
            "setup": "TEST_SETUP",
            "margin_used": 0.0,
            "candleTime": "2026-06-05 07:55:00",
            "ticketId": None
        }
        
        dbManager.buscaTrade(dummyTrade)
        print("✅ Función buscaTrade/insertarTrade ejecutada sin excepciones.")
        
    except Exception as e:
        print(f"❌ ERROR durante las consultas o inserciones: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    runTest()
