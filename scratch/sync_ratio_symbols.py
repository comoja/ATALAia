import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def sync_symbols():
    print("Iniciando sincronización de símbolos...")
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        
        # 1. Leer los símbolos activos de SentinelSymbol
        print("Leyendo símbolos activos desde SentinelSymbol...")
        cursor.execute("SELECT symbol, Activo, tipo FROM SentinelSymbol WHERE Activo = 1")
        sentinel_symbols = cursor.fetchall()
        
        if not sentinel_symbols:
            print("No se encontraron símbolos activos en SentinelSymbol.")
            conn.close()
            return
            
        print(f"Encontrados {len(sentinel_symbols)} símbolos activos.")
        
        # 2. Insertar/Actualizar en RatioSymbol usando el esquema real
        insert_sql = """
        INSERT INTO RatioSymbol (symbol, Activo, tipo)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE 
            Activo = VALUES(Activo),
            tipo = VALUES(tipo)
        """
        
        values = []
        for s in sentinel_symbols:
            values.append((s['symbol'], s['Activo'], s['tipo']))
            
        print(f"Insertando/Actualizando {len(values)} símbolos en RatioSymbol...")
        cursor.executemany(insert_sql, values)
        conn.commit()
        
        print(f"✅ Sincronización exitosa. Registros afectados: {cursor.rowcount}")
        
        # 3. Mostrar catálogo resultante en RatioSymbol
        cursor.execute("SELECT symbol, Activo, tipo FROM RatioSymbol")
        ratio_symbols = cursor.fetchall()
        print("\n--- Catálogo de RatioSymbol Resultante ---")
        for r in ratio_symbols:
            estado = "Activo" if r['Activo'] else "Inactivo"
            print(f"Par: {r['symbol']} | Tipo: {r['tipo']} | Estado: {estado}")
            
        conn.close()
        
    except Exception as e:
        print(f"❌ Error durante la sincronización: {e}", file=sys.stderr)

if __name__ == "__main__":
    sync_symbols()
