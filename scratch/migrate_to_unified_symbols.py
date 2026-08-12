import sys
import os

project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

from middleware.database import dbConnection

def create_compatibility_views():
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    
    print("1. Deshabilitando FK checks...")
    cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
    
    print("2. Eliminando tablas antiguas sentinelSymbol y ratiosymbol...")
    cursor.execute("DROP TABLE IF EXISTS sentinelSymbol;")
    cursor.execute("DROP TABLE IF EXISTS ratiosymbol;")
    
    print("3. Creando vista `sentinelSymbol`...")
    cursor.execute("""
    CREATE OR REPLACE VIEW sentinelSymbol AS 
    SELECT symbol, tipo, activoSentinel AS Activo, startDate, margen, min_lots, pip, quote_currency, multiplo, sniper_threshold_adjust_pct, sniper_min_confidence_adjust_pct, sniper_extra_confirmations, sniper_max_rr, priceOffset, idForex, broker, precioMaximo, precioMinimo
    FROM symbols;
    """)
    
    print("4. Creando vista `ratiosymbol`...")
    cursor.execute("""
    CREATE OR REPLACE VIEW ratiosymbol AS 
    SELECT symbol, activoRatio AS Activo, tipo
    FROM symbols;
    """)
    
    cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Vistas `sentinelSymbol` y `ratiosymbol` creadas exitosamente sobre la tabla `symbols`!")

if __name__ == '__main__':
    create_compatibility_views()
