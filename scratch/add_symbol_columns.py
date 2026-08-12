import sys
import os

project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

from middleware.database import dbConnection

def add_columns():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    
    print("1. Verificando/añadiendo columnas a `symbols`...")
    columns_to_add = [
        ("FOREX", "VARCHAR(20) DEFAULT NULL"),
        ("MT5", "VARCHAR(20) DEFAULT NULL"),
        ("TradingView", "VARCHAR(20) DEFAULT NULL")
    ]
    
    cursor.execute("DESCRIBE symbols;")
    existing_cols = [row['Field'] for row in cursor.fetchall()]
    
    for col_name, col_type in columns_to_add:
        if col_name not in existing_cols:
            cursor.execute(f"ALTER TABLE symbols ADD COLUMN {col_name} {col_type};")
            print(f"  + Columna `{col_name}` agregada a `symbols`.")
        else:
            print(f"  i Columna `{col_name}` ya existe en `symbols`.")

    print("\n2. Actualizando vista de compatibilidad `sentinelSymbol`...")
    view_sql = """
    CREATE OR REPLACE VIEW sentinelSymbol AS 
    SELECT symbol, tipo, activoSentinel AS Activo, startDate, margen, min_lots, pip, quote_currency, FOREX, MT5, TradingView, multiplo, sniper_threshold_adjust_pct, sniper_min_confidence_adjust_pct, sniper_extra_confirmations, sniper_max_rr, priceOffset, idForex, broker, precioMaximo, precioMinimo
    FROM symbols;
    """
    cursor.execute(view_sql)
    conn.commit()
    print("✅ Vista `sentinelSymbol` actualizada exitosamente.")

    print("\n3. Estructura resultante de `symbols`:")
    cursor.execute("DESCRIBE symbols;")
    for row in cursor.fetchall():
        print(f"  {row['Field']:<20} : {row['Type']}")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    add_columns()
