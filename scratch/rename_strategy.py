from middleware.database import dbConnection

def rename():
    conn = dbConnection.getConnection()
    if not conn:
        print("No se pudo conectar a la DB")
        return
    cur = conn.cursor(dictionary=True)
    try:
        # 1. Obtener la fila actual de Regresivol
        cur.execute("SELECT * FROM strategyConfig WHERE strategy = 'Regresivol'")
        row = cur.fetchone()
        
        if row:
            # 2. Insertar la nueva fila ReversionMedia copiando valores
            cols = [k for k in row.keys() if k != 'id' and k != 'updated_at']
            placeholders = ", ".join(["%s"] * len(cols))
            col_names = ", ".join(cols)
            
            # Cambiar el valor de la estrategia en los valores a insertar
            vals = []
            for col in cols:
                if col == 'strategy':
                    vals.append('ReversionMedia')
                else:
                    vals.append(row[col])
            
            insert_query = f"INSERT IGNORE INTO strategyConfig ({col_names}) VALUES ({placeholders})"
            cur.execute(insert_query, tuple(vals))
            print("Nueva estrategia ReversionMedia insertada en strategyConfig")
        else:
            # Si no existe, al menos nos aseguramos de que exista ReversionMedia
            cur.execute("INSERT IGNORE INTO strategyConfig (strategy, min_rr, min_confidence) VALUES ('ReversionMedia', 2.5, 70)")
            print("Estrategia ReversionMedia sembrada directamente")
            
        # 3. Actualizar CuentaEstrategia para que use ReversionMedia
        cur.execute("UPDATE CuentaEstrategia SET strategy = 'ReversionMedia' WHERE strategy = 'Regresivol'")
        print("CuentaEstrategia actualizada")
        
        # 4. Actualizar trades para que use ReversionMedia
        cur.execute("UPDATE trades SET strategy = 'ReversionMedia' WHERE strategy = 'Regresivol'")
        print("trades actualizada")
        
        # 5. Eliminar la antigua de strategyConfig
        cur.execute("DELETE FROM strategyConfig WHERE strategy = 'Regresivol'")
        print("Antigua estrategia Regresivol eliminada de strategyConfig")
        
        conn.commit()
        print("Transacción confirmada con éxito!")
    except Exception as e:
        conn.rollback()
        print(f"Error en actualización: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    rename()
