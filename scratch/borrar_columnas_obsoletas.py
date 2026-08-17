import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def borrar_columnas():
    conn = dbConnection.getConnection()
    if not conn:
        print("❌ No se pudo conectar a la base de datos.")
        return

    cursor = conn.cursor(dictionary=True)
    try:
        # Obtener columnas actuales
        cursor.execute("DESCRIBE symbolStrategyConfig")
        cols = [c['Field'] for c in cursor.fetchall()]
        print("Columnas actuales:", cols)

        columnas_a_borrar = ['useImpulseMacdFilter', 'macdFast', 'macdSlow', 'macdSignal']
        for col in columnas_a_borrar:
            if col in cols:
                print(f"Borrando columna obsoleta {col}...")
                cursor.execute(f"ALTER TABLE symbolStrategyConfig DROP COLUMN {col}")
                print(f"✅ Columna {col} eliminada.")
        
        conn.commit()
        print("🎉 Proceso de limpieza finalizado con éxito.")
    except Exception as e:
        print(f"❌ Error durante el borrado de columnas: {e}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    borrar_columnas()
