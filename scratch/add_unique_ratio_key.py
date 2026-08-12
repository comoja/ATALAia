import os
import pymysql

def add_unique_key():
    conn = pymysql.connect(
        host=os.getenv('DB_HOST', '192.168.68.65'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD', 'M1x&J34ny'),
        database=os.getenv('DB_DATABASE', 'ATALAia'),
        autocommit=True
    )
    cursor = conn.cursor()

    print("1. Limpiando posibles registros duplicados en `user_ratios`...")
    # Eliminar duplicados manteniendo el de ID más reciente
    cursor.execute("""
    DELETE ur1 FROM user_ratios ur1
    INNER JOIN user_ratios ur2 
    WHERE ur1.id < ur2.id 
      AND ur1.idUsuario = ur2.idUsuario 
      AND ur1.numerador = ur2.numerador 
      AND ur1.denominador = ur2.denominador;
    """)
    print("✅ Registros duplicados eliminados.")

    print("\n2. Agregando Unique Key `ukUserRatioPair` (idUsuario, numerador, denominador)...")
    try:
        cursor.execute("ALTER TABLE user_ratios ADD UNIQUE KEY ukUserRatioPair (idUsuario, numerador, denominador);")
        print("✅ Restricción Unique Key `ukUserRatioPair` agregada exitosamente.")
    except Exception as e:
        print("Info:", e)

    print("\n3. Estructura actual de índices en `user_ratios`:")
    cursor.execute("SHOW INDEX FROM user_ratios;")
    for row in cursor.fetchall():
        print("  ", row)

    conn.close()

if __name__ == '__main__':
    add_unique_key()
