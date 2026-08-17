import os
import pymysql

def add_ema_columns():
    conn = pymysql.connect(
        host=os.getenv('DB_HOST', '192.168.68.65'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD', 'M1x&J34ny'),
        database=os.getenv('DB_DATABASE', 'ATALAia'),
        autocommit=True
    )
    cursor = conn.cursor()

    cursor.execute("DESCRIBE user_ratios;")
    cols = [r[0] for r in cursor.fetchall()]
    print("Columnas actuales en `user_ratios`:", cols)

    if 'EMARapida' not in cols:
        cursor.execute("ALTER TABLE user_ratios ADD COLUMN EMARapida INT DEFAULT 3;")
        print("✅ Columna `EMARapida` agregada.")
    
    if 'EMALenta' not in cols:
        cursor.execute("ALTER TABLE user_ratios ADD COLUMN EMALenta INT DEFAULT 20;")
        print("✅ Columna `EMALenta` agregada.")

    print("\nEstructura actualizada de `user_ratios`:")
    cursor.execute("DESCRIBE user_ratios;")
    for row in cursor.fetchall():
        print("  ", row)

    conn.close()

if __name__ == '__main__':
    add_ema_columns()
