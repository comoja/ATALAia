import os
import pymysql

def fix_schema():
    conn = pymysql.connect(
        host=os.getenv('DB_HOST', '192.168.68.65'),
        user=os.getenv('DB_USER', 'root'),
        password=os.getenv('DB_PASSWORD', 'M1x&J34ny'),
        database=os.getenv('DB_DATABASE', 'ATALAia'),
        autocommit=True
    )
    cursor = conn.cursor()
    
    try:
        cursor.execute("ALTER TABLE user_ratios CHANGE COLUMN created_at createdAt DATETIME DEFAULT CURRENT_TIMESTAMP;")
        print("✅ Columna `created_at` renombrada a `createdAt` en la tabla `user_ratios`.")
    except Exception as e:
        print("Info:", e)

    cursor.execute("DESCRIBE user_ratios;")
    for row in cursor.fetchall():
        print("  ", row)

    conn.close()

if __name__ == '__main__':
    fix_schema()
