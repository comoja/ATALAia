import sys
import os

project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

from middleware.database import dbConnection

def create_table():
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    
    print("1. Creando tabla `user_ratios` en MySQL...")
    create_sql = """
    CREATE TABLE IF NOT EXISTS user_ratios (
        id INT AUTO_INCREMENT PRIMARY KEY,
        idUsuario INT NOT NULL,
        numerador VARCHAR(20) NOT NULL,
        denominador VARCHAR(20) NOT NULL,
        periodo VARCHAR(20) NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT fk_user_ratios_usuario FOREIGN KEY (idUsuario) REFERENCES Usuario(idUsuario) ON DELETE CASCADE ON UPDATE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
    """
    
    cursor.execute(create_sql)
    conn.commit()
    print("✅ Tabla `user_ratios` creada exitosamente.")
    
    cursor.execute("DESCRIBE user_ratios;")
    for row in cursor.fetchall():
        print("  ", row)

    cursor.close()
    conn.close()

if __name__ == '__main__':
    create_table()
