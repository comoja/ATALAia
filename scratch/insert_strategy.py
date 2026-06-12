import logging
import mysql.connector
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scratch")

def inspectAndInsertStrategy():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo establecer conexión con la base de datos")
        return

    cursor = conn.cursor(dictionary=True)
    try:
        # 1. Describir la tabla strategyConfig para ver las columnas reales
        logger.info("Inspeccionando la tabla strategyConfig...")
        cursor.execute("DESCRIBE strategyConfig")
        columns = cursor.fetchall()
        for col in columns:
            logger.info(f"Columna: {col['Field']} | Tipo: {col['Type']} | Null: {col['Null']} | Key: {col['Key']}")

        # 2. Verificar si ya existe 'PremiumConfluence'
        # Determinamos si el campo clave es 'strategy' o 'nombre'
        columnName = 'strategy' if any(c['Field'] == 'strategy' for c in columns) else 'nombre'
        
        sqlCheck = f"SELECT * FROM strategyConfig WHERE {columnName} = %s"
        cursor.execute(sqlCheck, ('PremiumConfluence',))
        existing = cursor.fetchone()

        if existing:
            logger.info("La estrategia PremiumConfluence ya existe en la base de datos:")
            logger.info(existing)
        else:
            # 3. Insertar la nueva estrategia
            logger.info("Insertando la estrategia PremiumConfluence...")
            sqlInsert = f"""
                INSERT INTO strategyConfig ({columnName}, enabled, min_rr, min_confidence)
                VALUES (%s, %s, %s, %s)
            """
            cursor.execute(sqlInsert, ('PremiumConfluence', True, 1.5, 80))
            conn.commit()
            logger.info("Estrategia PremiumConfluence insertada correctamente")

            # 4. Habilitar la estrategia para todas las cuentas activas (idCuenta) en CuentaEstrategia
            # Primero verifiquemos si la tabla CuentaEstrategia existe y sus columnas
            cursor.execute("SHOW TABLES LIKE 'CuentaEstrategia'")
            if cursor.fetchone():
                cursor.execute("SELECT idCuenta FROM CUENTA WHERE Activo = 1")
                cuentas = cursor.fetchall()
                for c in cuentas:
                    idCuenta = c['idCuenta']
                    # Insertar en CuentaEstrategia
                    try:
                        cursor.execute("""
                            INSERT INTO CuentaEstrategia (idCuenta, strategy)
                            VALUES (%s, %s)
                            ON DUPLICATE KEY UPDATE strategy = strategy
                        """, (idCuenta, 'PremiumConfluence'))
                        logger.info(f"Estrategia PremiumConfluence asociada a cuenta {idCuenta}")
                    except Exception as e:
                        logger.warning(f"No se pudo asociar a cuenta {idCuenta}: {e}")
                conn.commit()

    except Exception as e:
        logger.error(f"Error operando en la base de datos: {e}", exc_info=True)
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    inspectAndInsertStrategy()
