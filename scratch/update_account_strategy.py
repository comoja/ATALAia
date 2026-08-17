import logging
from middleware.database import dbConnection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("scratch")

def updateAccountStrategy():
    conn = dbConnection.getConnection()
    if not conn:
        logger.error("No se pudo establecer conexión con la base de datos")
        return

    cursor = conn.cursor()
    try:
        # 1. Eliminar todas las asociaciones de PremiumConfluence que no sean para la cuenta 2
        logger.info("Eliminando asociaciones de PremiumConfluence para otras cuentas...")
        cursor.execute("""
            DELETE FROM CuentaEstrategia 
            WHERE strategy = 'PremiumConfluence' AND idCuenta <> 2
        """)
        deletedRows = cursor.rowcount
        logger.info(f"Se eliminaron {deletedRows} registros de asociación obsoletos.")

        # 2. Insertar/Asegurar la asociación para la cuenta 2
        logger.info("Asegurando asociación de PremiumConfluence para la cuenta 2...")
        cursor.execute("""
            INSERT INTO CuentaEstrategia (idCuenta, strategy)
            VALUES (2, 'PremiumConfluence')
            ON DUPLICATE KEY UPDATE strategy = strategy
        """)
        conn.commit()
        logger.info("Asociación actualizada exitosamente. Ahora PremiumConfluence está habilitada únicamente para la cuenta 2.")

    except Exception as e:
        logger.error(f"Error operando en la base de datos: {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    updateAccountStrategy()
