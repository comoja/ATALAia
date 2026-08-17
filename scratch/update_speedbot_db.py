import mysql.connector
from middleware.database import dbConnection
import logging

logging.basicConfig(level=logging.INFO)ImbalanceNY,ImbalanceLDN,SMA20_200,Sniper,Patron4h,SesgoBiasHTF,EMA20200,SilverBullet,GenericFVG,ImbalancePMNY,FVGDiario
logger = logging.getLogger(__name__)

def update_database():
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()
        ImbalanceNY,ImbalanceLDN,SMA20_200,Sniper,Patron4h,SesgoBiasHTF,EMA20200,SilverBullet,GenericFVG,ImbalancePMNY,FVGDiario,SpeedBot
        # 1. Insertar SpeedBot en strategyConfig
        logger.info("Añadiendo SpeedBot a strategyConfig...")
        sqlInsert = """
            INSERT IGNORE INTO strategyConfig 
            (strategy, enabled, min_rr, min_confidence, max_minutos_fvg, max_minutos_signal) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sqlInsert, ('SpeedBot', 1, 1.5, 70, 40, 40))
        
        # 2. Concatenar SpeedBot en CUENTA.estrategias
        logger.info("Actualizando CUENTA.estrategias para incluir SpeedBot...")
        # Usamos concat y verificamos que no exista ya para evitar duplicados
        sqlUpdate = """
            UPDATE CUENTA 
            SET estrategias = CASE 
                WHEN estrategias IS NULL OR estrategias = '' THEN 'SpeedBot'
                WHEN estrategias NOT LIKE '%SpeedBot%' THEN CONCAT(estrategias, ',SpeedBot')
                ELSE estrategias
            END
            WHERE Activo = 1
        """
        cursor.execute(sqlUpdate)
        
        conn.commit()
        logger.info(f"✅ Cambios aplicados con éxito. Filas afectadas en CUENTA: {cursor.rowcount}")
        
        conn.close()
    except Exception as e:
        logger.error(f"❌ Error al actualizar la base de datos: {e}")

if __name__ == "__main__":
    update_database()
