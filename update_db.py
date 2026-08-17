import sys
sys.path.append('/Volumes/TimeMachine/ATALAia')
from middleware.database import dbConnection

def update_db():
    pool = dbConnection.DBConnectionPool()
    conn = pool.get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        # 1. Get all distinct strategies
        cursor.execute("SELECT DISTINCT strategy FROM symbolStrategyConfig")
        strategies = cursor.fetchall()
        print("Strategies found:", [s['strategy'] for s in strategies])
        
        # 2. Add column jsonIMACD if not exist
        cursor.execute("DESCRIBE symbolStrategyConfig")
        cols = [c['Field'] for c in cursor.fetchall()]
        
        if 'jsonIMACD' not in cols:
            print("Adding column jsonIMACD...")
            cursor.execute("ALTER TABLE symbolStrategyConfig ADD COLUMN jsonIMACD JSON DEFAULT NULL")
            conn.commit()
            
            # Recargar columnas
            cursor.execute("DESCRIBE symbolStrategyConfig")
            cols = [c['Field'] for c in cursor.fetchall()]
        
        # 3. Update jsonIMACD based on conclusions
        print("Enabling useImpulseMacdFilter inside jsonIMACD for all FVG and imbalance strategies...")
        cursor.execute("""
            UPDATE symbolStrategyConfig 
            SET jsonIMACD = JSON_OBJECT(
                'useImpulseMacdFilter', 1,
                'macdFast', 12,
                'macdSlow', 26,
                'macdSignal', 9
            )
            WHERE strategy IN ('GenericFVG', 'SilverBullet', 'ImbalanceNY', 'ImbalanceLDN', 'ImbalancePMNY', 'Patron4h', 'SesgoBiasHTF')
            OR strategy LIKE '%FVG%' OR strategy LIKE '%Imbalance%' OR strategy LIKE '%Bullet%'
        """)
        conn.commit()
        print(f"Rows updated: {cursor.rowcount}")
        
    except Exception as e:
        print(f"Error: {e}")
        conn.rollback()
        
if __name__ == "__main__":
    update_db()
