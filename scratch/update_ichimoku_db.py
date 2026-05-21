import sys
import os

# Add root directory to sys.path
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbManager

def update_db():
    conn = dbManager.dbConnection.getConnection()
    if not conn:
        print("No connection to database.")
        return
        
    try:
        cursor = conn.cursor()
        
        # Insert or update strategyConfig
        query_strategy = """
            INSERT INTO strategyConfig (strategy, enabled, min_rr, min_confidence) 
            VALUES ('Ichimoku', 1, 1.5, 75) 
            ON DUPLICATE KEY UPDATE enabled = 1;
        """
        cursor.execute(query_strategy)
        print("Added Ichimoku to strategyConfig.")
        
        # Update Cuenta to include Ichimoku
        query_cuenta = """
            UPDATE Cuenta 
            SET estrategias = CONCAT(estrategias, ',Ichimoku') 
            WHERE estrategias NOT LIKE '%Ichimoku%';
        """
        cursor.execute(query_cuenta)
        print("Updated estrategias field in Cuenta table.")
        
        conn.commit()
        print("Database update successful.")
        
    except Exception as e:
        print(f"Error updating database: {e}")
        if conn:
            conn.rollback()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    update_db()
