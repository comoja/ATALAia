import sys
sys.path.append('/Volumes/TimeMachine/ATALAia')
from middleware.database import dbConnection

def check_table():
    pool = dbConnection.DBConnectionPool()
    conn = pool.get_connection()
    try:
        cursor = conn.cursor(dictionary=True)
        # Try to find the table in current DB
        cursor.execute("SHOW TABLES LIKE '%symbolStrategyConfig%'")
        tables = cursor.fetchall()
        print("Tables found:", tables)
        
        if tables:
            table_name = list(tables[0].values())[0]
            cursor.execute(f"DESCRIBE {table_name}")
            cols = cursor.fetchall()
            print(f"\n--- Schema for {table_name} ---")
            for col in cols:
                print(f"{col['Field']} - {col['Type']}")
            
            # Check values
            if any(c['Field'] == 'useImpulseMacdFilter' for c in cols):
                cursor.execute(f"SELECT symbol, strategy, useImpulseMacdFilter FROM {table_name} WHERE useImpulseMacdFilter IS NOT NULL LIMIT 5")
                rows = cursor.fetchall()
                print("\n--- Rows with useImpulseMacdFilter ---")
                for r in rows:
                    print(r)
            else:
                print("\nColumn 'useImpulseMacdFilter' NOT FOUND!")
        else:
            print("Table symbolStrategyConfig not found in default DB.")
            
    except Exception as e:
        print(f"Error: {e}")
        
if __name__ == "__main__":
    check_table()
