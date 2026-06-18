from middleware.database.dbConnection import getConnection
import json

def check_and_insert():
    try:
        conn = getConnection()
        if not conn:
            print("Failed to get DB connection")
            return
            
        cursor = conn.cursor(dictionary=True)
        query = "SELECT strategy, symbol, parametersJson FROM symbolStrategyConfig WHERE symbol = 'USD/MXN' AND strategy = 'BreakoutProbability'"
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            print(f"Record already exists: {result}")
            params = json.loads(result['parametersJson']) if result['parametersJson'] else {}
            params['minMlProb'] = 0.65
            
            update_query = "UPDATE symbolStrategyConfig SET parametersJson = %s, enabled = 1 WHERE symbol = 'USD/MXN' AND strategy = 'BreakoutProbability'"
            cursor.execute(update_query, (json.dumps(params),))
            conn.commit()
            print("Updated existing record.")
        else:
            print("Record does not exist. Inserting...")
            insert_query = """
                INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson, useImpulseMacdFilter) 
                VALUES (%s, %s, %s, %s, %s)
            """
            params = {
                "minMlProb": 0.65,
                "minConfidence": 80
            }
            cursor.execute(insert_query, ('BreakoutProbability', 'USD/MXN', 1, json.dumps(params), 1))
            conn.commit()
            print("Inserted successfully.")
            
        cursor.close()
        conn.close()
            
    except Exception as e:
        print(f"Error: {e}")

check_and_insert()
