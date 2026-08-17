import os, sys, json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection

def update_breakout_config():
    conn = dbConnection.getConnection()
    if not conn:
        print("Error conectando a DB.")
        return
    
    cur = conn.cursor()
    try:
        cur.execute("SELECT symbol, parametersJson FROM SymbolStrategyConfig WHERE strategy = 'BreakoutNY'")
        rows = cur.fetchall()
        
        for row in rows:
            symbol = row[0]
            params = json.loads(row[1]) if row[1] else {}
            
            # Parametrizar a las 8:30 AM (Hora de NY, = Apertura de Forex 8:00 AM + 30 min)
            params["start_hour"] = 8
            params["start_minute"] = 30
            params["range_duration"] = 15
            params["min_rr"] = 1.0
            
            new_params_json = json.dumps(params)
            
            cur.execute("""
                UPDATE SymbolStrategyConfig 
                SET parametersJson = %s 
                WHERE strategy = 'BreakoutNY' AND symbol = %s
            """, (new_params_json, symbol))
        
        print(f"Se actualizaron {len(rows)} simbolos de BreakoutNY (Configurados para 8:30 AM Hora Nueva York).")
        conn.commit()
    except Exception as e:
        print("Error:", e)
        conn.rollback()
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    update_breakout_config()
