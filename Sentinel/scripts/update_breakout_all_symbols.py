import os
import sys
import pandas as pd
import json

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database.dbConnection import getConnection

def update_configs():
    best_results_path = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/breakoutprobability_grid_results_best.csv"
    if not os.path.exists(best_results_path):
        print(f"Error: {best_results_path} no existe.")
        return

    df = pd.read_csv(best_results_path)
    if df.empty:
        print("El archivo CSV está vacío.")
        return
        
    conn = getConnection()
    if not conn:
        print("Fallo de conexión a la base de datos.")
        return
        
    cursor = conn.cursor(dictionary=True)
    strategy = "BreakoutProbability"
    
    updated_count = 0
    inserted_count = 0
    
    for index, row in df.iterrows():
        symbol = row['Símbolo']
        min_prob = float(row['Min Prob'])
        channel_len = int(row['Channel Len'])
        atr_mult = float(row['ATR Mult'])
        min_rr = float(row['Min RR'])
        
        # Consultar si ya existe
        cursor.execute("SELECT parametersJson FROM symbolStrategyConfig WHERE symbol = %s AND strategy = %s", (symbol, strategy))
        result = cursor.fetchone()
        
        if result:
            params = json.loads(result['parametersJson']) if result['parametersJson'] else {}
            params['minMlProb'] = min_prob / 100.0  # Convertir a decimal (ej. 65.0 -> 0.65)
            params['channelLen'] = channel_len
            params['targetAtrMult'] = atr_mult
            params['minRr'] = min_rr
            params.pop('useImpulseMacdFilter', None)
            params.pop('minConfidence', None)
            
            imacd_params = {
                "useImpulseMacdFilter": 1,
                "macdFast": 12,
                "macdSlow": 26,
                "macdSignal": 9
            }
            
            update_query = """
                UPDATE symbolStrategyConfig 
                SET parametersJson = %s, jsonIMACD = %s, enabled = 1 
                WHERE symbol = %s AND strategy = %s
            """
            cursor.execute(update_query, (json.dumps(params), json.dumps(imacd_params), symbol, strategy))
            updated_count += 1
            print(f"[OK] Actualizado {symbol}: minMlProb={params['minMlProb']}")
        else:
            params = {
                "minMlProb": min_prob / 100.0,
                "channelLen": channel_len,
                "targetAtrMult": atr_mult,
                "minRr": min_rr
            }
            imacd_params = {
                "useImpulseMacdFilter": 1,
                "macdFast": 12,
                "macdSlow": 26,
                "macdSignal": 9
            }
            insert_query = """
                INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson, jsonIMACD) 
                VALUES (%s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (strategy, symbol, 1, json.dumps(params), json.dumps(imacd_params)))
            inserted_count += 1
            print(f"[NEW] Insertado {symbol}: minMlProb={params['minMlProb']}")
            
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\nOperación finalizada. {updated_count} actualizados, {inserted_count} insertados.")

if __name__ == '__main__':
    update_configs()
