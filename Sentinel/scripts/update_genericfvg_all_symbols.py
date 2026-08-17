import os
import sys
import pandas as pd
import json

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database.dbConnection import getConnection

def update_configs():
    best_results_path = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/genericfvg_grid_results_best.csv"
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
    strategy = "GenericFVG"
    
    # Todos los símbolos
    all_symbols = [
        'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
        'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
        'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
    ]
    
    winning_symbols = df['Símbolo'].tolist()
    
    updated_count = 0
    inserted_count = 0
    blocked_count = 0
    
    for symbol in all_symbols:
        if symbol in winning_symbols:
            # Procesar ganador
            row = df[df['Símbolo'] == symbol].iloc[0]
            min_rr = float(row['Min RR'])
            min_conf = float(row['Min Conf'])
            req_sweep = bool(row['Require Sweep'])
            
            # Consultar si ya existe
            cursor.execute("SELECT parametersJson FROM symbolStrategyConfig WHERE symbol = %s AND strategy = %s", (symbol, strategy))
            result = cursor.fetchone()
            
            if result:
                params = json.loads(result['parametersJson']) if result['parametersJson'] else {}
                params['minMlProb'] = min_conf / 100.0  # Mapear min_conf a minMlProb en BD para GenericFVG
                params['minRr'] = min_rr
                params['requireHtfSweep'] = int(req_sweep)
                params.pop('minConfidence', None) # Quitar redundancia de minConfidence
                
                update_query = "UPDATE symbolStrategyConfig SET parametersJson = %s, enabled = 1 WHERE symbol = %s AND strategy = %s"
                cursor.execute(update_query, (json.dumps(params), symbol, strategy))
                updated_count += 1
                print(f"[OK] Actualizado GANADOR {symbol}: minMlProb={params['minMlProb']}")
            else:
                params = {
                    "minMlProb": min_conf / 100.0,
                    "minRr": min_rr,
                    "requireHtfSweep": int(req_sweep)
                }
                params.pop('minConfidence', None) # Quitar redundancia de minConfidence
                imacd_params = {
                    "useImpulseMacdFilter": 0,
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
                print(f"[NEW] Insertado GANADOR {symbol}: minMlProb={params['minMlProb']}")
                
            # Eliminar de la tabla de bloqueos si estuviera bloqueado
            cursor.execute("DELETE FROM symbolNotStrategia WHERE symbol = %s AND strategy = %s", (symbol, strategy))
            
        else:
            # Procesar perdedor -> Deshabilitar y bloquear
            cursor.execute("UPDATE symbolStrategyConfig SET enabled = 0 WHERE symbol = %s AND strategy = %s", (symbol, strategy))
            
            cursor.execute("SELECT 1 FROM symbolNotStrategia WHERE symbol = %s AND strategy = %s", (symbol, strategy))
            if not cursor.fetchone():
                reason = "Bloqueado por optimizador intensivo GenericFVG: Sin configuraciones rentables."
                cursor.execute(
                    "INSERT INTO symbolNotStrategia (symbol, strategy, reason) VALUES (%s, %s, %s)",
                    (symbol, strategy, reason)
                )
                blocked_count += 1
                print(f"[BLOCKED] Bloqueado PERDEDOR: {symbol}")
            else:
                print(f"[SKIP] Ya estaba bloqueado PERDEDOR: {symbol}")
                
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\nOperación finalizada. {updated_count} actualizados, {inserted_count} insertados, {blocked_count} bloqueados.")

if __name__ == '__main__':
    update_configs()
