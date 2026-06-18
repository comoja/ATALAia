import os
import sys
import pandas as pd
import json
import argparse

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database.dbConnection import getConnection

def update_configs(csv_path: str, strategy: str):
    if not os.path.exists(csv_path):
        print(f"Error: {csv_path} no existe.")
        return

    df = pd.read_csv(csv_path)
    if df.empty:
        print("El archivo CSV está vacío.")
        return
        
    conn = getConnection()
    if not conn:
        print("Fallo de conexión a la base de datos.")
        return
        
    cursor = conn.cursor(dictionary=True)
    
    # Todos los símbolos
    all_symbols = [
        'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
        'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
        'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
    ]
    
    # Manejar mayúsculas/minúsculas en el CSV
    symbol_col = 'Símbolo' if 'Símbolo' in df.columns else 'symbol'
    pf_col = 'Profit Factor' if 'Profit Factor' in df.columns else 'profitFactor'
    
    winning_symbols = df[symbol_col].tolist()
    
    updated_count = 0
    inserted_count = 0
    blocked_count = 0
    
    for symbol in all_symbols:
        if symbol in winning_symbols:
            row = df[df[symbol_col] == symbol].iloc[0]
            pf = float(row[pf_col])
            
            # Extraer parámetros dinámicamente según la estrategia
            params = {}
            imacd_params = None
            
            # Obtener el nombre del símbolo en el CSV
            symbol_key = symbol
            
            if strategy == 'Sniper':
                params['minMlProb'] = float(row['probaThresholdLong']) if 'probaThresholdLong' in row else float(row.get('Min Prob', 65.0))/100.0
                params['minRr'] = float(row['minRr']) if 'minRr' in row else float(row.get('Min RR', 1.5))
            elif strategy == 'SilverBullet':
                params['minMlProb'] = float(row['Min Prob'])/100.0 if 'Min Prob' in row else float(row.get('probaThresholdLong', 65.0))/100.0
                params['minRr'] = float(row['Min RR']) if 'Min RR' in row else float(row.get('minRr', 1.5))
            elif strategy in ['GenericFVG', 'FVGDiario', 'FvgDiario']:
                min_conf = float(row.get('Min Conf', row.get('Min Prob', 65.0)))
                params['minMlProb'] = min_conf / 100.0 if min_conf > 1.0 else min_conf
                params['minRr'] = float(row.get('Min RR', row.get('minRr', 1.5)))
                params['requireHtfSweep'] = int(row.get('Require Sweep', row.get('RequireHtfSweep', 0)))
            elif strategy == 'BreakoutProbability':
                min_prob = float(row.get('Min Prob', row.get('Min Conf', 65.0)))
                params['minMlProb'] = min_prob / 100.0 if min_prob > 1.0 else min_prob
                params['channelLen'] = int(row.get('Channel Len', 20))
                params['targetAtrMult'] = float(row.get('ATR Mult', 1.5))
                params['minRr'] = float(row.get('Min RR', 1.5))
                imacd_params = {
                    "useImpulseMacdFilter": 1,
                    "macdFast": 12,
                    "macdSlow": 26,
                    "macdSignal": 9
                }
            elif strategy == 'BreakoutNY':
                params['minRr'] = float(row.get('Min RR', 1.5))
                params['rangeDuration'] = int(row.get('Range Duration', 15))
                params['tradingWindow'] = int(row.get('Trading Window', 150))
            elif strategy == 'CruceEMA':
                params['emaFast'] = int(row.get('EMA Fast', 10))
                params['emaSlow'] = int(row.get('EMA Slow', 20))
                params['minRr'] = float(row.get('Min RR', 1.5))
                imacd_params = {
                    "useImpulseMacdFilter": 1,
                    "macdFast": 12,
                    "macdSlow": 26,
                    "macdSignal": 9
                }
            elif strategy == 'Patron4h':
                min_conf = float(row.get('Min Conf', 70.0))
                params['minMlProb'] = min_conf / 100.0 if min_conf > 1.0 else min_conf
                params['minRr'] = float(row.get('Min RR', 1.5))
                params['displacementPct'] = float(row.get('Displacement Pct', 0.0005))
                params['fvgMinPct'] = float(row.get('FVG Min Pct', 0.00005))
            elif strategy == 'SpeedBot':
                params['atrMult'] = float(row.get('ATR Mult', 1.5))
                params['bodyRatio'] = float(row.get('Body Ratio', 0.75))
                params['confirmRatio'] = float(row.get('Confirm Ratio', 0.5))
                params['minRr'] = float(row.get('Min RR', 1.5))
            elif strategy == 'Ichimoku':
                params['tenkanPeriod'] = int(row.get('Tenkan', 9))
                params['kijunPeriod'] = int(row.get('Kijun', 26))
                params['senkouPeriod'] = int(row.get('Senkou', 52))
                params['displacementPeriod'] = int(row.get('Displacement', 26))
                params['minRr'] = float(row.get('Min RR', 1.5))
            elif strategy in ['Imbalance', 'ImbalanceLDN', 'ImbalanceNY', 'ImbalancePMNY']:
                min_conf = float(row.get('Min Conf', 65.0))
                params['minMlProb'] = min_conf / 100.0 if min_conf > 1.0 else min_conf
                params['minRr'] = float(row.get('Min RR', 1.5))
                params['maxMinutosFvg'] = int(row.get('Max Minutos Fvg', 30))
            elif strategy == 'QTrend':
                params['supertrendPeriod'] = int(row.get('supertrendPeriod', 14))
                params['supertrendMultiplier'] = float(row.get('supertrendMultiplier', 3.0))
                params['qtrendFast'] = int(row.get('qtrendFast', 12))
                params['qtrendSlow'] = int(row.get('qtrendSlow', 26))
                params['minRr'] = float(row.get('tpParam', row.get('Min RR', 1.5)))
            elif strategy == 'ReversionMedia':
                params['emaPeriod'] = int(row.get('EMA Period', 20))
                params['zScoreEntry'] = float(row.get('Z-Score Entry', 2.0))
                params['zScoreExit'] = float(row.get('Z-Score Exit', 0.0))
                params['minRr'] = float(row.get('Min RR', 1.5))
            elif strategy == 'SesgoBiasHTF':
                params['biasPeriod'] = int(row.get('Bias Period', 14))
                params['atrMultSl'] = float(row.get('ATR Mult SL', 1.5))
                params['minRr'] = float(row.get('Min RR', 1.5))
            elif strategy == 'PremiumConfluence':
                params['supertrend_period'] = 10
                params['supertrend_mult'] = float(row.get('Supertrend Mult', 1.5))
                params['ha_period1'] = int(row.get('HA Period', 10))
                params['ha_period2'] = int(row.get('HA Period', 10))
                
                macd_params_str = str(row.get('MACD Params', "20,9"))
                parts = macd_params_str.split(',')
                macd_len = int(parts[0]) if len(parts) > 0 else 20
                macd_sig = int(parts[1]) if len(parts) > 1 else 9
                
                params['macd_length'] = macd_len
                params['macd_signal'] = macd_sig
                params['min_rr'] = float(row.get('Min RR', 1.5))
                
                imacd_params = {
                    "useImpulseMacdFilter": 1,
                    "macdFast": 12,
                    "macdSlow": macd_len,
                    "macdSignal": macd_sig
                }
            else:
                # Fallback genérico
                if 'Min RR' in row: params['minRr'] = float(row['Min RR'])
                if 'minRr' in row: params['minRr'] = float(row['minRr'])
                if 'Min Prob' in row: params['minMlProb'] = float(row['Min Prob'])/100.0
                if 'probaThresholdLong' in row: params['minMlProb'] = float(row['probaThresholdLong'])
                
            # Remover minConfidence redundante de parametersJson
            params.pop('minConfidence', None)
            
            enabled_flag = 1 if pf >= 1.25 else 0
            
            # Consultar si ya existe
            cursor.execute("SELECT parametersJson FROM symbolStrategyConfig WHERE symbol = %s AND strategy = %s", (symbol, strategy))
            result = cursor.fetchone()
            
            if result:
                existing_params = json.loads(result['parametersJson']) if result['parametersJson'] else {}
                existing_params.update(params)
                # También remover minConfidence de la configuración existente
                existing_params.pop('minConfidence', None)
                
                if imacd_params:
                    update_query = "UPDATE symbolStrategyConfig SET parametersJson = %s, jsonIMACD = %s, enabled = %s WHERE symbol = %s AND strategy = %s"
                    cursor.execute(update_query, (json.dumps(existing_params), json.dumps(imacd_params), enabled_flag, symbol, strategy))
                else:
                    update_query = "UPDATE symbolStrategyConfig SET parametersJson = %s, enabled = %s WHERE symbol = %s AND strategy = %s"
                    cursor.execute(update_query, (json.dumps(existing_params), enabled_flag, symbol, strategy))
                updated_count += 1
                status_str = "ACTIVADO" if enabled_flag else "INACTIVO"
                print(f"[OK] Actualizado ({status_str}) {symbol}: PF={pf}")
            else:
                if imacd_params:
                    insert_query = """
                        INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson, jsonIMACD) 
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (strategy, symbol, enabled_flag, json.dumps(params), json.dumps(imacd_params)))
                else:
                    insert_query = """
                        INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson) 
                        VALUES (%s, %s, %s, %s)
                    """
                    cursor.execute(insert_query, (strategy, symbol, enabled_flag, json.dumps(params)))
                inserted_count += 1
                status_str = "ACTIVADO" if enabled_flag else "INACTIVO"
                print(f"[NEW] Insertado ({status_str}) {symbol}: PF={pf}")
                
            # Eliminar de la tabla de bloqueos si estuviera bloqueado
            cursor.execute("DELETE FROM symbolNotStrategia WHERE symbol = %s AND strategy = %s", (symbol, strategy))
            
        else:
            # Procesar perdedor definitivo (PF < 1.0 incluso con Deep Grid)
            cursor.execute("UPDATE symbolStrategyConfig SET enabled = 0 WHERE symbol = %s AND strategy = %s", (symbol, strategy))
            
            cursor.execute("SELECT 1 FROM symbolNotStrategia WHERE symbol = %s AND strategy = %s", (symbol, strategy))
            if not cursor.fetchone():
                reason = f"Bloqueado por optimizador Deep Grid {strategy}: Ninguna combinación supera PF 1.0."
                cursor.execute(
                    "INSERT INTO symbolNotStrategia (symbol, strategy, reason) VALUES (%s, %s, %s)",
                    (symbol, strategy, reason)
                )
                blocked_count += 1
                print(f"[BLOCKED] Bloqueado PERDEDOR absoluto: {symbol}")
            else:
                print(f"[SKIP] Ya estaba bloqueado: {symbol}")
                
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"\nOperación finalizada. {updated_count} actualizados, {inserted_count} insertados, {blocked_count} bloqueados.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Inyectar resultados de optimización a BD.")
    parser.add_argument('--csv', type=str, required=True, help="Ruta al CSV con mejores resultados")
    parser.add_argument('--strategy', type=str, required=True, help="Nombre de la estrategia")
    args = parser.parse_args()
    
    update_configs(args.csv, args.strategy)
