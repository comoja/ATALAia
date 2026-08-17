import os

STRATEGY_MAPPINGS = {
    'run_reversionmedia_optimization.py': {
        'strategy': 'ReversionMedia',
        'params': '{"ema_period": combo["EMA Period"], "z_score_threshold": combo["Z-Score"], "min_rr": combo["Min RR"]}'
    },
    'run_sesgobiashtf_optimization.py': {
        'strategy': 'SesgoBiasHTF',
        'params': '{"timeframe_htf": "D1", "sma_period": combo.get("SMA Period", 14), "min_rr": combo["Min RR"]}'
    },
    'run_breakoutprobability_optimization.py': {
        'strategy': 'BreakoutProbability',
        'params': '{"min_prob": combo.get("Min Prob", 0), "min_rr": combo["Min RR"]}'
    },
    'run_breakoutny_optimization.py': {
        'strategy': 'BreakoutNY',
        'params': '{"min_rr": combo["Min RR"]}'
    },
    'run_fvgdiario_optimization.py': {
        'strategy': 'FvgDiario',
        'params': '{"min_rr": combo["Min RR"]}'
    },
    'run_imbalance_optimization.py': {
        'strategy': 'Imbalance',
        'params': '{"min_rr": combo["Min RR"], "session": combo.get("Session", "NY")}'
    },
    'run_silverbullet_optimization.py': {
        'strategy': 'SilverBullet',
        'params': '{"min_rr": combo["Min RR"]}'
    },
    'run_speedbot_optimization.py': {
        'strategy': 'SpeedBot',
        'params': '{"threshold": combo.get("Threshold", 0.0), "min_rr": combo["Min RR"]}'
    },
    'run_qtrend_optimization.py': {
        'strategy': 'QTrend',
        'params': '{"min_rr": combo["Min RR"]}'
    },
    'run_ichimoku_optimization.py': {
        'strategy': 'Ichimoku',
        'params': '{"min_rr": combo["Min RR"]}'
    }
}

INLINE_SUCCESS = """            try:
                import json
                from middleware.database import dbConnection
                conn = dbConnection.getConnection()
                cursor = conn.cursor()
                combo = {combo_var}
                params = {params_dict}
                paramsJson = json.dumps(params)
                
                sql = \"\"\"
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                    VALUES ('{strategy}', %s, TRUE, %s)
                    ON DUPLICATE KEY UPDATE parametersJson = VALUES(parametersJson), enabled = TRUE
                \"\"\"
                cursor.execute(sql, (symbol, paramsJson))
                conn.commit()
                print(f"✅ DB: Guardado {symbol} (TRUE)")
            except Exception as e:
                print(f"❌ Error DB {symbol}: {e}")
            finally:
                if 'cursor' in locals(): cursor.close()
                if 'conn' in locals() and hasattr(conn, 'close'): conn.close()
"""

INLINE_FAILURE = """            try:
                from middleware.database import dbConnection
                conn = dbConnection.getConnection()
                cursor = conn.cursor()
                
                sql = \"\"\"
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                    VALUES ('{strategy}', %s, FALSE, '{}')
                    ON DUPLICATE KEY UPDATE enabled = FALSE
                \"\"\"
                cursor.execute(sql, (symbol,))
                conn.commit()
                print(f"✅ DB: Desactivado {symbol} (FALSE)")
            except Exception as e:
                print(f"❌ Error DB {symbol}: {e}")
            finally:
                if 'cursor' in locals(): cursor.close()
                if 'conn' in locals() and hasattr(conn, 'close'): conn.close()
"""

import re

def patch_files():
    base_dir = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting"
    for filename, config in STRATEGY_MAPPINGS.items():
        filepath = os.path.join(base_dir, filename)
        if not os.path.exists(filepath):
            continue
            
        with open(filepath, 'r') as f:
            content = f.read()
            
        # find append match inside loop
        append_match = re.search(r"(\s+)(bestResults\.append\((.*?)\))", content)
        if not append_match:
            print(f"Could not find append in {filename}")
            continue
            
        combo_var = append_match.group(3)
        
        # We need to make sure we don't inject multiple times
        if "INSERT INTO symbolStrategyConfig" in content and "VALUES ('" + config['strategy'] + "'" in content:
            # Maybe it already has it inline or at the bottom.
            # If it's at the bottom, we should remove the bottom block
            bottom_match = re.search(r"(\n\s+try:\s*\n.*?conn = dbConnection\.getConnection\(\).*?conn\.commit\(\).*?)(?=\nif __name__)", content, flags=re.DOTALL)
            if bottom_match:
                content = content.replace(bottom_match.group(1), "\n")
            elif "✅ DB: Guardado" in content:
                print(f"Already inline patched {filename}")
                continue
                
        # Inject inline
        success_block = INLINE_SUCCESS.replace('{strategy}', config['strategy']).replace('{params_dict}', config['params']).replace('{combo_var}', combo_var)
        failure_block = INLINE_FAILURE.replace('{strategy}', config['strategy'])
        
        new_content = content.replace(append_match.group(0), append_match.group(0) + "\n" + success_block)
        
        else_block = re.search(r"(\s+else:\s+\n\s+)(print\(f\"[^\"]+?\{symbol\}.*?\)|logger\.warning\(f\"[^\"]+?\{symbol\}.*?\))", new_content)
        if else_block:
             new_content = new_content.replace(else_block.group(2), else_block.group(2) + "\n" + failure_block)
        
        with open(filepath, 'w') as f:
            f.write(new_content)
        print(f"✅ Inline Patched {filename}")

if __name__ == "__main__":
    patch_files()
