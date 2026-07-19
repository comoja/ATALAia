import os
import re

STRATEGY_MAPPINGS = {
    'run_cruceema_optimization.py': {
        'strategy': 'CruceEMA',
        'params': '{"emaFast": combo["EMA Fast"], "emaSlow": combo["EMA Slow"], "minRr": combo["Min RR"]}',
        'imacd': '{"useImpulseMacdFilter": 1, "macdFast": 12, "macdSlow": combo["IMACD Slow"], "macdSignal": combo["IMACD Signal"]}'
    },
    'run_genericfvg_optimization.py': {
        'strategy': 'GenericFVG',
        'params': '{"fvgMinPct": combo["FVG Min Pct"], "displacementPct": combo["Displacement Pct"], "minRr": combo["Min RR"], "maxMinutosFvg": 240.0, "minConfidence": 0}',
        'imacd': '{"useImpulseMacdFilter": 1, "macdFast": 12, "macdSlow": combo["IMACD Slow"], "macdSignal": combo["IMACD Signal"]}'
    },
    'run_sniper_optimization.py': {
        'strategy': 'Sniper',
        'params': '{"minRr": combo["minRr"], "minConfidence": combo["minConfidence"], "probaThresholdLong": combo["probaThresholdLong"]}'
    },
    'run_patron4h_optimization.py': {
        'strategy': 'Patron4h', # DB uses Patron4H but let's check
        'params': '{"fvgMinPct": combo["FVG Min Pct"], "displacementPct": combo["Displacement Pct"], "rrRatioMin": combo["Min RR"], "maxMinutosFvg": 240.0, "minConfidence": combo["Min Conf"], "lookback": 50}'
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
                {imacd_block}
                sql = \"\"\"
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson{imacd_col})
                    VALUES ('{strategy}', %s, TRUE, %s{imacd_val})
                    ON DUPLICATE KEY UPDATE parametersJson = VALUES(parametersJson){imacd_upd}, enabled = TRUE
                \"\"\"
                cursor.execute(sql, (symbol, paramsJson{imacd_exec}))
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

def patch_files():
    base_dir = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting"
    for filename, config in STRATEGY_MAPPINGS.items():
        filepath = os.path.join(base_dir, filename)
        if not os.path.exists(filepath):
            continue
            
        with open(filepath, 'r') as f:
            content = f.read()
            
        append_match = re.search(r"(\s+)(bestResults\.append\((.*?)\))", content)
        if not append_match:
            print(f"Could not find append in {filename}")
            continue
            
        combo_var = append_match.group(3)
        
        if "✅ DB: Guardado" in content:
            print(f"Already patched {filename}")
            continue

        bottom_match = re.search(r"(\n\s+try:\s*\n.*?conn = dbConnection\.getConnection\(\).*?conn\.commit\(\).*?)(?=\nif __name__|\n\s*if __name__)", content, flags=re.DOTALL)
        if bottom_match:
            content = content.replace(bottom_match.group(1), "\n")

        imacd_block = ""
        imacd_col = ""
        imacd_val = ""
        imacd_upd = ""
        imacd_exec = ""
        if 'imacd' in config:
            imacd_block = f"imacd_params = {config['imacd']}\n                imacdJson = json.dumps(imacd_params)"
            imacd_col = ", jsonIMACD"
            imacd_val = ", %s"
            imacd_upd = ", jsonIMACD = VALUES(jsonIMACD)"
            imacd_exec = ", imacdJson"

        success_block = INLINE_SUCCESS.replace('{strategy}', config['strategy']) \
            .replace('{params_dict}', config['params']) \
            .replace('{combo_var}', combo_var) \
            .replace('{imacd_block}', imacd_block) \
            .replace('{imacd_col}', imacd_col) \
            .replace('{imacd_val}', imacd_val) \
            .replace('{imacd_upd}', imacd_upd) \
            .replace('{imacd_exec}', imacd_exec)
            
        failure_block = INLINE_FAILURE.replace('{strategy}', config['strategy'])
        
        new_content = content.replace(append_match.group(0), append_match.group(0) + "\n" + success_block)
        
        else_block = re.search(r"(\s+else:\s+\n\s+)(logger\.warning\(f\"[^\"]+?\{symbol\}.*?\)|print\(f\"[^\"]+?\{symbol\}.*?\))", new_content)
        if else_block:
             new_content = new_content.replace(else_block.group(2), else_block.group(2) + "\n" + failure_block)
        
        with open(filepath, 'w') as f:
            f.write(new_content)
        print(f"✅ Inline Patched {filename}")

if __name__ == "__main__":
    patch_files()
