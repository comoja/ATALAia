import re
import glob

ALL_FILES = glob.glob("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_*_optimization.py")
ALL_FILES = [f for f in ALL_FILES if "run_all_optimizations.py" not in f and "compounding" not in f and "grid_search" not in f]

for filepath in ALL_FILES:
    with open(filepath, 'r') as f:
        content = f.read()
    
    # 1. SPLIT the file safely to find the bottom block
    # Usually after dfBest = pd.DataFrame(...) or similar
    parts = re.split(r"(dfBest = pd\.DataFrame\(.*?|if bestResults:)", content)
    if len(parts) < 3:
        print(f"Skipping {filepath}: could not split by dfBest/bestResults")
        continue
    
    top_part = "".join(parts[:-1])
    bottom_part = parts[-1]
    
    # 2. In the bottom_part, find the DB save block
    db_block_match = re.search(r"(\s+try:\s*\n\s+conn = dbConnection\.getConnection\(\).*?)(?=\s*if __name__ ==)", bottom_part, flags=re.DOTALL)
    if not db_block_match:
        # maybe it has import json inside try
        db_block_match = re.search(r"(\s+try:\s*\n.*conn = dbConnection\.getConnection\(\).*?)(?=\s*if __name__ ==)", bottom_part, flags=re.DOTALL)
        if not db_block_match:
            print(f"Skipping {filepath}: could not find db block at bottom")
            continue
            
    db_block = db_block_match.group(1)
    
    # 3. Extract strategy name
    strategy_match = re.search(r"INSERT INTO symbolStrategyConfig[^\)]*\)\s*VALUES\s*\(\s*'([^']+)'", db_block, flags=re.IGNORECASE)
    if not strategy_match:
        print(f"Skipping {filepath}: No strategy name found")
        continue
    strategy_name = strategy_match.group(1)
    
    # 4. Extract params dict
    params_match = re.search(r"params\s*=\s*({[^}]+})", db_block)
    if not params_match:
        print(f"Skipping {filepath}: No params found")
        continue
    params_dict_str = params_match.group(1)
    
    imacd_match = re.search(r"imacd_params\s*=\s*({[^}]+})", db_block)
    
    # Now find append match in TOP PART
    append_match = re.search(r"(\s+)(bestResults\.append\((.*?)\))", top_part)
    if not append_match:
        print(f"Skipping {filepath}: No bestResults.append found in top part")
        continue
        
    combo_var_name = append_match.group(3)
    params_dict_str = re.sub(r"\bcombo\[", f"{combo_var_name}[", params_dict_str)
    params_dict_str = re.sub(r"\bcombo\.get\(", f"{combo_var_name}.get(", params_dict_str)
    
    imacd_code = ""
    jsonIMACD_col = ""
    jsonIMACD_val = ""
    jsonIMACD_update = ""
    imacd_execute = ""
    if imacd_match:
        imacd_str = imacd_match.group(1)
        imacd_str = re.sub(r"\bcombo\[", f"{combo_var_name}[", imacd_str)
        imacd_str = re.sub(r"\bcombo\.get\(", f"{combo_var_name}.get(", imacd_str)
        imacd_code = f"""
            imacd_params = {imacd_str}
            imacdJson = json.dumps(imacd_params)
        """
        jsonIMACD_col = ", jsonIMACD"
        jsonIMACD_val = ", %s"
        jsonIMACD_update = ", jsonIMACD = VALUES(jsonIMACD)"
        imacd_execute = ", imacdJson"

    inline_success = f"""
            try:
                import json
                from middleware.database import dbConnection
                conn = dbConnection.getConnection()
                cursor = conn.cursor()
                
                params = {params_dict_str}
                paramsJson = json.dumps(params)
                {imacd_code}
                
                sql = \"\"\"
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson{jsonIMACD_col})
                    VALUES ('{strategy_name}', %s, TRUE, %s{jsonIMACD_val})
                    ON DUPLICATE KEY UPDATE parametersJson = VALUES(parametersJson){jsonIMACD_update}, enabled = TRUE
                \"\"\"
                cursor.execute(sql, (symbol, paramsJson{imacd_execute}))
                conn.commit()
                print(f"✅ DB: Guardado {{symbol}} (TRUE)")
            except Exception as e:
                print(f"❌ Error DB {{symbol}}: {{e}}")
            finally:
                if 'cursor' in locals(): cursor.close()
                if 'conn' in locals() and hasattr(conn, 'close'): conn.close()
"""
    inline_failure = f"""
            try:
                from middleware.database import dbConnection
                conn = dbConnection.getConnection()
                cursor = conn.cursor()
                
                sql = \"\"\"
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                    VALUES ('{strategy_name}', %s, FALSE, '{{}}')
                    ON DUPLICATE KEY UPDATE enabled = FALSE
                \"\"\"
                cursor.execute(sql, (symbol,))
                conn.commit()
                print(f"✅ DB: Desactivado {{symbol}} (FALSE)")
            except Exception as e:
                print(f"❌ Error DB {{symbol}}: {{e}}")
            finally:
                if 'cursor' in locals(): cursor.close()
                if 'conn' in locals() and hasattr(conn, 'close'): conn.close()
"""

    top_part = top_part.replace(append_match.group(0), append_match.group(0) + inline_success)
    
    # find else failure
    else_match = re.search(r"(\s+else:\s+\n\s+)(logger\.warning\(f\"[\s\S]*?viable.*?\)|print\(f\"[\s\S]*?viable.*?\))", top_part)
    if else_match:
        top_part = top_part.replace(else_match.group(2), else_match.group(2) + inline_failure)
    else:
        # Some scripts might not have the "viable" print, or the print is different
        # fallback: find else that matches the if of bestResults
        else_block = re.search(r"(\s+else:\s+\n\s+)(print\(f\"[^\"]+?\{symbol\}.*?\)|logger\.warning\(f\"[^\"]+?\{symbol\}.*?\))", top_part)
        if else_block:
             top_part = top_part.replace(else_block.group(2), else_block.group(2) + inline_failure)
        else:
             print(f"Could not find else print for {filepath}")

    # remove bottom block
    bottom_part = bottom_part.replace(db_block, "\n")
    
    with open(filepath, 'w') as f:
        f.write(top_part + bottom_part)
    
    print(f"✅ Patched {filepath}")

