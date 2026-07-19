import os
import re
import glob

ALL_FILES = glob.glob("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_*_optimization.py")
# excluding the main runner
ALL_FILES = [f for f in ALL_FILES if "run_all_optimizations.py" not in f and "compounding" not in f and "grid_search" not in f]

for filepath in ALL_FILES:
    with open(filepath, 'r') as f:
        content = f.read()

    # Find the "bestResults.append(" line which usually looks like:
    # bestResults.append(symbolBestCombo) OR bestResults.append(bestCombo) OR bestResults.append(best)
    append_match = re.search(r"(\s+)(bestResults\.append\((.*?)\))", content)
    if not append_match:
        print(f"Skipping {filepath}: no bestResults.append found")
        continue
    
    indent = append_match.group(1)
    combo_var_name = append_match.group(3)

    # Find the DB save block at the bottom, it usually starts with:
    # try:
    #     conn = dbConnection.getConnection()
    # OR
    # try:
    #     import json
    #     from middleware.database import dbConnection
    # AND ends with "conn.commit()" or "finally:" block.
    # We look for the "for combo in bestResults:" loop or "for sym in ALL_SYMBOLS:"
    
    # We will just insert a generic DB save function into each file, but we need the Strategy name!
    # Let's extract the strategy name from the SQL insert at the bottom.
    strategy_match = re.search(r"INSERT INTO symbolStrategyConfig[^\)]*\)\s*VALUES\s*\(\s*'([^']+)'", content, flags=re.IGNORECASE)
    if not strategy_match:
        print(f"Skipping {filepath}: No strategy name found in INSERT statement.")
        continue
    strategy_name = strategy_match.group(1)

    # To be extremely safe, we will just use the standard template, but what about the specific parameters?
    # We can extract the parameter dictionary assignment from the bottom block!
    params_match = re.search(r"params\s*=\s*({[^}]+})", content)
    if not params_match:
        print(f"Skipping {filepath}: No params dict found.")
        continue
    params_dict_str = params_match.group(1)
    
    # Replace `combo[` with `combo_var_name[` inside the params dict
    params_dict_str = re.sub(r"combo\[", f"{combo_var_name}[", params_dict_str)
    params_dict_str = re.sub(r"combo\.get\(", f"{combo_var_name}.get(", params_dict_str)
    
    # Same for imacd_params if it exists
    imacd_match = re.search(r"imacd_params\s*=\s*({[^}]+})", content)
    imacd_code = ""
    jsonIMACD_col = ""
    jsonIMACD_val = ""
    jsonIMACD_update = ""
    imacd_execute = ""
    if imacd_match:
        imacd_str = imacd_match.group(1)
        imacd_str = re.sub(r"combo\[", f"{combo_var_name}[", imacd_str)
        imacd_str = re.sub(r"combo\.get\(", f"{combo_var_name}.get(", imacd_str)
        imacd_code = f"""
            imacd_params = {imacd_str}
            imacdJson = json.dumps(imacd_params)
        """
        jsonIMACD_col = ", jsonIMACD"
        jsonIMACD_val = ", %s"
        jsonIMACD_update = ", jsonIMACD = VALUES(jsonIMACD)"
        imacd_execute = ", imacdJson"

    # Create the inline success block
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
    # Create the inline failure block
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

    # Inject into the content
    # First, the success block goes right after bestResults.append
    new_content = content.replace(
        append_match.group(0), 
        append_match.group(0) + inline_success
    )
    
    # Second, the failure block goes into the "else:" corresponding to the "if symbolBestCombo:"
    # We look for the "else:" block that prints "No se encontró combo viable"
    else_match = re.search(r"(\s+else:\s+\n\s+)(print\(f\"[\s\S]*?viable.*?\))", new_content)
    if not else_match:
        else_match = re.search(r"(\s+else:\s+\n\s+)(logger\.warning\(f\"[\s\S]*?viable.*?\))", new_content)

    if else_match:
        # replace the print with the print + inline_failure
        new_content = new_content.replace(
            else_match.group(2),
            else_match.group(2) + inline_failure
        )
    else:
        print(f"Could not find 'else' failure print in {filepath}")
    
    # Finally, remove the big block at the bottom
    # They usually start with "if not dfBest.empty:" or "if bestResults:" and contain the cursor.execute
    bottom_block_match = re.search(r"(\n\s+try:\s*\n\s+.*?(conn = dbConnection\.getConnection\(\)).*?conn\.commit\(\).*?)(?=\nif __name__)", new_content, flags=re.DOTALL)
    if bottom_block_match:
        new_content = new_content.replace(bottom_block_match.group(1), "\n")
    else:
        # try a different pattern for sniper / cruceema
        bottom_block_match = re.search(r"(\n\s+try:\s*\n\s+.*?(conn = dbConnection\.getConnection\(\)).*?conn\.commit\(\).*?)(?=\n\s*if __name__)", new_content, flags=re.DOTALL)
        if bottom_block_match:
            new_content = new_content.replace(bottom_block_match.group(1), "\n")
        else:
            print(f"Could not remove bottom block for {filepath}")

    with open(filepath, 'w') as f:
        f.write(new_content)
    
    print(f"✅ Patched inline DB saving for {filepath}")

