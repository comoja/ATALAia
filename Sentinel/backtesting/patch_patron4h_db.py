import re

with open("run_patron4h_optimization.py", "r") as f:
    content = f.read()

# Remove the DB block at the bottom
db_block_pattern = r"(\s+try:\s+conn = dbConnection\.getConnection.*?)(?=\n\s+return|\n\s+if __name__)"
db_match = re.search(db_block_pattern, content, flags=re.DOTALL)
if db_match:
    content = content.replace(db_match.group(1), "")
else:
    print("DB block not found at bottom.")

# Inject DB logic inside the loop
# Find where bestResults.append(symbolBestCombo) is
target = "bestResults.append(symbolBestCombo)"
replacement = """bestResults.append(symbolBestCombo)
            
            # DB INJECTION START
            try:
                conn = dbConnection.getConnection()
                cursor = conn.cursor()
                params = {
                    "fvgMinPct": symbolBestCombo['FVG Min Pct'],
                    "displacementPct": symbolBestCombo['Displacement Pct'],
                    "rrRatioMin": symbolBestCombo['Min RR'],
                    "maxMinutosFvg": 240.0,
                    "minConfidence": symbolBestCombo['Min Conf'],
                    "lookback": 50
                }
                import json
                paramsJson = json.dumps(params)
                sql = \"\"\"
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                    VALUES ('Patron4HBot', %s, TRUE, %s)
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
            # DB INJECTION END
"""
content = content.replace(target, replacement)

target_else = """print(f"  ❌ No se encontró combo viable (WinRate >= 42%, PF >= 1.25) para {symbol}.")"""
replacement_else = """print(f"  ❌ No se encontró combo viable (WinRate >= 42%, PF >= 1.25) para {symbol}.")
            
            # DB INJECTION FALSE
            try:
                conn = dbConnection.getConnection()
                cursor = conn.cursor()
                sql = \"\"\"
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                    VALUES ('Patron4HBot', %s, FALSE, '{}')
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
            # DB INJECTION FALSE END
"""
content = content.replace(target_else, replacement_else)

with open("run_patron4h_optimization.py", "w") as f:
    f.write(content)
print("Patched patron4h to save DB instantly.")
