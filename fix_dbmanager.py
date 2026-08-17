import re

with open("middleware/database/dbManager.py", "r") as f:
    content = f.read()

# Patrón 1: 
#        return False
#        if 'dbCursor' in locals(): dbCursor.close()
#        if 'dbConn' in locals(): dbConn.close()
# Que está mal indentado después del return, y falta finally.

pattern_cursor_conn = re.compile(
    r'(^[ \t]*return [^\n]+\n)([ \t]*if (?:cursor|dbCursor)(?:.*?):\s*(?:try:\s*)?(?:cursor|dbCursor)\.close\(\)\n(?:[ \t]*except:\s*pass\n)?)([ \t]*if (?:conn|dbConn)(?:.*?):\s*(?:try:\s*)?(?:conn|dbConn)\.close\(\)\n(?:[ \t]*except:\s*pass\n)?)', 
    re.MULTILINE
)

# Move the cleanup inside a finally block:
def repl_cursor_conn(match):
    indent = match.group(1).split("return")[0]
    # We remove the indent to align finally with except
    # except is usually at indent - 4 spaces.
    except_indent = indent[:-4] if len(indent) >= 4 else ""
    return (
        match.group(1) + 
        f"{except_indent}finally:\n" + 
        match.group(2) + 
        match.group(3)
    )

new_content = pattern_cursor_conn.sub(repl_cursor_conn, content)

# Patrón 2: 
#    except Exception as e:
#        ...
#        return ...
# (No hay cleanup).
# En saveStockPrices:
#    except Exception as e:
#        logger.error(...)
#        return 0
#    <empty line>
#    async def ...

pattern_save_stock = re.compile(
    r'(    except Exception as e:\n        logger.error\(f"Error guardando StockPrices para {symbol}: {e}"\)\n        return 0\n)',
    re.MULTILINE
)
new_content = pattern_save_stock.sub(r'\1    finally:\n        if \'dbCursor\' in locals(): dbCursor.close()\n        if \'dbConn\' in locals(): dbConn.close()\n\n', new_content)


with open("middleware/database/dbManager.py", "w") as f:
    f.write(new_content)

print("dbManager.py modificado.")
