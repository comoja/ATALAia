import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/api/routes.py"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

content = content.replace("SELECT bid_c FROM candles WHERE UPPER(REPLACE(symbol, '/', '')) = :s ORDER BY datetime", "SELECT close FROM candles WHERE UPPER(REPLACE(symbol, '/', '')) = :s ORDER BY timestamp")
content = content.replace("SELECT closePrice FROM candles WHERE UPPER(REPLACE(symbol, '/', '')) = :s ORDER BY datetime", "SELECT close FROM candles WHERE UPPER(REPLACE(symbol, '/', '')) = :s ORDER BY timestamp")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Fixed close and timestamp in routes.py")
