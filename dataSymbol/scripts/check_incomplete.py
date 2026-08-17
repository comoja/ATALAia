import sys
import os
import pandas as pd
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from middleware.database.dbManager import _call_connection_pool

res = _call_connection_pool("GET", "/candles/symbol/EUR%2FUSD/timeframe/1h", params={"limit": 10})
if res and isinstance(res, list):
    print("=== Primeras 10 velas 1h EUR/USD via ConnectionPool ===")
    for row in res:
        ts = pd.to_datetime(row["timestamp"])
        print(f"{ts} | hour: {ts.hour}")
else:
    print("No se pudo obtener respuesta del microservicio ConnectionPool.")