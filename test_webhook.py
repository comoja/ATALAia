import requests
import json
import sqlite3
import mysql.connector

# We can query the database to get MT5 id if needed. But let's try 1 and 2
for acc_id in [1, 2]:
    payload = {
        "strategy": "CRUCE EMA",
        "symbol": "AUD/USD",
        "direction": "VENTA",
        "entryPrice": 0.69974,
        "stopLoss": 0.70079,
        "takeProfit": 0.69785,
        "size": 15000.0,
        "confidence": 50.0,
        "setup": "EMA Pullback + IMACD",
        "is_adjustment": False,
        "idCuenta": acc_id
    }
    
    print(f"Trying idCuenta: {acc_id}")
    try:
        response = requests.post("http://127.0.0.1:8000/webhook/tradingview", json=payload)
        print(f"Status Code: {response.status_code}")
        print("Response:")
        print(json.dumps(response.json(), indent=2))
        if response.status_code == 200:
            break
    except Exception as e:
        print(f"Error: {e}")
