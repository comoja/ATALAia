import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="M1x&J34ny",
    database="ATALAia"
)

cursor = conn.cursor()

cursor.execute("SELECT symbol, timestamp FROM candles WHERE timeframe = '1h' AND symbol = 'EUR/USD' ORDER BY timestamp LIMIT 10")
print("=== Primeras 10 velas 1h EUR/USD (verificar si hour=00,01,02...) ===")
for row in cursor:
    ts = row[1]
    print(f"{ts} | hour: {ts.hour}")

conn.close()