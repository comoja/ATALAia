import asyncio
import httpx
import sys
import os
import pymysql
import time
sys.path.insert(0, '/Volumes/TimeMachine/Webhook')
from services.forex_client import ForexClient

async def update_symbols():
    client = ForexClient(
        username="comoja66@gmail.comauui",
        password="PruebaJ34ny.",
        app_key="Ja.Morales",
        api_url="https://ciapi.cityindex.com/TradingApi"
    )
    for i in range(5):
        if await client.authenticate():
            break
        time.sleep(2)
        
    if not client.session_token:
        print("Final Login Failed")
        return
        
    db_host = os.getenv("DB_HOST", "192.168.68.54")
    db_user = os.getenv("DB_USER", "root")
    db_password = os.getenv("DB_PASSWORD", "M1x&J34ny")
    db_name = os.getenv("DB_DATABASE", "ATALAia")
    
    conn = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        cursorclass=pymysql.cursors.DictCursor
    )
    
    headers = {"UserName": client.username, "Session": client.session_token}
    
    with conn.cursor() as cursor:
        cursor.execute("SELECT symbol FROM SentinelSymbol WHERE Activo = 1")
        symbols = [row['symbol'] for row in cursor.fetchall()]
        
        async with httpx.AsyncClient() as c:
            for sym in symbols:
                query = sym.replace("/", "")
                url = f"{client.api_url}/market/searchwithtags"
                resp = await c.get(url, params={"Query": query, "MaxResults": 10}, headers=headers)
                if resp.status_code == 200:
                    data = resp.json()
                    markets = data.get("Markets", [])
                    found = False
                    for m in markets:
                        if m.get("Name") == sym:
                            print(f"Found {sym} -> MarketId: {m.get('MarketId')}")
                            # ACTUALLY UPDATE DB
                            cursor.execute("UPDATE SentinelSymbol SET idForex = %s WHERE symbol = %s", (m.get('MarketId'), sym))
                            # ALSO update availableSymbols, RatioSymbol if needed? The user said "todos los instrumentos en SentinelSymbol"
                            found = True
                            break
                    if not found:
                        print(f"NOT FOUND EXACT MATCH: {sym} -> {data}")
                else:
                    print(f"Error fetching {sym}: {resp.status_code} - {resp.text}")
                
        conn.commit()

asyncio.run(update_symbols())
