import asyncio
import httpx
import sys
import json
sys.path.insert(0, '/Volumes/TimeMachine/Webhook')
from services.forex_client import ForexClient

async def get_market_info():
    client = ForexClient(
        username="comoja66@gmail.comauui",
        password="PruebaJ34ny.",
        app_key="Ja.Morales",
        api_url="https://ciapi.cityindex.com/TradingApi"
    )
    await client.authenticate()
    headers = {"UserName": client.username, "Session": client.session_token}
    async with httpx.AsyncClient() as c:
        url = f"{client.api_url}/market/402044081/information"
        resp = await c.get(url, headers=headers)
        if resp.status_code == 200:
            info = resp.json().get("MarketInformation", {})
            print("Name:", info.get("Name"))
            print("WebMinSize:", info.get("WebMinSize"))
            print("IncrementSize:", info.get("IncrementSize"))
        else:
            print("Error")

asyncio.run(get_market_info())
