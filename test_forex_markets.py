import asyncio
import httpx
import sys
sys.path.insert(0, '/Volumes/TimeMachine/Webhook')
from services.forex_client import ForexClient

async def test():
    client = ForexClient(
        username="comoja66@gmail.comauui",
        password="PruebaJ34ny.",
        app_key="Ja.Morales",
        api_url="https://ciapi.cityindex.com/TradingApi"
    )
    await client.authenticate()
    
    url = f"{client.api_url}/market/searchwithtags"
    headers = {"UserName": client.username, "Session": client.session_token}
    
    async with httpx.AsyncClient() as c:
        resp = await c.get(url, params={"SearchByMarketName": "EUR", "MaxResults": 20}, headers=headers)
        print("Markets:", resp.text)

asyncio.run(test())
