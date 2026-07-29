import asyncio
import httpx
import sys
sys.path.insert(0, '/Volumes/TimeMachine/Webhook')
from services.forex_client import ForexClient

async def get_decimals():
    client = ForexClient(
        username="comoja66@gmail.comauui",
        password="PruebaJ34ny.",
        app_key="Ja.Morales",
        api_url="https://ciapi.cityindex.com/TradingApi"
    )
    await client.authenticate()
    headers = {"UserName": client.username, "Session": client.session_token}
    async with httpx.AsyncClient() as c:
        for market in [406358904, 401449254, 402044422, 402044081]: # XAG/USD, USD/JPY, BTC/USD, EUR/USD
            url = f"{client.api_url}/market/{market}/information"
            resp = await c.get(url, headers=headers)
            if resp.status_code == 200:
                info = resp.json().get("MarketInformation", {})
                print(f"{info.get('Name')} -> PriceDecimalPlaces: {info.get('PriceDecimalPlaces')}")

asyncio.run(get_decimals())
