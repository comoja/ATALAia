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
        url = f"{client.api_url}/market/402044422/information"
        resp = await c.get(url, headers=headers)
        if resp.status_code == 200:
            info = resp.json().get("MarketInformation", {})
            print("WebMinOrderSize:", info.get("WebMinOrderSize"))
            print("WebMinOrderSizeMultiplier:", info.get("WebMinOrderSizeMultiplier"))
            print("MinQuantity:", info.get("MinQuantity"))
            print("MaxQuantity:", info.get("MaxQuantity"))
            print("TradingStartTime:", info.get("TradingStartTime"))
            print("TradingEndTime:", info.get("TradingEndTime"))
            print("PriceTolerance:", info.get("PriceTolerance"))
            print(json.dumps(info, indent=2))
        else:
            print("Error", resp.status_code, resp.text)

asyncio.run(get_market_info())
