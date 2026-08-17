import asyncio
import httpx
import sys
sys.path.insert(0, '/Volumes/TimeMachine/Webhook')
from services.forex_client import ForexClient

async def test_order():
    client = ForexClient(
        username="comoja66@gmail.comauui",
        password="PruebaJ34ny.",
        app_key="Ja.Morales",
        api_url="https://ciapi.cityindex.com/TradingApi"
    )
    await client.authenticate()
    headers = {"UserName": client.username, "Session": client.session_token}
    
    payload = {
        "Direction": "sell",
        "Quantity": 0.3510000000000003,
        "MarketId": 402044422,
        "TradingAccountId": client.trading_account_id,
        "AuditId": "WebhookOrder"
    }
    
    async with httpx.AsyncClient() as c:
        url = f"{client.api_url}/order/newtradeorder"
        # First with bad quantity
        resp = await c.post(url, json=payload, headers=headers)
        print("Bad Quantity:", resp.json())
        
        # Now with rounded quantity
        payload["Quantity"] = round(0.3510000000000003, 2)
        resp2 = await c.post(url, json=payload, headers=headers)
        print("Rounded Quantity:", resp2.json())

asyncio.run(test_order())
