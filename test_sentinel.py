import sys, os, asyncio
project_root = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, project_root)

from middleware.execution.broker_gateway import gateway
from middleware.database import dbManager

async def test_sentinel():
    print("Obteniendo cuenta 2...")
    accounts = dbManager.getAccount(2)
    account = accounts[0]
    
    tradeData = {
        "idTrade": None,
        "idCuenta": 2,
        "accountName": account.get('Nombre', 'N/A'),
        "symbol": "EUR/GBP",
        "direction": "CORTO",
        "entryPrice": 0.85444,
        "stopLoss": 0.85463,
        "takeProfit": 0.85417,
        "size": 17000.0,
        "margin_used": 0.0,
        "intervalo": "15min",
        "strategy": "Sniper",
        "setup": "Test Setup",
        "openTime": "2026-07-23 23:35:00",
        "status": "OPEN",
        "candleTime": "2026-07-23 23:35:00"
    }
    
    signalDict = {
        "strategy": "Sniper",
        "symbol": "EUR/GBP",
        "direction": "CORTO",
        "entryPrice": 0.85444,
        "stopLoss": 0.85463,
        "takeProfit": 0.85417,
        "slDistance": 0.00019,
        "riesgo_pips": 1.9,
        "rr_ratio": 1.42,
        "confidence": 99.9,
        "setup": "Test Setup",
        "status": "ACTIVA ✅",
        "candleTime": "2026-07-23 23:35:00",
        "intervalo": "15min",
        "profit": 5.0,
        "size": 17000.0,
        "is_adjustment": False,
        "marketSentiment": 0.0,
        "latestMetrics": {}
    }
    
    print("Ejecutando trade_data...")
    success, msgId = await gateway.execute_trade(tradeData, signalDict, account, "Sniper")
    print(f"Resultado: {success}, msgId: {msgId}")

if __name__ == "__main__":
    asyncio.run(test_sentinel())
