import os
import sys
import asyncio
from datetime import datetime

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbManager
from middleware.execution.broker_gateway import gateway

async def insert_signal():
    print("🚀 Cargando datos de la cuenta MT5 (ID: 5)...")
    accounts = dbManager.getAccount(5)
    if not accounts:
        print("❌ Error: No se encontró la cuenta MT5 con ID 5.")
        return
        
    account = accounts[0]
    print(f"Cuenta cargada: {account['Nombre']} (ID: {account['idCuenta']})")
    
    # Asegurar que pase la verificación de cuenta activa
    dbManager.getAccount = lambda *args, **kwargs: [account]
    
    # 1. Definir tradeData (datos de la operación)
    tradeData = {
        "idTrade": None,
        "idCuenta": account['idCuenta'],
        "accountName": account['Nombre'],
        "symbol": "BTC/USD",
        "direction": "LARGO",
        "entryPrice": 63166.14976,
        "stopLoss": 62126.82368,
        "takeProfit": 64413.34105,
        "size": 0.03,
        "margin_used": 0.00,
        "intervalo": "15min",
        "strategy": "CruceEMA",
        "setup": "EMA Pullback + IMACD",
        "openTime": "2026-06-19 09:55:21",
        "status": "OPEN",
        "candleTime": "2026-06-19 09:55:21"
    }
    
    # 2. Definir signal (datos de la alerta para Telegram)
    signalDict = {
        "strategy": "CruceEMA",
        "symbol": "BTC/USD",
        "direction": "LARGO",
        "entryPrice": 63166.14976,
        "stopLoss": 62126.82368,
        "takeProfit": 64413.34105,
        "slDistance": 1039.32608,
        "riesgo_pips": 103932.6,
        "rr_ratio": 1.20,
        "confidence": 50.00,
        "setup": "EMA Pullback + IMACD",
        "status": "ACTIVA ✅",
        "candleTime": "2026-06-19 09:55:21",
        "intervalo": "15min",
        "profit": 36.31,
        "size": 0.03,
        "is_adjustment": True,
        "marketSentiment": 0.00,
        "latestMetrics": {
            "rsi": 50.00,
            "pendienteRsi": 0.0,
            "macdHist": 0.0,
            "atr": 692.884
        }
    }
    
    print("⚡ Insertando señal y ejecutando vía BrokerGateway...")
    
    # Cambiamos temporalmente a modo live para emular la ejecución en Broker
    original_mode = gateway.mode
    gateway.mode = "live"
    
    try:
        success, msgId = await gateway.execute_trade(
            trade_data=tradeData,
            signal=signalDict,
            account=account,
            strategy_name="CruceEMA"
        )
        
        if success:
            print(f"✅ Señal procesada con éxito. Mensaje enviado a Telegram: {msgId}")
        else:
            print(f"❌ Falló el procesamiento de la señal: {msgId}")
            
    except Exception as e:
        print(f"❌ Excepción ejecutando la señal: {e}")
    finally:
        gateway.mode = original_mode

if __name__ == "__main__":
    asyncio.run(insert_signal())
