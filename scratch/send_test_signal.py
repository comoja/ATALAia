import sys
import os
import asyncio
from datetime import datetime

# Configuración de rutas
projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from middleware.database import dbManager
from middleware.execution.broker_gateway import gateway
from Sentinel.analysis import risk

# Mockear filtros para la prueba
dbManager.is_trade_duplicate = lambda *args, **kwargs: False
dbManager.is_alert_sent = lambda *args, **kwargs: False
dbManager.buscaTrade = lambda *args, **kwargs: None
risk.isDailyDrawdownLimitReached = lambda *args, **kwargs: False

async def main():
    print("Obteniendo cuenta de destino...")
    accounts = dbManager.getAccount()
    
    testAccount = None
    for acc in accounts:
        if acc['idCuenta'] == 2:
            testAccount = acc
            break
            
    if not testAccount:
        # Fallback a cualquier cuenta que no sea ID 1 (Sentinel Master)
        testAccount = next((acc for acc in accounts if acc['idCuenta'] != 1), None)
        
    if not testAccount:
        print("Error: No se encontró una cuenta adecuada.")
        return

    print(f"Cuenta seleccionada: {testAccount['Nombre']} (ID: {testAccount['idCuenta']})")
    
    # Asegurar que pase la verificación de cuenta activa
    dbManager.getAccount = lambda *args, **kwargs: [testAccount]

    # Datos simulados de la operación (ajuste de venta en EUR/USD)
    tradeData = {
        "idTrade": None,
        "idCuenta": testAccount['idCuenta'],
        "accountName": testAccount.get('Nombre', 'JAIME'),
        "symbol": "EUR/USD",
        "direction": "CORTO",
        "entryPrice": 1.08500,
        "stopLoss": 1.08800,
        "takeProfit": 1.07900,
        "size": 10000,
        "margin_used": 15.50,
        "intervalo": "15min",
        "strategy": "Sniper",
        "setup": "ML SNIPER 15MIN",
        "openTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "status": "OPEN",
        "candleTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    signalDict = {
        "strategy": "Sniper",
        "symbol": "EUR/USD",
        "direction": "CORTO",
        "entryPrice": 1.08500,
        "stopLoss": 1.08800,
        "takeProfit": 1.07900,
        "slDistance": 0.00300,
        "riesgo_pips": 30.0,
        "rr_ratio": 2.0,
        "confidence": 85.50,
        "setup": "ML SNIPER 15MIN",
        "status": "ACTIVA ✅",
        "candleTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "intervalo": "15min",
        "profit": 15.50,
        "size": 10000,
        "is_adjustment": True,
        "marketSentiment": -0.25,
        "latestMetrics": {
            "rsi": 42.10,
            "pendienteRsi": -1.0,
            "macdHist": -0.00012,
            "atr": 0.00120
        }
    }

    print("Enviando señal de prueba a Telegram...")
    success, msgId = await gateway.execute_trade(
        trade_data=tradeData,
        signal=signalDict,
        account=testAccount,
        strategy_name="Sniper"
    )

    if success:
        print(f"✅ ¡Señal de prueba enviada con éxito! Telegram Msg ID: {msgId}")
    else:
        print(f"❌ Falló el envío de la señal: {msgId}")

if __name__ == "__main__":
    asyncio.run(main())
