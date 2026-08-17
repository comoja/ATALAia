import sys
from pathlib import Path
import asyncio

# Añadir el workspace al PATH para poder importar los módulos
workspaceDir = Path(__file__).parent.parent.resolve()
sys.path.append(str(workspaceDir))

from middleware.execution.broker_gateway import BrokerGateway
from datetime import datetime
import pytz
from middleware.config.constants import TIMEZONE

async def testOrderXauusd():
    print("Iniciando prueba de compra a mercado de XAU/USD con volumen de 1.0 lote (size=100) y Stops lógicos (TP=4300, SL=4000)...")
    
    # Instanciamos Gateway en modo live real
    gateway = BrokerGateway(mode="live")
    
    nowLocal = datetime.now(pytz.timezone(TIMEZONE))
    
    # Preparar datos de trade (Compra, XAU/USD, TP=4300, SL=4000, size=100.0 para 1.0 lotes)
    tradeData = {
        "symbol": "XAU/USD",
        "direction": "BUY",
        "entryPrice": 0.0,      # Tomará el precio de mercado
        "stopLoss": 4000.0,
        "takeProfit": 4300.0,
        "size": 100.0,          # 100.0 / 100 = 1.0 lotes (mínimo de Forex.com)
        "strategy": "TestManual",
        "candleTime": nowLocal,
        "openTime": nowLocal.strftime('%Y-%m-%d %H:%M:%S')
    }

    try:
        success = await gateway._execute_live(tradeData)
        print(f"\n📢 Resultado de _execute_live:")
        print(f"- Éxito: {success}")
        print(f"- ticketId asignado: {tradeData.get('ticketId')}")
    except Exception as error:
        print(f"\n❌ Error al ejecutar el trade: {error}")

if __name__ == "__main__":
    asyncio.run(testOrderXauusd())
