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

async def sendRealTestTrade():
    print("Iniciando prueba de venta real directa de EUR/USD con cantidad = 1000...")
    
    # Instanciamos Gateway en modo live real
    gateway = BrokerGateway(mode="live")
    
    # Preparar datos de trade (Venta, EUR/USD, size=1000)
    nowLocal = datetime.now(pytz.timezone(TIMEZONE))
    tradeData = {
        "symbol": "EUR/USD",
        "direction": "SELL",
        "entryPrice": 1.0800,   # En orden de mercado MT5 tomará el precio bid real
        "stopLoss": 1.0900,     # Nivel de SL referencial
        "takeProfit": 1.0700,    # Nivel de TP referencial
        "size": 1000.0,          # Cantidad solicitada: 1000 unidades (0.01 lotes Forex)
        "strategy": "TestManual",
        "candleTime": nowLocal,
        "openTime": nowLocal.strftime('%Y-%m-%d %H:%M:%S')
    }

    try:
        # Llamamos directamente a la ejecución de MT5 para evitar requerir conexión a MySQL
        success = await gateway._execute_live(tradeData)
        print(f"\n📢 Resultado de _execute_live:")
        print(f"- Éxito: {success}")
        print(f"- ticketId asignado: {tradeData.get('ticketId')}")
    except Exception as e:
        print(f"\n❌ Error al ejecutar el trade: {e}")

if __name__ == "__main__":
    asyncio.run(sendRealTestTrade())
