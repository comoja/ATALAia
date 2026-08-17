import sys
from pathlib import Path
import asyncio

# Añadir el workspace al PATH para poder importar los módulos
workspaceDir = Path(__file__).parent.parent.resolve()
sys.path.append(str(workspaceDir))

from middleware.execution.broker_gateway import BrokerGateway

async def main():
    ticketId = 12221484
    newTp = 4146.0
    print(f"Modificando Take Profit (TP) de la posición {ticketId} a {newTp}...")
    
    # Instanciamos el gateway
    gateway = BrokerGateway(mode="live")
    
    # Llamar a la función de actualizar SL/TP
    success = gateway.updateLiveSLTP(ticketId=ticketId, new_tp=newTp, mt5Symbol="XAUUSD")
    print(f"\n📢 Resultado de la modificación:")
    print(f"- Éxito: {success}")

if __name__ == "__main__":
    asyncio.run(main())
