import os
import sys
import asyncio
import json

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database.dbManager import getCandlesFromDb
from backend.services.correlation_engine import engine

async def test():
    print("Obteniendo velas de la base de datos...")
    # Probamos con un par que sabemos que tiene velas por el log de hoy
    symbol = "USD/CHF"
    # Usamos 15000 velas que es el límite real del endpoint de FastAPI
    df = await getCandlesFromDb(symbol=symbol, timeframe="5min", limit=15000)
    
    if df is None or df.empty:
        print(f"No se encontraron velas para {symbol} en la base de datos.")
        return
        
    print(f"Cargadas {len(df)} velas de 5min exitosamente.")
    
    # Resample diario
    df_daily = df.resample('D').agg({'close': 'last'}).dropna()
    df_daily = df_daily.rename(columns={'close': 'price'})
    print(f"Resampleadas a {len(df_daily)} velas diarias para emular ATALAia.")
    
    # Procesar con el motor parametrizado
    res = engine.process_pair(
        df_daily,
        amplitude=1.5,
        freq=0.05,
        phase=1.2,
        offset=0.2,
        r=0.06,
        tYears=45 / 252,
        sigmaWindow=7
    )
    
    if not res.get("success"):
        print(f"\n❌ Error del Motor: {res.get('error')}")
        return
        
    print("\n--- Resultado del Motor (Resumen `latest`) ---")
    print(json.dumps(res.get("latest"), indent=2))
    
    history = res.get("history")
    print(f"\n--- Resultado del Motor (Historial) ---")
    print(f"Total de registros históricos devueltos: {len(history)}")
    if history:
        print("Muestra del primer registro histórico:")
        print(json.dumps(history[0], indent=2))
        print("Muestra del último registro histórico:")
        print(json.dumps(history[-1], indent=2))

if __name__ == "__main__":
    asyncio.run(test())
