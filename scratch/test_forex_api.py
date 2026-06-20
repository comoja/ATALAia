import os
import sys
import asyncio
import logging

# Resolver la raíz del proyecto de forma dinámica para compatibilidad multiplataforma (Mac/Windows)
baseDir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if baseDir not in sys.path:
    sys.path.append(baseDir)
from middleware.api import forex

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

async def main():
    print("🧪 Iniciando pruebas de middleware/api/forex.py...")
    
    # Intentar obtener velas para EUR/USD
    params = {
        "symbol": "EUR/USD",
        "interval": "5min",
        "outputSize": 10
    }
    
    print(f"📡 Llamando a forex.getTimeSeries() con parámetros: {params}")
    df = await forex.getTimeSeries(params)
    
    if df is not None:
        print("✅ Datos obtenidos correctamente:")
        print(df)
    else:
        print("⚠️ forex.getTimeSeries() retornó None.")
        print("Esto es correcto y esperado si el script corre en macOS (donde no hay terminal físico de MT5).")
        print("Verifica los logs para confirmar que el bypass funcionó sin lanzar excepciones críticas.")

if __name__ == "__main__":
    asyncio.run(main())
