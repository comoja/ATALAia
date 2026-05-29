import sys, os, asyncio
import pandas as pd
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, rutaRaiz)
from middleware.database import dbManager

dates_str = ["2025-12-14", "2026-01-26", "2026-03-08", "2026-03-30", "2026-04-21", "2026-05-27"]
target_dates = pd.to_datetime(dates_str)

async def main():
    print("Obteniendo datos...")
    # Obtenemos historial grande para asegurarnos de incluir 2025 y 2026
    df_gbp = await dbManager.getCandlesFromDb("GBP/JPY", "5min", 100000)
    df_usd = await dbManager.getCandlesFromDb("USD/JPY", "5min", 100000)
    
    if df_gbp.empty or df_usd.empty:
        print("Faltan datos de alguno de los pares.")
        return

    # Agrupar a velas diarias
    df_gbp_d = df_gbp.resample('D').agg({'close': 'last'}).dropna()
    df_usd_d = df_usd.resample('D').agg({'close': 'last'}).dropna()
    
    # Remover timezone timezone info for matching
    df_gbp_d.index = df_gbp_d.index.tz_localize(None)
    df_usd_d.index = df_usd_d.index.tz_localize(None)

    print("\nPrecios de Cierre:")
    for dt in target_dates:
        gbp_price = df_gbp_d.loc[dt, 'close'] if dt in df_gbp_d.index else "Sin datos"
        usd_price = df_usd_d.loc[dt, 'close'] if dt in df_usd_d.index else "Sin datos"
        print(f"{dt.strftime('%d/%m/%Y')}: GBP/JPY = {gbp_price}, USD/JPY = {usd_price}")

asyncio.run(main())
