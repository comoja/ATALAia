import sys
import os
import asyncio
import pandas as pd
import numpy as np
import pytz

projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from middleware.database import dbManager
from middleware.utils.momentum import calcularAngulos, obtenerEstado
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval

async def main():
    symbols = dbManager.getSymbols()
    for symbolData in symbols:
        symbol = symbolData['symbol']
        df_5m = await dbManager.getCandles(symbol, n_velas=100)
        if df_5m is not None and len(df_5m) >= 20:
            df_5m = df_5m.dropna(subset=['close', 'high', 'low', 'open'])
            df_15m = resample_to_interval(df_5m, "15min")
            df_15m = calculateFeatures(df_15m)
            if df_15m is not None and len(df_15m) >= 15:
                df_calc = calcularAngulos(df_15m.copy())
                last = df_calc.iloc[-1]
                estado, msg = obtenerEstado(last.get('ang_rsi'), last.get('ang_close'))
                print(f"Símbolo: {symbol} | RSI: {last.get('rsi'):.2f} | ang_rsi: {last.get('ang_rsi'):.2f} | ang_close: {last.get('ang_close'):.2f} | Estado: {estado}")
            else:
                print(f"Símbolo: {symbol} - No hay suficientes velas de 15min o calculateFeatures falló")
        else:
            print(f"Símbolo: {symbol} - No hay suficientes velas de 5min")

if __name__ == "__main__":
    asyncio.run(main())
