import sys
import os
sys.path.append('/Volumes/TimeMachine/ATALAia')

import pandas as pd
import numpy as np
from datetime import datetime

from Sentinel.core.BreakoutProbability import BreakoutProbabilityBot
from middleware.config.constants import MODEL_FEATURES

print("Inicializando bot...")
bot = BreakoutProbabilityBot()
print("Modelo cargado:", bot.ml_model is not None)

# Crear DataFrame dummy
np.random.seed(42)
df = pd.DataFrame(np.random.randn(200, len(MODEL_FEATURES)), columns=MODEL_FEATURES)
df['open'] = np.random.randn(200) * 10 + 100
df['high'] = df['open'] + np.random.rand(200) * 5
df['low'] = df['open'] - np.random.rand(200) * 5
df['close'] = df['open'] + np.random.randn(200) * 2
df['atr'] = np.random.rand(200) * 2
df['ema200'] = np.random.rand(200) * 100

symbolInfo = {'symbol': 'BTC/USD', 'refCapital': 10000.0, 'refRiskPct': 1.0}

import asyncio
print("Llamando a runAnalysisCycleForSymbol...")
try:
    result = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData={'BTC/USD': df}))
    print("Resultado:", result)
except Exception as e:
    import traceback
    traceback.print_exc()

print("Prueba finalizada.")
