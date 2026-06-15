import unittest
import pandas as pd
import numpy as np
import talib as ta
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.EMA20200 import EMA20200Bot
from middleware.database import dbManager

class TestEMA20200(unittest.TestCase):
    """Pruebas unitarias para la estrategia EMA20200."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente con su nombre de estrategia."""
        bot = EMA20200Bot()
        self.assertEqual(bot.strategy_name, "EMA20200")
        self.assertEqual(bot.pullbackTolerance, 0.0015)

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese cruces y pullbacks correctamente."""
        bot = EMA20200Bot()
        
        # Generar datos de prueba para timeframe 1h
        np.random.seed(42)
        dates = pd.date_range(start="2026-06-01 00:00:00", periods=250, freq="1h")
        prices = np.ones(250) * 1.0800
        
        df = pd.DataFrame({
            "open": prices,
            "high": prices + 0.0002,
            "low": prices - 0.0002,
            "close": prices,
            "volume": np.random.randint(100, 500, 250)
        }, index=dates)
        
        # Forzar que venga con EMAs calculadas
        df["ema20"] = ta.EMA(df["close"].values, timeperiod=20)
        df["ema200"] = ta.EMA(df["close"].values, timeperiod=200)
        df["atr"] = ta.ATR(df["high"].values, df["low"].values, df["close"].values, timeperiod=14)
        
        symbolInfo = {
            "symbol": "EUR/USD",
            "refCapital": 10000.0,
            "refRiskPct": 1.0,
            "pipMultiplier": 10000.0,
            "minDistLimit": 5.0,
            "maxDistLimit": 100.0,
            "lotStep": 0.01,
            "minLot": 0.01,
            "contractSize": 100000.0
        }
        
        preloadedData = {
            "EUR/USD": {
                "1h": df
            }
        }
        
        # Mocks para base de datos, detectCross y ML
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        orig_getConfig = dbManager.getStrategyConfig
        orig_detectCross = bot.detectCross
        orig_evaluateML = bot.evaluateML
        
        dbManager.getSymbolStrategyConfig = lambda s, sym: {
            "emaFast": 20,
            "emaSlow": 200,
            "pullbackTolerance": 0.0015,
            "minRr": 1.5,
            "minConfidence": 70.0
        }
        dbManager.getStrategyConfig = lambda s: {
            "min_rr": 1.5,
            "min_confidence": 70.0
        }
        bot.evaluateML = lambda df: 0.85 # Forzar probabilidad alta
        
        try:
            # 1. Simular cruce alcista (Largo)
            # Mock de detectCross para retornar Largo en el primer ciclo
            bot.detectCross = lambda emaF, emaS: "LARGO"
            
            # Ejecutar análisis
            signal1 = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            # Debería retornar None y guardar el estado waitingPullback
            self.assertIsNone(signal1)
            self.assertIn("EUR/USD", bot.waitingPullback)
            self.assertEqual(bot.waitingPullback["EUR/USD"]["direction"], "LARGO")
            
            # 2. Siguiente ciclo: simular pullback
            # Mock de detectCross para que no retorne cruce (para que no re-gatille y procese el pullback)
            bot.detectCross = lambda emaF, emaS: None
            
            # Modificar df para que el precio de cierre esté muy cerca de la ema20 (pullback exitoso)
            df.loc[df.index[-1], "close"] = df["ema20"].iloc[-1] + 0.0001
            df.loc[df.index[-1], "atr"] = 0.0005
            
            signal2 = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal2)
            self.assertEqual(signal2.strategy, "EMA20200")
            self.assertEqual(signal2.direction, "LARGO")
            self.assertTrue(signal2.confidence >= 70)
            
        finally:
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            dbManager.getStrategyConfig = orig_getConfig
            bot.detectCross = orig_detectCross
            bot.evaluateML = orig_evaluateML

if __name__ == "__main__":
    unittest.main()
