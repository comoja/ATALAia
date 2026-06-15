import unittest
import pandas as pd
import numpy as np
import talib as ta
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.FVGDiario import FVGDiarioBot
from middleware.database import dbManager

class TestFVGDiario(unittest.TestCase):
    """Pruebas unitarias para la estrategia FVGDiario."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente con su nombre de estrategia."""
        bot = FVGDiarioBot()
        self.assertEqual(bot.strategy_name, "FVGDiario")

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese señales sin errores."""
        bot = FVGDiarioBot()
        
        # Generar datos de prueba para timeframe 15m (100 velas) y 1D (2 velas)
        np.random.seed(42)
        dates15m = pd.date_range(start="2026-06-01 00:00:00", periods=100, freq="15min")
        prices15m = np.ones(100) * 1.0800
        
        df15m = pd.DataFrame({
            "open": prices15m,
            "high": prices15m + 0.0001,
            "low": prices15m - 0.0001,
            "close": prices15m,
            "volume": np.random.randint(100, 500, 100)
        }, index=dates15m)
        
        dates1d = pd.date_range(start="2026-05-30 00:00:00", periods=3, freq="1D")
        df1d = pd.DataFrame({
            "open": [1.0800, 1.0780, 1.0810],
            "high": [1.0820, 1.0830, 1.0820],
            "low": [1.0770, 1.0770, 1.0800],
            "close": [1.0810, 1.0820, 1.0815],
            "volume": [1000, 2000, 1500]
        }, index=dates1d)
        
        symbolInfo = {
            "symbol": "EUR/USD",
            "weekly_trend": "ALCISTA",
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
                "15min": df15m,
                "1d": df1d
            }
        }
        
        # Mocks
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        orig_getConfig = dbManager.getStrategyConfig
        
        import Sentinel.core.FVGDiario as FVGDiarioModule
        orig_exhaustion = FVGDiarioModule.check_tp_exhaustion
        orig_health = FVGDiarioModule.check_signal_health
        
        dbManager.getSymbolStrategyConfig = lambda s, sym: {
            "minRr": 2.0,
            "minConfidence": 70.0,
            "minFvgPips": 3.0,
            "riskUsd": 100.0,
            "minUsdProfit": 10.0
        }
        dbManager.getStrategyConfig = lambda s: {
            "min_rr": 2.0,
            "min_confidence": 70.0,
            "min_fvg_pips": 5.0
        }
        FVGDiarioModule.check_tp_exhaustion = lambda *args, **kwargs: (True, 0.0, "OK")
        FVGDiarioModule.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        # Mock de _detectManipulation, _checkMarketStructureShift y _findFvgAfterManipulation para forzar confluencia
        bot._detectManipulation = lambda df, pdh, pdl, bias: {"type": "MANIPULATION_DOWN", "level": pdl, "idx": 80}
        bot._checkMarketStructureShift = lambda df, manip, bias: True
        bot._findFvgAfterManipulation = lambda df, manip, bias: {
            "type": "Bullish_FVG", "size": 0.0006, "mid": 1.0803, "top": 1.0806, "bottom": 1.0800, "timestamp": "2026-06-01 20:00:00"
        }
        
        try:
            signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.strategy, "FVGDiario")
            self.assertEqual(signal.direction, "LARGO")
            self.assertTrue(signal.confidence >= 70)
        finally:
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            dbManager.getStrategyConfig = orig_getConfig
            FVGDiarioModule.check_tp_exhaustion = orig_exhaustion
            FVGDiarioModule.check_signal_health = orig_health

if __name__ == "__main__":
    unittest.main()
