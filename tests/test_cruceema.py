import unittest
import pandas as pd
import numpy as np
import talib as ta
import asyncio
from datetime import datetime

from Sentinel.core.CruceEMA import CruceEMABot
from middleware.database import dbManager

class TestCruceEMA(unittest.TestCase):
    """Pruebas unitarias para la estrategia CruceEMA."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente con su nombre de estrategia."""
        bot = CruceEMABot()
        self.assertEqual(bot.strategy_name, "CruceEMA")

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol no falle con un DataFrame vacío o insuficiente."""
        bot = CruceEMABot()
        symbolInfo = {
            "symbol": "EUR/USD",
            "refCapital": 10000.0,
            "refRiskPct": 1.0,
            "pipMultiplier": 10000.0,
            "contractSize": 100000.0
        }
        preloadedData = {
            "EUR/USD": None
        }
        signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
        self.assertIsNone(signal)

    def testCalcImacd(self):
        """Verifica el cálculo del Impulse MACD (IMACD)."""
        bot = CruceEMABot()
        
        # Generar datos de prueba
        np.random.seed(42)
        dates = pd.date_range(start="2026-06-01 00:00:00", periods=55, freq="15min")
        prices = np.sin(np.linspace(0, 10, 55)) + 10.0
        df = pd.DataFrame({
            "open": prices,
            "high": prices + 0.1,
            "low": prices - 0.1,
            "close": prices,
            "volume": np.random.randint(100, 500, 55)
        }, index=dates)
        
        df = bot.calc_imacd(df, lengthMA=34, lengthSignal=9)
        self.assertIn("imacd_md", df.columns)
        self.assertIn("imacd_sb", df.columns)
        self.assertEqual(len(df), 55)

if __name__ == "__main__":
    unittest.main()
