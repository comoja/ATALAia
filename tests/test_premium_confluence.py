import unittest
import pandas as pd
import numpy as np
import talib as ta
from Sentinel.core.PremiumConfluence import PremiumConfluenceBot

class TestPremiumConfluence(unittest.TestCase):
    """Pruebas unitarias para la estrategia PremiumConfluence."""

    def testCalculateSmoothedHeikinAshi(self):
        """Verifica que el cálculo de velas Heikin Ashi Suavizadas devuelva series válidas y sin NaNs."""
        bot = PremiumConfluenceBot()
        
        # Generar datos de prueba
        np.random.seed(42)
        prices = np.linspace(1.1500, 1.1550, 100) + np.random.normal(0, 0.0001, 100)
        df = pd.DataFrame({
            "open": prices - 0.0001,
            "high": prices + 0.0003,
            "low": prices - 0.0003,
            "close": prices
        })
        
        haCloseSmooth, haOpenSmooth = bot.calculateSmoothedHeikinAshi(df, period1=10, period2=10)
        
        self.assertEqual(len(haCloseSmooth), 100)
        self.assertEqual(len(haOpenSmooth), 100)
        self.assertFalse(np.isnan(haCloseSmooth.iloc[-1]))
        self.assertFalse(np.isnan(haOpenSmooth.iloc[-1]))

    def testCalculateSuperTrend(self):
        """Verifica el cálculo de Supertrend en PremiumConfluenceBot."""
        bot = PremiumConfluenceBot()
        
        np.random.seed(42)
        prices = np.linspace(1.1500, 1.1550, 100)
        df = pd.DataFrame({
            "open": prices - 0.0001,
            "high": prices + 0.0003,
            "low": prices - 0.0003,
            "close": prices
        })
        
        stTrend, stTrail = bot.calculateSuperTrend(df, supertrendPeriod=10, supertrendMultiplier=1.0)
        
        self.assertEqual(len(stTrend), 100)
        self.assertEqual(len(stTrail), 100)

    def testGetSymbolStrategyConfig(self):
        """Verifica la recuperación de la configuración relacional por activo/estrategia."""
        from middleware.database import dbManager
        
        # Probar el fallback global de un símbolo inexistente
        fallbackConfig = dbManager.getSymbolStrategyConfig("PremiumConfluence", "INVALID/SYMBOL")
        self.assertIsNotNone(fallbackConfig)
        
        # Probar la recuperación de una configuración sembrada
        goldConfig = dbManager.getSymbolStrategyConfig("PremiumConfluence", "XAU/USD")
        self.assertIsNotNone(goldConfig)
        if goldConfig:
            self.assertEqual(goldConfig.get("supertrend_mult"), 2.5)

if __name__ == "__main__":
    unittest.main()
