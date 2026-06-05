import unittest
import pandas as pd
import numpy as np
from Sentinel.analysis.technical import calculateImpulseMacd, calculateFeatures

class TestTechnicalIndicators(unittest.TestCase):
    """Pruebas unitarias para las rutinas del módulo técnico."""

    def testCalculateImpulseMacd(self) -> None:
        """Verifica que el Impulse MACD se calcule correctamente y no retorne nulos en el output final."""
        # Crear datos de prueba sintéticos (50 velas)
        np.random.seed(42)
        dates = pd.date_range(start="2026-06-01 00:00:00", periods=50, freq="5min")
        
        # Simular tendencia alcista
        close = np.linspace(1.08000, 1.09000, 50) + np.random.normal(0, 0.0001, 50)
        high = close + 0.0005
        low = close - 0.0005
        open_val = close - 0.0001
        volume = np.random.randint(100, 1000, 50)

        df = pd.DataFrame({
            "open": open_val,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume
        }, index=dates)

        impulseMacd, impulseSignal = calculateImpulseMacd(df)

        self.assertEqual(len(impulseMacd), len(df))
        self.assertEqual(len(impulseSignal), len(df))
        
        # Verificar que no haya NaNs
        self.assertFalse(impulseMacd.isnull().any())
        self.assertFalse(impulseSignal.isnull().any())

    def testCalculateFeaturesIncludesImpulseMacd(self) -> None:
        """Verifica que calculateFeatures incluya las columnas de Impulse MACD."""
        dates = pd.date_range(start="2026-06-01 00:00:00", periods=250, freq="5min")
        close = np.linspace(1.08000, 1.09000, 250)
        high = close + 0.0005
        low = close - 0.0005
        open_val = close - 0.0001
        volume = np.random.randint(100, 1000, 250)

        df = pd.DataFrame({
            "open": open_val,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume
        }, index=dates)

        dfFeatured = calculateFeatures(df)
        
        self.assertIn("impulseMacd", dfFeatured.columns)
        self.assertIn("impulseSignal", dfFeatured.columns)
        self.assertFalse(dfFeatured["impulseMacd"].isnull().any())
        self.assertFalse(dfFeatured["impulseSignal"].isnull().any())

if __name__ == "__main__":
    unittest.main()
