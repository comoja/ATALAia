import unittest
import pandas as pd
import numpy as np
import talib as ta
from Sentinel.core.QTrend import QTrendBot

class TestQTrend(unittest.TestCase):
    """Pruebas unitarias para la estrategia QTrend."""

    def testCalculateQTrend(self):
        """Verifica que el cálculo de EMAs de QTrend devuelva arreglos válidos."""
        bot = QTrendBot()
        
        # Generar datos de prueba
        np.random.seed(42)
        close = np.linspace(1.0800, 1.0900, 100) + np.random.normal(0, 0.0002, 100)
        df = pd.DataFrame({
            "close": close
        })
        
        emaFast, emaSlow = bot.calculateQTrend(df, qtrendFast=9, qtrendSlow=21)
        
        self.assertEqual(len(emaFast), 100)
        self.assertEqual(len(emaSlow), 100)
        self.assertFalse(np.isnan(emaFast[-1]))
        self.assertFalse(np.isnan(emaSlow[-1]))

    def testCalculateSuperTrend(self):
        """Verifica el cálculo del Trailing Stop del canal de SuperTrend."""
        bot = QTrendBot()
        
        np.random.seed(42)
        close = np.linspace(1.0800, 1.0900, 100)
        high = close + 0.0005
        low = close - 0.0005
        open_val = close - 0.0001
        
        df = pd.DataFrame({
            "open": open_val,
            "high": high,
            "low": low,
            "close": close
        })
        
        # SuperTrend requiere ATR
        df['atr'] = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
        
        stTrend, stTrail = bot.calculateSuperTrend(df, supertrendPeriod=10, supertrendMultiplier=3.0)
        
        self.assertEqual(len(stTrend), 100)
        self.assertEqual(len(stTrail), 100)

if __name__ == "__main__":
    unittest.main()
