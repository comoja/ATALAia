import unittest
import pandas as pd
import numpy as np
import talib as ta
from Sentinel.core.BreakoutProbability import BreakoutProbabilityBot

class TestBreakoutProbability(unittest.TestCase):
    """Pruebas unitarias para la estrategia BreakoutProbability."""

    def testCalculateBreakoutProbability(self):
        """Verifica que el cálculo de probabilidad de ruptura devuelva valores correctos y no genere nulos."""
        bot = BreakoutProbabilityBot()
        
        # Generar datos sintéticos de 350 velas para tener suficiente historial
        np.random.seed(42)
        dates = pd.date_range(start="2026-06-01 00:00:00", periods=350, freq="15min")
        
        # Simular una tendencia oscilatoria con rupturas alcistas y bajistas
        base = np.sin(np.linspace(0, 10 * np.pi, 350)) * 0.01 + 1.08000
        close = base + np.random.normal(0, 0.0001, 350)
        high = close + 0.0005
        low = close - 0.0005
        open_val = close - 0.0001
        
        df = pd.DataFrame({
            "open": open_val,
            "high": high,
            "low": low,
            "close": close,
            "volume": np.random.randint(100, 1000, 350)
        }, index=dates)

        # Calcular ATR para cumplir el requerimiento de la función
        df['atr'] = ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)

        longProb, shortProb = bot.calculateBreakoutProbability(df, lookback=250, channelLen=20, targetAtrMult=1.5)
        
        # Validar tipo de retorno y límites
        self.assertIsInstance(longProb, float)
        self.assertIsInstance(shortProb, float)
        self.assertTrue(0.0 <= longProb <= 100.0)
        self.assertTrue(0.0 <= shortProb <= 100.0)

if __name__ == "__main__":
    unittest.main()
