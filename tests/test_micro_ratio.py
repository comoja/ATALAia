import unittest
import pandas as pd
import numpy as np
from backend.services.microRatio import computeRatioSignals, resamplePriceData

class TestMicroRatio(unittest.TestCase):
    def test_compute_ratio_signals_synthetic(self):
        dates = pd.date_range(start="2026-01-01", periods=100, freq="D")
        pxA = [1.08 + 0.05 * np.sin(i / 5.0) for i in range(100)]
        pxB = [18.5 - 1.5 * np.sin(i / 5.0) for i in range(100)]
        
        dfA = pd.DataFrame({"closePrice": pxA}, index=dates)
        dfB = pd.DataFrame({"closePrice": pxB}, index=dates)
        
        signals = computeRatioSignals(
            dfA=dfA,
            dfB=dfB,
            pairA="EUR/USD",
            pairB="USD/MXN",
            smaPeriod=3,
            sigmaWindow=15
        )
        
        self.assertIsInstance(signals, dict)
        self.assertIn("hasSignal", signals)
        self.assertIn("isExit", signals)
        self.assertIn("candleTime", signals)

    def test_resample_price_data(self):
        dates = pd.date_range(start="2026-01-01", periods=24*10, freq="h")
        df = pd.DataFrame({"closePrice": range(240)}, index=dates)
        
        df_daily = resamplePriceData(df, "1d")
        self.assertLessEqual(len(df_daily), 11)
        self.assertIn("closePrice", df_daily.columns)

if __name__ == "__main__":
    unittest.main()
