import unittest
import pandas as pd
import numpy as np
import os
from Sentinel.analysis.fvg_analyzer import FvgAnalyzer

class TestFvgAnalyzer(unittest.TestCase):
    """
    Unit tests for the FvgAnalyzer class, verifying base detection,
    filtering, classification, mitigation, and visualization.
    """
    def setUp(self):
        # Generate mock data replicating the scenarios from high probability FVG tests
        dates = pd.date_range(start="2026-05-20 09:00:00", periods=30, freq="15min")
        
        # Base trend ascending above EMA 200
        data = {
            'open':  [100.0] * 30,
            'high':  [101.0] * 30,
            'low':   [99.0] * 30,
            'close': [100.0] * 30,
            'volume': [1000] * 30
        }
        self.df = pd.DataFrame(data, index=dates)
        
        for idx in range(30):
            self.df.iloc[idx, self.df.columns.get_loc('open')]  = 100.0 + idx * 0.5
            self.df.iloc[idx, self.df.columns.get_loc('close')] = 100.5 + idx * 0.5
            self.df.iloc[idx, self.df.columns.get_loc('high')]  = 101.0 + idx * 0.5
            self.df.iloc[idx, self.df.columns.get_loc('low')]   = 99.8 + idx * 0.5

        # SCENARIO 1: Bullish FVG High Probability (Vela 3 body < 60%)
        # Vela 1 (i-2) = index 15
        self.df.iloc[15, self.df.columns.get_loc('open')]  = 108.0
        self.df.iloc[15, self.df.columns.get_loc('close')] = 108.5
        self.df.iloc[15, self.df.columns.get_loc('high')]  = 109.0
        self.df.iloc[15, self.df.columns.get_loc('low')]   = 107.5
        
        # Vela 2 (i-1) = index 16 (Displacement candle, breaks local high)
        self.df.iloc[16, self.df.columns.get_loc('open')]  = 108.6
        self.df.iloc[16, self.df.columns.get_loc('close')] = 112.0
        self.df.iloc[16, self.df.columns.get_loc('high')]  = 112.5
        self.df.iloc[16, self.df.columns.get_loc('low')]   = 108.5
        
        # Vela 3 (i) = index 17 (Body = 55% of range, mecha = 45% < 50% -> Alta Probabilidad)
        self.df.iloc[17, self.df.columns.get_loc('open')]  = 110.2
        self.df.iloc[17, self.df.columns.get_loc('close')] = 110.75
        self.df.iloc[17, self.df.columns.get_loc('high')]  = 111.0
        self.df.iloc[17, self.df.columns.get_loc('low')]   = 110.0  # low (110.0) > high_n2 (109.0) -> Gap size 1.0
        
        # SCENARIO 2: Bullish FVG Breakaway Gap (Vela 3 body > 80% + closes far + high ATR)
        # Vela 1 (i-2) = index 18 (touches FVG 17 gap to mitgate it: 109.5 <= 110.0 -> mitigated=True)
        self.df.iloc[18, self.df.columns.get_loc('open')]  = 111.0
        self.df.iloc[18, self.df.columns.get_loc('close')] = 111.5
        self.df.iloc[18, self.df.columns.get_loc('high')]  = 112.0
        self.df.iloc[18, self.df.columns.get_loc('low')]   = 109.5
        
        # Vela 2 (i-1) = index 19
        self.df.iloc[19, self.df.columns.get_loc('open')]  = 111.6
        self.df.iloc[19, self.df.columns.get_loc('close')] = 116.0
        self.df.iloc[19, self.df.columns.get_loc('high')]  = 116.5
        self.df.iloc[19, self.df.columns.get_loc('low')]   = 111.5
        
        # Vela 3 (i) = index 20 (Body > 80%, closes far. Range: 6.0, Body: 5.0 (83.3%))
        self.df.iloc[20, self.df.columns.get_loc('open')]  = 113.5
        self.df.iloc[20, self.df.columns.get_loc('close')] = 118.5
        self.df.iloc[20, self.df.columns.get_loc('high')]  = 119.0
        self.df.iloc[20, self.df.columns.get_loc('low')]   = 113.0  # low (113.0) > high_n2 (112.0) -> Gap size 1.0
        
        # Keep prices high to prevent premature mitigation of Scenario 2
        for idx in range(21, 30):
            self.df.iloc[idx, self.df.columns.get_loc('open')]  = 118.0
            self.df.iloc[idx, self.df.columns.get_loc('close')] = 118.5
            self.df.iloc[idx, self.df.columns.get_loc('high')]  = 119.0
            self.df.iloc[idx, self.df.columns.get_loc('low')]   = 117.5

        self.analyzer = FvgAnalyzer(minGapPct=0.0001)

    def testDetectFvgBase(self):
        """Verifies base FVG detection detects the expected gaps."""
        fvgs = self.analyzer.detectFvg(self.df)
        self.assertGreaterEqual(len(fvgs), 2)
        
        # Verify first FVG at index 17
        fvg17 = next((f for f in fvgs if f['idx'] == 17), None)
        self.assertIsNotNone(fvg17)
        self.assertEqual(fvg17['type'], 'Bullish_FVG')
        self.assertAlmostEqual(fvg17['gapLow'], 109.0)
        self.assertAlmostEqual(fvg17['gapHigh'], 110.0)

        # Verify second FVG at index 20
        fvg20 = next((f for f in fvgs if f['idx'] == 20), None)
        self.assertIsNotNone(fvg20)
        self.assertEqual(fvg20['type'], 'Bullish_FVG')
        self.assertAlmostEqual(fvg20['gapLow'], 112.0)
        self.assertAlmostEqual(fvg20['gapHigh'], 113.0)

    def testApplyFiltersClassification(self):
        """Verifies that classification and mitigation are correctly calculated."""
        rawFvgs = self.analyzer.detectFvg(self.df)
        filteredFvgs = self.analyzer.applyFilters(self.df, rawFvgs, applyHighProbFilters=False, validateMitigation=False)
        
        # FVG at 17 is Alta Probabilidad
        fvg17 = next((f for f in filteredFvgs if f['idx'] == 17), None)
        self.assertIsNotNone(fvg17)
        self.assertEqual(fvg17['classification'], 'Alta Probabilidad')
        self.assertTrue(fvg17['highProbability'])
        self.assertTrue(fvg17['mitigated']) # Touched at index 25

        # FVG at 20 is Breakaway Gap
        fvg20 = next((f for f in filteredFvgs if f['idx'] == 20), None)
        self.assertIsNotNone(fvg20)
        self.assertEqual(fvg20['classification'], 'Breakaway Gap')
        self.assertTrue(fvg20['breakawayGap'])
        self.assertFalse(fvg20['mitigated']) # Never touched by lows (min low in [21,30] is 117.5)

    def testApplyFiltersHighProbability(self):
        """Verifies that high probability filters (EMA/MSS) discard non-conforming gaps."""
        # Scenario: Put a FVG below EMA 200 to test if EMA filter discards it.
        # Elevate initial prices of the DataFrame to construct a high EMA 200.
        dfCopy = self.df.copy()
        dfCopy.iloc[:15, dfCopy.columns.get_loc('open')]  = 500.0
        dfCopy.iloc[:15, dfCopy.columns.get_loc('close')] = 500.0
        dfCopy.iloc[:15, dfCopy.columns.get_loc('high')]  = 501.0
        dfCopy.iloc[:15, dfCopy.columns.get_loc('low')]   = 499.0

        rawFvgs = self.analyzer.detectFvg(dfCopy)
        filteredFvgs = self.analyzer.applyFilters(dfCopy, rawFvgs, applyHighProbFilters=True, validateMitigation=False)
        
        # Since closes are below EMA 200, Bullish FVGs should be filtered out
        self.assertEqual(len(filteredFvgs), 0)

    def testSnakeCaseAliases(self):
        """Verifies snake_case method aliases work correctly."""
        rawFvgs = self.analyzer.detect_fvg(self.df)
        self.assertGreaterEqual(len(rawFvgs), 2)
        
        filteredFvgs = self.analyzer.apply_filters(self.df, rawFvgs, applyHighProbFilters=False, validateMitigation=False)
        self.assertGreaterEqual(len(filteredFvgs), 2)

    def testVisualizeZones(self):
        """Verifies that visualizeZones runs and saves an image without errors."""
        rawFvgs = self.analyzer.detectFvg(self.df)
        filteredFvgs = self.analyzer.applyFilters(self.df, rawFvgs, applyHighProbFilters=False, validateMitigation=False)
        
        outputPath = "tests/test_fvg_chart.png"
        if os.path.exists(outputPath):
            os.remove(outputPath)
            
        self.analyzer.visualizeZones(self.df, filteredFvgs, outputPath)
        self.assertTrue(os.path.exists(outputPath))
        
        # Clean up
        if os.path.exists(outputPath):
            os.remove(outputPath)

if __name__ == "__main__":
    unittest.main()
