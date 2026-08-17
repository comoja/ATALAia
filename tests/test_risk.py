import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Sentinel.analysis.risk import calculatePositionSize


class TestCalculatePositionSize(unittest.TestCase):
    """Tests for calculatePositionSize function."""

    # --- FOREX Tests ---
    
    def test_forex_standard_pair(self):
        """Test EUR/USD: capital $10,000, risk 1%, SL 50 pips (0.0050)"""
        symbolInfo = {"tipo": "FOREX", "symbol": "EUR/USD"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=0.0050,  # 50 pips
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 100)  # 1% of 10000
        self.assertEqual(position, 20000)

    def test_forex_jpy_pair(self):
        """Test GBP/JPY: capital $10,000, risk 1%, SL 50 pips (0.50)"""
        symbolInfo = {"tipo": "FOREX", "symbol": "GBP/JPY", "quote_currency": "JPY", "margen": 0.02}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=0.50,  # 50 pips for JPY pairs
            symbolInfo=symbolInfo,
            entryPrice=150.0
        )
        self.assertEqual(risk, 100)
        self.assertEqual(position, 29000)

    def test_forex_larger_position(self):
        """Test EUR/USD with larger SL: capital $10,000, risk 2%, SL 20 pips"""
        symbolInfo = {"tipo": "FOREX", "symbol": "EUR/USD"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=2,
            slDistance=0.0020,  # 20 pips
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 200)
        # lots = 200 / (20 * 10) = 1 lot = 100,000 units
        self.assertEqual(position, 100000)

    # --- METALS Tests ---

    def test_metals_xau_usd(self):
        """Test XAU/USD: capital $10,000, risk 1%, SL $15 (150 pips)"""
        symbolInfo = {"tipo": "METALES", "symbol": "XAU/USD"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=15.0,  # $15 = 150 pips
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 100)
        self.assertEqual(position, 6.0)

    def test_metals_xau_usd_small_sl(self):
        """Test XAU/USD with small SL: capital $10,000, risk 1%, SL $5 (50 pips)"""
        symbolInfo = {"tipo": "METALES", "symbol": "XAU/USD"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=5.0,  # $5 = 50 pips
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 100)
        self.assertEqual(position, 20.0)

    def test_metals_xau_usd_large_position(self):
        """Test XAU/USD: capital $10,000, risk 2%, SL $3 (30 pips)"""
        symbolInfo = {"tipo": "METALES", "symbol": "XAU/USD"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=2,
            slDistance=3.0,  # $3 = 30 pips
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 200)
        self.assertEqual(position, 66.0)

    # --- INDICES Tests ---

    def test_indices_us30(self):
        """Test US30: capital $10,000, risk 1%, SL 50 points"""
        symbolInfo = {"tipo": "INDICES", "symbol": "US30"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=50,  # 50 points
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 100)
        # contracts = 100 / 50 = 2
        self.assertEqual(position, 2.0)

    def test_indices_sp500(self):
        """Test US500: capital $10,000, risk 1%, SL 20 points"""
        symbolInfo = {"tipo": "INDICES", "symbol": "US500"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=20,  # 20 points
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 100)
        # contracts = 100 / 20 = 5
        self.assertEqual(position, 5.0)

    # --- CRYPTO Tests ---

    def test_crypto_btc_usd(self):
        """Test BTC/USD: capital $10,000, risk 1%, SL $500"""
        symbolInfo = {"tipo": "CRIPTO", "symbol": "BTC/USD"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=500,  # $500
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 100)
        # units = 100 / 500 = 0.2 BTC
        self.assertEqual(position, 0.2)

    # --- Edge Cases ---

    def test_zero_sl_distance(self):
        """Test with zero SL distance returns None"""
        symbolInfo = {"tipo": "FOREX", "symbol": "EUR/USD"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=0,
            symbolInfo=symbolInfo
        )
        self.assertIsNone(position)
        self.assertIsNone(risk)

    def test_negative_sl_distance(self):
        """Test with negative SL distance returns None"""
        symbolInfo = {"tipo": "FOREX", "symbol": "EUR/USD"}
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=-10,
            symbolInfo=symbolInfo
        )
        self.assertIsNone(position)
        self.assertIsNone(risk)

    def test_default_symbol_type(self):
        """Test default tipo defaults to FOREX"""
        symbolInfo = {"symbol": "AUD/USD"}  # no 'tipo' key
        position, risk, _ = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=0.0050,
            symbolInfo=symbolInfo
        )
        self.assertEqual(risk, 100)
        self.assertEqual(position, 20000)  # min for Forex


if __name__ == "__main__":
    unittest.main()
