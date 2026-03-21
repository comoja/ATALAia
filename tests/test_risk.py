import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Sentinel.analysis.risk import calculatePositionSize


class TestCalculatePositionSize:
    """Tests for calculatePositionSize function."""

    # --- FOREX Tests ---
    
    def test_forex_standard_pair(self):
        """Test EUR/USD: capital $10,000, risk 1%, SL 50 pips (0.0050)"""
        symbolInfo = {"tipo": "FOREX", "symbol": "EUR/USD"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=0.0050,  # 50 pips
            symbolInfo=symbolInfo
        )
        assert risk == 100  # 1% of 10000
        assert position == 1000  # min 1000

    def test_forex_jpy_pair(self):
        """Test GBP/JPY: capital $10,000, risk 1%, SL 50 pips (0.50)"""
        symbolInfo = {"tipo": "FOREX", "symbol": "GBP/JPY"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=0.50,  # 50 pips for JPY pairs
            symbolInfo=symbolInfo
        )
        assert risk == 100
        assert position == 1000  # min 1000

    def test_forex_larger_position(self):
        """Test EUR/USD with larger SL: capital $10,000, risk 2%, SL 20 pips"""
        symbolInfo = {"tipo": "FOREX", "symbol": "EUR/USD"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=2,
            slDistance=0.0020,  # 20 pips
            symbolInfo=symbolInfo
        )
        assert risk == 200
        # lots = 200 / (20 * 10) = 1 lot = 100,000 units = 1000 (thousands)
        assert position == 1000

    # --- METALS Tests ---

    def test_metals_xau_usd(self):
        """Test XAU/USD: capital $10,000, risk 1%, SL $15 (150 pips)"""
        symbolInfo = {"tipo": "METALES", "symbol": "XAU/USD"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=15.0,  # $15 = 150 pips
            symbolInfo=symbolInfo
        )
        assert risk == 100
        # pips = 15 / 0.10 = 150
        # units = 100 / (150 * 10) = 100 / 1500 = 0.066...
        # max(1.0, round(0.066, 2)) = 1.0
        assert position == 1.0

    def test_metals_xau_usd_small_sl(self):
        """Test XAU/USD with small SL: capital $10,000, risk 1%, SL $5 (50 pips)"""
        symbolInfo = {"tipo": "METALES", "symbol": "XAU/USD"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=5.0,  # $5 = 50 pips
            symbolInfo=symbolInfo
        )
        assert risk == 100
        # pips = 5 / 0.10 = 50
        # units = 100 / (50 * 10) = 100 / 500 = 0.2
        # max(1.0, round(0.2, 2)) = 1.0
        assert position == 1.0

    def test_metals_xau_usd_large_position(self):
        """Test XAU/USD: capital $10,000, risk 2%, SL $3 (30 pips)"""
        symbolInfo = {"tipo": "METALES", "symbol": "XAU/USD"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=2,
            slDistance=3.0,  # $3 = 30 pips
            symbolInfo=symbolInfo
        )
        assert risk == 200
        # pips = 3 / 0.10 = 30
        # units = 200 / (30 * 10) = 200 / 300 = 0.67
        # max(1.0, round(0.67, 2)) = 1.0
        assert position == 1.0

    # --- INDICES Tests ---

    def test_indices_us30(self):
        """Test US30: capital $10,000, risk 1%, SL 50 points"""
        symbolInfo = {"tipo": "INDICES", "symbol": "US30"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=50,  # 50 points
            symbolInfo=symbolInfo
        )
        assert risk == 100
        # contracts = 100 / 50 = 2
        assert position == 2.0

    def test_indices_sp500(self):
        """Test US500: capital $10,000, risk 1%, SL 20 points"""
        symbolInfo = {"tipo": "INDICES", "symbol": "US500"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=20,  # 20 points
            symbolInfo=symbolInfo
        )
        assert risk == 100
        # contracts = 100 / 20 = 5
        assert position == 5.0

    # --- CRYPTO Tests ---

    def test_crypto_btc_usd(self):
        """Test BTC/USD: capital $10,000, risk 1%, SL $500"""
        symbolInfo = {"tipo": "CRIPTO", "symbol": "BTC/USD"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=500,  # $500
            symbolInfo=symbolInfo
        )
        assert risk == 100
        # units = 100 / 500 = 0.2 BTC
        assert position == 0.2

    # --- Edge Cases ---

    def test_zero_sl_distance(self):
        """Test with zero SL distance returns None"""
        symbolInfo = {"tipo": "FOREX", "symbol": "EUR/USD"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=0,
            symbolInfo=symbolInfo
        )
        assert position is None
        assert risk is None

    def test_negative_sl_distance(self):
        """Test with negative SL distance returns None"""
        symbolInfo = {"tipo": "FOREX", "symbol": "EUR/USD"}
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=-10,
            symbolInfo=symbolInfo
        )
        assert position is None
        assert risk is None

    def test_default_symbol_type(self):
        """Test default tipo defaults to FOREX"""
        symbolInfo = {"symbol": "AUD/USD"}  # no 'tipo' key
        position, risk = calculatePositionSize(
            capital=10000,
            riskPercentage=1,
            slDistance=0.0050,
            symbolInfo=symbolInfo
        )
        assert risk == 100
        assert position == 1000  # min for Forex


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
