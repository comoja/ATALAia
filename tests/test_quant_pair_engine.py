import os
import sys
sys.path.insert(0, '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia')

import numpy as np
import pandas as pd
from backend.services.quant_pair_engine import QuantPairEngine, quantEngine

def test_identify_fvg():
    dates = pd.date_range("2026-01-01", periods=10, freq="1D")
    df = pd.DataFrame({
        "datetime": dates,
        "open":  [100, 102, 108, 107, 105, 104, 98, 92, 94, 95],
        "high":  [101, 103, 110, 109, 106, 105, 99, 93, 95, 96],
        "low":   [99,  101, 105, 106, 104, 102, 96, 90, 92, 93],
        "close": [100, 102, 109, 108, 105, 103, 97, 91, 94, 95]
    })
    
    fvgs = quantEngine.identifyFvg(df)
    assert len(fvgs) >= 2, f"Se esperaban al menos 2 FVGs, encontrados: {len(fvgs)}"
    assert any(f["type"] == "BULLISH_FVG" for f in fvgs)
    assert any(f["type"] == "BEARISH_FVG" for f in fvgs)
    print("✅ test_identify_fvg superado:", len(fvgs), "FVGs identificados")

def test_dynamic_zscore_bands():
    series = pd.Series(np.random.normal(1.5, 0.2, 100))
    dfBands = quantEngine.calculateDynamicZScoreBands(series, window=20, stdThreshold=2.0)
    assert "zScore" in dfBands.columns
    assert "upperBand2Std" in dfBands.columns
    assert "lowerBand2Std" in dfBands.columns
    assert "upperBand3Std" in dfBands.columns
    assert len(dfBands) == 100
    print("✅ test_dynamic_zscore_bands superado!")

def test_fft_spectral_analysis():
    x = np.arange(100)
    signal = 1.0 + 0.5 * np.sin(2 * np.pi * x / 20.0) + np.random.normal(0, 0.05, 100)
    series = pd.Series(signal)
    
    result = quantEngine.computeFftSpectralAnalysis(series, numHarmonics=3)
    assert "reconstructed" in result
    assert "dominantPeriod" in result
    assert "periodsToMeanCross" in result
    assert 15 <= result["dominantPeriod"] <= 25, f"Periodo detectado: {result['dominantPeriod']}"
    print("✅ test_fft_spectral_analysis superado: Periodo Dominante =", result["dominantPeriod"])

def test_ornstein_uhlenbeck_half_life():
    np.random.seed(42)
    n = 200
    y = np.zeros(n)
    theta = 0.15
    mu = 1.5
    y[0] = 1.8
    for t in range(1, n):
        y[t] = y[t-1] - theta * (y[t-1] - mu) + np.random.normal(0, 0.05)
        
    res = quantEngine.calculateOrnsteinUhlenbeckHalfLife(pd.Series(y))
    assert res["isMeanReverting"] is True
    assert res["halfLife"] is not None
    assert 2.0 <= res["halfLife"] <= 10.0, f"Half life estimada: {res['halfLife']}"
    print("✅ test_ornstein_uhlenbeck_half_life superado: Half Life =", res["halfLife"], "periodos")

def test_vectorized_backtest():
    dates = pd.date_range("2026-01-01", periods=150, freq="1D")
    x = np.arange(150)
    ratio = 1.0 + 0.3 * np.sin(2 * np.pi * x / 25.0)
    df = pd.DataFrame({
        "datetime": dates,
        "ratio": ratio
    })
    dfBands = quantEngine.calculateDynamicZScoreBands(df["ratio"], window=20)
    df["zScore"] = dfBands["zScore"]
    
    bt = quantEngine.runVectorizedBacktest(df, entryZThreshold=1.5, exitZThreshold=0.0)
    assert "totalTrades" in bt
    assert "winRate" in bt
    assert "profitFactor" in bt
    assert "equityCurve" in bt
    assert bt["totalTrades"] > 0
    print("✅ test_vectorized_backtest superado! Trades:", bt["totalTrades"], "WinRate:", bt["winRate"], "% ProfitFactor:", bt["profitFactor"])

if __name__ == "__main__":
    test_identify_fvg()
    test_dynamic_zscore_bands()
    test_fft_spectral_analysis()
    test_ornstein_uhlenbeck_half_life()
    test_vectorized_backtest()
    print("\n🎉 TODOS LOS TESTS DEL MOTOR CUANTITATIVO PASARON EXITOSAMENTE!")
