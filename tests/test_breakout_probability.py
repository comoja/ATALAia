import unittest
import pandas as pd
import numpy as np
import talib as ta
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.BreakoutProbability import BreakoutProbabilityBot
from middleware.database import dbManager

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

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol ejecute el flujo de análisis completo sin errores."""
        bot = BreakoutProbabilityBot()
        
        # Generar df de 15min con 120 velas
        np.random.seed(10)
        dates = pd.date_range(start="2026-06-01 00:00:00", periods=120, freq="15min")
        prices = np.ones(120) * 1.0800
        
        df = pd.DataFrame({
            "open": prices,
            "high": prices + 0.0001,
            "low": prices - 0.0001,
            "close": prices,
            "volume": np.random.randint(100, 500, 120)
        }, index=dates)
        
        # Generar una ruptura alcista en la última vela
        # Canal previo de 20 velas: max_val = 1.0801, min_val = 1.0799
        df.iloc[-1] = {
            "open": 1.0800,
            "high": 1.0807,
            "low": 1.0799,
            "close": 1.0806, # Ruptura
            "volume": 600
        }
        
        df["atr"] = ta.ATR(df["high"].values, df["low"].values, df["close"].values, timeperiod=14)
        df["impulseMacd"] = np.linspace(-0.0001, 0.0002, 120) # Valor alcista
        df["impulseSignal"] = np.zeros(120)
        
        # Mock de dbManager.getSymbolStrategyConfig y getStrategyConfig
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        orig_getConfig = dbManager.getStrategyConfig
        
        dbManager.getSymbolStrategyConfig = lambda s, sym: {
            "channelLen": 20,
            "targetAtrMult": 1.5,
            "minProbThreshold": 50.0,
            "minRr": 1.5
        }
        dbManager.getStrategyConfig = lambda s: {
            "start_hour": 20,
            "min_rr": 1.5,
            "min_confidence": 50.0
        }
        
        # Mock de calculateBreakoutProbability para forzar éxito
        bot.calculateBreakoutProbability = lambda df, lookback, channelLen, targetAtrMult, minRrVal: (75.0, 75.0)
        
        symbolInfo = {
            "symbol": "EUR/USD",
            "refCapital": 10000.0,
            "refRiskPct": 1.0,
            "pipMultiplier": 10000.0,
            "minDistLimit": 5.0,
            "maxDistLimit": 100.0,
            "lotStep": 0.01,
            "minLot": 0.01,
            "contractSize": 100000.0
        }
        
        preloadedData = {
            "EUR/USD": {
                "15min": df
            }
        }
        
        try:
            signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.strategy, "BreakoutProbability")
            self.assertEqual(signal.symbol, "EUR/USD")
            self.assertEqual(signal.direction, "LARGO")
            self.assertTrue(signal.confidence >= 50)
        finally:
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            dbManager.getStrategyConfig = orig_getConfig

if __name__ == "__main__":
    unittest.main()
