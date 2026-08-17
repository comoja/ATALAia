import unittest
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.Sniper import SniperBot
from middleware.database import dbManager

class TestSniper(unittest.TestCase):
    """Pruebas unitarias para la estrategia Sniper (SniperBot)."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente."""
        mockModel = "dummy_model"
        bot = SniperBot(mlModelInstance=mockModel)
        self.assertIsNotNone(bot)
        self.assertEqual(bot.model, mockModel)

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol genere una señal Sniper con mocks de ML y base de datos."""
        mockModel = "dummy_model"
        bot = SniperBot(mlModelInstance=mockModel)
        
        # Generar datos de prueba para timeframe 15min (200 velas)
        dates15m = pd.date_range(start="2026-06-01 00:00:00", periods=200, freq="15min", tz="America/Mexico_City")
        prices15m = np.ones(200) * 1.0800
        
        df15m = pd.DataFrame({
            "open": prices15m,
            "high": prices15m + 0.0001,
            "low": prices15m - 0.0001,
            "close": prices15m,
            "volume": np.random.randint(100, 500, 200),
            "atr": np.ones(200) * 0.0010,
            "impulseMacd": np.ones(200) * 0.0001,
            "impulseSignal": np.ones(200) * 0.00005,
            "rsi": np.ones(200) * 55.0,
            "pendienteRsi": np.ones(200) * 0.2,
            "pendienteCci": np.ones(200) * 0.6,
            "ema20": np.ones(200) * 1.0810,
            "ema50": np.ones(200) * 1.0800
        }, index=dates15m)
        
        symbolInfo = {
            "symbol": "EUR/USD",
            "refCapital": 10000.0,
            "refRiskPct": 1.0,
            "pipMultiplier": 10000.0,
            "minDistLimit": 5.0,
            "maxDistLimit": 100.0,
            "lotStep": 0.01,
            "minLot": 0.01,
            "contractSize": 100000.0,
            "broker": 1,
            "tipo": "FOREX",
            "pip": 0.0001,
            "intervalo": "15min",
            "momentum": "🚀 ALCISTA"
        }
        
        preloadedData = {
            "EUR/USD": df15m
        }
        
        # Mocks de base de datos
        origGetSymbolConfig = dbManager.getSymbolStrategyConfig
        dbManager.getSymbolStrategyConfig = lambda strat, sym: {
            "probaThresholdLong": 0.60,
            "probaThresholdShort": 0.40,
            "minConfidence": 70.0,
            "minRr": 1.5,
            "riskUsd": 100.0,
            "minUsdProfit": 10.0,
            "jpyThresholdAdjustPct": 0.0,
            "jpyMinConfidenceAdjustPct": 0.0,
            "jpyExtraConfirmations": 0,
            "maxRr": 2.5
        }
        
        # Mock de métodos del Bot y ML
        origGetAndPrepare = bot._get_and_prepare_data
        async def mockGetAndPrepare(*args, **kwargs):
            return df15m
        bot._get_and_prepare_data = mockGetAndPrepare
        origCalculateAdx = bot._calculate_dynamic_adx
        bot._calculate_dynamic_adx = lambda df: 25.0
        
        import Sentinel.core.Sniper as SniperModule
        origCleanData = SniperModule.mlModel.cleanDataForModel
        origPredictProba = SniperModule.mlModel.predictProba
        origDetectOB = SniperModule.detect_order_blocks
        origOBConfluence = SniperModule.ob_confluence_score
        origGetStructural = SniperModule.technical.get_structural_levels
        origCheckTp = SniperModule.check_tp_exhaustion
        origCheckHealth = SniperModule.check_signal_health
        
        # Retornar el mismo DF para X
        SniperModule.mlModel.cleanDataForModel = lambda df: (df15m, None)
        # Retornar probabilidad alta para LARGO (0.75 >= 0.60)
        SniperModule.mlModel.predictProba = lambda model, X: 0.75
        
        SniperModule.detect_order_blocks = lambda df, obDir, lookback=60: []
        SniperModule.ob_confluence_score = lambda close, obs, obDir, atr=0.001: {"score": 10, "in_ob_zone": True}
        SniperModule.technical.get_structural_levels = lambda df, lookback=20: {
            "swing_low": 1.0790,
            "swing_high": 1.0810,
            "low_zone": 1.0790,
            "high_zone": 1.0820
        }
        
        SniperModule.check_tp_exhaustion = lambda *args, **kwargs: (True, 0.0, "OK")
        SniperModule.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        try:
            signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.strategy, "Sniper")
            self.assertEqual(signal.direction, "LARGO")
            self.assertEqual(signal.entry_price, 1.0800)
            self.assertGreater(signal.take_profit, 1.0800)
            self.assertLess(signal.stop_loss, 1.0800)
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = origGetSymbolConfig
            bot._get_and_prepare_data = origGetAndPrepare
            bot._calculate_dynamic_adx = origCalculateAdx
            SniperModule.mlModel.cleanDataForModel = origCleanData
            SniperModule.mlModel.predictProba = origPredictProba
            SniperModule.detect_order_blocks = origDetectOB
            SniperModule.ob_confluence_score = origOBConfluence
            SniperModule.technical.get_structural_levels = origGetStructural
            SniperModule.check_tp_exhaustion = origCheckTp
            SniperModule.check_signal_health = origCheckHealth

if __name__ == "__main__":
    unittest.main()
