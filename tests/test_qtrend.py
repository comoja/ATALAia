import unittest
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.QTrend import QTrendBot
from middleware.database import dbManager

class TestQTrend(unittest.TestCase):
    """Pruebas unitarias para la estrategia QTrend (QTrendBot)."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente."""
        bot = QTrendBot()
        self.assertIsNotNone(bot)

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese señales y genere un objeto Signal en QTrend."""
        bot = QTrendBot()
        
        # Generar datos de prueba para timeframe 15m (100 velas)
        dates15m = pd.date_range(start="2026-06-01 00:00:00", periods=100, freq="15min", tz="America/Mexico_City")
        prices15m = np.ones(100) * 1.0800
        
        df15m = pd.DataFrame({
            "open": prices15m,
            "high": prices15m + 0.0001,
            "low": prices15m - 0.0001,
            "close": prices15m,
            "volume": np.random.randint(100, 500, 100)
        }, index=dates15m)
        
        symbolInfo = {
            "symbol": "GBP/USD",
            "refCapital": 10000.0,
            "refRiskPct": 1.0,
            "pipMultiplier": 10000.0,
            "minDistLimit": 5.0,
            "maxDistLimit": 100.0,
            "lotStep": 0.01,
            "minLot": 0.01,
            "contractSize": 100000.0,
            "broker": 1
        }
        
        preloadedData = {
            "GBP/USD": df15m
        }
        
        # Mocks de base de datos
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        dbManager.getSymbolStrategyConfig = lambda strat, sym: {
            "supertrendPeriod": 10,
            "supertrendMultiplier": 3.0,
            "qtrendFast": 9,
            "qtrendSlow": 21,
            "minRr": 1.5,
            "min_confidence": 70,
            "min_usd_profit": 10.0
        }
        
        # Mocks técnicos
        import Sentinel.core.QTrend as QTrendModule
        
        orig_calculateAtrStop = QTrendModule.technical.calculateAtrStop
        orig_check_tp_exhaustion = QTrendModule.check_tp_exhaustion
        orig_check_signal_health = QTrendModule.check_signal_health
        import Sentinel.analysis.risk as riskModule
        orig_calculatePositionSize = riskModule.calculatePositionSize
        orig_ta_atr = QTrendModule.ta.ATR
        
        QTrendModule.ta.ATR = lambda *args, **kwargs: np.ones(100) * 0.01
        
        # SuperTrend alcista y stop trail
        stTrend = np.ones(100)
        stTrend[-1] = 1
        stTrend[-2] = -1 # Genera crossover alcista
        stTrail = np.ones(100) * 1.0780
        QTrendModule.technical.calculateAtrStop = lambda df, period, mult: (stTrend, stTrail)
        
        # EMAs de QTrend: emaFast cruza por encima de emaSlow en la última vela
        emaFast = np.ones(100) * 1.0805
        emaFast[-2] = 1.0790
        emaSlow = np.ones(100) * 1.0800
        emaSlow[-2] = 1.0800
        bot.calculateQTrend = lambda df, fast, slow: (emaFast, emaSlow)
        
        # Mock de salud y agotamiento
        QTrendModule.check_tp_exhaustion = lambda *args, **kwargs: (True, 0.0, "OK")
        QTrendModule.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        # Mock de tamaño de posición
        riskModule.calculatePositionSize = lambda capital, riskPct, slDist, symInfo, entryPrice: (0.1, 10.0, 15.0)
        
        try:
            signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.strategy, "QTrend")
            self.assertEqual(signal.direction, "LARGO")
            self.assertEqual(signal.entry_price, 1.0800)
            self.assertEqual(signal.stop_loss, 1.0780)
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            QTrendModule.technical.calculateAtrStop = orig_calculateAtrStop
            QTrendModule.check_tp_exhaustion = orig_check_tp_exhaustion
            QTrendModule.check_signal_health = orig_check_signal_health
            riskModule.calculatePositionSize = orig_calculatePositionSize
            QTrendModule.ta.ATR = orig_ta_atr

if __name__ == "__main__":
    unittest.main()
