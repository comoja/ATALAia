import unittest
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime, time, timedelta
import pytz

from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot
from middleware.database import dbManager

class TestSesgoBiasHTF(unittest.TestCase):
    """Pruebas unitarias para la estrategia SesgoBiasHTF."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente."""
        bot = SesgoBiasHTFBot()
        self.assertIsNotNone(bot)
        self.assertEqual(bot.fibonacciLevel, 0.50)
        self.assertEqual(bot.entryFibMin, 0.25)

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol genere una señal con mocks adecuados."""
        bot = SesgoBiasHTFBot()
        
        # Generar datos de prueba para timeframe 15min (200 velas)
        dates15m = pd.date_range(start="2026-06-01 00:00:00", periods=200, freq="15min", tz="America/Mexico_City")
        prices15m = np.ones(200) * 1.0800
        
        df15m = pd.DataFrame({
            "open": prices15m,
            "high": prices15m + 0.0001,
            "low": prices15m - 0.0001,
            "close": prices15m,
            "volume": np.random.randint(100, 500, 200)
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
            "weekly_trend": "ALCISTA"
        }
        
        preloadedData = {
            "EUR/USD": {
                "15min": df15m,
                "1h": df15m.resample("1h").first().dropna(),
                "1d": df15m.resample("1D").first().dropna(),
                "1w": df15m.resample("1W").first().dropna(),
                "1m": df15m.resample("ME").first().dropna()
            }
        }
        
        # Mocks de base de datos
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        dbManager.getSymbolStrategyConfig = lambda strat, sym: {
            "fibonacciLevel": 0.50,
            "entryFibMin": 0.25,
            "entryFibMax": 0.50,
            "swingLookback": 50,
            "fvgMinPct": 0.0001,
            "minDistancePips": 10.0,
            "maxSignalAgeMinutes": 60,
            "mssLookback": 5,
            "useKillzones": False,
            "volatilityThreshold": 0.5,
            "useOteFilter": False,
            "oteFibMin": 0.62,
            "oteFibMax": 0.79,
            "oteReduceConf": 15,
            "minConfidence": 70,
            "minRr": 1.5,
            "minUsdProfit": 10.0
        }
        
        # Mock de métodos del Bot
        orig_get_htf_bias = bot.get_htf_bias
        orig_get_consensus_bias = bot.get_consensus_bias
        orig_analyze_po3 = bot.analyze_po3_cycle
        orig_get_ob = bot.get_ob_analysis
        orig_check_ote = bot.check_ote
        
        bot.get_htf_bias = lambda *args, **kwargs: {"H1": "LARGO", "D": "LARGO", "W": "LARGO", "M": "LARGO"}
        bot.get_consensus_bias = lambda biases: ("LARGO", 0.95)
        bot.analyze_po3_cycle = lambda *args, **kwargs: {"idx": 190, "type": "LIQUIDITY_SWEEP_LOW", "level": 1.0790, "swept_price": 1.0785, "candle_range": 0.001, "mss": True}
        bot.get_ob_analysis = lambda *args, **kwargs: {"order_blocks": [], "breaker_blocks": [], "ob_score": 15, "in_ob_zone": True, "nearest_ob": None, "ob_count": 1}
        bot.check_ote = lambda *args, **kwargs: {"in_ote": True, "ote_zone": {"ote_low": 1.0780, "ote_high": 1.0790, "sweet_spot": 1.0785}, "swing_high": 1.0805, "swing_low": 1.0780}
        
        import Sentinel.core.SesgoBiasHTF as HTFModule
        orig_check_tp = HTFModule.check_tp_exhaustion
        orig_check_health = HTFModule.check_signal_health
        
        HTFModule.check_tp_exhaustion = lambda *args, **kwargs: (True, 0.0, "OK")
        HTFModule.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        try:
            signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.strategy, "SesgoBiasHTF")
            self.assertEqual(signal.direction, "LARGO")
            self.assertEqual(signal.entry_price, 1.0800)
            self.assertEqual(signal.stop_loss, 1.0784) # 1.0785 - (0.001 * 0.1) = 1.0784
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            bot.get_htf_bias = orig_get_htf_bias
            bot.get_consensus_bias = orig_get_consensus_bias
            bot.analyze_po3_cycle = orig_analyze_po3
            bot.get_ob_analysis = orig_get_ob
            bot.check_ote = orig_check_ote
            HTFModule.check_tp_exhaustion = orig_check_tp
            HTFModule.check_signal_health = orig_check_health

if __name__ == "__main__":
    unittest.main()
