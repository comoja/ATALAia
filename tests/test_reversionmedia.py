import unittest
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.ReversionMedia import ReversionMediaBot
from middleware.database import dbManager

class TestReversionMedia(unittest.TestCase):
    """Pruebas unitarias para la estrategia ReversionMedia (ReversionMediaBot)."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente."""
        bot = ReversionMediaBot()
        self.assertIsNotNone(bot)

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese señales y genere un objeto Signal en ReversionMedia."""
        bot = ReversionMediaBot()
        
        # Generar datos de prueba para timeframe 1h (120 velas)
        dates1h = pd.date_range(start="2026-06-01 00:00:00", periods=120, freq="1h", tz="America/Mexico_City")
        prices1h = np.ones(120) * 1.0800
        
        # Última vela con cierre por debajo del LRC Lower para gatillar Largo
        prices1h[-1] = 1.0700
        
        df1h = pd.DataFrame({
            "open": prices1h,
            "high": prices1h + 0.0005,
            "low": prices1h - 0.0005,
            "close": prices1h,
            "volume": np.random.randint(100, 500, 120),
            "impulseMacd": np.ones(120) * 0.0,
            "impulseSignal": np.ones(120) * 0.0
        }, index=dates1h)
        
        # Configurar cruce del impulse MACD alcista en las últimas 2 velas
        df1h.loc[dates1h[-2], "impulseMacd"] = -0.5
        df1h.loc[dates1h[-2], "impulseSignal"] = 0.0
        df1h.loc[dates1h[-1], "impulseMacd"] = 0.5
        df1h.loc[dates1h[-1], "impulseSignal"] = 0.0
        
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
            "GBP/USD": df1h
        }
        
        # Mocks de base de datos
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        dbManager.getSymbolStrategyConfig = lambda strat, sym: {
            "lrcPeriod": 100,
            "lrcDev": 2.0,
            "minRr": 2.5,
            "rsiPeriod": 14,
            "atrPeriod": 14,
            "minConfidence": 70,
            "minUsdProfit": 10.0
        }
        
        # Mocks de métodos del Bot
        orig_calculateLrc = bot.calculateLrc
        orig_checkDivergence = bot.checkDivergence
        orig_rsi = bot.rsi
        orig_atr = bot.atr
        
        # Mock de LRC: bandas que envuelven el precio excepto al final
        center = np.ones(120) * 1.0800
        upper = np.ones(120) * 1.0850
        lower = np.ones(120) * 1.0750
        slope = np.ones(120) * 0.0001 # Tendencia alcista
        bot.calculateLrc = lambda closePrices, period, dev: (center, upper, lower, slope)
        
        # Mock de Divergencia: desactivada por defecto
        bot.checkDivergence = lambda df, rsiSeries, lookback: {"bullish": False, "bearish": False}
        
        # Mock de RSI y ATR
        bot.rsi = lambda df, period: pd.Series(np.ones(120) * 25.0, index=df.index) # RSI < 30
        bot.atr = lambda df, period: pd.Series(np.ones(120) * 0.0010, index=df.index)
        
        # Mock de importaciones externas
        import Sentinel.core.ReversionMedia as ReversionMediaModule
        orig_check_signal_health = ReversionMediaModule.check_signal_health
        import Sentinel.analysis.risk as riskModule
        orig_calculatePositionSize = riskModule.calculatePositionSize
        
        ReversionMediaModule.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        riskModule.calculatePositionSize = lambda capital, riskPct, slDist, symInfo, entryPrice: (0.1, 10.0, 15.0)
        
        try:
            signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.strategy, "ReversionMedia")
            self.assertEqual(signal.direction, "LARGO")
            self.assertEqual(signal.entry_price, 1.0700)
            self.assertTrue(signal.stop_loss < 1.0700)
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            bot.calculateLrc = orig_calculateLrc
            bot.checkDivergence = orig_checkDivergence
            bot.rsi = orig_rsi
            bot.atr = orig_atr
            ReversionMediaModule.check_signal_health = orig_check_signal_health
            riskModule.calculatePositionSize = orig_calculatePositionSize

if __name__ == "__main__":
    unittest.main()
