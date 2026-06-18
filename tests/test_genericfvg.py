import unittest
import pandas as pd
import numpy as np
import talib as ta
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.GenericFVG import GenericFVGBot
from middleware.database import dbManager

class TestGenericFVG(unittest.TestCase):
    """Pruebas unitarias para la estrategia GenericFVG."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente con su nombre y temporalidades."""
        bot = GenericFVGBot()
        self.assertEqual(bot.strategy_name, "GenericFVG")
        self.assertIn("15min", bot.intervals)

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese señales y genere un objeto Signal."""
        bot = GenericFVGBot(intervals=["15min"])
        
        # Generar datos de prueba para timeframe 15m (220 velas para satisfacer mínimo de 200)
        np.random.seed(42)
        dates15m = pd.date_range(start="2026-06-01 00:00:00", periods=220, freq="15min")
        prices15m = np.ones(220) * 1.0800
        
        df15m = pd.DataFrame({
            "open": prices15m,
            "high": prices15m + 0.0001,
            "low": prices15m - 0.0001,
            "close": prices15m,
            "volume": np.random.randint(100, 500, 220)
        }, index=dates15m)
        
        symbolInfo = {
            "symbol": "EUR/USD",
            "weekly_trend": "NEUTRAL",
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
                "15min": df15m
            }
        }
        
        # Mocks
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        
        import Sentinel.core.GenericFVG as GenericFVGModule
        orig_detect_fvgs = GenericFVGModule.technical.detect_fvgs
        orig_get_prev_day_high_low = GenericFVGModule.technical.get_prev_day_high_low
        orig_detectLiquiditySweep = GenericFVGModule.technical.detectLiquiditySweep
        orig_calculate_fvg_setup = GenericFVGModule.technical.calculate_fvg_setup
        orig_check_signal_health = GenericFVGModule.technical.check_signal_health
        orig_get_structural_levels = GenericFVGModule.technical.get_structural_levels
        orig_capTpByAtr = GenericFVGModule.technical.capTpByAtr
        
        # Seteamos el mock para la configuración por activo
        dbManager.getSymbolStrategyConfig = lambda s, sym: {
            "minRr": 1.5,
            "minConfidence": 70.0,
            "requireHtfSweep": True,
            "minUsdProfit": 10.0
        }
        
        # Mock de detección de FVG (confirmado en la última vela del dataframe de 220 velas)
        fvgMock = {
            "idx": 219,
            "type": "Bullish_FVG",
            "top": 1.0805,
            "bottom": 1.0800,
            "size": 0.0005,
            "timestamp": df15m.index[-1],
            "candle_time": df15m.index[-1],
            "classification": "Alta Probabilidad"
        }
        GenericFVGModule.technical.detect_fvgs = lambda *args, **kwargs: [fvgMock]
        
        # Mock de niveles HTF y Sweep
        GenericFVGModule.technical.get_prev_day_high_low = lambda df: {"pdh": 1.0820, "pdl": 1.0780}
        GenericFVGModule.technical.detectLiquiditySweep = lambda df, htfHigh, htfLow, lookback: {
            "type": "MANIPULATION_DOWN",
            "level": htfLow,
            "idx": 200,
            "timestamp": str(df15m.index[200]),
            "sweepPrice": 1.0775
        }
        
        # Mock de calculate_fvg_setup para evitar distancia de riesgo cero
        GenericFVGModule.technical.calculate_fvg_setup = lambda fvg, current, atr: {
            "entry": 1.0805,
            "sl": 1.0790,
            "sl_dist": 0.0015,
            "direction": "LARGO",
            "is_opportunistic": False
        }
        
        # Mock de salud de la señal
        GenericFVGModule.technical.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        # Mock de niveles estructurales
        GenericFVGModule.technical.get_structural_levels = lambda df, lookback: {
            "swing_high_body": 1.0830,
            "swing_low_body": 1.0770
        }
        
        # Mock de Cap ATR
        GenericFVGModule.technical.capTpByAtr = lambda tp, current, atr, dir_str, maxAtrMult: 1.0825
        
        try:
            signals = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertEqual(len(signals), 1)
            sig = signals[0]
            self.assertEqual(sig.strategy, "GenericFVG")
            self.assertEqual(sig.direction, "LARGO")
            self.assertTrue(sig.confidence >= 70)
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            GenericFVGModule.technical.detect_fvgs = orig_detect_fvgs
            GenericFVGModule.technical.get_prev_day_high_low = orig_get_prev_day_high_low
            GenericFVGModule.technical.detectLiquiditySweep = orig_detectLiquiditySweep
            GenericFVGModule.technical.calculate_fvg_setup = orig_calculate_fvg_setup
            GenericFVGModule.technical.check_signal_health = orig_check_signal_health
            GenericFVGModule.technical.get_structural_levels = orig_get_structural_levels
            GenericFVGModule.technical.capTpByAtr = orig_capTpByAtr

if __name__ == "__main__":
    unittest.main()
