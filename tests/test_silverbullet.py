import unittest
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime, time, timedelta
import pytz

from Sentinel.core.SilverBullet import SilverBulletBot
from middleware.database import dbManager

class TestSilverBullet(unittest.TestCase):
    """Pruebas unitarias para la estrategia SilverBullet (SilverBulletBot)."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente."""
        bot = SilverBulletBot()
        self.assertIsNotNone(bot)

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese señales y genere un objeto Signal en SilverBullet."""
        bot = SilverBulletBot()
        
        # Generar datos de prueba para timeframe 5min (100 velas)
        dates5m = pd.date_range(start="2026-06-01 08:00:00", periods=100, freq="5min", tz="America/Mexico_City")
        prices5m = np.ones(100) * 1.0800
        
        df5m = pd.DataFrame({
            "open": prices5m,
            "high": prices5m + 0.0001,
            "low": prices5m - 0.0001,
            "close": prices5m,
            "volume": np.random.randint(100, 500, 100)
        }, index=dates5m)
        
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
            "GBP/USD": {
                "5min": df5m
            }
        }
        
        # Mocks de base de datos
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        dbManager.getSymbolStrategyConfig = lambda strat, sym: {
            "fvgMinPct": 0.0001,
            "minRr": 1.5,
            "minConfidence": 70,
            "minUsdProfit": 10.0,
            "filterByHtfTrend": False,
            "minAdx": 15.0
        }
        
        # Mock de métodos del Bot
        orig_now_ny = bot._now_ny
        orig_now_mx = bot._now_mx
        orig_calc_adx = bot._calc_adx
        orig_get_ref_range = bot._get_reference_range
        orig_detect_sweep = bot._detect_sweep
        orig_detect_mss = bot._detect_mss
        orig_detect_fvg = bot._detect_fvg
        
        # Forzar hora de la sesión de trading (9:15 AM NY del 1 de Junio de 2026)
        ny_tz = pytz.timezone("America/New_York")
        mx_tz = pytz.timezone("America/Mexico_City")
        fixed_ny = ny_tz.localize(datetime(2026, 6, 1, 9, 15, 0))
        fixed_mx = fixed_ny.astimezone(mx_tz)
        
        bot._now_ny = lambda: fixed_ny
        bot._now_mx = lambda: fixed_mx
        
        bot._calc_adx = lambda df: 25.0
        bot._get_reference_range = lambda symbol, df, window_start_ny, ref_min=15: {"high": 1.0805, "low": 1.0795, "open": 1.0800, "n_candles": 3}
        bot._detect_sweep = lambda symbol, df, ref, window_start_ny: {"type": "LARGO", "swept_level": 1.0795, "sweep_low": 1.0790, "candle_idx": df.index[-1]}
        bot._detect_mss = lambda df, sweep: True
        bot._detect_fvg = lambda df, direction: {"type": "LARGO_FVG", "mid": 1.0800, "idx": 95, "candle_time": df.index[-1]}
        
        import Sentinel.core.SilverBullet as SBModule
        orig_check_tp = SBModule.technical.check_tp_exhaustion
        orig_check_health = SBModule.technical.check_signal_health
        orig_calculate_fvg_setup = SBModule.technical.calculate_fvg_setup
        
        SBModule.technical.calculate_fvg_setup = lambda fvg, price, atr: {
            "entry": 1.0800,
            "sl": 1.0780,
            "direction": "LARGO"
        }
        SBModule.technical.check_tp_exhaustion = lambda *args, **kwargs: (True, 0.0, "OK")
        SBModule.technical.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        try:
            signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.strategy, "SilverBullet")
            self.assertEqual(signal.direction, "LARGO")
            self.assertEqual(signal.entry_price, 1.0800)
            self.assertEqual(signal.stop_loss, 1.0780)
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            bot._now_ny = orig_now_ny
            bot._now_mx = orig_now_mx
            bot._calc_adx = orig_calc_adx
            bot._get_reference_range = orig_get_ref_range
            bot._detect_sweep = orig_detect_sweep
            bot._detect_mss = orig_detect_mss
            bot._detect_fvg = orig_detect_fvg
            SBModule.technical.check_tp_exhaustion = orig_check_tp
            SBModule.technical.check_signal_health = orig_check_health
            SBModule.technical.calculate_fvg_setup = orig_calculate_fvg_setup

if __name__ == "__main__":
    unittest.main()
