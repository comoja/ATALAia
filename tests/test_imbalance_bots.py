import unittest
import pandas as pd
import numpy as np
import talib as ta
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.ImbalanceLDN import ImbalanceLDNBot
from middleware.database import dbManager
import Sentinel.analysis.technical as technical_mod
import dataSymbol.mainOrchestrator as orchestrator_mod

class TestImbalanceBots(unittest.TestCase):
    """Pruebas unitarias para las estrategias basadas en Imbalances (LDN/NY/PMNY)."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente con su nombre de estrategia."""
        bot = ImbalanceLDNBot()
        self.assertEqual(bot.strategy_name, "ImbalanceLDN")

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese señales y genere un objeto Signal."""
        bot = ImbalanceLDNBot()
        
        # Generar datos de prueba para timeframe 5m (100 velas)
        np.random.seed(42)
        dates5m = pd.date_range(start="2026-06-01 00:00:00", periods=100, freq="5min")
        prices5m = np.ones(100) * 1.0800
        
        df5m = pd.DataFrame({
            "open": prices5m,
            "high": prices5m + 0.0001,
            "low": prices5m - 0.0001,
            "close": prices5m,
            "volume": np.random.randint(100, 500, 100)
        }, index=dates5m)
        
        symbolInfo = {
            "symbol": "EUR/USD",
            "weekly_trend": "NEUTRAL",
            "precioMaximo": 1.0790,
            "precioMinimo": 1.0750,
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
                "5min": df5m
            }
        }
        
        # Mocks
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        
        orig_detect_fvgs = technical_mod.detect_fvgs
        orig_is_fvg_mitigated = technical_mod._is_fvg_mitigated
        orig_get_structural_levels = technical_mod.get_structural_levels
        orig_capTpByAtr = technical_mod.capTpByAtr
        orig_get_last_closed_candle = orchestrator_mod.get_last_closed_candle
        
        import Sentinel.core.BaseImbalanceBot as BaseImbalanceModule
        orig_check_tp_exhaustion = BaseImbalanceModule.check_tp_exhaustion
        orig_check_signal_health = BaseImbalanceModule.check_signal_health
        
        # Seteamos el mock para la configuración por activo
        dbManager.getSymbolStrategyConfig = lambda s, sym: {
            "maxMinutosFvg": 20,
            "minRr": 1.5,
            "minConfidence": 70.0,
            "minUsdProfit": 10.0
        }
        
        # Mock de velaCorte forzado y getMexicoTime
        bot.velaCorte = {
            'type': 'LARGO',
            'idx': 50,
            'precioRuptura': 1.0805
        }
        bot.getMexicoTime = lambda: df5m.index[-1]

        
        # Mock de detección de FVG (confirmado en la última vela del dataframe)
        fvgMock = {
            "idx": 99,
            "type": "Bullish_FVG",
            "top": 1.0805,
            "bottom": 1.0800,
            "mid": 1.08025,
            "size": 0.0005,
            "timestamp": df5m.index[-1],
            "candle_time": df5m.index[-1],
            "classification": "Alta Probabilidad"
        }
        technical_mod.detect_fvgs = lambda *args, **kwargs: [fvgMock]

        
        # Mock de no mitigado
        technical_mod._is_fvg_mitigated = lambda df, fvg_idx, fvg: False
        
        # Mock de filtros de salud y agotamiento
        BaseImbalanceModule.check_tp_exhaustion = lambda *args, **kwargs: (True, 0.0, "OK")
        BaseImbalanceModule.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        # Mock de niveles estructurales
        technical_mod.get_structural_levels = lambda df, lookback: {
            "swing_high": 1.0820,
            "swing_low": 1.0780,
            "high_zone": 1.0830,
            "low_zone": 1.0770
        }
        
        # Mock de Cap ATR y last closed candle
        technical_mod.capTpByAtr = lambda tp, current, atr, dir_str, maxAtrMult: 1.0825
        orchestrator_mod.get_last_closed_candle = lambda dt, interval, df: df.iloc[-1]
        
        try:
            signals = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertEqual(len(signals), 1)
            sig = signals[0]
            self.assertEqual(sig.strategy, "ImbalanceLDN")
            self.assertEqual(sig.direction, "LARGO")
            self.assertTrue(sig.confidence >= 70)
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            technical_mod.detect_fvgs = orig_detect_fvgs
            technical_mod._is_fvg_mitigated = orig_is_fvg_mitigated
            technical_mod.get_structural_levels = orig_get_structural_levels
            technical_mod.capTpByAtr = orig_capTpByAtr
            orchestrator_mod.get_last_closed_candle = orig_get_last_closed_candle
            BaseImbalanceModule.check_tp_exhaustion = orig_check_tp_exhaustion
            BaseImbalanceModule.check_signal_health = orig_check_signal_health

if __name__ == "__main__":
    unittest.main()
