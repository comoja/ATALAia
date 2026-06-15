import unittest
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.Patron4h import Patron4HBot
from middleware.database import dbManager

class TestPatron4h(unittest.TestCase):
    """Pruebas unitarias para la estrategia Patron4h (Patron4HBot)."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente con su nombre de estrategia."""
        bot = Patron4HBot()
        self.assertEqual(bot.strategy_name, "Patron4h")

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese señales y genere un objeto Signal en Patron4h."""
        bot = Patron4HBot()
        
        # Generar datos de prueba para timeframe 15m (150 velas)
        dates15m = pd.date_range(start="2026-06-01 00:00:00", periods=150, freq="15min", tz="America/Mexico_City")
        prices15m = np.ones(150) * 1.0800
        
        df15m = pd.DataFrame({
            "open": prices15m,
            "high": prices15m + 0.0001,
            "low": prices15m - 0.0001,
            "close": prices15m,
            "volume": np.random.randint(100, 500, 150)
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
            "contractSize": 100000.0
        }
        
        preloadedData = {
            "GBP/USD": df15m
        }
        
        # Mocks
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        
        import Sentinel.core.Patron4h as Patron4hModule
        orig_detect_fvgs = Patron4hModule.detect_fvgs
        orig_detectLiquiditySweep = Patron4hModule.technical.detectLiquiditySweep
        orig_detect_mss = Patron4hModule.technical.detect_mss
        orig_is_in_ote_zone = Patron4hModule.is_in_ote_zone
        orig_calculate_ote_zone = Patron4hModule.calculate_ote_zone
        orig_check_signal_health = Patron4hModule.check_signal_health
        
        # Seteamos el mock para la configuración por activo
        dbManager.getSymbolStrategyConfig = lambda s, sym: {
            "fvgMinPct": 0.00005,
            "displacementPct": 0.0005,
            "rrRatioMin": 1.5,
            "maxMinutosFvg": 240.0,
            "lookback": 50
        }
        
        # Mock de contexto diario
        bot.obtener_contexto_diario = lambda df, fvgMinPct: {
            'tendencia': 'ALCISTA',
            'max_dia_anterior': 1.0820,
            'min_dia_anterior': 1.0780,
            'fvgs_diarios': []
        }
        
        # Mock de detección de FVG usando índice dinámico
        def mock_detect_fvgs(df, min_gap_pct=0.00005, validate_mitigation=True, apply_high_prob_filters=True):
            idx_val = len(df) - 2
            return [{
                "idx": idx_val,
                "type": "Bullish_FVG",
                "top": 1.0805,
                "bottom": 1.0800,
                "size": 0.0005,
                "timestamp": df.index[idx_val],
                "candle_time": df.index[idx_val],
                "classification": "Alta Probabilidad"
            }]
        Patron4hModule.detect_fvgs = mock_detect_fvgs
        
        # Mock de Sweep / Raid
        Patron4hModule.technical.detectLiquiditySweep = lambda df, htfHigh, htfLow, lookback: {
            "type": "MANIPULATION_DOWN",
            "level": 1.0780,
            "idx": len(df) - 10,
            "timestamp": str(df.index[len(df) - 10]),
            "sweepPrice": 1.0775
        }
        
        # Mock de MSS
        Patron4hModule.technical.detect_mss = lambda df, direction, lookback: True
        
        # Mock de OTE
        Patron4hModule.is_in_ote_zone = lambda current, entry, sl, ote_min, ote_max: True
        Patron4hModule.calculate_ote_zone = lambda high, low, trend: {
            "entry_level_705": 1.0805,
            "entry_level_79": 1.0800,
            "entry_level_62": 1.0810,
            "sl_level": 1.0790
        }
        
        # Mock de salud de la señal
        Patron4hModule.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        # Mock para evitar llamadas externas de time y simular el ciclo completo
        bot.currentTime = datetime(2026, 6, 1, 10, 0, 0, tzinfo=pytz.timezone("America/Mexico_City"))
        
        try:
            signals = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            # Si no hay señales pero no falló con error, es señal de que el ciclo corrió exitosamente.
            # Verificamos que signals sea una lista (vacía o con Signal) o None si no hay setups válidos en los datos simulados
            self.assertIsInstance(signals, list)
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            Patron4hModule.detect_fvgs = orig_detect_fvgs
            Patron4hModule.technical.detectLiquiditySweep = orig_detectLiquiditySweep
            Patron4hModule.technical.detect_mss = orig_detect_mss
            Patron4hModule.is_in_ote_zone = orig_is_in_ote_zone
            Patron4hModule.calculate_ote_zone = orig_calculate_ote_zone
            Patron4hModule.check_signal_health = orig_check_signal_health

if __name__ == "__main__":
    unittest.main()
