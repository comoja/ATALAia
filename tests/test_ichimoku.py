import unittest
import pandas as pd
import numpy as np
from Sentinel.core.Ichimoku import IchimokuBot

class TestIchimoku(unittest.TestCase):
    """Pruebas unitarias para la estrategia Ichimoku con soporte dinámico."""

    def testCalcIndicators(self):
        """Verifica que el cálculo de Ichimoku dinámico funcione sin errores."""
        bot = IchimokuBot()
        
        # Generar datos de prueba
        np.random.seed(42)
        prices = np.linspace(100.0, 105.0, 100) + np.random.normal(0, 0.5, 100)
        df = pd.DataFrame({
            "open": prices - 0.5,
            "high": prices + 1.0,
            "low": prices - 1.0,
            "close": prices,
            "volume": np.random.randint(100, 1000, 100)
        })
        
        # Probar con parámetros por defecto
        dfResult = bot._calc_indicators(df)
        self.assertIn('tenkan_sen', dfResult.columns)
        self.assertIn('kijun_sen', dfResult.columns)
        self.assertIn('senkou_span_a', dfResult.columns)
        self.assertIn('senkou_span_b', dfResult.columns)
        self.assertFalse(dfResult['tenkan_sen'].isna().all())
        
        # Probar con parámetros personalizados (7, 22, 44, 22)
        dfResultCustom = bot._calc_indicators(df, tenkanPeriod=7, kijunPeriod=22, senkouPeriod=44, displacement=22)
        self.assertFalse(dfResultCustom['tenkan_sen'].isna().all())
        self.assertFalse(dfResultCustom['senkou_span_b'].isna().all())

    def testCalcHtfTrend(self):
        """Verifica que el sesgo de tendencia HTF se calcule correctamente con parámetros dinámicos."""
        bot = IchimokuBot()
        
        # Generar datos de tendencia alcista clara en HTF
        prices = np.linspace(100.0, 120.0, 150)
        dfH1 = pd.DataFrame({
            "open": prices - 0.5,
            "high": prices + 1.0,
            "low": prices - 1.0,
            "close": prices,
            "volume": np.random.randint(100, 1000, 150)
        })
        
        # Usar _calc_indicators para generar los campos necesarios para _calc_htf_trend
        # ya que _calc_htf_trend los calcula internamente.
        trend = bot._calc_htf_trend(dfH1, tenkanPeriod=7, kijunPeriod=22, senkouPeriod=44, displacement=22)
        self.assertIn(trend, ["ALCISTA", "BAJISTA", "NEUTRAL"])
        self.assertEqual(trend, "ALCISTA")

    def testGetSymbolStrategyConfig(self):
        """Verifica que la obtención de configuraciones de Ichimoku en dbManager sea correcta."""
        from middleware.database import dbManager
        
        # Recuperar configuración de un símbolo cualquiera
        config = dbManager.getSymbolStrategyConfig("Ichimoku", "INVALID/SYMBOL")
        self.assertIsNotNone(config)

if __name__ == "__main__":
    unittest.main()
