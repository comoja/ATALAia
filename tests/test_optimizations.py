import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np
import asyncio
import logging

# Configurar logger para que salga en stdout durante la prueba
logging.basicConfig(level=logging.INFO)

from Sentinel.core.QTrend import QTrendBot
from Sentinel.core.Sniper import SniperBot
from Sentinel.core.models import Signal

class TestStrategyOptimizations(unittest.TestCase):
    """Pruebas unitarias para validar las optimizaciones implementadas en QTrend y Sniper."""

    def setUp(self):
        # Generar datos sintéticos para las pruebas
        np.random.seed(42)
        dates = pd.date_range(start="2026-06-01 00:00:00", periods=150, freq="15min")
        close = np.linspace(1.08000, 1.09000, 150)
        high = close + 0.0005
        low = close - 0.0005
        open_val = close - 0.0001
        
        self.df = pd.DataFrame({
            "open": open_val,
            "high": high,
            "low": low,
            "close": close,
            "volume": np.random.randint(100, 1000, 150),
            "atr": [0.001] * 150,
            "impulseMacd": [0.1] * 150,
            "impulseSignal": [0.05] * 150,
            "rsi": [60.0] * 150,
            "ema20": [1.085] * 150,
            "ema50": [1.082] * 150,
            "pendienteRsi": [0.2] * 150,
            "pendienteCci": [0.6] * 150
        }, index=dates)

        self.symbolInfo = {
            'symbol': 'EUR/USD',
            'tipo': 'MONEDA',
            'broker': 1,
            'refCapital': 10000.0,
            'refRiskPct': 1.0,
            'pip': 0.0001
        }

    @patch('middleware.database.dbManager.getStrategyConfig')
    @patch('Sentinel.analysis.risk.calculatePositionSize')
    @patch('Sentinel.analysis.technical.check_tp_exhaustion')
    @patch('Sentinel.analysis.technical.check_signal_health')
    @patch('dataSymbol.mainOrchestrator.get_last_closed_candle')
    def testQTrendCrossoverSynchronization(self, mock_last_candle, mock_health, mock_exhaustion, mock_risk, mock_config):
        """Prueba que QTrend descarte señales si el crossover es tardío/desincronizado."""
        bot = QTrendBot()
        
        # Simular config de la base de datos
        mock_config.return_value = {
            'max_minutos_fvg': 10,
            'min_rr': 1.5,
            'start_hour': 9,
            'max_minutos_signal': 21,
            'max_rr': 1.5,
            'min_confidence': 70,
            'min_usd_profit': 10.0
        }
        
        mock_risk.return_value = (1.0, 100.0, 50.0)
        mock_exhaustion.return_value = (True, 0.2, "Válido")
        mock_health.return_value = (True, 0.2, "Válido")
        mock_last_candle.return_value = self.df.index[-1]

        # Caso 1: Crossovers sincronizados (ambos ocurrieron en la última vela)
        stTrend = [-1] * 149 + [1]
        stTrail = [1.08] * 149 + [1.0890]
        emaFast = [1.08] * 148 + [1.08, 1.09]
        emaSlow = [1.085] * 148 + [1.085, 1.085]

        async def run_case_1():
            with patch.object(bot, 'calculateSuperTrend', return_value=(stTrend, stTrail)), \
                 patch.object(bot, 'calculateQTrend', return_value=(emaFast, emaSlow)):
                signal = await bot.runAnalysisCycleForSymbol(self.symbolInfo, preloadedData={'EUR/USD': {'15min': self.df}})
                self.assertIsNotNone(signal, "Debería generar señal cuando ambos crossovers están sincronizados (antigüedad = 0)")

        asyncio.run(run_case_1())

        # Caso 2: Crossover desincronizado (SuperTrend cruzó hace 5 velas, la EMA cruza hoy)
        stTrend_desinc = [-1] * 145 + [1] * 5
        async def run_case_2():
            with patch.object(bot, 'calculateSuperTrend', return_value=(stTrend_desinc, stTrail)), \
                 patch.object(bot, 'calculateQTrend', return_value=(emaFast, emaSlow)):
                signal_desinc = await bot.runAnalysisCycleForSymbol(self.symbolInfo, preloadedData={'EUR/USD': {'15min': self.df}})
                self.assertIsNone(signal_desinc, "Debería descartar la señal cuando el crossover de SuperTrend es tardío (antigüedad > 3)")

        asyncio.run(run_case_2())

    @patch('middleware.database.dbManager.getStrategyConfig')
    @patch('Sentinel.ml.model.predictProba')
    @patch('Sentinel.ml.model.cleanDataForModel')
    @patch('Sentinel.analysis.technical.check_tp_exhaustion')
    @patch('Sentinel.analysis.technical.check_signal_health')
    @patch('dataSymbol.mainOrchestrator.get_last_closed_candle')
    def testSniperStructuralExhaustionAndProba(self, mock_last_candle, mock_health, mock_exhaustion, mock_clean, mock_predict, mock_config):
        """Prueba que Sniper aplique el descarte por sobre-extensión y los umbrales mínimos de ML."""
        mock_model = MagicMock()
        bot = SniperBot(mock_model)
        
        # Simular config laxo en la base de datos (0.55 / 0.45)
        mock_config.return_value = {
            'proba_threshold_long': 0.55,
            'proba_threshold_short': 0.45,
            'min_confidence': 50,
            'min_rr': 1.2,
            'risk_usd': 100.0,
            'min_usd_profit': 10.0
        }
        
        mock_clean.return_value = (self.df, self.df['close'])
        mock_exhaustion.return_value = (True, 0.2, "Válido")
        mock_health.return_value = (True, 0.2, "Válido")
        mock_last_candle.return_value = self.df.index[-1]

        # Caso 1: Probabilidad ML débil de 0.58.
        # Aunque es >= 0.55 (límite de la BD), debe ser rechazada por el límite de código estricto de 0.60
        mock_predict.return_value = 0.58
        
        async def run_case_1():
            signal_weak = await bot.runAnalysisCycleForSymbol(self.symbolInfo, preloaded_data={'EUR/USD': {'15min': self.df}})
            print(f"DEBUG CASO 1 - Signal weak: {signal_weak}")
            self.assertIsNone(signal_weak, "Debería rechazar la señal por probabilidad ML débil (< 0.60)")
            
        asyncio.run(run_case_1())

        # Caso 2: Probabilidad fuerte de 0.65 pero sobre-extendido estructuralmente
        mock_predict.return_value = 0.65
        
        async def run_case_2():
            with patch('Sentinel.analysis.technical.get_structural_levels') as mock_levels:
                mock_levels.return_value = {
                    'swing_low': 1.0780,
                    'swing_high': 1.0860,
                    'high_zone': 1.0850, # Menor que close[-1] que es ~1.0900
                    'low_zone': 1.0790
                }
                signal_overextended = await bot.runAnalysisCycleForSymbol(self.symbolInfo, preloaded_data={'EUR/USD': {'15min': self.df}})
                print(f"DEBUG CASO 2 - Signal overextended: {signal_overextended}")
                self.assertIsNone(signal_overextended, "Debería rechazar la señal por estar sobre-extendida estructuralmente (close > high_zone)")

                # Caso 3: Probabilidad fuerte de 0.65 y estructura válida (close = 1.0900, high_zone = 1.0950)
                mock_levels.return_value = {
                    'swing_low': 1.0780,
                    'swing_high': 1.0960,
                    'high_zone': 1.0950, # Mayor que close[-1]
                    'low_zone': 1.0790
                }
                signal_valid = await bot.runAnalysisCycleForSymbol(self.symbolInfo, preloaded_data={'EUR/USD': {'15min': self.df}})
                print(f"DEBUG CASO 3 - Signal valid: {signal_valid}")
                self.assertIsNotNone(signal_valid, "Debería generar señal cuando la probabilidad es fuerte y la estructura es válida")

        asyncio.run(run_case_2())

    @patch('middleware.database.dbManager.getStrategyConfig')
    @patch('Sentinel.analysis.risk.calculatePositionSize')
    @patch('Sentinel.analysis.technical.check_tp_exhaustion')
    @patch('Sentinel.analysis.technical.check_signal_health')
    @patch('dataSymbol.mainOrchestrator.get_last_closed_candle')
    def testQTrendSpikeAndSlFilters(self, mock_last_candle, mock_health, mock_exhaustion, mock_risk, mock_config):
        """Prueba que QTrend descarte señales por velas spike y por stop loss sobre-extendido."""
        bot = QTrendBot()
        
        mock_config.return_value = {
            'max_minutos_fvg': 10,
            'min_rr': 1.5,
            'start_hour': 9,
            'max_minutos_signal': 21,
            'max_rr': 1.5,
            'min_confidence': 70,
            'min_usd_profit': 10.0,
            'max_trigger_atr_mult': 2.0,
            'max_sl_atr_mult': 2.0
        }
        
        mock_risk.return_value = (1.0, 100.0, 50.0)
        mock_exhaustion.return_value = (True, 0.2, "Válido")
        mock_health.return_value = (True, 0.2, "Válido")
        mock_last_candle.return_value = self.df.index[-1]

        # Configuración base de indicadores: crossovers sincronizados
        stTrend = [-1] * 149 + [1]
        emaFast = [1.08] * 148 + [1.08, 1.09]
        emaSlow = [1.085] * 148 + [1.085, 1.085]

        # Caso 1: Todo normal -> Genera señal
        df_normal = self.df.copy()
        stTrail_normal = [1.08] * 149 + [df_normal['close'].iloc[-1] - 0.0010] # slDist = 0.0010 (1.0 * ATR)
        
        async def run_case_normal():
            with patch.object(bot, 'calculateSuperTrend', return_value=(stTrend, stTrail_normal)), \
                 patch.object(bot, 'calculateQTrend', return_value=(emaFast, emaSlow)):
                signal = await bot.runAnalysisCycleForSymbol(self.symbolInfo, preloadedData={'EUR/USD': {'15min': df_normal}})
                self.assertIsNotNone(signal, "Debería generar señal cuando las condiciones de rango y stop loss son normales")

        asyncio.run(run_case_normal())

        # Caso 2: Vela de disparo Spike (gigante) -> Descarta señal
        df_spike = self.df.copy()
        # Modificar la última vela para que tenga un rango gigante de 0.0035 (> 2.0 * ATR que es ~0.0010)
        df_spike.loc[df_spike.index[-1], 'high'] = df_spike['close'].iloc[-1] + 0.0030
        df_spike.loc[df_spike.index[-1], 'low'] = df_spike['close'].iloc[-1] - 0.0005
        
        async def run_case_spike():
            with patch.object(bot, 'calculateSuperTrend', return_value=(stTrend, stTrail_normal)), \
                 patch.object(bot, 'calculateQTrend', return_value=(emaFast, emaSlow)):
                signal_spike = await bot.runAnalysisCycleForSymbol(self.symbolInfo, preloadedData={'EUR/USD': {'15min': df_spike}})
                self.assertIsNone(signal_spike, "Debería descartar la señal si la vela de disparo es un spike gigante (> 2.0 * ATR)")

        asyncio.run(run_case_spike())

        # Caso 3: Stop Loss sobre-extendido -> Descarta señal
        df_wide_sl = self.df.copy()
        # Colocar el Stop Loss muy lejos (ej. slDist = 0.0030, que es > 2.0 * ATR que es ~0.0010)
        stTrail_wide = [1.08] * 149 + [df_wide_sl['close'].iloc[-1] - 0.0030]
        
        async def run_case_wide_sl():
            with patch.object(bot, 'calculateSuperTrend', return_value=(stTrend, stTrail_wide)), \
                 patch.object(bot, 'calculateQTrend', return_value=(emaFast, emaSlow)):
                signal_wide = await bot.runAnalysisCycleForSymbol(self.symbolInfo, preloadedData={'EUR/USD': {'15min': df_wide_sl}})
                self.assertIsNone(signal_wide, "Debería descartar la señal si el Stop Loss está demasiado alejado (> 2.0 * ATR)")

        asyncio.run(run_case_wide_sl())

if __name__ == "__main__":
    unittest.main()
