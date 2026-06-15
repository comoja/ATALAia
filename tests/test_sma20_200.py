import unittest
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime
import pytz

from Sentinel.core.SMA20_200 import SMABot
from middleware.database import dbManager

class TestSMA20200(unittest.TestCase):
    """Pruebas unitarias para la estrategia SMA20_200 (SMABot)."""

    def testInit(self):
        """Verifica que el bot inicialice correctamente."""
        # Mockear los cargadores de modelos de ML para evitar que carguen archivos reales
        import Sentinel.ml.model as mlModel
        orig_loadModel = mlModel.loadModel
        orig_loadRegModel = mlModel.loadRegModel
        
        mlModel.loadModel = lambda *args: None
        mlModel.loadRegModel = lambda *args: None
        
        try:
            bot = SMABot()
            self.assertIsNotNone(bot)
        finally:
            mlModel.loadModel = orig_loadModel
            mlModel.loadRegModel = orig_loadRegModel

    def testRunAnalysisCycle(self):
        """Verifica que runAnalysisCycleForSymbol procese señales y genere un objeto Signal en SMA20_200."""
        import Sentinel.ml.model as mlModel
        orig_loadModel = mlModel.loadModel
        orig_loadRegModel = mlModel.loadRegModel
        mlModel.loadModel = lambda *args: None
        mlModel.loadRegModel = lambda *args: None
        
        try:
            bot = SMABot()
        finally:
            mlModel.loadModel = orig_loadModel
            mlModel.loadRegModel = orig_loadRegModel

        # Generar datos de prueba para timeframe 15m (350 velas)
        now_time = datetime.now(pytz.timezone("America/Mexico_City"))
        dates15m = pd.date_range(end=now_time, periods=350, freq="15min", tz="America/Mexico_City")
        prices15m = np.ones(350) * 1.0800
        
        # Última vela con cierre por debajo del SMA20 para gatillar Largo
        prices15m[-1] = 1.0700
        
        df15m = pd.DataFrame({
            "open": prices15m,
            "high": prices15m + 0.0001,
            "low": prices15m - 0.0001,
            "close": prices15m,
            "volume": np.random.randint(100, 500, 350)
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
            "broker": 1,
            "intervalo": "15min"
        }
        
        preloadedData = {
            "GBP/USD": {
                "15min": df15m
            }
        }
        
        # Mocks de base de datos
        orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
        orig_getStrategyConfig = dbManager.getStrategyConfig
        
        dbManager.getSymbolStrategyConfig = lambda strat, sym: {
            "slopeThreshold": 0.005,
            "minRr": 1.5,
            "minConfidence": 70,
            "minUsdProfit": 10.0,
            "riskUsd": 100.0
        }
        dbManager.getStrategyConfig = lambda strat: {
            "slope_threshold": 0.005,
            "min_rr": 1.5,
            "min_confidence": 70,
            "min_usd_profit": 10.0,
            "risk_usd": 100.0
        }
        
        # Mock de métodos del Bot
        orig_detectar_rebote = bot.detectar_rebote_sma_doble
        orig_validar_filtros = bot._validar_filtros_basicos
        orig_validar_ml = bot._validar_ml
        orig_validar_1h = bot.validarTendencia1h
        orig_identificar = bot.identificarTendencia
        
        import talib as ta
        orig_adx = ta.ADX
        ta.ADX = lambda *args, **kwargs: np.ones(350) * 30.0
        
        bot.identificarTendencia = lambda *args, **kwargs: "ALCISTA"
        bot.detectar_rebote_sma_doble = lambda df, sma20, interval, symbol, tendencia: ("LARGO", df.index[-1])
        bot._validar_filtros_basicos = lambda df, close, sma20, sma200, atr, direction, symbol: True
        bot._validar_ml = lambda df, close, sma20, atr: (True, 0.75, 0.5)
        bot.validarTendencia1h = lambda symbol, tendencia, apiKey: asyncio.sleep(0.01) or True
        
        import Sentinel.core.SMA20_200 as SMA20Module
        orig_check_tp = SMA20Module.check_tp_exhaustion
        orig_check_health = SMA20Module.check_signal_health
        import Sentinel.analysis.risk as riskModule
        orig_calculatePositionSize = riskModule.calculatePositionSize
        
        SMA20Module.check_tp_exhaustion = lambda *args, **kwargs: (True, 0.0, "OK")
        SMA20Module.check_signal_health = lambda *args, **kwargs: (True, 0.0, "OK")
        
        try:
            signal = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
            self.assertIsNotNone(signal)
            self.assertEqual(signal.strategy, "SMA20_200")
            self.assertEqual(signal.direction, "LARGO")
            self.assertEqual(signal.entry_price, 1.0800) # La vela utilizable cerrada es la penúltima (1.0800) debido al filtro de 15min
        finally:
            # Restaurar originales
            dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
            dbManager.getStrategyConfig = orig_getStrategyConfig
            bot.detectar_rebote_sma_doble = orig_detectar_rebote
            bot._validar_filtros_basicos = orig_validar_filtros
            bot._validar_ml = orig_validar_ml
            bot.validarTendencia1h = orig_validar_1h
            bot.identificarTendencia = orig_identificar
            ta.ADX = orig_adx
            SMA20Module.check_tp_exhaustion = orig_check_tp
            SMA20Module.check_signal_health = orig_check_health

if __name__ == "__main__":
    unittest.main()
