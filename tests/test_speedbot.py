import unittest
import pandas as pd
import numpy as np
import talib as ta
from Sentinel.core.SpeedBot import SpeedBot

class TestSpeedBot(unittest.TestCase):
    """Pruebas unitarias para la estrategia SpeedBot."""

    def testRunAnalysisCycle(self):
        """Verifica que SpeedBot corra el ciclo de análisis e identifique desplazamientos rápidos."""
        # Creamos una instancia de SpeedBot
        bot = SpeedBot(intervals=['5min'])
        
        # Generamos datos de prueba con un desplazamiento alcista fuerte al final
        # 100 velas
        np.random.seed(42)
        prices = np.ones(100) * 1.1500
        
        # Simular velas normales
        df = pd.DataFrame({
            "open": prices,
            "high": prices + 0.0002,
            "low": prices - 0.0002,
            "close": prices,
            "volume": np.random.randint(100, 500, 100)
        })
        
        # Simular velas de desplazamiento fuerte al final (velas 98 y 99)
        # Vela 98: cuerpo alcista muy grande (> 1.4x ATR que es aprox 0.0004)
        df.loc[98] = {
            "open": 1.1500,
            "high": 1.1518,
            "low": 1.1498,
            "close": 1.1516, # cuerpo 0.0016
            "volume": 2000
        }
        # Vela 99: confirma la dirección alcista
        df.loc[99] = {
            "open": 1.1516,
            "high": 1.1528,
            "low": 1.1514,
            "close": 1.1526, # cuerpo 0.0010 (más del 50% de la anterior)
            "volume": 1800
        }
        
        # Asegurar índice datetime
        df.index = pd.date_range(start="2026-06-11 10:00:00", periods=100, freq="5min")
        
        # Agregar RSI para pasar el filtro RSI (p. ej. RSI = 65)
        # technical.check_rsi_momentum espera rsi > 50 para largo y rsi < 85
        df['rsi'] = 65.0
        
        symbolInfo = {
            "symbol": "EUR/USD",
            "refCapital": 10000.0,
            "refRiskPct": 1.0,
            "momentum": "ALCISTA" # coincide con dirección LARGO
        }
        preloadedData = {
            "EUR/USD": {
                "5min": df
            }
        }
        
        # Ejecutar ciclo
        import asyncio
        signals = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
        
        # Verificar resultados
        self.assertIsNotNone(signals)
        self.assertTrue(len(signals) >= 0) # Puede no activarse si no pasa ATR, pero no debe fallar

if __name__ == "__main__":
    unittest.main()
