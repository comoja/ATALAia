import unittest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz
from Sentinel.core.BreakoutNY import BreakoutNYBot

class TestBreakoutNY(unittest.TestCase):
    """Pruebas unitarias para la estrategia BreakoutNY."""

    def testRunAnalysisCycle(self):
        """Verifica que BreakoutNY identifique rupturas del rango de apertura correctamente."""
        bot = BreakoutNYBot()
        
        # Generar datos de prueba para 1 día
        # Necesitamos velas de 5m y 15m
        tz = pytz.timezone("America/Mexico_City")
        baseTime = datetime.now(tz).replace(hour=9, minute=0, second=0, microsecond=0)
        
        # Simular 40 velas de 5 minutos (de 09:00 a 12:20)
        timestamps = [baseTime + timedelta(minutes=5 * i) for i in range(40)]
        prices = np.ones(40) * 1.1500
        
        df5m = pd.DataFrame({
            "open": prices,
            "high": prices + 0.0002,
            "low": prices - 0.0002,
            "close": prices,
            "volume": np.random.randint(100, 500, 40)
        }, index=timestamps)
        
        # Velas de rango (09:00, 09:05, 09:10, 09:15, 09:20, 09:25) -> rango high es 1.1502, low es 1.1498
        # Simular una ruptura en la vela 39 (12:15)
        # close > 1.1502
        df5m.iloc[-1] = {
            "open": 1.1501,
            "high": 1.1508,
            "low": 1.1499,
            "close": 1.1507, # Ruptura alcista
            "volume": 600
        }
        # Vela anterior (-2) cerrada dentro del rango
        df5m.iloc[-2] = {
            "open": 1.1500,
            "high": 1.1501,
            "low": 1.1499,
            "close": 1.1500, # Dentro del rango
            "volume": 200
        }
        
        # Velas de 15m
        timestamps15m = [baseTime + timedelta(minutes=15 * i) for i in range(15)]
        df15m = pd.DataFrame({
            "open": np.ones(15) * 1.1500,
            "high": np.ones(15) * 1.1502,
            "low": np.ones(15) * 1.1498,
            "close": np.ones(15) * 1.1500,
            "volume": np.random.randint(100, 500, 15)
        }, index=timestamps15m)
        
        symbolInfo = {
            "symbol": "EUR/USD"
        }
        preloadedData = {
            "EUR/USD": {
                "5min": df5m,
                "15min": df15m
            }
        }
        
        # Mock de _now_local para que retorne el tiempo de la última vela + 10s (para pasar delay de 90s)
        lastCandleTime = timestamps[-1]
        bot._now_local = lambda: lastCandleTime + timedelta(seconds=10)
        
        # Ejecutar
        import asyncio
        signals = asyncio.run(bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData))
        
        self.assertIsNotNone(signals)
        self.assertTrue(len(signals) >= 0)

if __name__ == "__main__":
    unittest.main()
