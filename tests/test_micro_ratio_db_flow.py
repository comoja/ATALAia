import unittest
from datetime import datetime
from sqlalchemy import text
from backend.database.models import SessionLocal
from backend.services.microRatio import openRatioTrades, closeRatioTrades, checkActiveOpenTrades

class TestMicroRatioDbFlow(unittest.TestCase):
    def setUp(self):
        self.db = SessionLocal()
        self.testIdCuenta = 9999
        # Crear cuenta temporal con ganancia
        self.db.execute(text("""
            INSERT INTO cuenta (idCuenta, Nombre, Capital, ganancia, Activo)
            VALUES (:idc, 'TEST_RATIO_ACC', 300.00, 1.0, 1)
            ON DUPLICATE KEY UPDATE Capital = 300.00, ganancia = 1.0, Activo = 1
        """), {"idc": self.testIdCuenta})
        self.db.commit()

    def tearDown(self):
        # Limpiar trades y cuenta temporal
        self.db.execute(text("DELETE FROM trades WHERE idCuenta = :idc"), {"idc": self.testIdCuenta})
        self.db.execute(text("DELETE FROM cuenta WHERE idCuenta = :idc"), {"idc": self.testIdCuenta})
        self.db.commit()
        self.db.close()

    def test_open_and_close_trades_lifecycle(self):
        ratioConfig = {
            "id": 999,
            "idUsuario": 1,
            "idCuenta": self.testIdCuenta,
            "numerador": "EUR/USD",
            "denominador": "USD/MXN",
            "periodo": "1d"
        }
        signalInfo = {
            "signalType": "TRIANGULO_VERDE (EUR/USD + USD/MXN)",
            "directionA": "LARGO",
            "directionB": "CORTO",
            "candleTime": datetime.now(),
            "latestPriceA": 1.0850,
            "latestPriceB": 19.5000
        }
        symA = {"minLots": 1000.0, "margenRate": 0.0025, "pip": 0.0001, "quoteCurrency": "USD"}
        symB = {"minLots": 1000.0, "margenRate": 0.0100, "pip": 0.0100, "quoteCurrency": "MXN"}
        accData = {"idCuenta": self.testIdCuenta, "capital": 300.00}

        # 1. Abrir órdenes de 2 patas
        resOpen = openRatioTrades(self.db, ratioConfig, signalInfo, symA, symB, accData)
        self.assertTrue(resOpen)

        # 2. Verificar que se hayan insertado 2 registros en trades
        openTrades = checkActiveOpenTrades(self.db, self.testIdCuenta, "EUR/USD - USD/MXN")
        self.assertEqual(len(openTrades), 2)
        
        symbols = [t["symbol"] for t in openTrades]
        self.assertIn("EUR/USD", symbols)
        self.assertIn("USD/MXN", symbols)

        for t in openTrades:
            self.assertEqual(t["strategy"], "RATIO ATALAia")
            self.assertEqual(t["setup"], "EUR/USD - USD/MXN")
            self.assertEqual(t["status"], "OPEN")
            self.assertGreater(t["margin_used"], 0.0)

        # 3. Verificar que el capital en cuenta haya disminuido
        capRow = self.db.execute(text("SELECT Capital FROM cuenta WHERE idCuenta = :idc"), {"idc": self.testIdCuenta}).fetchone()
        self.assertLess(float(capRow[0]), 300.00)

        # 4. Cerrar en Círculo de Media (●)
        symMap = {"EUR/USD": symA, "USD/MXN": symB}
        priceMap = {"EUR/USD": 1.0900, "USD/MXN": 19.2000}
        resClose = closeRatioTrades(self.db, self.testIdCuenta, "EUR/USD - USD/MXN", openTrades, symMap, priceMap)
        self.assertTrue(resClose)

        # 5. Verificar que los trades estén cerrados
        closedTrades = self.db.execute(text("SELECT status, pnl, exitPrice FROM trades WHERE idCuenta = :idc"), {"idc": self.testIdCuenta}).fetchall()
        for ct in closedTrades:
            self.assertEqual(ct[0], "CLOSED")
            self.assertIsNotNone(ct[2])

if __name__ == "__main__":
    unittest.main()
