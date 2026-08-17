import sys
import os
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataSymbol.core.databaseManager import DatabaseManager

def test_datasymbol_db_manager():
    print("Testing dataSymbol DatabaseManager integration with ConnectionPool...")
    db = DatabaseManager()
    
    # 1. Probar getLastTimestamp
    last_ts = db.getLastTimestamp("EUR/USD", "5min")
    print(f"getLastTimestamp('EUR/USD', '5min') -> {last_ts}")
    
    # 2. Probar getCandleCount
    cnt = db.getCandleCount("EUR/USD", "5min")
    print(f"getCandleCount('EUR/USD', '5min') -> {cnt}")
    
    # 3. Probar saveBulkData con DataFrame dummy
    df_dummy = pd.DataFrame([{
        "datetime": "2099-01-01 00:00:00",
        "open": 1.1000,
        "high": 1.1050,
        "low": 1.0950,
        "close": 1.1020,
        "volume": 100
    }])
    inserted = db.saveBulkData(df_dummy, "TEST_PAIR", "5min")
    print(f"saveBulkData('TEST_PAIR', '5min') -> {inserted} filas procesadas/insertadas")
    
    print("=== Prueba completada con éxito ===")

if __name__ == "__main__":
    test_datasymbol_db_manager()
