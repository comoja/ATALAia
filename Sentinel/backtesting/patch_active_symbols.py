import re
import glob

ALL_FILES = glob.glob("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_*_optimization.py")
ALL_FILES = [f for f in ALL_FILES if "run_all_optimizations.py" not in f and "compounding" not in f and "grid_search" not in f]

ACTIVE_SYMBOLS_FUNC = """
def getActiveSymbols():
    try:
        from middleware.database import dbConnection
        connection = dbConnection.getConnection()
        if connection is None:
            return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']
        cursor = connection.cursor()
        cursor.execute("SELECT symbol FROM SentinelSymbol WHERE Activo = 1")
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        symbolsList = [row[0] for row in rows]
        if not symbolsList:
            return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']
        return symbolsList
    except Exception as e:
        print(f"Error fetching active symbols: {e}")
        return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']
"""

for filepath in ALL_FILES:
    with open(filepath, 'r') as f:
        content = f.read()

    modified = False

    # 1. Inject getActiveSymbols if not exists
    if "def getActiveSymbols(" not in content:
        # insert it after imports. A safe place is before ALL_SYMBOLS
        content = re.sub(r"(ALL_SYMBOLS\s*=)", ACTIVE_SYMBOLS_FUNC + r"\n\1", content, count=1)
        modified = True
    
    # 2. Replace the hardcoded array
    if "ALL_SYMBOLS = [" in content:
        # It's an array that might span multiple lines until the closing ]
        content = re.sub(r"ALL_SYMBOLS\s*=\s*\[.*?\]", "ALL_SYMBOLS = getActiveSymbols()", content, flags=re.DOTALL)
        modified = True

    if modified:
        with open(filepath, 'w') as f:
            f.write(content)
        print(f"✅ Patched active symbols dynamically for {filepath}")

