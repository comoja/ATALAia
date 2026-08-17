from middleware.database.dbManager import getSymbols
try:
    symbols = getSymbols()
    print([s['symbol'] for s in symbols])
except Exception as e:
    print(f"Error: {e}")
