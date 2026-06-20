import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbManager

symbols = dbManager.getSymbols()
active_symbols = [s['symbol'] for s in symbols if s.get('Activo') == 1]
print("Símbolos activos:", active_symbols)
