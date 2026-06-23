import sys
import os

# Configuración de rutas
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import MetaTrader5 as mt5
from middleware.config.constants import mt5Login, mt5Password, mt5Server

def check_btc():
    print("Conectando a MT5...")
    if mt5Login > 0:
        initialized = mt5.initialize(login=mt5Login, password=mt5Password, server=mt5Server, timeout=15000)
    else:
        initialized = mt5.initialize(timeout=15000)
        
    if not initialized:
        print(f"Error al inicializar MT5: {mt5.last_error()}")
        return

    print("Conexión exitosa. Buscando símbolos que contengan 'BTC'...")
    symbols = mt5.symbols_get()
    if symbols is None:
        print("No se encontraron símbolos o no se pudo leer la lista.")
    else:
        btc_symbols = [s.name for s in symbols if "BTC" in s.name.upper()]
        if btc_symbols:
            print("\nSímbolos encontrados:")
            for sym in btc_symbols:
                print(f" - {sym}")
        else:
            print("\nNo se encontró ningún símbolo que contenga 'BTC'.")
            print("¿Tu broker ofrece CFDs de criptomonedas en esta cuenta?")
            
    mt5.shutdown()

if __name__ == "__main__":
    check_btc()
