import MetaTrader5 as mt5
from datetime import datetime

# === CONFIGURA ESTO ===
LOGIN = 24366125               # tu login
PASSWORD = "M11xtli."        # tu password de trading
SERVER = "Forex.com-Live"       # o el que te dieron

# ======================

def connect_mt5():
    print("Inicializando MT5...")
    
    if not mt5.initialize():
        print("❌ Error al inicializar MT5:", mt5.last_error())
        return False

    print("Conectando con credenciales...")
    
    authorized = mt5.login(LOGIN, password=PASSWORD, server=SERVER)

    if not authorized:
        print("❌ Login fallido:", mt5.last_error())
        return False

    print("✅ Conectado correctamente")

    # Info de cuenta
    account_info = mt5.account_info()
    if account_info:
        print("\n📊 Cuenta:")
        print(f"Login: {account_info.login}")
        print(f"Balance: {account_info.balance}")
        print(f"Equity: {account_info.equity}")
        print(f"Servidor: {account_info.server}")
    else:
        print("⚠️ No se pudo obtener info de cuenta")

    # Verifica símbolos
    print("\n🔍 Verificando símbolos...")
    symbols = mt5.symbols_get()

    if symbols:
        print(f"Total símbolos disponibles: {len(symbols)}")

        # prueba con XAUUSD
        symbol = "XAUUSD"
        symbol_info = mt5.symbol_info(symbol)

        if symbol_info:
            print(f"✅ {symbol} disponible")
        else:
            print(f"⚠️ {symbol} NO disponible (puede tener otro nombre)")
    else:
        print("❌ No se pudieron obtener símbolos")

    # Test de datos (muy importante)
    print("\n📈 Probando descarga de datos...")
    rates = mt5.copy_rates_from_pos("EURUSD", mt5.TIMEFRAME_M5, 0, 10)

    if rates is not None and len(rates) > 0:
        print("✅ Datos recibidos correctamente")
    else:
        print("❌ No se pudieron obtener datos:", mt5.last_error())

    return True


if __name__ == "__main__":
    connect_mt5()
    mt5.shutdown()