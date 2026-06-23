import MetaTrader5 as mt5

def checkXauusdInfo():
    if not mt5.initialize():
        print("Error inicializando MT5")
        return
        
    symbol = "XAUUSD"
    info = mt5.symbol_info(symbol)
    if info is None:
        print(f"No se pudo obtener información para {symbol}")
        # Intentar con XAU/USD
        symbol = "XAU/USD"
        info = mt5.symbol_info(symbol)
        
    if info is not None:
        print(f"--- Información de {symbol} ---")
        print(f"Volume Min: {info.volume_min}")
        print(f"Volume Max: {info.volume_max}")
        print(f"Volume Step: {info.volume_step}")
        print(f"Volume Limit: {info.volume_limit}")
        print(f"Trade Mode: {info.trade_mode}")
        print(f"Execution Mode: {info.execution_mode}")
        print(f"Stops Level: {info.stops_level}")
        print(f"Contract Size: {info.trade_contract_size}")
    else:
        print("Símbolo no encontrado en MT5.")
        
    mt5.shutdown()

if __name__ == "__main__":
    checkXauusdInfo()
