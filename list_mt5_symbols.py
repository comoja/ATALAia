import MetaTrader5 as mt5

if not mt5.initialize():
    print("initialize() failed")
    mt5.shutdown()
    exit()

symbols = mt5.symbols_get()
if symbols:
    print("Posibles nombres para US OIL:")
    for s in symbols:
        name = s.name.upper()
        if 'OIL' in name or 'WTI' in name or 'CRUDE' in name or 'XTI' in name:
            print(f" - {s.name} (Descripción: {s.description})")
else:
    print("No se encontraron símbolos.")
    
mt5.shutdown()
