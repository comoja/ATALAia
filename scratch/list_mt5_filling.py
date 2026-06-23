import MetaTrader5 as mt5

def listMt5Attributes():
    if not mt5.initialize():
        print("Error inicializando MT5")
        return
    
    print("Atributos de MetaTrader5 que contienen 'FILLING':")
    for attr in dir(mt5):
        if "FILLING" in attr:
            print(f"- {attr}: {getattr(mt5, attr)}")
            
    mt5.shutdown()

if __name__ == "__main__":
    listMt5Attributes()
