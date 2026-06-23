import MetaTrader5 as mt5

def listOpenPositions():
    if not mt5.initialize():
        print("Error inicializando MT5")
        return
        
    positions = mt5.positions_get()
    if positions is not None and len(positions) > 0:
        print(f"--- Posiciones Abiertas ({len(positions)}) ---")
        for pos in positions:
            print(f"- Ticket: {pos.ticket} | Symbol: {pos.symbol} | Type: {'BUY' if pos.type == mt5.POSITION_TYPE_BUY else 'SELL'} | Volume: {pos.volume} | Price: {pos.price_open} | SL: {pos.sl} | TP: {pos.tp}")
    else:
        print("No hay posiciones abiertas en la cuenta.")
        
    mt5.shutdown()

if __name__ == "__main__":
    listOpenPositions()
