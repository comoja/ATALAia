import sys
sys.path.append('/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/Sentinel')

# Compare Sentinel calculatePnl logic
def sentinel_pnl(entryPrice, exitPrice, size, side, symbol, quoteCurr, commission=0.0):
    if side == "LARGO" or side == "BUY" or side == "LONG":
        grossPnl = (exitPrice - entryPrice) * size
    else:
        grossPnl = (entryPrice - exitPrice) * size
        
    if quoteCurr != 'USD' and quoteCurr != '':
        if symbol.startswith("USD/"):
            rate = exitPrice
            if rate > 0:
                grossPnl = grossPnl / rate
                
    netPnl = grossPnl - commission
    return netPnl

# Test trade 1:
# EUR/USD: SHORT 1000 lotes, Entry 1.17706, Exit 1.16861
pnlA = sentinel_pnl(1.17706, 1.16861, 1000, "SHORT", "EUR/USD", "USD")
# USD/MXN: LONG 1000 lotes, Entry 17.97893, Exit 17.48500
pnlB = sentinel_pnl(17.97893, 17.48500, 1000, "LONG", "USD/MXN", "MXN")

print(f"Sentinel PnL Par A (EUR/USD): ${pnlA:.2f} USD")
print(f"Sentinel PnL Par B (USD/MXN): ${pnlB:.2f} USD")
print(f"Sentinel Net PnL Total: ${pnlA + pnlB:.2f} USD")
