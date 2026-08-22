quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Update entryCapital in runVectorizedBacktest
target_init = 'position = 0\n        entryPrice = 0.0\n        entryDate = None\n        entryIndex = 0'
replacement_init = 'position = 0\n        entryPrice = 0.0\n        entryDate = None\n        entryIndex = 0\n        entryCapital = initialCapital'

if target_init in code:
    code = code.replace(target_init, replacement_init)

old_entry = '''                if z <= -entryZThreshold:
                    # Sobreventa extrema -> Entrada LARGO
                    position = 1
                    entryPrice = price
                    entryDate = d
                    entryIndex = i
                elif z >= entryZThreshold:
                    # Sobrecompra extrema -> Entrada CORTO
                    position = -1
                    entryPrice = price
                    entryDate = d
                    entryIndex = i'''

new_entry = '''                if z <= -entryZThreshold:
                    # Sobreventa extrema -> Entrada LARGO
                    position = 1
                    entryPrice = price
                    entryDate = d
                    entryIndex = i
                    entryCapital = equity
                elif z >= entryZThreshold:
                    # Sobrecompra extrema -> Entrada CORTO
                    position = -1
                    entryPrice = price
                    entryDate = d
                    entryIndex = i
                    entryCapital = equity'''

if old_entry in code:
    code = code.replace(old_entry, new_entry)

old_t_append = '''                    trades.append({
                        "tradeNum": len(trades) + 1,
                        "type": "LARGO_RATIO",
                        "entryDate": str(entryDate),
                        "exitDate": str(d),
                        "entryPrice": round(float(entryPrice), 6),
                        "exitPrice": round(float(price), 6),
                        "durationBars": i - entryIndex,
                        "returnPct": round(float(netReturnPct * 100.0), 2),
                        "pnl": round(float(tradePnl), 2),
                        "exitReason": "TAKE_PROFIT_MEAN" if isTakeProfit else "STOP_LOSS_DIVERGENCE",
                        "isWin": bool(tradePnl > 0)
                    })'''

new_t_append = '''                    trades.append({
                        "tradeNum": len(trades) + 1,
                        "type": "LARGO_RATIO",
                        "entryDate": str(entryDate),
                        "exitDate": str(d),
                        "allocatedCapital": round(float(entryCapital), 2),
                        "entryPrice": round(float(entryPrice), 6),
                        "exitPrice": round(float(price), 6),
                        "durationBars": i - entryIndex,
                        "returnPct": round(float(netReturnPct * 100.0), 2),
                        "pnl": round(float(tradePnl), 2),
                        "exitReason": "TAKE_PROFIT_MEAN" if isTakeProfit else "STOP_LOSS_DIVERGENCE",
                        "isWin": bool(tradePnl > 0)
                    })'''

if old_t_append in code:
    code = code.replace(old_t_append, new_t_append)

old_t_append2 = '''                    trades.append({
                        "tradeNum": len(trades) + 1,
                        "type": "CORTO_RATIO",
                        "entryDate": str(entryDate),
                        "exitDate": str(d),
                        "entryPrice": round(float(entryPrice), 6),
                        "exitPrice": round(float(price), 6),
                        "durationBars": i - entryIndex,
                        "returnPct": round(float(netReturnPct * 100.0), 2),
                        "pnl": round(float(tradePnl), 2),
                        "exitReason": "TAKE_PROFIT_MEAN" if isTakeProfit else "STOP_LOSS_DIVERGENCE",
                        "isWin": bool(tradePnl > 0)
                    })'''

new_t_append2 = '''                    trades.append({
                        "tradeNum": len(trades) + 1,
                        "type": "CORTO_RATIO",
                        "entryDate": str(entryDate),
                        "exitDate": str(d),
                        "allocatedCapital": round(float(entryCapital), 2),
                        "entryPrice": round(float(entryPrice), 6),
                        "exitPrice": round(float(price), 6),
                        "durationBars": i - entryIndex,
                        "returnPct": round(float(netReturnPct * 100.0), 2),
                        "pnl": round(float(tradePnl), 2),
                        "exitReason": "TAKE_PROFIT_MEAN" if isTakeProfit else "STOP_LOSS_DIVERGENCE",
                        "isWin": bool(tradePnl > 0)
                    })'''

if old_t_append2 in code:
    code = code.replace(old_t_append2, new_t_append2)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated runVectorizedBacktest with allocatedCapital!")
