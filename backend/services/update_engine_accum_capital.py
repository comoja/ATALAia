quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Update cycle_trades dict in quant_pair_engine.py
target_trade_dict = '''                    cycle_trades.append({
                        "tradeNum": raw_trades_count,
                        "isSubtotal": False,
                        "cycleNum": cycle_counter,
                        "signalType": t["signalType"],
                        "direction": t["direction"],
                        "entryDate": t["entryDate"],
                        "exitDate": d,
                        "allocatedCapital": round(float(t["margin"]), 2), # Margen invertido de la cuenta'''

new_trade_dict = '''                    cycle_trades.append({
                        "tradeNum": raw_trades_count,
                        "isSubtotal": False,
                        "cycleNum": cycle_counter,
                        "signalType": t["signalType"],
                        "direction": t["direction"],
                        "entryDate": t["entryDate"],
                        "exitDate": d,
                        "allocatedCapital": round(float(t["margin"]), 2), # Margen invertido de la cuenta
                        "accumCapital": round(float(equity), 2), # Capital acumulado tras la operación'''

code = code.replace(target_trade_dict, new_trade_dict, 1)

# 2. Update subtotal trade dict
target_subtotal_dict = '''                    "entryDate": f"Entradas: {entrySpan}",
                    "exitDate": d,
                    "allocatedCapital": round(float(cycle_margin), 2),
                    "multA": None,'''

new_subtotal_dict = '''                    "entryDate": f"Entradas: {entrySpan}",
                    "exitDate": d,
                    "allocatedCapital": round(float(cycle_margin), 2),
                    "accumCapital": round(float(equity), 2),
                    "multA": None,'''

code = code.replace(target_subtotal_dict, new_subtotal_dict, 1)

# 3. Update open_cycle_trades dict
target_open_trade = '''                open_cycle_trades.append({
                    "tradeNum": raw_trades_count,
                    "isSubtotal": False,
                    "isOpen": True,
                    "cycleNum": cycle_counter + 1,
                    "signalType": f"{t['signalType']} (EN CURSO)",
                    "direction": t["direction"],
                    "entryDate": t["entryDate"],
                    "exitDate": "EN CURSO",
                    "allocatedCapital": round(float(t["margin"]), 2),'''

new_open_trade = '''                open_cycle_trades.append({
                    "tradeNum": raw_trades_count,
                    "isSubtotal": False,
                    "isOpen": True,
                    "cycleNum": cycle_counter + 1,
                    "signalType": f"{t['signalType']} (EN CURSO)",
                    "direction": t["direction"],
                    "entryDate": t["entryDate"],
                    "exitDate": "EN CURSO",
                    "allocatedCapital": round(float(t["margin"]), 2),
                    "accumCapital": round(float(equity + tradePnl), 2),'''

code = code.replace(target_open_trade, new_open_trade, 1)

# 4. Update open subtotal trade dict
target_open_subtotal = '''                "entryDate": f"Entradas: {openEntrySpan}",
                "exitDate": "EN CURSO",
                "allocatedCapital": round(float(open_margin), 2),
                "multA": None,'''

new_open_subtotal = '''                "entryDate": f"Entradas: {openEntrySpan}",
                "exitDate": "EN CURSO",
                "allocatedCapital": round(float(open_margin), 2),
                "accumCapital": round(float(equity + open_pnl), 2),
                "multA": None,'''

code = code.replace(target_open_subtotal, new_open_subtotal, 1)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py with accumCapital tracking!")
