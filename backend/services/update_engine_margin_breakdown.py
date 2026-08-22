quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. In active_trades.append
target_active_append = '''                active_trades.append({
                    "signalType": sigType,
                    "direction": direction,
                    "entryDate": d,
                    "entryIndex": i,
                    "entryPxA": pxA,
                    "entryPxB": pxB,
                    "margin": realMargenA + realMargenB,
                    "multA": multA,
                    "multB": multB,
                    "unitsA": unitsA,
                    "unitsB": unitsB
                })'''

new_active_append = '''                active_trades.append({
                    "signalType": sigType,
                    "direction": direction,
                    "entryDate": d,
                    "entryIndex": i,
                    "entryPxA": pxA,
                    "entryPxB": pxB,
                    "margin": realMargenA + realMargenB,
                    "marginA": round(float(realMargenA), 2),
                    "marginB": round(float(realMargenB), 2),
                    "multA": multA,
                    "multB": multB,
                    "unitsA": unitsA,
                    "unitsB": unitsB
                })'''

code = code.replace(target_active_append, new_active_append, 1)

# 2. In cycle_trades.append
target_cycle_trades_append = '''                        "allocatedCapital": round(float(t["margin"]), 2), # Margen invertido de la cuenta
                        "accumCapital": round(float(equity), 2), # Capital acumulado tras la operación
                        "multA": t["multA"],'''

new_cycle_trades_append = '''                        "allocatedCapital": round(float(t["margin"]), 2), # Margen invertido de la cuenta
                        "marginA": round(float(t.get("marginA", 0.0)), 2),
                        "marginB": round(float(t.get("marginB", 0.0)), 2),
                        "accumCapital": round(float(equity), 2), # Capital acumulado tras la operación
                        "multA": t["multA"],'''

code = code.replace(target_cycle_trades_append, new_cycle_trades_append, 1)

# 3. In subtotal finished_trades.append
target_subtotal_cycle = '''                cycle_margin = sum(t["margin"] for t in active_trades)
                cycle_units_a = sum(t["unitsA"] for t in active_trades)
                cycle_units_b = sum(t["unitsB"] for t in active_trades)'''

new_subtotal_cycle = '''                cycle_margin = sum(t["margin"] for t in active_trades)
                cycle_margin_a = sum(t.get("marginA", 0.0) for t in active_trades)
                cycle_margin_b = sum(t.get("marginB", 0.0) for t in active_trades)
                cycle_units_a = sum(t["unitsA"] for t in active_trades)
                cycle_units_b = sum(t["unitsB"] for t in active_trades)'''

code = code.replace(target_subtotal_cycle, new_subtotal_cycle, 1)

target_subtotal_append = '''                    "entryDate": entrySpan,
                    "exitDate": d,
                    "allocatedCapital": round(float(cycle_margin), 2),
                    "accumCapital": round(float(equity), 2),
                    "multA": None,'''

new_subtotal_append = '''                    "entryDate": entrySpan,
                    "exitDate": d,
                    "allocatedCapital": round(float(cycle_margin), 2),
                    "marginA": round(float(cycle_margin_a), 2),
                    "marginB": round(float(cycle_margin_b), 2),
                    "accumCapital": round(float(equity), 2),
                    "multA": None,'''

code = code.replace(target_subtotal_append, new_subtotal_append, 1)

# 4. In open_cycle_trades
target_open_trade_append = '''                    "entryDate": t["entryDate"],
                    "exitDate": "EN CURSO",
                    "allocatedCapital": round(float(t["margin"]), 2),
                    "accumCapital": round(float(equity + tradePnl), 2),
                    "multA": t["multA"],'''

new_open_trade_append = '''                    "entryDate": t["entryDate"],
                    "exitDate": "EN CURSO",
                    "allocatedCapital": round(float(t["margin"]), 2),
                    "marginA": round(float(t.get("marginA", 0.0)), 2),
                    "marginB": round(float(t.get("marginB", 0.0)), 2),
                    "accumCapital": round(float(equity + tradePnl), 2),
                    "multA": t["multA"],'''

code = code.replace(target_open_trade_append, new_open_trade_append, 1)

# 5. In open subtotal append
target_open_subtotal_vars = '''            open_margin = sum(t["margin"] for t in active_trades)
            open_units_a = sum(t["unitsA"] for t in active_trades)
            open_units_b = sum(t["unitsB"] for t in active_trades)'''

new_open_subtotal_vars = '''            open_margin = sum(t["margin"] for t in active_trades)
            open_margin_a = sum(t.get("marginA", 0.0) for t in active_trades)
            open_margin_b = sum(t.get("marginB", 0.0) for t in active_trades)
            open_units_a = sum(t["unitsA"] for t in active_trades)
            open_units_b = sum(t["unitsB"] for t in active_trades)'''

code = code.replace(target_open_subtotal_vars, new_open_subtotal_vars, 1)

target_open_subtotal_append = '''                "entryDate": openEntrySpan,
                "exitDate": "EN CURSO",
                "allocatedCapital": round(float(open_margin), 2),
                "accumCapital": round(float(equity + open_pnl), 2),
                "multA": None,'''

new_open_subtotal_append = '''                "entryDate": openEntrySpan,
                "exitDate": "EN CURSO",
                "allocatedCapital": round(float(open_margin), 2),
                "marginA": round(float(open_margin_a), 2),
                "marginB": round(float(open_margin_b), 2),
                "accumCapital": round(float(equity + open_pnl), 2),
                "multA": None,'''

code = code.replace(target_open_subtotal_append, new_open_subtotal_append, 1)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py with marginA and marginB breakdown!")
