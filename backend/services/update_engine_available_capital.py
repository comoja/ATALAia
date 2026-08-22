quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Initialize cycle_available_equity
target_equity_init = '''        equity = float(initialCapital) if initialCapital > 0 else 800.0
        cycle_start_equity = equity # Capital base del ciclo para calcular el 3%'''

new_equity_init = '''        equity = float(initialCapital) if initialCapital > 0 else 800.0
        cycle_start_equity = equity # Capital base del ciclo para calcular el 3%
        cycle_available_equity = equity # Capital disminuido decrementando margen para entradas sucesivas'''

code = code.replace(target_equity_init, new_equity_init, 1)

# 2. In cycle_trades.append add availableCapital
target_ct_append = '''                        "marginA": round(float(t.get("marginA", 0.0)), 2),
                        "marginB": round(float(t.get("marginB", 0.0)), 2),
                        "accumCapital": round(float(equity), 2), # Capital acumulado tras la operación'''

new_ct_append = '''                        "marginA": round(float(t.get("marginA", 0.0)), 2),
                        "marginB": round(float(t.get("marginB", 0.0)), 2),
                        "availableCapital": round(float(t.get("availableCapital", 0.0)), 2), # Capital disminuido
                        "accumCapital": round(float(equity), 2), # Capital acumulado tras la operación'''

code = code.replace(target_ct_append, new_ct_append, 1)

# 3. In subtotal finished_trades.append add availableCapital
target_st_append = '''                    "allocatedCapital": round(float(cycle_margin), 2),
                    "marginA": round(float(cycle_margin_a), 2),
                    "marginB": round(float(cycle_margin_b), 2),
                    "accumCapital": round(float(equity), 2),'''

new_st_append = '''                    "allocatedCapital": round(float(cycle_margin), 2),
                    "marginA": round(float(cycle_margin_a), 2),
                    "marginB": round(float(cycle_margin_b), 2),
                    "availableCapital": round(float(equity), 2),
                    "accumCapital": round(float(equity), 2),'''

code = code.replace(target_st_append, new_st_append, 1)

# 4. In reinversion reset cycle_available_equity
target_reinversion = '''                # REINVERSIÓN AL CIERRE
                cycle_start_equity = equity
                active_trades = []'''

new_reinversion = '''                # REINVERSIÓN AL CIERRE
                cycle_start_equity = equity
                cycle_available_equity = equity
                active_trades = []'''

code = code.replace(target_reinversion, new_reinversion, 1)

# 5. In signal entry evaluation use cycle_available_equity and decrement margin
target_entry_eval = '''            if sigType and direction:
                totalEntryBudget = cycle_start_equity * allocationRate
                budgetA = totalEntryBudget / 2.0
                budgetB = totalEntryBudget / 2.0

                # Margen invertido = symbols.margen * lote
                margen1LotA = minLotsA * margenRateA
                margen1LotB = minLotsB * margenRateB
                sample_req_margin_lot = margen1LotA + margen1LotB

                multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA) else 1
                multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB) else 1

                realMargenA = multA * margen1LotA
                realMargenB = multB * margen1LotB

                unitsA = multA * minLotsA
                unitsB = multB * minLotsB

                active_trades.append({
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

new_entry_eval = '''            if sigType and direction:
                totalEntryBudget = max(0.0, cycle_available_equity) * allocationRate
                budgetA = totalEntryBudget / 2.0
                budgetB = totalEntryBudget / 2.0

                # Margen invertido = symbols.margen * lote
                margen1LotA = minLotsA * margenRateA
                margen1LotB = minLotsB * margenRateB
                sample_req_margin_lot = margen1LotA + margen1LotB

                multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA) else 1
                multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB) else 1

                realMargenA = multA * margen1LotA
                realMargenB = multB * margen1LotB
                totalMargen = realMargenA + realMargenB

                unitsA = multA * minLotsA
                unitsB = multB * minLotsB

                # Decrementar capital disminuido con el margen retenido
                cycle_available_equity = max(0.0, cycle_available_equity - totalMargen)

                active_trades.append({
                    "signalType": sigType,
                    "direction": direction,
                    "entryDate": d,
                    "entryIndex": i,
                    "entryPxA": pxA,
                    "entryPxB": pxB,
                    "margin": totalMargen,
                    "marginA": round(float(realMargenA), 2),
                    "marginB": round(float(realMargenB), 2),
                    "availableCapital": round(float(cycle_available_equity), 2),
                    "multA": multA,
                    "multB": multB,
                    "unitsA": unitsA,
                    "unitsB": unitsB
                })'''

code = code.replace(target_entry_eval, new_entry_eval, 1)

# 6. In open trades append
target_open_append = '''                    "allocatedCapital": round(float(t["margin"]), 2),
                    "marginA": round(float(t.get("marginA", 0.0)), 2),
                    "marginB": round(float(t.get("marginB", 0.0)), 2),
                    "accumCapital": round(float(equity + tradePnl), 2),'''

new_open_append = '''                    "allocatedCapital": round(float(t["margin"]), 2),
                    "marginA": round(float(t.get("marginA", 0.0)), 2),
                    "marginB": round(float(t.get("marginB", 0.0)), 2),
                    "availableCapital": round(float(t.get("availableCapital", 0.0)), 2),
                    "accumCapital": round(float(equity + tradePnl), 2),'''

code = code.replace(target_open_append, new_open_append, 1)

# 7. In open subtotal append
target_open_st_append = '''                "allocatedCapital": round(float(open_margin), 2),
                "marginA": round(float(open_margin_a), 2),
                "marginB": round(float(open_margin_b), 2),
                "accumCapital": round(float(equity + open_pnl), 2),'''

new_open_st_append = '''                "allocatedCapital": round(float(open_margin), 2),
                "marginA": round(float(open_margin_a), 2),
                "marginB": round(float(open_margin_b), 2),
                "availableCapital": round(float(cycle_available_equity), 2),
                "accumCapital": round(float(equity + open_pnl), 2),'''

code = code.replace(target_open_st_append, new_open_st_append, 1)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("SUCCESS: Updated quant_pair_engine.py with availableCapital progression!")
