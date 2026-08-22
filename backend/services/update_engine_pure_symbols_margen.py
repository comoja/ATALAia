quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Update margenRateA and margenRateB calculation
target_margen_rates = '''        # Ratios de margen institucional desde symbols.margen
        margenRateA = (margenPctA / 100.0) if margenPctA > 0 else 0.01
        margenRateB = (margenPctB / 100.0) if margenPctB > 0 else 0.01'''

new_margen_rates = '''        # Ratios de margen institucional directo desde symbols.margen
        margenRateA = (margenPctA / 100.0) if margenPctA >= 0.05 else margenPctA
        margenRateB = (margenPctB / 100.0) if margenPctB >= 0.05 else margenPctB'''

code = code.replace(target_margen_rates, new_margen_rates, 1)

# 2. Update margen1LotA and margen1LotB in signal entry evaluation
target_margen_eval = '''                margen1LotA = (minLotsA * pxA) * margenRateA
                margen1LotB = (minLotsB * (1.0 if quoteB == "MXN" else pxB)) * margenRateB'''

new_margen_eval = '''                # Margen invertido = symbols.margen * lote
                margen1LotA = minLotsA * margenRateA
                margen1LotB = minLotsB * margenRateB'''

code = code.replace(target_margen_eval, new_margen_eval, 1)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("SUCCESS: Updated quant_pair_engine.py with pure symbols.margen * lotes calculation!")
