quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_d = 'd = dates[i]'
new_d = 'd = str(dates[i])[:10]'

target_exit_reason = '"exitReason": f"CIERRE EN MEDIA (●) {d}",'
new_exit_reason = '"exitReason": f"CIERRE EN MEDIA (●) {d}",'

code = code.replace(target_d, new_d, 1)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py: Date format set to YYYY-MM-DD (date only without time)!")
