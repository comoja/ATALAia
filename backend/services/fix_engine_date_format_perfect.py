quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = 'dates = [str(d) for d in commonIdx]'
replacement = 'dates = [str(d)[:10] for d in commonIdx]'

code = code.replace(target, replacement, 1)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated dates array to [:10] in quant_pair_engine.py!")
