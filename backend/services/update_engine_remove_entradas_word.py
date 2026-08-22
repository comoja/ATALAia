quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

code = code.replace('"entryDate": f"Entradas: {entrySpan}",', '"entryDate": entrySpan,')
code = code.replace('"entryDate": f"Entradas: {openEntrySpan}",', '"entryDate": openEntrySpan,')

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py: removed 'Entradas:' from subtotal entryDate!")
