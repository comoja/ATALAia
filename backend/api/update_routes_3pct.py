routes_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/api/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    code = f.read()

code = code.replace('allocationPct=20.0,', 'allocationPct=3.0,')

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated routes.py with allocationPct=3.0!")
