dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_title = 'Comparativa de Curvas de Equidad (Capital Real: cuenta.Capital | Apalancamiento: 100:1 | Margen: 20% | Salida: Cruce Media ●)'
new_title = 'Comparativa de Curvas de Equidad (Capital: cuenta.Capital | Margen BD: symbols.margen | Asignación: 20% | Salida: Cruce Media ●)'
dash = dash.replace(old_title, new_title)

dash = dash.replace('headerText="Margen Invertido ($) [100:1]"', 'headerText="Margen Invertido ($) [symbols.margen]"')

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with symbols.margen badges and column headers!")
