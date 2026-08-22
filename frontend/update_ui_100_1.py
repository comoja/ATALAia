dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

dash = dash.replace('Apalancamiento: 1:100', 'Apalancamiento: 100:1')
dash = dash.replace('headerText="Margen Invertido ($) [1:100]"', 'headerText="Margen Invertido ($) [100:1]"')

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with 100:1 notation!")
