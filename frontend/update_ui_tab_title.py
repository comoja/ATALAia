dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

target = '<p:tab title="Cruces EMA">'
replacement = '<p:tab title="Análisis de Cruces EMA">'

dash = dash.replace(target, replacement, 1)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated tab title to 'Análisis de Cruces EMA' in dashboard.xhtml!")
