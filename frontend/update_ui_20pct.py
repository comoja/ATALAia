dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Update subtitle
dash = dash.replace(
    'Asignación: 15% por Trade (7.5% Par A + 7.5% Par B) | Salida: Cruce Media ●',
    'Asignación: 20% por Trade (10% Par A + 10% Par B | min_lots BD) | Salida: Cruce Media ●'
)

# Update column header
dash = dash.replace(
    '<p:column headerText="Monto Invertido ($)" style="width: 110px;',
    '<p:column headerText="Monto Invertido ($) [20%]" style="width: 130px;'
)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with 20% allocation and min_lots labels!")
