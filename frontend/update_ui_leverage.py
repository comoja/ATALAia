dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Update subtitle of equity curves
old_title = 'Comparativa de Curvas de Equidad (Capital: $10,000 USD | Asignación: 20% por Trade (10% Par A + 10% Par B | min_lots BD) | Salida: Cruce Media ●)'
new_title = 'Comparativa de Curvas de Equidad (Capital Real: cuenta.Capital | Apalancamiento: 1:100 | Margen: 20% | Salida: Cruce Media ●)'

dash = dash.replace(old_title, new_title)

# Update column header for Margen Invertido
dash = dash.replace('headerText="Monto Invertido ($) [20%]"', 'headerText="Margen Invertido ($) [1:100]"')

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with leverage 1:100 and cuenta.Capital badges!")
