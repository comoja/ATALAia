dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_title = 'Comparativa de Curvas de Equidad (Capital: cuenta.Capital | Margen BD: symbols.margen | Asignación: 20% | Salida: Cruce Media ●)'
new_title = 'Comparativa de Curvas de Equidad (Capital: cuenta.Capital | Asignación: 3% por Entrada | Reinversión al Cierre | Salida: Cruce Media ●)'
dash = dash.replace(old_title, new_title)

dash = dash.replace(
    'headerText="Margen Invertido ($) [symbols.margen]"',
    'headerText="Margen Invertido ($) [Req: ~$#{dashboardBean.signalBtCombinedMetrics.reqMarginPerMinLot} / min_lot | Asig: 3%]"'
)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with required margin per min_lot header and 3% allocation!")
