dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

target_dash = '<span rendered="#{st.isSubtotal}" style="color: #94a3b8;">—</span>'
dash = dash.replace(target_dash, '')

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Removed all dash placeholders from Precios Entrada and Precios Salida in dashboard.xhtml!")
