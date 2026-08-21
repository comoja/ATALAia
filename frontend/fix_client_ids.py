import re

dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Fix all expressions referencing leftAccordion or non-existent paths
dash = dash.replace('update=":aetherForm:leftAccordion"', 'update=":aetherForm:mainTabView:leftAccordion"')
dash = dash.replace('update=":aetherForm:leftAccordion :aetherForm:growl"', 'update=":aetherForm:mainTabView:leftAccordion :aetherForm:growl"')
dash = dash.replace('update="symbolSelect2 calibrationInputsPanel operarContainerTop"', 'update=":aetherForm:mainTabView:leftAccordion"')

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Fixed all client IDs to :aetherForm:mainTabView:leftAccordion in dashboard.xhtml!")
