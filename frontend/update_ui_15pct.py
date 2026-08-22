dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Update chart subtitle
dash = dash.replace(
    '50% Par A + 50% Par B | Salida: Cruce Media ●',
    'Asignación: 15% por Trade (7.5% Par A + 7.5% Par B) | Salida: Cruce Media ●'
)

# 2. Add column "Cap. Invertido" to signalTradesTable
old_cols = '''                            <p:column headerText="Precios Salida" style="font-size: 1.0rem; width: 140px;">
                                <span>A: #{st.exitPriceA} | B: #{st.exitPriceB}</span>
                            </p:column>
                            <p:column headerText="Velas" style="width: 50px; text-align: center;">
                                <h:outputText value="#{st.durationBars}" />
                            </p:column>'''

new_cols = '''                            <p:column headerText="Precios Salida" style="font-size: 1.0rem; width: 140px;">
                                <span>A: #{st.exitPriceA} | B: #{st.exitPriceB}</span>
                            </p:column>
                            <p:column headerText="Cap. Invertido (15%)" style="width: 110px; text-align: right; font-weight: 700; font-size: 1.0rem; color: #475569;">
                                <span>$#{st.allocatedCapital}</span>
                            </p:column>
                            <p:column headerText="Velas" style="width: 50px; text-align: center;">
                                <h:outputText value="#{st.durationBars}" />
                            </p:column>'''

dash = dash.replace(old_cols, new_cols)

# Update footer colspan from 7 to 8
old_footer = '<p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />'
new_footer = '<p:column colspan="8" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />'
dash = dash.replace(old_footer, new_footer)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with 15% allocation details and capital column!")
