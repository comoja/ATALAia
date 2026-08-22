dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Update Quant Backtest Table in Tab 4
old_quant_cols = '''                                <p:column headerText="Salida" style="font-size: 1.05rem;">
                                    <h:outputText value="#{t.exitDate}" />
                                </p:column>
                                <p:column headerText="Retorno" style="width: 75px; text-align: right;">'''

new_quant_cols = '''                                <p:column headerText="Salida" style="font-size: 1.05rem;">
                                    <h:outputText value="#{t.exitDate}" />
                                </p:column>
                                <p:column headerText="Monto Invertido ($)" style="width: 120px; text-align: right; font-weight: 700; font-size: 1.0rem; color: #475569;">
                                    <span>$#{t.allocatedCapital}</span>
                                </p:column>
                                <p:column headerText="Retorno" style="width: 75px; text-align: right;">'''

dash = dash.replace(old_quant_cols, new_quant_cols)

# Update footer colspan in Quant Backtest Table from 4 to 5
old_quant_footer = '<p:column colspan="4" style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />'
new_quant_footer = '<p:column colspan="5" style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />'
dash = dash.replace(old_quant_footer, new_quant_footer)

# 2. Update Signal Backtest Table header name in Tab 5
dash = dash.replace(
    'headerText="Cap. Invertido (15%)"',
    'headerText="Monto Invertido ($)"'
)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with Monto Invertido column in both tables!")
