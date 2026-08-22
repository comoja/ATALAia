dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

target_prices = '''                            <p:column headerText="Precios Entrada" style="font-size: 0.95rem; width: 175px;">
                                <span rendered="#{!st.isSubtotal}">#{dashboardBean.selectedPair}: #{st.entryPriceA} | #{dashboardBean.selectedPair2}: #{st.entryPriceB}</span>
                                <span rendered="#{st.isSubtotal}" style="color: #94a3b8;">—</span>
                            </p:column>
                            <p:column headerText="Fecha Salida (● Media)" style="font-size: 1.05rem; width: 130px;">
                                <h:outputText value="#{st.exitDate}" style="font-weight: #{st.isSubtotal ? '800; color: #1e293b;' : 'normal'};" />
                            </p:column>
                            <p:column headerText="Precios Salida" style="font-size: 0.95rem; width: 175px;">
                                <span rendered="#{!st.isSubtotal}">#{dashboardBean.selectedPair}: #{st.exitPriceA} | #{dashboardBean.selectedPair2}: #{st.exitPriceB}</span>
                                <span rendered="#{st.isSubtotal}" style="color: #94a3b8;">—</span>
                            </p:column>'''

new_prices = '''                            <p:column headerText="Precios Entrada" style="font-size: 0.95rem; width: 160px;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-size: 0.92rem; font-weight: 600; color: #1e293b;">#{dashboardBean.selectedPair}: #{st.entryPriceA}</div>
                                    <div style="font-size: 0.92rem; font-weight: 600; color: #475569;">#{dashboardBean.selectedPair2}: #{st.entryPriceB}</div>
                                </h:panelGroup>
                                <span rendered="#{st.isSubtotal}" style="color: #94a3b8;">—</span>
                            </p:column>
                            <p:column headerText="Fecha Salida (● Media)" style="font-size: 1.05rem; width: 120px;">
                                <h:outputText value="#{st.exitDate}" style="font-weight: #{st.isSubtotal ? '800; color: #1e293b;' : 'normal'};" />
                            </p:column>
                            <p:column headerText="Precios Salida" style="font-size: 0.95rem; width: 160px;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-size: 0.92rem; font-weight: 600; color: #1e293b;">#{dashboardBean.selectedPair}: #{st.exitPriceA}</div>
                                    <div style="font-size: 0.92rem; font-weight: 600; color: #475569;">#{dashboardBean.selectedPair2}: #{st.exitPriceB}</div>
                                </h:panelGroup>
                                <span rendered="#{st.isSubtotal}" style="color: #94a3b8;">—</span>
                            </p:column>'''

dash = dash.replace(target_prices, new_prices)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml: Entry and Exit prices now formatted with line breaks instead of pipe separator!")
