dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

target_signal_col = '''                            <p:column headerText="Señal Origen" style="width: 185px;">
                                <span style="padding: 2px 6px; border-radius: 4px; font-weight: 700; font-size: 0.92rem; background: #{st.isSubtotal ? '#0f172a' : (st.signalType.contains('TRIANGULO') ? '#e0f2fe' : '#f3e8ff')}; color: #{st.isSubtotal ? '#38bdf8' : (st.signalType.contains('TRIANGULO') ? '#0369a1' : '#7e22ce')}; display: inline-block;">
                                    #{st.signalType}
                                </span>
                            </p:column>'''

new_signal_col = '''                            <p:column headerText="Señal Origen" style="width: 185px;">
                                <span style="padding: 2px 6px; border-radius: 4px; font-weight: 700; font-size: 0.92rem; background: #{st.isSubtotal ? (st.isOpen ? '#0369a1' : '#0f172a') : (st.isOpen ? '#fef3c7' : (st.signalType.contains('TRIANGULO') ? '#e0f2fe' : '#f3e8ff'))}; color: #{st.isSubtotal ? '#ffffff' : (st.isOpen ? '#b45309' : (st.signalType.contains('TRIANGULO') ? '#0369a1' : '#7e22ce'))}; display: inline-block;">
                                    #{st.signalType}
                                </span>
                            </p:column>'''

target_exit_date_col = '''                            <p:column headerText="Fecha Salida (● Media)" style="font-size: 1.05rem; width: 120px;">
                                <h:outputText value="#{st.exitDate}" style="font-weight: #{st.isSubtotal ? '800; color: #1e293b;' : 'normal'};" />
                            </p:column>'''

new_exit_date_col = '''                            <p:column headerText="Fecha Salida (● Media)" style="font-size: 1.05rem; width: 130px; text-align: center;">
                                <h:panelGroup rendered="#{st.isOpen}">
                                    <span style="background: #e0f2fe; color: #0369a1; padding: 2px 8px; border-radius: 4px; font-weight: 800; font-size: 0.88rem; border: 1px solid #bae6fd;">
                                        ● EN CURSO
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{!st.isOpen}">
                                    <h:outputText value="#{st.exitDate}" style="font-weight: #{st.isSubtotal ? '800; color: #1e293b;' : 'normal'};" />
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(target_signal_col, new_signal_col).replace(target_exit_date_col, new_exit_date_col)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with styling for open/in-progress trades!")
