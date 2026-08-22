dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

target_columns = '''                            <p:column headerText="Señal Origen" style="width: 185px;">
                                <span style="padding: 2px 6px; border-radius: 4px; font-weight: 700; font-size: 0.92rem; background: #{st.isSubtotal ? (st.isOpen ? '#0369a1' : '#0f172a') : (st.isOpen ? '#fef3c7' : (st.signalType.contains('TRIANGULO') ? '#e0f2fe' : '#f3e8ff'))}; color: #{st.isSubtotal ? '#ffffff' : (st.isOpen ? '#b45309' : (st.signalType.contains('TRIANGULO') ? '#0369a1' : '#7e22ce'))}; display: inline-block;">
                                    #{st.signalType}
                                </span>
                            </p:column>
                            <p:column headerText="Dirección Arbitraje" style="width: 220px; font-weight: 700; font-size: 0.95rem;">
                                <span style="color: #{st.isSubtotal ? '#0369a1' : (st.direction.startsWith('LONG') ? '#15803d' : '#b91c1c')};">
                                    #{st.direction}
                                </span>
                            </p:column>'''

new_columns = '''                            <p:column headerText="Señal Origen" style="width: 170px;">
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isOpen ? '#0369a1' : '#0f172a'}; color: #ffffff; padding: 3px 8px; border-radius: 6px; font-weight: 800; font-size: 0.92rem; display: inline-block;">
                                        #{st.signalType}
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="display: inline-flex; align-items: center; gap: 7px;">
                                        <!-- Cuadro Dibujado -->
                                        <h:panelGroup rendered="#{st.isSquare()}">
                                            <span style="width: 14px; height: 14px; background-color: #{st.signalColor}; border-radius: 3px; display: inline-block; box-shadow: 0 1px 3px rgba(0,0,0,0.25);"></span>
                                        </h:panelGroup>
                                        <!-- Triángulo Dibujado -->
                                        <h:panelGroup rendered="#{st.isTriangle()}">
                                            <span style="color: #{st.signalColor}; font-size: 1.15rem; line-height: 1; font-weight: 900; display: inline-block;">
                                                #{st.isTriangleUp() ? '▲' : '▼'}
                                            </span>
                                        </h:panelGroup>
                                        <!-- Moneda / Par que origina la señal del mismo color -->
                                        <span style="color: #{st.signalColor}; font-weight: 800; font-size: 1.02rem;">
                                            #{st.signalOriginPair}
                                        </span>
                                        <!-- Badge EN CURSO si aplica -->
                                        <h:panelGroup rendered="#{st.isOpen}">
                                            <span style="background: #fef3c7; color: #b45309; font-size: 0.72rem; font-weight: 800; padding: 1px 5px; border-radius: 4px; border: 1px solid #fde68a;">
                                                EN CURSO
                                            </span>
                                        </h:panelGroup>
                                    </div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Dirección Arbitraje" style="width: 205px; font-size: 0.95rem;">
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="color: #{st.isOpen ? '#0284c7' : '#0369a1'}; font-weight: 800; font-size: 0.95rem;">
                                        #{st.direction}
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="display: inline-flex; align-items: center; gap: 6px; font-size: 1.05rem; font-weight: 800;">
                                        <span style="color: #{st.pairAColor};">#{dashboardBean.selectedPair}</span>
                                        <span style="color: #94a3b8; font-weight: 600; font-size: 0.88rem;">/</span>
                                        <span style="color: #{st.pairBColor};">#{dashboardBean.selectedPair2}</span>
                                    </div>
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(target_columns, new_columns)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml: Señal de Origen with drawn shapes & origin pair colors, and Dirección de Arbitraje with color-coded pairs!")
