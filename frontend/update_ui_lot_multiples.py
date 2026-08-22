dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_col = '''                            <p:column headerText="Margen Invertido ($) [Req: ~$#{dashboardBean.signalBtCombinedMetrics.reqMarginPerMinLot} / min_lot | Asig: 3%]" style="width: 140px; text-align: right; font-size: 1.0rem;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <span style="font-weight: 700; color: #475569;">$#{st.allocatedCapital}</span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.allocatedCapital}
                                    </span>
                                </h:panelGroup>
                            </p:column>'''

new_col = '''                            <p:column headerText="Margen Invertido ($) [Req: ~$#{dashboardBean.signalBtCombinedMetrics.reqMarginPerMinLot} / min_lot | Asig: 3%]" style="width: 180px; text-align: right; font-size: 1.0rem;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 700; color: #1e293b; font-size: 1.05rem;">$#{st.allocatedCapital}</div>
                                    <div style="font-size: 0.82rem; color: #64748b; font-weight: 600;">A: #{st.unitsA} lotes (#{st.multA}x) | B: #{st.unitsB} lotes (#{st.multB}x)</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.allocatedCapital}
                                    </span>
                                    <div style="font-size: 0.82rem; color: #334155; font-weight: 700; margin-top: 2px;">Total: A: #{st.unitsA} | B: #{st.unitsB} lotes</div>
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(old_col, new_col)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with lot multiples display in Margen Invertido column!")
