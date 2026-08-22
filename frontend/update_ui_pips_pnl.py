dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_pnl = '''                            <p:column headerText="PnL ($)" style="width: 105px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <span style="font-weight: 800; font-size: 1.05rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                        $#{st.pnl}
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.pnl}
                                    </span>
                                </h:panelGroup>
                            </p:column>'''

new_pnl = '''                            <p:column headerText="PnL ($)" style="width: 120px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 800; font-size: 1.1rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                        $#{st.pnl}
                                    </div>
                                    <div style="font-size: 0.80rem; color: #64748b; font-weight: 600;">A: #{st.pipsA}p | B: #{st.pipsB}p</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.pnl}
                                    </span>
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(old_pnl, new_pnl)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with PnL pip breakdown!")
