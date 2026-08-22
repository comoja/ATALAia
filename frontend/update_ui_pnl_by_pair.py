dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

target_pnl_col = '''                            <p:column headerText="PnL ($)" style="width: 135px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 800; font-size: 1.1rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                        $#{st.pnl}
                                    </div>
                                    <div style="font-size: 0.80rem; color: #64748b; font-weight: 600;">#{dashboardBean.selectedPair}: #{st.pipsA}p | #{dashboardBean.selectedPair2}: #{st.pipsB}p</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.pnl}
                                    </span>
                                </h:panelGroup>
                            </p:column>'''

new_pnl_col = '''                            <p:column headerText="PnL ($) [Neto y Por Par]" style="width: 185px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 800; font-size: 1.1rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                        #{st.pnl >= 0 ? '+' : ''}$#{st.pnl}
                                    </div>
                                    <div style="font-size: 0.78rem; font-weight: 600; color: #{st.pnlA >= 0 ? '#15803d' : '#b91c1c'};">
                                        #{dashboardBean.selectedPair}: #{st.pnlA >= 0 ? '+' : ''}$#{st.pnlA} (#{st.pipsA > 0 ? '+' : ''}#{st.pipsA}p)
                                    </div>
                                    <div style="font-size: 0.78rem; font-weight: 600; color: #{st.pnlB >= 0 ? '#15803d' : '#b91c1c'};">
                                        #{dashboardBean.selectedPair2}: #{st.pnlB >= 0 ? '+' : ''}$#{st.pnlB} (#{st.pipsB > 0 ? '+' : ''}#{st.pipsB}p)
                                    </div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        #{st.pnl >= 0 ? '+' : ''}$#{st.pnl}
                                    </span>
                                    <div style="font-size: 0.78rem; font-weight: 700; color: #{st.pnlA >= 0 ? '#15803d' : '#b91c1c'}; margin-top: 2px;">
                                        #{dashboardBean.selectedPair}: #{st.pnlA >= 0 ? '+' : ''}$#{st.pnlA} (#{st.pipsA > 0 ? '+' : ''}#{st.pipsA}p)
                                    </div>
                                    <div style="font-size: 0.78rem; font-weight: 700; color: #{st.pnlB >= 0 ? '#15803d' : '#b91c1c'};">
                                        #{dashboardBean.selectedPair2}: #{st.pnlB >= 0 ? '+' : ''}$#{st.pnlB} (#{st.pipsB > 0 ? '+' : ''}#{st.pipsB}p)
                                    </div>
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(target_pnl_col, new_pnl_col)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with PnL separated by pair!")
