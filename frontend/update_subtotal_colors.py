dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Update Monto Invertido column in signalTradesTable
old_monto_col = '''                            <p:column headerText="Monto Invertido ($) [20%]" style="width: 130px; text-align: right; font-weight: 700; font-size: 1.0rem; color: #{st.isSubtotal ? '#0f172a' : '#475569'};">
                                <span style="font-weight: #{st.isSubtotal ? '900' : '700'};">$#{st.allocatedCapital}</span>
                            </p:column>'''

new_monto_col = '''                            <p:column headerText="Monto Invertido ($) [20%]" style="width: 140px; text-align: right; font-size: 1.0rem;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <span style="font-weight: 700; color: #475569;">$#{st.allocatedCapital}</span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.allocatedCapital}
                                    </span>
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(old_monto_col, new_monto_col)

# Update Retorno Neto & PnL in subtotal row as well to make it completely uniform and high-contrast
old_ret_col = '''                            <p:column headerText="Retorno Neto" style="width: 90px; text-align: right;">
                                <span style="font-weight: 800; font-size: 1.1rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                    #{st.returnPct > 0 ? '+' : ''}#{st.returnPct}%
                                </span>
                            </p:column>
                            <p:column headerText="PnL ($)" style="width: 90px; text-align: right; font-weight: 800;">
                                <span style="color: #{st.isWin ? '#10b981' : '#ef4444'}; font-size: 1.1rem;">
                                    $#{st.pnl}
                                </span>
                            </p:column>'''

new_ret_col = '''                            <p:column headerText="Retorno Neto" style="width: 105px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <span style="font-weight: 800; font-size: 1.05rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                        #{st.returnPct > 0 ? '+' : ''}#{st.returnPct}%
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        #{st.returnPct > 0 ? '+' : ''}#{st.returnPct}%
                                    </span>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="PnL ($)" style="width: 105px; text-align: right;">
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

dash = dash.replace(old_ret_col, new_ret_col)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with strong green/red styling for subtotal amounts and metrics!")
