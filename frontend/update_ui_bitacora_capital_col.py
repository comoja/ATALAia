dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Update subtab title and inner header
dash = dash.replace('<p:tab title="Bitácora Detallada de Trades">', '<p:tab title="Bitácora detallada de Trades">')
dash = dash.replace('<i class="pi pi-list" style="margin-right: 6px; color: #10b981;"></i>Bitácora Detallada de Trades Ejecutados', '<i class="pi pi-list" style="margin-right: 6px; color: #10b981;"></i>Bitácora detallada de Trades')

# 2. Add Capital ($) column after PnL column
target_pnl_col_end = '''                                    <div style="font-size: 0.78rem; font-weight: 700; color: #{st.pnlB >= 0 ? '#15803d' : '#b91c1c'};">
                                        #{dashboardBean.selectedPair2}: #{st.pnlB >= 0 ? '+' : ''}$#{st.formattedPnlB} (#{st.pipsB > 0 ? '+' : ''}#{st.pipsB}p)
                                    </div>
                                </h:panelGroup>
                            </p:column>'''

new_pnl_and_capital_col = '''                                    <div style="font-size: 0.78rem; font-weight: 700; color: #{st.pnlB >= 0 ? '#15803d' : '#b91c1c'};">
                                        #{dashboardBean.selectedPair2}: #{st.pnlB >= 0 ? '+' : ''}$#{st.formattedPnlB} (#{st.pipsB > 0 ? '+' : ''}#{st.pipsB}p)
                                    </div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Capital ($)" style="width: 125px; text-align: right; font-size: 1.05rem;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <span style="font-weight: 800; font-size: 1.05rem; color: #1e293b;">
                                        $#{st.formattedAccumCapital}
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isOpen ? '#0369a1' : '#0f172a'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.formattedAccumCapital}
                                    </span>
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(target_pnl_col_end, new_pnl_and_capital_col, 1)

# 3. Update footer to include capital final
target_footer = '''                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtCombinedMetrics.totalTrades} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.formattedSignalBtTotalPnl}" />
                                </p:row>
                            </p:columnGroup>'''

new_footer = '''                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtCombinedMetrics.totalTrades} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.formattedSignalBtTotalPnl}" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #0284c7;" footerText="$#{dashboardBean.signalBtCombinedMetrics.finalCapital}" />
                                </p:row>
                            </p:columnGroup>'''

dash = dash.replace(target_footer, new_footer, 1)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml: renamed tab to 'Bitácora detallada de Trades' and added 'Capital ($)' progressive column!")
