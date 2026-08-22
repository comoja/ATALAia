dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Add footer to quantTradesList table
quant_target = '''                                <p:column headerText="PnL ($)" style="width: 85px; text-align: right; font-weight: 700;">
                                    <span style="color: #{t.isWin ? '#10b981' : '#ef4444'};">
                                        $#{t.pnl}
                                    </span>
                                </p:column>
                            </p:dataTable>'''

quant_replacement = '''                                <p:column headerText="PnL ($)" style="width: 85px; text-align: right; font-weight: 700;">
                                    <span style="color: #{t.isWin ? '#10b981' : '#ef4444'};">
                                        $#{t.pnl}
                                    </span>
                                </p:column>
                                <p:columnGroup type="footer">
                                    <p:row>
                                        <p:column colspan="4" style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                        <p:column style="text-align: right; font-weight: 900; font-size: 1.15rem; background: #f8fafc; color: #{dashboardBean.quantMetrics.totalReturnPct >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.quantMetrics.totalReturnPct >= 0 ? '+' : ''}#{dashboardBean.quantMetrics.totalReturnPct}%" />
                                        <p:column style="text-align: right; font-weight: 900; font-size: 1.15rem; background: #f8fafc; color: #{dashboardBean.quantTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.quantTotalPnl}" />
                                    </p:row>
                                </p:columnGroup>
                            </p:dataTable>'''

if quant_target in dash and 'TOTAL NETO (SUMATORIA):' not in dash:
    dash = dash.replace(quant_target, quant_replacement)
    print("Added footer to Quant Backtest table!")

# 2. Add footer to signalTradesTable
signal_target = '''                            <p:column headerText="PnL ($)" style="width: 90px; text-align: right; font-weight: 800;">
                                <span style="color: #{st.isWin ? '#10b981' : '#ef4444'}; font-size: 1.1rem;">
                                    $#{st.pnl}
                                </span>
                            </p:column>
                        </p:dataTable>'''

signal_replacement = '''                            <p:column headerText="PnL ($)" style="width: 90px; text-align: right; font-weight: 800;">
                                <span style="color: #{st.isWin ? '#10b981' : '#ef4444'}; font-size: 1.1rem;">
                                    $#{st.pnl}
                                </span>
                            </p:column>
                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtTradesList.size()} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.signalBtTotalPnl}" />
                                </p:row>
                            </p:columnGroup>
                        </p:dataTable>'''

if signal_target in dash:
    dash = dash.replace(signal_target, signal_replacement)
    print("Added footer to Signal Backtest table!")

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml updated with table footers!")
