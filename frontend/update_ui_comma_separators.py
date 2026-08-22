dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Update top cards
dash = dash.replace(
    '+$#{dashboardBean.signalBtTrianglesMetrics.netProfit}',
    '+$#{dashboardBean.signalBtTrianglesMetrics.formattedNetProfit}'
)
dash = dash.replace(
    '+$#{dashboardBean.signalBtCombinedMetrics.netProfit}',
    '+$#{dashboardBean.signalBtCombinedMetrics.formattedNetProfit}'
)

# 2. Update Table Header
dash = dash.replace(
    'reqMarginPerMinLot',
    'formattedReqMarginPerMinLot'
)

# 3. Update Table Columns (Margen, PnL, Footer)
old_table_chunk = '''                            <p:column headerText="Margen Invertido ($) [Req: ~$#{dashboardBean.signalBtCombinedMetrics.formattedReqMarginPerMinLot} / min_lot | Asig: 3%]" style="width: 210px; text-align: right; font-size: 1.0rem;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 700; color: #1e293b; font-size: 1.05rem;">$#{st.allocatedCapital}</div>
                                    <div style="font-size: 0.80rem; color: #64748b; font-weight: 600;">#{dashboardBean.selectedPair}: #{st.unitsA} lotes (#{st.multA}x) | #{dashboardBean.selectedPair2}: #{st.unitsB} lotes (#{st.multB}x)</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.allocatedCapital}
                                    </span>
                                    <div style="font-size: 0.80rem; color: #334155; font-weight: 700; margin-top: 2px;">Total: #{dashboardBean.selectedPair}: #{st.unitsA} | #{dashboardBean.selectedPair2}: #{st.unitsB} lotes</div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Velas" style="width: 50px; text-align: center;">
                                <h:outputText value="#{st.durationBars}" rendered="#{!st.isSubtotal}" />
                                <h:outputText value="#{st.durationBars} ops" rendered="#{st.isSubtotal}" style="font-weight: 800; color: #0369a1;" />
                            </p:column>
                            <p:column headerText="Retorno Neto" style="width: 105px; text-align: right;">
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
                            <p:column headerText="PnL ($) [Neto y Por Par]" style="width: 185px; text-align: right;">
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
                            </p:column>
                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtCombinedMetrics.totalTrades} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.signalBtTotalPnl}" />
                                </p:row>
                            </p:columnGroup>'''

new_table_chunk = '''                            <p:column headerText="Margen Invertido ($) [Req: ~$#{dashboardBean.signalBtCombinedMetrics.formattedReqMarginPerMinLot} / min_lot | Asig: 3%]" style="width: 210px; text-align: right; font-size: 1.0rem;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 700; color: #1e293b; font-size: 1.05rem;">$#{st.formattedAllocatedCapital}</div>
                                    <div style="font-size: 0.80rem; color: #64748b; font-weight: 600;">#{dashboardBean.selectedPair}: #{st.formattedUnitsA} lotes (#{st.multA}x) | #{dashboardBean.selectedPair2}: #{st.formattedUnitsB} lotes (#{st.multB}x)</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        $#{st.formattedAllocatedCapital}
                                    </span>
                                    <div style="font-size: 0.80rem; color: #334155; font-weight: 700; margin-top: 2px;">Total: #{dashboardBean.selectedPair}: #{st.formattedUnitsA} | #{dashboardBean.selectedPair2}: #{st.formattedUnitsB} lotes</div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Velas" style="width: 50px; text-align: center;">
                                <h:outputText value="#{st.durationBars}" rendered="#{!st.isSubtotal}" />
                                <h:outputText value="#{st.durationBars} ops" rendered="#{st.isSubtotal}" style="font-weight: 800; color: #0369a1;" />
                            </p:column>
                            <p:column headerText="Retorno Neto" style="width: 105px; text-align: right;">
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
                            <p:column headerText="PnL ($) [Neto y Por Par]" style="width: 185px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 800; font-size: 1.1rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                        #{st.pnl >= 0 ? '+' : ''}$#{st.formattedPnl}
                                    </div>
                                    <div style="font-size: 0.78rem; font-weight: 600; color: #{st.pnlA >= 0 ? '#15803d' : '#b91c1c'};">
                                        #{dashboardBean.selectedPair}: #{st.pnlA >= 0 ? '+' : ''}$#{st.formattedPnlA} (#{st.pipsA > 0 ? '+' : ''}#{st.pipsA}p)
                                    </div>
                                    <div style="font-size: 0.78rem; font-weight: 600; color: #{st.pnlB >= 0 ? '#15803d' : '#b91c1c'};">
                                        #{dashboardBean.selectedPair2}: #{st.pnlB >= 0 ? '+' : ''}$#{st.formattedPnlB} (#{st.pipsB > 0 ? '+' : ''}#{st.pipsB}p)
                                    </div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.15); display: inline-block;">
                                        #{st.pnl >= 0 ? '+' : ''}$#{st.formattedPnl}
                                    </span>
                                    <div style="font-size: 0.78rem; font-weight: 700; color: #{st.pnlA >= 0 ? '#15803d' : '#b91c1c'}; margin-top: 2px;">
                                        #{dashboardBean.selectedPair}: #{st.pnlA >= 0 ? '+' : ''}$#{st.formattedPnlA} (#{st.pipsA > 0 ? '+' : ''}#{st.pipsA}p)
                                    </div>
                                    <div style="font-size: 0.78rem; font-weight: 700; color: #{st.pnlB >= 0 ? '#15803d' : '#b91c1c'};">
                                        #{dashboardBean.selectedPair2}: #{st.pnlB >= 0 ? '+' : ''}$#{st.formattedPnlB} (#{st.pipsB > 0 ? '+' : ''}#{st.pipsB}p)
                                    </div>
                                </h:panelGroup>
                            </p:column>
                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtCombinedMetrics.totalTrades} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.formattedSignalBtTotalPnl}" />
                                </p:row>
                            </p:columnGroup>'''

dash = dash.replace(old_table_chunk, new_table_chunk)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with thousands comma formatting across all amounts and lot counts!")
