dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Replace the whole <p:dataTable id="signalTradesTable" ... </p:dataTable>
start_tag = '<p:dataTable id="signalTradesTable"'
end_tag = '</p:dataTable>'

start_idx = dash.find(start_tag)
end_idx = dash.find(end_tag, start_idx) + len(end_tag)

new_table = '''<p:dataTable id="signalTradesTable" value="#{dashboardBean.signalBtTradesList}" var="st" scrollable="true" scrollHeight="360px" styleClass="aether-table-compact" rowStyleClass="#{st.isSubtotal ? 'aether-subtotal-row' : ''}" emptyMessage="Sin operaciones registradas para esta estrategia" style="width: 100%;">
                            <p:column headerText="#" style="width: 25px; text-align: center;">
                                <h:outputText value="#{st.tradeNum}" rendered="#{!st.isSubtotal}" />
                                <i class="pi pi-check-circle" style="color: #10b981; font-weight: bold;" rendered="#{st.isSubtotal}" title="Subtotal Cierre"></i>
                            </p:column>
                            <p:column headerText="Señal Origen" style="width: 125px;">
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isOpen ? '#0369a1' : '#0f172a'}; color: #ffffff; padding: 2px 6px; border-radius: 5px; font-weight: 800; font-size: 0.88rem; display: inline-block;">
                                        #{st.signalType}
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="display: inline-flex; align-items: center; gap: 5px;">
                                        <!-- Cuadro Dibujado -->
                                        <h:panelGroup rendered="#{st.isSquare()}">
                                            <span style="width: 12px; height: 12px; background-color: #{st.signalColor}; border-radius: 2px; display: inline-block; box-shadow: 0 1px 2px rgba(0,0,0,0.25);"></span>
                                        </h:panelGroup>
                                        <!-- Triángulo Dibujado -->
                                        <h:panelGroup rendered="#{st.isTriangle()}">
                                            <span style="color: #{st.signalColor}; font-size: 1.05rem; line-height: 1; font-weight: 900; display: inline-block;">
                                                #{st.isTriangleUp() ? '▲' : '▼'}
                                            </span>
                                        </h:panelGroup>
                                        <!-- Moneda / Par que origina la señal del mismo color -->
                                        <span style="color: #{st.signalColor}; font-weight: 800; font-size: 0.95rem;">
                                            #{st.signalOriginPair}
                                        </span>
                                        <!-- Badge EN CURSO si aplica -->
                                        <h:panelGroup rendered="#{st.isOpen}">
                                            <span style="background: #fef3c7; color: #b45309; font-size: 0.70rem; font-weight: 800; padding: 1px 4px; border-radius: 3px; border: 1px solid #fde68a;">
                                                EN CURSO
                                            </span>
                                        </h:panelGroup>
                                    </div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Dirección Arbitraje" style="width: 140px; text-align: center;">
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="color: #{st.isOpen ? '#0284c7' : '#0369a1'}; font-weight: 800; font-size: 0.90rem;">
                                        #{st.direction}
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="display: inline-flex; align-items: center; gap: 5px; font-size: 0.98rem; font-weight: 800;">
                                        <span style="color: #{st.pairAColor};">#{dashboardBean.selectedPair}</span>
                                        <span style="color: #94a3b8; font-weight: 600; font-size: 0.85rem;">/</span>
                                        <span style="color: #{st.pairBColor};">#{dashboardBean.selectedPair2}</span>
                                    </div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Fecha Entrada" style="font-size: 0.95rem; width: 105px;">
                                <h:outputText value="#{st.entryDate}" style="font-weight: #{st.isSubtotal ? '700' : 'normal'};" />
                            </p:column>
                            <p:column headerText="Precios Entrada" style="font-size: 0.90rem; width: 125px;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-size: 0.88rem; font-weight: 600; color: #1e293b;">#{dashboardBean.selectedPair}: #{st.entryPriceA}</div>
                                    <div style="font-size: 0.88rem; font-weight: 600; color: #475569;">#{dashboardBean.selectedPair2}: #{st.entryPriceB}</div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Fecha Salida (● Media)" style="font-size: 0.95rem; width: 100px; text-align: center;">
                                <h:panelGroup rendered="#{st.isOpen}">
                                    <span style="background: #e0f2fe; color: #0369a1; padding: 2px 6px; border-radius: 4px; font-weight: 800; font-size: 0.82rem; border: 1px solid #bae6fd;">
                                        ● EN CURSO
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{!st.isOpen}">
                                    <h:outputText value="#{st.exitDate}" style="font-weight: #{st.isSubtotal ? '800; color: #1e293b;' : 'normal'};" />
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Precios Salida" style="font-size: 0.90rem; width: 125px;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-size: 0.88rem; font-weight: 600; color: #1e293b;">#{dashboardBean.selectedPair}: #{st.exitPriceA}</div>
                                    <div style="font-size: 0.88rem; font-weight: 600; color: #475569;">#{dashboardBean.selectedPair2}: #{st.exitPriceB}</div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Margen Invertido ($)" style="width: 140px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 700; color: #1e293b; font-size: 1.0rem;">$#{st.formattedAllocatedCapital}</div>
                                    <div style="font-size: 0.76rem; color: #64748b; font-weight: 600; line-height: 1.2;">#{dashboardBean.selectedPair}: #{st.formattedUnitsA} lotes (#{st.multA}x)</div>
                                    <div style="font-size: 0.76rem; color: #64748b; font-weight: 600; line-height: 1.2;">#{dashboardBean.selectedPair2}: #{st.formattedUnitsB} lotes (#{st.multB}x)</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 3px 6px; border-radius: 5px; font-weight: 900; font-size: 1.02rem; display: inline-block;">
                                        $#{st.formattedAllocatedCapital}
                                    </span>
                                    <div style="font-size: 0.76rem; color: #334155; font-weight: 700; margin-top: 2px; line-height: 1.2;">#{dashboardBean.selectedPair}: #{st.formattedUnitsA} lotes</div>
                                    <div style="font-size: 0.76rem; color: #334155; font-weight: 700; line-height: 1.2;">#{dashboardBean.selectedPair2}: #{st.formattedUnitsB} lotes</div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Velas" style="width: 40px; text-align: center;">
                                <h:outputText value="#{st.durationBars}" rendered="#{!st.isSubtotal}" />
                                <h:outputText value="#{st.durationBars} ops" rendered="#{st.isSubtotal}" style="font-weight: 800; color: #0369a1;" />
                            </p:column>
                            <p:column headerText="Retorno Neto" style="width: 75px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <span style="font-weight: 800; font-size: 0.98rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                        #{st.returnPct > 0 ? '+' : ''}#{st.returnPct}%
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 3px 6px; border-radius: 5px; font-weight: 900; font-size: 0.98rem; display: inline-block;">
                                        #{st.returnPct > 0 ? '+' : ''}#{st.returnPct}%
                                    </span>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="PnL ($)" style="width: 140px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 800; font-size: 1.05rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                        #{st.pnl >= 0 ? '+' : ''}$#{st.formattedPnl}
                                    </div>
                                    <div style="font-size: 0.76rem; font-weight: 600; color: #{st.pnlA >= 0 ? '#15803d' : '#b91c1c'}; line-height: 1.2;">
                                        #{dashboardBean.selectedPair}: #{st.pnlA >= 0 ? '+' : ''}$#{st.formattedPnlA} (#{st.pipsA > 0 ? '+' : ''}#{st.pipsA}p)
                                    </div>
                                    <div style="font-size: 0.76rem; font-weight: 600; color: #{st.pnlB >= 0 ? '#15803d' : '#b91c1c'}; line-height: 1.2;">
                                        #{dashboardBean.selectedPair2}: #{st.pnlB >= 0 ? '+' : ''}$#{st.formattedPnlB} (#{st.pipsB > 0 ? '+' : ''}#{st.pipsB}p)
                                    </div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 3px 6px; border-radius: 5px; font-weight: 900; font-size: 1.02rem; display: inline-block;">
                                        #{st.pnl >= 0 ? '+' : ''}$#{st.formattedPnl}
                                    </span>
                                    <div style="font-size: 0.76rem; font-weight: 700; color: #{st.pnlA >= 0 ? '#15803d' : '#b91c1c'}; margin-top: 2px; line-height: 1.2;">
                                        #{dashboardBean.selectedPair}: #{st.pnlA >= 0 ? '+' : ''}$#{st.formattedPnlA} (#{st.pipsA > 0 ? '+' : ''}#{st.pipsA}p)
                                    </div>
                                    <div style="font-size: 0.76rem; font-weight: 700; color: #{st.pnlB >= 0 ? '#15803d' : '#b91c1c'}; line-height: 1.2;">
                                        #{dashboardBean.selectedPair2}: #{st.pnlB >= 0 ? '+' : ''}$#{st.formattedPnlB} (#{st.pipsB > 0 ? '+' : ''}#{st.pipsB}p)
                                    </div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Capital ($)" style="width: 90px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <span style="font-weight: 800; font-size: 1.02rem; color: #1e293b;">
                                        $#{st.formattedAccumCapital}
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isOpen ? '#0369a1' : '#0f172a'}; color: #ffffff; padding: 3px 6px; border-radius: 5px; font-weight: 900; font-size: 1.02rem; display: inline-block;">
                                        $#{st.formattedAccumCapital}
                                    </span>
                                </h:panelGroup>
                            </p:column>
                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.0rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtCombinedMetrics.totalTrades} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.1rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.1rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.formattedSignalBtTotalPnl}" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.1rem; background: #f8fafc; color: #0284c7;" footerText="$#{dashboardBean.signalBtCombinedMetrics.finalCapital}" />
                                </p:row>
                            </p:columnGroup>
                        </p:dataTable>'''

dash = dash[:start_idx] + new_table + dash[end_idx:]

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml: compact widths, no horizontal scroll, 2 lines for lots in Margen Invertido!")
