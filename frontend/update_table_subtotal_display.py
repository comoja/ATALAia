dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Add CSS for aether-subtotal-row if not present
subtotal_css = '''
    .aether-subtotal-row {
        background-color: #f1f5f9 !important;
        border-top: 2px solid #94a3b8 !important;
        border-bottom: 2px solid #94a3b8 !important;
    }
    .aether-subtotal-row td {
        background-color: #f1f5f9 !important;
        font-weight: 700 !important;
        padding-top: 6px !important;
        padding-bottom: 6px !important;
    }
'''

if '.aether-subtotal-row' not in dash:
    dash = dash.replace('</style>', subtotal_css + '\n    </style>')

# Format signalTradesTable
signal_table_markup = '''                        <p:dataTable id="signalTradesTable" value="#{dashboardBean.signalBtTradesList}" var="st" scrollable="true" scrollHeight="320px" styleClass="aether-table-compact" rowStyleClass="#{st.isSubtotal ? 'aether-subtotal-row' : ''}" emptyMessage="Sin operaciones registradas para esta estrategia">
                            <p:column headerText="#" style="width: 28px; text-align: center;">
                                <h:outputText value="#{st.tradeNum}" rendered="#{!st.isSubtotal}" />
                                <i class="pi pi-check-circle" style="color: #10b981; font-weight: bold;" rendered="#{st.isSubtotal}" title="Subtotal Cierre"></i>
                            </p:column>
                            <p:column headerText="Señal Origen" style="width: 170px;">
                                <span style="padding: 2px 6px; border-radius: 4px; font-weight: 700; font-size: 0.95rem; background: #{st.isSubtotal ? '#0f172a' : (st.signalType.contains('TRIANGULO') ? '#e0f2fe' : '#f3e8ff')}; color: #{st.isSubtotal ? '#38bdf8' : (st.signalType.contains('TRIANGULO') ? '#0369a1' : '#7e22ce')};">
                                    #{st.signalType}
                                </span>
                            </p:column>
                            <p:column headerText="Dirección Arbitraje" style="width: 140px; font-weight: 700; font-size: 1.0rem;">
                                <span style="color: #{st.isSubtotal ? '#0369a1' : (st.direction.startsWith('LONG A') ? '#15803d' : '#b91c1c')};">
                                    #{st.direction}
                                </span>
                            </p:column>
                            <p:column headerText="Fecha Entrada" style="font-size: 1.05rem; width: 110px;">
                                <h:outputText value="#{st.entryDate}" style="font-weight: #{st.isSubtotal ? '700' : 'normal'};" />
                            </p:column>
                            <p:column headerText="Precios Entrada" style="font-size: 1.0rem; width: 140px;">
                                <span rendered="#{!st.isSubtotal}">A: #{st.entryPriceA} | B: #{st.entryPriceB}</span>
                                <span rendered="#{st.isSubtotal}" style="color: #94a3b8;">—</span>
                            </p:column>
                            <p:column headerText="Fecha Salida (● Media)" style="font-size: 1.05rem; width: 130px;">
                                <h:outputText value="#{st.exitDate}" style="font-weight: #{st.isSubtotal ? '800; color: #1e293b;' : 'normal'};" />
                            </p:column>
                            <p:column headerText="Precios Salida" style="font-size: 1.0rem; width: 140px;">
                                <span rendered="#{!st.isSubtotal}">A: #{st.exitPriceA} | B: #{st.exitPriceB}</span>
                                <span rendered="#{st.isSubtotal}" style="color: #94a3b8;">—</span>
                            </p:column>
                            <p:column headerText="Monto Invertido ($) [20%]" style="width: 130px; text-align: right; font-weight: 700; font-size: 1.0rem; color: #{st.isSubtotal ? '#0f172a' : '#475569'};">
                                <span style="font-weight: #{st.isSubtotal ? '900' : '700'};">$#{st.allocatedCapital}</span>
                            </p:column>
                            <p:column headerText="Velas" style="width: 50px; text-align: center;">
                                <h:outputText value="#{st.durationBars}" rendered="#{!st.isSubtotal}" />
                                <h:outputText value="#{st.durationBars} ops" rendered="#{st.isSubtotal}" style="font-weight: 800; color: #0369a1;" />
                            </p:column>
                            <p:column headerText="Retorno Neto" style="width: 90px; text-align: right;">
                                <span style="font-weight: 800; font-size: 1.1rem; color: #{st.isWin ? '#10b981' : '#ef4444'};">
                                    #{st.returnPct > 0 ? '+' : ''}#{st.returnPct}%
                                </span>
                            </p:column>
                            <p:column headerText="PnL ($)" style="width: 90px; text-align: right; font-weight: 800;">
                                <span style="color: #{st.isWin ? '#10b981' : '#ef4444'}; font-size: 1.1rem;">
                                    $#{st.pnl}
                                </span>
                            </p:column>
                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtCombinedMetrics.totalTrades} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.signalBtTotalPnl}" />
                                </p:row>
                            </p:columnGroup>
                        </p:dataTable>'''

import re
dash = re.sub(r'<p:dataTable id="signalTradesTable".*?</p:dataTable>', signal_table_markup, dash, flags=re.DOTALL)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with guaranteed visual subtotal rows in table!")
