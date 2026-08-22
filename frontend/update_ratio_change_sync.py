# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    bean_code = f.read()

old_denom = '''    public void onDenominadorChange() {
        log.info("Selección de Denominador cambiada a: {}. Consultando BD...", selectedPair2);
        fetchUserRatioDetails();
    }'''

new_denom = '''    public void onDenominadorChange() {
        log.info("Selección de Denominador cambiada a: {}. Consultando BD...", selectedPair2);
        fetchUserRatioDetails();
        analyzePair();
    }'''

if old_denom in bean_code:
    bean_code = bean_code.replace(old_denom, new_denom)
    with open(bean_path, 'w', encoding='utf-8') as f:
        f.write(bean_code)
    print("Updated onDenominadorChange to automatically analyzePair()!")

# 2. Update dashboard.xhtml
dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Fix symbolSelect and symbolSelect2 AJAX
old_sym1 = '<p:ajax process="@this" update=":aetherForm:mainTabView:leftAccordion" listener="#{dashboardBean.onNumeradorChange}" />'
new_sym1 = '<p:ajax process="@this" update=":aetherForm" oncomplete="renderAetherChart(); renderSignalComparisonChart();" listener="#{dashboardBean.onNumeradorChange}" />'

old_sym2 = '<p:ajax process="@this" update=":aetherForm" oncomplete="renderAetherChart()" listener="#{dashboardBean.onUsuarioChange}" />'
new_sym2 = '<p:ajax process="@this" update=":aetherForm" oncomplete="renderAetherChart(); renderSignalComparisonChart();" listener="#{dashboardBean.onDenominadorChange}" />'

dash = dash.replace(old_sym1, new_sym1)
dash = dash.replace(old_sym2, new_sym2)

# Fix oncomplete across ratio/user selection buttons
dash = dash.replace('listener="#{dashboardBean.onRowSelect}" update=":aetherForm" oncomplete="renderAetherChart()"',
                    'listener="#{dashboardBean.onRowSelect}" update=":aetherForm" oncomplete="renderAetherChart(); renderSignalComparisonChart();"')

dash = dash.replace('action="#{dashboardBean.onSelectUserRatio(r)}"\n                                                 update=":aetherForm"\n                                                 oncomplete="renderAetherChart()"',
                    'action="#{dashboardBean.onSelectUserRatio(r)}"\n                                                 update=":aetherForm"\n                                                 oncomplete="renderAetherChart(); renderSignalComparisonChart();"')

dash = dash.replace('listener="#{dashboardBean.onCuentaChange}" />',
                    'listener="#{dashboardBean.onCuentaChange}" />')

dash = dash.replace('listener="#{dashboardBean.onUsuarioChange}" />',
                    'listener="#{dashboardBean.onUsuarioChange}" />')

# Fix quantTradesList table (remove erroneous p:summaryRow that got placed there)
import re
dash = re.sub(r'(<p:dataTable value="#\{dashboardBean\.quantTradesList\}".*?)(<p:summaryRow>.*?</p:summaryRow>\s*)(<p:columnGroup type="footer">)',
              r'\1\3',
              dash,
              flags=re.DOTALL)

# Format signalTradesTable with clean p:summaryRow grouped by exitDate
signal_table_full = '''                        <p:dataTable id="signalTradesTable" value="#{dashboardBean.signalBtTradesList}" var="st" sortBy="#{st.exitDate}" scrollable="true" scrollHeight="290px" styleClass="aether-table-compact" emptyMessage="Sin operaciones registradas para esta estrategia">
                            <p:column headerText="#" style="width: 28px; text-align: center;">
                                <h:outputText value="#{st.tradeNum}" />
                            </p:column>
                            <p:column headerText="Señal Origen" style="width: 170px;">
                                <span style="padding: 2px 6px; border-radius: 4px; font-weight: 700; font-size: 0.95rem; background: #{st.signalType.contains('TRIANGULO') ? '#e0f2fe' : '#f3e8ff'}; color: #{st.signalType.contains('TRIANGULO') ? '#0369a1' : '#7e22ce'};">
                                    #{st.signalType}
                                </span>
                            </p:column>
                            <p:column headerText="Dirección Arbitraje" style="width: 140px; font-weight: 700; font-size: 1.0rem;">
                                <span style="color: #{st.direction.startsWith('LONG A') ? '#15803d' : '#b91c1c'};">
                                    #{st.direction}
                                </span>
                            </p:column>
                            <p:column headerText="Fecha Entrada" style="font-size: 1.05rem; width: 110px;">
                                <h:outputText value="#{st.entryDate}" />
                            </p:column>
                            <p:column headerText="Precios Entrada" style="font-size: 1.0rem; width: 140px;">
                                <span>A: #{st.entryPriceA} | B: #{st.entryPriceB}</span>
                            </p:column>
                            <p:column headerText="Fecha Salida (● Media)" style="font-size: 1.05rem; width: 130px;">
                                <h:outputText value="#{st.exitDate}" />
                            </p:column>
                            <p:column headerText="Precios Salida" style="font-size: 1.0rem; width: 140px;">
                                <span>A: #{st.exitPriceA} | B: #{st.exitPriceB}</span>
                            </p:column>
                            <p:column headerText="Monto Invertido ($) [20%]" style="width: 130px; text-align: right; font-weight: 700; font-size: 1.0rem; color: #475569;">
                                <span>$#{st.allocatedCapital}</span>
                            </p:column>
                            <p:column headerText="Velas" style="width: 50px; text-align: center;">
                                <h:outputText value="#{st.durationBars}" />
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
                            <p:summaryRow>
                                <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f1f5f9; color: #1e293b;">
                                    <i class="pi pi-check-circle" style="color: #10b981; margin-right: 4px;"></i>
                                    <span>SUBTOTAL CIERRE (#{st.exitDate}) [#{st.cycleTotalTrades} ops]:</span>
                                </p:column>
                                <p:column style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f1f5f9; color: #334155;">
                                    <span>$#{st.cycleTotalInvested}</span>
                                </p:column>
                                <p:column style="text-align: center; font-weight: 700; font-size: 0.95rem; background: #f1f5f9; color: #64748b;">
                                    <span>#{st.cycleTotalTrades} ops</span>
                                </p:column>
                                <p:column style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f1f5f9; color: #{st.cycleAvgReturnPct >= 0 ? '#10b981' : '#ef4444'};">
                                    <span>#{st.cycleAvgReturnPct >= 0 ? '+' : ''}#{st.cycleAvgReturnPct}%</span>
                                </p:column>
                                <p:column style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f1f5f9; color: #{st.cycleTotalPnl >= 0 ? '#10b981' : '#ef4444'};">
                                    <span>$#{st.cycleTotalPnl}</span>
                                </p:column>
                            </p:summaryRow>
                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtTradesList.size()} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.signalBtTotalPnl}" />
                                </p:row>
                            </p:columnGroup>
                        </p:dataTable>'''

dash = re.sub(r'<p:dataTable id="signalTradesTable".*?</p:dataTable>', signal_table_full, dash, flags=re.DOTALL)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with subtotals by exitDate and full AJAX sync on ratio/user change!")
