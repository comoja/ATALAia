dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_table_def = '<p:dataTable id="signalTradesTable" value="#{dashboardBean.signalBtTradesList}" var="st" scrollable="true" scrollHeight="260px" styleClass="aether-table-compact" emptyMessage="Sin operaciones registradas para esta estrategia">'
new_table_def = '<p:dataTable id="signalTradesTable" value="#{dashboardBean.signalBtTradesList}" var="st" sortBy="#{st.cycleNum}" scrollable="true" scrollHeight="290px" styleClass="aether-table-compact" emptyMessage="Sin operaciones registradas para esta estrategia">'

dash = dash.replace(old_table_def, new_table_def)

# Add p:summaryRow inside signalTradesTable right before p:columnGroup
target_before_footer = '                            <p:columnGroup type="footer">'
summary_row = '''                            <p:summaryRow>
                                <p:column colspan="5" style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f1f5f9; color: #1e293b;">
                                    <i class="pi pi-check-circle" style="color: #10b981; margin-right: 4px;"></i>
                                    <span>SUBTOTAL CIERRE ##{st.cycleNum} (Salida ● #{st.exitDate}):</span>
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
                            </p:summaryRow>'''

if target_before_footer in dash and '<p:summaryRow>' not in dash:
    dash = dash.replace(target_before_footer, summary_row + '\n' + target_before_footer)
    print("Added p:summaryRow for closing cycle subtotals to dashboard.xhtml!")

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml updated with subtotal rows per closing cycle!")
