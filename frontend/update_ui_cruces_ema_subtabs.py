dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Replace Tab Title
old_tab_title = '<p:tab title="Backtest de Señales (Triángulos / Cuadros)">'
new_tab_title = '<p:tab title="Cruces EMA">'
dash = dash.replace(old_tab_title, new_tab_title)

# Replace the lower content (chart panel and table panel) with sub-tabview
old_chart_and_table_structure = '''                    <!-- Panel de Gráfico Comparativo de Curva de Equidad -->
                    <div style="background: #ffffff; border-radius: 8px; border: 1px solid #e2e8f0; padding: 14px; margin-bottom: 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.03);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                            <span style="font-size: 1.25rem; font-weight: 700; color: #1e293b;">
                                <i class="pi pi-chart-line" style="margin-right: 6px; color: #0284c7;"></i>Comparativa de Curvas de Equidad (Capital: cuenta.Capital | Asignación: 3% por Entrada | Reinversión al Cierre | Salida: Cruce Media ●)
                            </span>
                            <div style="display: flex; align-items: center; gap: 14px; font-size: 1.05rem; font-weight: 600;">
                                <span style="display: flex; align-items: center; gap: 4px; color: #0284c7;">
                                    <span style="width: 12px; height: 12px; background: #0284c7; border-radius: 2px; display: inline-block;"></span> Solo Triángulos
                                </span>
                                <span style="display: flex; align-items: center; gap: 4px; color: #7e22ce;">
                                    <span style="width: 12px; height: 12px; background: #7e22ce; border-radius: 2px; display: inline-block;"></span> Triángulos + Cuadros (Con Equidad)
                                </span>
                            </div>
                        </div>
                        <div style="height: 260px; width: 100%; position: relative;">
                            <canvas id="signalComparisonChart"></canvas>
                        </div>
                    </div>

                    <!-- Tabla de Trades con Selector de Estrategia -->
                    <div style="background: #ffffff; border-radius: 8px; border: 1px solid #e2e8f0; padding: 14px;">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                            <span style="font-size: 1.25rem; font-weight: 700; color: #1e293b;">
                                <i class="pi pi-list" style="margin-right: 6px; color: #10b981;"></i>Bitácora Detallada de Trades Ejecutados
                            </span>
                            <div style="display: flex; align-items: center; gap: 8px;">
                                <span style="font-size: 1.05rem; font-weight: 600; color: #64748b;">Mostrar Estrategia:</span>
                                <p:selectOneButton value="#{dashboardBean.signalBtStrategySelected}">
                                    <f:selectItem itemLabel="Triángulos + Cuadros" itemValue="COMBINED" />
                                    <f:selectItem itemLabel="Solo Triángulos" itemValue="TRIANGLES" />
                                    <p:ajax listener="#{dashboardBean.onSignalStrategyChange}" update="signalTradesTable" />
                                </p:selectOneButton>
                            </div>
                        </div>'''

new_chart_and_table_structure = '''                    <!-- Sub-Pestañas Internas de Cruces EMA -->
                    <p:tabView id="crucesEmaInnerTabs" onTabChange="setTimeout(function(){ renderSignalComparisonChart(); }, 150);">
                        
                        <!-- SUB-PESTAÑA 1: GRÁFICA DE CURVAS DE EQUIDAD -->
                        <p:tab title="Gráfica de Curva de Equidad">
                            <div style="background: #ffffff; border-radius: 8px; padding: 14px; box-shadow: 0 2px 6px rgba(0,0,0,0.03);">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; border-bottom: 1px solid #f1f5f9; padding-bottom: 8px;">
                                    <span style="font-size: 1.25rem; font-weight: 700; color: #1e293b;">
                                        <i class="pi pi-chart-line" style="margin-right: 6px; color: #0284c7;"></i>Comparativa de Curvas de Equidad (Capital: cuenta.Capital | Asignación: 3% por Entrada | Reinversión al Cierre | Salida: Cruce Media ●)
                                    </span>
                                    <div style="display: flex; align-items: center; gap: 14px; font-size: 1.05rem; font-weight: 600;">
                                        <span style="display: flex; align-items: center; gap: 4px; color: #0284c7;">
                                            <span style="width: 12px; height: 12px; background: #0284c7; border-radius: 2px; display: inline-block;"></span> Solo Triángulos
                                        </span>
                                        <span style="display: flex; align-items: center; gap: 4px; color: #7e22ce;">
                                            <span style="width: 12px; height: 12px; background: #7e22ce; border-radius: 2px; display: inline-block;"></span> Triángulos + Cuadros (Con Equidad)
                                        </span>
                                    </div>
                                </div>
                                <div style="height: 420px; width: 100%; position: relative;">
                                    <canvas id="signalComparisonChart"></canvas>
                                </div>
                            </div>
                        </p:tab>

                        <!-- SUB-PESTAÑA 2: BITÁCORA DETALLADA DE TRADES -->
                        <p:tab title="Bitácora Detallada de Trades">
                            <div style="background: #ffffff; border-radius: 8px; padding: 14px;">
                                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                                    <span style="font-size: 1.25rem; font-weight: 700; color: #1e293b;">
                                        <i class="pi pi-list" style="margin-right: 6px; color: #10b981;"></i>Bitácora Detallada de Trades Ejecutados
                                    </span>
                                    <div style="display: flex; align-items: center; gap: 8px;">
                                        <span style="font-size: 1.05rem; font-weight: 600; color: #64748b;">Mostrar Estrategia:</span>
                                        <p:selectOneButton value="#{dashboardBean.signalBtStrategySelected}">
                                            <f:selectItem itemLabel="Triángulos + Cuadros" itemValue="COMBINED" />
                                            <f:selectItem itemLabel="Solo Triángulos" itemValue="TRIANGLES" />
                                            <p:ajax listener="#{dashboardBean.onSignalStrategyChange}" update="signalTradesTable" />
                                        </p:selectOneButton>
                                    </div>
                                </div>'''

dash = dash.replace(old_chart_and_table_structure, new_chart_and_table_structure)

# Close the tab and inner tabview before </p:tab> of Cruces EMA
old_table_close = '''                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtCombinedMetrics.totalTrades} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.formattedSignalBtTotalPnl}" />
                                </p:row>
                            </p:columnGroup>
                        </p:dataTable>
                    </div>

                </div>
            </p:tab>'''

new_table_close = '''                            <p:columnGroup type="footer">
                                <p:row>
                                    <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.1rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
                                    <p:column footerText="" style="background: #f8fafc;" />
                                    <p:column style="text-align: center; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #64748b;" footerText="#{dashboardBean.signalBtCombinedMetrics.totalTrades} trades" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalReturn >= 0 ? '#10b981' : '#ef4444'};" footerText="#{dashboardBean.signalBtTotalReturn >= 0 ? '+' : ''}#{dashboardBean.signalBtTotalReturn}%" />
                                    <p:column style="text-align: right; font-weight: 900; font-size: 1.2rem; background: #f8fafc; color: #{dashboardBean.signalBtTotalPnl >= 0 ? '#10b981' : '#ef4444'};" footerText="$#{dashboardBean.formattedSignalBtTotalPnl}" />
                                </p:row>
                            </p:columnGroup>
                        </p:dataTable>
                            </div>
                        </p:tab>
                    </p:tabView>

                </div>
            </p:tab>'''

dash = dash.replace(old_table_close, new_table_close)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml: Pestaña renamed to 'Cruces EMA' with inner subtabs for Graph and Trades!")
