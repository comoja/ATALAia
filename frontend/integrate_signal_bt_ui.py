dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Add hidden input
target_hidden = '<h:inputHidden id="quantTradesJsonData" value="#{dashboardBean.quantTradesJson}" />'
replacement_hidden = target_hidden + '\n        <h:inputHidden id="signalBtComparisonCurveJsonData" value="#{dashboardBean.signalBtComparisonCurveJson}" />'
if 'signalBtComparisonCurveJsonData' not in dash:
    dash = dash.replace(target_hidden, replacement_hidden)
    print("Added signalBtComparisonCurveJsonData hidden input!")

# 2. Update onTabShow in p:tabView
dash = dash.replace(
    'onTabShow="if (index === 0) renderAetherChart(); if (index === 3) setTimeout(renderEquityChart, 50);"',
    'onTabShow="if (index === 0) renderAetherChart(); if (index === 3) setTimeout(renderEquityChart, 50); if (index === 4) setTimeout(renderSignalComparisonChart, 50);"'
)

# 3. Add Tab 5: Backtest de Señales Gráficas
new_tab_content = '''
            <!-- PESTAÑA 5: BACKTEST DE SEÑALES GRÁFICAS (TRIÁNGULOS & CUADROS) -->
            <p:tab title="Backtest de Señales (Triángulos / Cuadros)">
                <div style="padding: 15px 15px 40px 15px; overflow-y: auto; height: 100%; box-sizing: border-box;">
                    
                    <!-- Tarjetas Comparativas de Estrategias -->
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-bottom: 16px;">
                        
                        <!-- Columna A: Solo Triángulos -->
                        <div style="background: #ffffff; border-radius: 8px; border: 1px solid #bae6fd; padding: 14px; box-shadow: 0 2px 6px rgba(0,0,0,0.03);">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; border-bottom: 1px solid #f1f5f9; padding-bottom: 8px;">
                                <span style="font-weight: 800; font-size: 1.25rem; color: #0369a1;">
                                    <i class="pi pi-caret-up" style="margin-right: 4px; color: #0284c7;"></i><i class="pi pi-caret-down" style="margin-right: 6px; color: #0284c7;"></i>Modo 1: Solo Triángulos (Coincidentes)
                                </span>
                                <span style="background: #e0f2fe; color: #0369a1; padding: 3px 8px; border-radius: 12px; font-weight: 700; font-size: 1.0rem;">
                                    Alta Confluencia
                                </span>
                            </div>
                            
                            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;">
                                <div style="background: #f8fafc; border-radius: 6px; padding: 8px; text-align: center;">
                                    <span style="display: block; font-size: 0.95rem; color: #64748b; font-weight: 600;">Retorno Total</span>
                                    <span style="font-size: 1.3rem; font-weight: 800; color: #{dashboardBean.signalBtTrianglesMetrics.totalReturnPct >= 0 ? '#10b981' : '#ef4444'};">
                                        #{dashboardBean.signalBtTrianglesMetrics.totalReturnPct}%
                                    </span>
                                    <span style="display: block; font-size: 0.9rem; color: #64748b;">+$#{dashboardBean.signalBtTrianglesMetrics.netProfit}</span>
                                </div>
                                <div style="background: #f8fafc; border-radius: 6px; padding: 8px; text-align: center;">
                                    <span style="display: block; font-size: 0.95rem; color: #64748b; font-weight: 600;">Win Rate</span>
                                    <span style="font-size: 1.3rem; font-weight: 800; color: #0284c7;">
                                        #{dashboardBean.signalBtTrianglesMetrics.winRate}%
                                    </span>
                                    <span style="display: block; font-size: 0.9rem; color: #64748b;">#{dashboardBean.signalBtTrianglesMetrics.winningTrades}/#{dashboardBean.signalBtTrianglesMetrics.totalTrades} Trades</span>
                                </div>
                                <div style="background: #f8fafc; border-radius: 6px; padding: 8px; text-align: center;">
                                    <span style="display: block; font-size: 0.95rem; color: #64748b; font-weight: 600;">Profit Factor</span>
                                    <span style="font-size: 1.3rem; font-weight: 800; color: #334155;">
                                        #{dashboardBean.signalBtTrianglesMetrics.profitFactor}
                                    </span>
                                    <span style="display: block; font-size: 0.9rem; color: #64748b;">Sharpe: #{dashboardBean.signalBtTrianglesMetrics.sharpeRatio}</span>
                                </div>
                                <div style="background: #f8fafc; border-radius: 6px; padding: 8px; text-align: center;">
                                    <span style="display: block; font-size: 0.95rem; color: #64748b; font-weight: 600;">Max Drawdown</span>
                                    <span style="font-size: 1.3rem; font-weight: 800; color: #ef4444;">
                                        -#{dashboardBean.signalBtTrianglesMetrics.maxDrawdown}%
                                    </span>
                                    <span style="display: block; font-size: 0.9rem; color: #64748b;">Salida en Círculo ●</span>
                                </div>
                            </div>
                        </div>

                        <!-- Columna B: Triángulos + Cuadros (Con Equidad) -->
                        <div style="background: #ffffff; border-radius: 8px; border: 1px solid #e9d5ff; padding: 14px; box-shadow: 0 2px 6px rgba(0,0,0,0.03);">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; border-bottom: 1px solid #f1f5f9; padding-bottom: 8px;">
                                <span style="font-weight: 800; font-size: 1.25rem; color: #7e22ce;">
                                    <i class="pi pi-th-large" style="margin-right: 6px; color: #a855f7;"></i>Modo 2: Triángulos + Cuadros (Con Equidad)
                                </span>
                                <span style="background: #f3e8ff; color: #7e22ce; padding: 3px 8px; border-radius: 12px; font-weight: 700; font-size: 1.0rem;">
                                    Frecuencia &amp; Equidad
                                </span>
                            </div>
                            
                            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px;">
                                <div style="background: #f8fafc; border-radius: 6px; padding: 8px; text-align: center;">
                                    <span style="display: block; font-size: 0.95rem; color: #64748b; font-weight: 600;">Retorno Total</span>
                                    <span style="font-size: 1.3rem; font-weight: 800; color: #{dashboardBean.signalBtCombinedMetrics.totalReturnPct >= 0 ? '#10b981' : '#ef4444'};">
                                        #{dashboardBean.signalBtCombinedMetrics.totalReturnPct}%
                                    </span>
                                    <span style="display: block; font-size: 0.9rem; color: #64748b;">+$#{dashboardBean.signalBtCombinedMetrics.netProfit}</span>
                                </div>
                                <div style="background: #f8fafc; border-radius: 6px; padding: 8px; text-align: center;">
                                    <span style="display: block; font-size: 0.95rem; color: #64748b; font-weight: 600;">Win Rate</span>
                                    <span style="font-size: 1.3rem; font-weight: 800; color: #7e22ce;">
                                        #{dashboardBean.signalBtCombinedMetrics.winRate}%
                                    </span>
                                    <span style="display: block; font-size: 0.9rem; color: #64748b;">#{dashboardBean.signalBtCombinedMetrics.winningTrades}/#{dashboardBean.signalBtCombinedMetrics.totalTrades} Trades</span>
                                </div>
                                <div style="background: #f8fafc; border-radius: 6px; padding: 8px; text-align: center;">
                                    <span style="display: block; font-size: 0.95rem; color: #64748b; font-weight: 600;">Profit Factor</span>
                                    <span style="font-size: 1.3rem; font-weight: 800; color: #334155;">
                                        #{dashboardBean.signalBtCombinedMetrics.profitFactor}
                                    </span>
                                    <span style="display: block; font-size: 0.9rem; color: #64748b;">Sharpe: #{dashboardBean.signalBtCombinedMetrics.sharpeRatio}</span>
                                </div>
                                <div style="background: #f8fafc; border-radius: 6px; padding: 8px; text-align: center;">
                                    <span style="display: block; font-size: 0.95rem; color: #64748b; font-weight: 600;">Max Drawdown</span>
                                    <span style="font-size: 1.3rem; font-weight: 800; color: #ef4444;">
                                        -#{dashboardBean.signalBtCombinedMetrics.maxDrawdown}%
                                    </span>
                                    <span style="display: block; font-size: 0.9rem; color: #64748b;">Salida en Círculo ●</span>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- Panel de Gráfico Comparativo de Curva de Equidad -->
                    <div style="background: #ffffff; border-radius: 8px; border: 1px solid #e2e8f0; padding: 14px; margin-bottom: 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.03);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                            <span style="font-size: 1.25rem; font-weight: 700; color: #1e293b;">
                                <i class="pi pi-chart-line" style="margin-right: 6px; color: #0284c7;"></i>Comparativa de Curvas de Equidad (Capital Inicial: $10,000 USD | Salida: Cruce Media ●)
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
                        </div>

                        <p:dataTable id="signalTradesTable" value="#{dashboardBean.signalBtTradesList}" var="st" scrollable="true" scrollHeight="260px" styleClass="aether-table-compact" emptyMessage="Sin operaciones registradas para esta estrategia">
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
                        </p:dataTable>
                    </div>

                </div>
            </p:tab>
'''

if 'title="Backtest de Señales (Triángulos / Cuadros)"' not in dash:
    dash = dash.replace('</p:tabView>', new_tab_content + '\n        </p:tabView>')
    print("Added Tab 5 (Backtest de Señales) to dashboard.xhtml!")

# 4. Add renderSignalComparisonChart in JavaScript
js_signal_comparison = '''
        function renderSignalComparisonChart() {
            try {
                const sigCanvas = document.getElementById('signalComparisonChart');
                if (!sigCanvas) return;

                const inputData = document.querySelector("[id$='signalBtComparisonCurveJsonData']");
                const rawStr = inputData ? inputData.value : '{}';
                let dataObj = {};
                try {
                    dataObj = JSON.parse(rawStr);
                } catch(e) {
                    console.warn("Error parsing signal backtest comparison data", e);
                }

                const toCurve = (dataObj.trianglesOnly && dataObj.trianglesOnly.equityCurve) ? dataObj.trianglesOnly.equityCurve : [];
                const cbCurve = (dataObj.combined && dataObj.combined.equityCurve) ? dataObj.combined.equityCurve : [];

                if (toCurve.length === 0 && cbCurve.length === 0) return;

                const baseCurve = (cbCurve.length > 0) ? cbCurve : toCurve;
                const dates = baseCurve.map(d => d.x);

                const toMap = {};
                toCurve.forEach(d => { toMap[d.x] = d.y; });
                const cbMap = {};
                cbCurve.forEach(d => { cbMap[d.x] = d.y; });

                const toValues = dates.map(d => toMap[d] !== undefined ? toMap[d] : 10000);
                const cbValues = dates.map(d => cbMap[d] !== undefined ? cbMap[d] : 10000);

                const ctx = sigCanvas.getContext('2d');
                
                try {
                    if (window.aetherSignalComparisonChart) {
                        window.aetherSignalComparisonChart.destroy();
                    }
                } catch(e){}

                window.aetherSignalComparisonChart = new Chart(ctx, {
                    type: 'line',
                    data: {
                        labels: dates,
                        datasets: [
                            {
                                label: 'Solo Triángulos Coincidentes',
                                data: toValues,
                                borderColor: '#0284c7',
                                backgroundColor: 'rgba(2, 132, 199, 0.08)',
                                borderWidth: 2.5,
                                fill: false,
                                tension: 0.2,
                                pointRadius: 0,
                                pointHoverRadius: 5
                            },
                            {
                                label: 'Triángulos + Cuadros (Con Equidad)',
                                data: cbValues,
                                borderColor: '#7e22ce',
                                backgroundColor: 'rgba(126, 34, 206, 0.08)',
                                borderWidth: 2.5,
                                fill: false,
                                tension: 0.2,
                                pointRadius: 0,
                                pointHoverRadius: 5
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        maintainAspectRatio: false,
                        interaction: { mode: 'index', intersect: false },
                        scales: {
                            x: {
                                type: 'time',
                                ticks: { maxTicksLimit: 10, color: '#64748b' },
                                grid: { color: 'rgba(226, 232, 240, 0.6)' }
                            },
                            y: {
                                ticks: {
                                    color: '#64748b',
                                    callback: function(val) { return '$' + val.toLocaleString(); }
                                },
                                grid: { color: 'rgba(226, 232, 240, 0.6)' }
                            }
                        },
                        plugins: {
                            legend: {
                                display: true,
                                position: 'top',
                                labels: {
                                    usePointStyle: true,
                                    font: { family: 'Inter, sans-serif', size: 11, weight: 600 }
                                }
                            },
                            tooltip: {
                                mode: 'index',
                                intersect: false,
                                callbacks: {
                                    label: function(ctx) { return ' ' + ctx.dataset.label + ': $' + Number(ctx.raw).toFixed(2); }
                                }
                            }
                        }
                    }
                });
            } catch(err) {
                console.warn("renderSignalComparisonChart error ignored:", err);
            }
        }
'''

if 'function renderSignalComparisonChart()' not in dash:
    dash = dash.replace('function renderEquityChart() {', js_signal_comparison + '\n        function renderEquityChart() {')
    print("Added renderSignalComparisonChart JS function to dashboard.xhtml!")

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml updated with complete Signal Backtest tab and comparison chart!")
