import re

dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Add hidden inputs for quant data
hidden_target = '<h:inputHidden id="activePairB" value="#{dashboardBean.selectedPair2}" />'
hidden_replacement = '''<h:inputHidden id="activePairB" value="#{dashboardBean.selectedPair2}" />
        <h:inputHidden id="quantTimeSeriesJsonData" value="#{dashboardBean.quantTimeSeriesJson}" />
        <h:inputHidden id="equityCurveJsonData" value="#{dashboardBean.equityCurveJson}" />'''

if hidden_target in dash and 'quantTimeSeriesJsonData' not in dash:
    dash = dash.replace(hidden_target, hidden_replacement)
    print("Added quant hidden inputs to dashboard.xhtml")

# 2. Add Tab "Análisis Cuantitativo & Backtest" before </p:tabView>
quant_tab = '''
            <!-- PESTAÑA 4: ANÁLISIS CUANTITATIVO Y BACKTESTING -->
            <p:tab title="Análisis Cuantitativo &amp; Backtest">
                <div style="padding: 15px 15px 40px 15px; overflow-y: auto; height: 100%; box-sizing: border-box;">
                    
                    <!-- Tarjetas de Métricas Cuantitativas -->
                    <div style="display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin-bottom: 16px;">
                        
                        <!-- 1. Ornstein-Uhlenbeck Half-Life -->
                        <div class="stat-box" style="padding: 10px 14px; border-radius: 8px; background: rgba(56, 189, 248, 0.08); border-left: 4px solid #38bdf8; display: flex; flex-direction: column; justify-content: space-between;">
                            <span style="font-weight: 600; font-size: 1.05rem; color: #475569;">Vida Media (O-U)</span>
                            <span style="color: #0284c7; font-weight: 800; font-size: 1.4rem; margin: 4px 0;">#{dashboardBean.quantMetrics.halfLifeDescription}</span>
                            <span style="font-size: 0.95rem; color: #64748b;">λ = #{dashboardBean.quantMetrics.reversionSpeed} (p=#{dashboardBean.quantMetrics.PValue})</span>
                        </div>

                        <!-- 2. FFT Spectral Analysis -->
                        <div class="stat-box" style="padding: 10px 14px; border-radius: 8px; background: rgba(168, 85, 247, 0.08); border-left: 4px solid #a855f7; display: flex; flex-direction: column; justify-content: space-between;">
                            <span style="font-weight: 600; font-size: 1.05rem; color: #475569;">Ciclo FFT Dominante</span>
                            <span style="color: #7e22ce; font-weight: 800; font-size: 1.4rem; margin: 4px 0;">#{dashboardBean.quantMetrics.dominantPeriod} velas</span>
                            <span style="font-size: 0.95rem; color: #64748b;">Próx. cruce: #{dashboardBean.quantMetrics.periodsToMeanCross} periodos</span>
                        </div>

                        <!-- 3. Fair Value Gaps (FVGs) -->
                        <div class="stat-box" style="padding: 10px 14px; border-radius: 8px; background: rgba(245, 158, 11, 0.08); border-left: 4px solid #f59e0b; display: flex; flex-direction: column; justify-content: space-between;">
                            <span style="font-weight: 600; font-size: 1.05rem; color: #475569;">Zonas FVG Activas</span>
                            <span style="color: #b45309; font-weight: 800; font-size: 1.4rem; margin: 4px 0;">#{dashboardBean.quantMetrics.activeFvgs} Zonas</span>
                            <span style="font-size: 0.95rem; color: #64748b;">Total detectadas: #{dashboardBean.quantMetrics.totalFvgs}</span>
                        </div>

                        <!-- 4. Win Rate & Profit Factor -->
                        <div class="stat-box" style="padding: 10px 14px; border-radius: 8px; background: rgba(16, 185, 129, 0.08); border-left: 4px solid #10b981; display: flex; flex-direction: column; justify-content: space-between;">
                            <span style="font-weight: 600; font-size: 1.05rem; color: #475569;">Win Rate Backtest</span>
                            <span style="color: #047857; font-weight: 800; font-size: 1.4rem; margin: 4px 0;">#{dashboardBean.quantMetrics.winRate}%</span>
                            <span style="font-size: 0.95rem; color: #64748b;">Profit Factor: #{dashboardBean.quantMetrics.profitFactor}</span>
                        </div>

                        <!-- 5. Sharpe Ratio & Max Drawdown -->
                        <div class="stat-box" style="padding: 10px 14px; border-radius: 8px; background: rgba(239, 68, 68, 0.08); border-left: 4px solid #ef4444; display: flex; flex-direction: column; justify-content: space-between;">
                            <span style="font-weight: 600; font-size: 1.05rem; color: #475569;">Ratio de Sharpe</span>
                            <span style="color: #{dashboardBean.quantMetrics.sharpeRatio >= 0 ? '#10b981' : '#ef4444'}; font-weight: 800; font-size: 1.4rem; margin: 4px 0;">#{dashboardBean.quantMetrics.sharpeRatio}</span>
                            <span style="font-size: 0.95rem; color: #64748b;">Max DD: -#{dashboardBean.quantMetrics.maxDrawdown}%</span>
                        </div>

                        <!-- 6. Retorno Acumulado -->
                        <div class="stat-box" style="padding: 10px 14px; border-radius: 8px; background: rgba(99, 102, 241, 0.08); border-left: 4px solid #6366f1; display: flex; flex-direction: column; justify-content: space-between;">
                            <span style="font-weight: 600; font-size: 1.05rem; color: #475569;">Retorno Total</span>
                            <span style="color: #{dashboardBean.quantMetrics.totalReturnPct >= 0 ? '#10b981' : '#ef4444'}; font-weight: 800; font-size: 1.4rem; margin: 4px 0;">#{dashboardBean.quantMetrics.totalReturnPct}%</span>
                            <span style="font-size: 0.95rem; color: #64748b;">Trades: #{dashboardBean.quantMetrics.totalTrades}</span>
                        </div>
                    </div>

                    <!-- Panel de Gráfico de Curva de Equidad -->
                    <div style="background: #ffffff; border-radius: 8px; border: 1px solid #e2e8f0; padding: 14px; margin-bottom: 16px; box-shadow: 0 2px 6px rgba(0,0,0,0.03);">
                        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                            <span style="font-size: 1.25rem; font-weight: 700; color: #1e293b;">
                                <i class="pi pi-chart-line" style="margin-right: 6px; color: #6366f1;"></i>Curva de Equidad Simulada (Pair Trading Backtest)
                            </span>
                            <span style="font-size: 1.05rem; color: #64748b;">Capital Inicial: $10,000 USD | Costos: 3 bps</span>
                        </div>
                        <div style="height: 260px; width: 100%; position: relative;">
                            <canvas id="equityChart"></canvas>
                        </div>
                    </div>

                    <!-- Grid de Tablas: Historial de Trades & Zonas FVG -->
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 16px;">
                        
                        <!-- Tabla 1: Trades del Backtest -->
                        <div style="background: #ffffff; border-radius: 8px; border: 1px solid #e2e8f0; padding: 12px;">
                            <span style="font-size: 1.2rem; font-weight: 700; color: #1e293b; display: block; margin-bottom: 8px;">
                                <i class="pi pi-list" style="margin-right: 6px; color: #10b981;"></i>Historial de Trades Simulados
                            </span>
                            <p:dataTable value="#{dashboardBean.quantTradesList}" var="t" scrollable="true" scrollHeight="220px" styleClass="aether-table-compact" emptyMessage="Sin trades ejecutados">
                                <p:column headerText="#" style="width: 28px; text-align: center;">
                                    <h:outputText value="#{t.tradeNum}" />
                                </p:column>
                                <p:column headerText="Tipo" style="width: 80px;">
                                    <span class="badge" style="padding: 2px 6px; border-radius: 4px; font-weight: 700; font-size: 1.0rem; background: #{t.type == 'LARGO_RATIO' ? '#dcfce7' : '#fee2e2'}; color: #{t.type == 'LARGO_RATIO' ? '#15803d' : '#b91c1c'};">
                                        #{t.type == 'LARGO_RATIO' ? 'COMPRA' : 'VENTA'}
                                    </span>
                                </p:column>
                                <p:column headerText="Entrada" style="font-size: 1.05rem;">
                                    <h:outputText value="#{t.entryDate}" />
                                </p:column>
                                <p:column headerText="Salida" style="font-size: 1.05rem;">
                                    <h:outputText value="#{t.exitDate}" />
                                </p:column>
                                <p:column headerText="Retorno" style="width: 75px; text-align: right;">
                                    <span style="font-weight: 700; color: #{t.isWin ? '#10b981' : '#ef4444'};">
                                        #{t.returnPct > 0 ? '+' : ''}#{t.returnPct}%
                                    </span>
                                </p:column>
                                <p:column headerText="PnL ($)" style="width: 85px; text-align: right; font-weight: 700;">
                                    <span style="color: #{t.isWin ? '#10b981' : '#ef4444'};">
                                        $#{t.pnl}
                                    </span>
                                </p:column>
                            </p:dataTable>
                        </div>

                        <!-- Tabla 2: Zonas Fair Value Gap (FVG) -->
                        <div style="background: #ffffff; border-radius: 8px; border: 1px solid #e2e8f0; padding: 12px;">
                            <span style="font-size: 1.2rem; font-weight: 700; color: #1e293b; display: block; margin-bottom: 8px;">
                                <i class="pi pi-shield" style="margin-right: 6px; color: #f59e0b;"></i>Zonas Fair Value Gap (FVG) del Ratio
                            </span>
                            <p:dataTable value="#{dashboardBean.quantFvgsList}" var="f" scrollable="true" scrollHeight="220px" styleClass="aether-table-compact" emptyMessage="Sin zonas FVG detectadas">
                                <p:column headerText="Fecha" style="font-size: 1.05rem;">
                                    <h:outputText value="#{f.datetime}" />
                                </p:column>
                                <p:column headerText="Tipo" style="width: 95px;">
                                    <span class="badge" style="padding: 2px 6px; border-radius: 4px; font-weight: 700; font-size: 1.0rem; background: #{f.type == 'BULLISH_FVG' ? '#dbeafe' : '#fef3c7'}; color: #{f.type == 'BULLISH_FVG' ? '#1d4ed8' : '#b45309'};">
                                        #{f.type == 'BULLISH_FVG' ? 'Alcista (BISY)' : 'Bajista (SIBI)'}
                                    </span>
                                </p:column>
                                <p:column headerText="Rango FVG" style="font-size: 1.05rem;">
                                    <h:outputText value="[#{f.bottom} - #{f.top}]" />
                                </p:column>
                                <p:column headerText="Gap" style="width: 65px; text-align: right;">
                                    <h:outputText value="#{f.gapSize}" />
                                </p:column>
                                <p:column headerText="Estado" style="width: 80px; text-align: center;">
                                    <span style="font-weight: 700; font-size: 1.0rem; color: #{f.mitigated ? '#94a3b8' : '#10b981'};">
                                        #{f.mitigated ? 'Mitigado' : 'ACTIVO'}
                                    </span>
                                </p:column>
                            </p:dataTable>
                        </div>
                    </div>
                </div>
            </p:tab>
'''

if 'title="Análisis Cuantitativo &amp; Backtest"' not in dash:
    dash = dash.replace('</p:tabView>', quant_tab + '\n        </p:tabView>')
    print("Added Quantitative Analysis & Backtest Tab to dashboard.xhtml!")

# 3. Add Equity Curve Chart rendering logic in JS
js_equity_renderer = '''
        function renderEquityChart() {
            const eqCanvas = document.getElementById('equityChart');
            if (!eqCanvas) return;
            
            const eqDataStr = document.getElementById('aetherForm:equityCurveJsonData')?.value || '[]';
            let eqData = [];
            try {
                eqData = JSON.parse(eqDataStr);
            } catch(e) {
                console.warn("Error parsing equity curve data", e);
            }
            
            const ctx = eqCanvas.getContext('2d');
            const dates = eqData.map(d => d.x);
            const values = eqData.map(d => d.y);
            
            const gradient = ctx.createLinearGradient(0, 0, 0, 260);
            gradient.addColorStop(0, 'rgba(99, 102, 241, 0.25)');
            gradient.addColorStop(1, 'rgba(99, 102, 241, 0.01)');
            
            try {
                if (window.aetherEquityChart) {
                    window.aetherEquityChart.destroy();
                }
            } catch(e){}
            
            window.aetherEquityChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: dates,
                    datasets: [{
                        label: 'Curva de Equidad ($ USD)',
                        data: values,
                        borderColor: '#6366f1',
                        backgroundColor: gradient,
                        borderWidth: 2.5,
                        fill: true,
                        tension: 0.2,
                        pointRadius: 0,
                        pointHoverRadius: 5
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
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
                        legend: { display: false },
                        tooltip: {
                            mode: 'index',
                            intersect: false,
                            callbacks: {
                                label: function(ctx) { return ' Capital: $' + Number(ctx.raw).toFixed(2); }
                            }
                        }
                    }
                }
            });
        }
'''

if 'function renderEquityChart()' not in dash:
    dash = dash.replace('function renderAetherChart() {', js_equity_renderer + '\n        function renderAetherChart() {')
    # Call renderEquityChart inside renderAetherChart
    dash = dash.replace('window.aetherChart = new Chart(ctx, {', 'renderEquityChart();\n            window.aetherChart = new Chart(ctx, {')
    print("Added renderEquityChart JS logic to dashboard.xhtml!")

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml updated with complete quantitative UI and chart integration!")
