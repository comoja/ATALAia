dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Update renderEquityChart to be 100% resilient and fail-safe
old_eq_func = '''        function renderEquityChart() {
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
        }'''

new_eq_func = '''        function renderEquityChart() {
            try {
                const eqCanvas = document.getElementById('equityChart');
                if (!eqCanvas) return;
                
                const eqInput = document.querySelector("[id$='equityCurveJsonData']");
                const eqDataStr = eqInput ? eqInput.value : '[]';
                let eqData = [];
                try {
                    eqData = JSON.parse(eqDataStr);
                } catch(e) {
                    console.warn("Error parsing equity curve data", e);
                }
                
                if (!eqData || eqData.length === 0) return;

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
            } catch(err) {
                console.warn("renderEquityChart error ignored:", err);
            }
        }'''

if old_eq_func in dash:
    dash = dash.replace(old_eq_func, new_eq_func)
    print("Replaced renderEquityChart with bulletproof implementation!")

# 2. Fix trade mapping selector to use document.querySelector("[id$='quantTradesJsonData']")
old_trades_sel = "const tradesRawStr = document.getElementById('aetherForm:quantTradesJsonData')?.value || '[]';"
new_trades_sel = "const tradesInput = document.querySelector(\"[id$='quantTradesJsonData']\");\n            const tradesRawStr = tradesInput ? tradesInput.value : '[]';"
dash = dash.replace(old_trades_sel, new_trades_sel)

# 3. Call renderEquityChart AFTER window.aetherChart is created (not before!)
dash = dash.replace('renderEquityChart();\n            window.aetherChart = new Chart(ctx, {', 'window.aetherChart = new Chart(ctx, {')

if 'setTimeout(renderEquityChart, 200);' not in dash:
    dash = dash.replace('console.log("ATALAia Chart (Chart.js) Rendered Successfully.");', 'console.log("ATALAia Chart (Chart.js) Rendered Successfully.");\n            setTimeout(renderEquityChart, 200);')
    print("Moved renderEquityChart after main chart creation!")

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml chart rendering fixed successfully!")
