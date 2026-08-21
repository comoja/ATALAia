import re

dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Restore 'Distribución Estadística' tab before 'Indicador iMACD (Media)'
distribucion_tab = """            <p:tab title="Distribución Estadística">
                <div style="padding: 15px 15px 40px 15px; overflow-y: auto; height: 100%; box-sizing: border-box;">
                    <div style="display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-bottom: 15px;">
                        <div class="stat-box" style="padding: 8px 15px; border-radius: 8px; background-color: rgba(54, 162, 235, 0.1); border-left: 4px solid rgba(54, 162, 235, 1); display: flex; justify-content: space-between; align-items: center;">
                            <span style="font-weight: 600; font-size: 0.95em; color: #495057;">Norm. (0-1): #{dashboardBean.selectedPair}</span>
                            <span style="color: rgba(54, 162, 235, 1); font-weight: bold; font-size: 1.3em;">
                                <h:outputText value="#{dashboardBean.zScoreA}">
                                    <f:convertNumber pattern="#0.0000" />
                                </h:outputText>
                            </span>
                        </div>
                        <div class="stat-box" style="padding: 8px 15px; border-radius: 8px; background-color: rgba(255, 159, 64, 0.1); border-left: 4px solid rgba(255, 159, 64, 1); display: flex; justify-content: space-between; align-items: center;">
                            <span style="font-weight: 600; font-size: 0.95em; color: #495057;">Norm. (0-1): #{dashboardBean.selectedPair2}</span>
                            <span style="color: rgba(255, 159, 64, 1); font-weight: bold; font-size: 1.3em;">
                                <h:outputText value="#{dashboardBean.zScoreB}">
                                    <f:convertNumber pattern="#0.0000" />
                                </h:outputText>
                            </span>
                        </div>
                        <div class="stat-box" style="padding: 8px 15px; border-radius: 8px; background-color: rgba(255, 99, 132, 0.1); border-left: 4px solid rgba(255, 99, 132, 1); display: flex; justify-content: space-between; align-items: center;">
                            <span style="font-weight: 600; font-size: 0.95em; color: #495057;">Diferencia Norm. (|A - B|)</span>
                            <span style="color: rgba(255, 99, 132, 1); font-weight: bold; font-size: 1.3em;">
                                <h:outputText value="#{dashboardBean.zScoreDiff}">
                                    <f:convertNumber pattern="#0.0000" />
                                </h:outputText>
                            </span>
                        </div>
                    </div>

                    <div class="card" style="margin-bottom: 25px;">
                        <h3 style="margin-bottom: 5px;">Histograma de Frecuencia por Rangos de Diferencia (|Z_A - Z_B|)</h3>
                        <div style="display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 15px;">
                            <p style="color: #6c757d; font-size: 0.9em; margin: 0;">
                                * La <span style="color: rgba(255, 99, 132, 1); font-weight: bold;">barra roja</span> marca el rango de la diferencia actual entre los precios normalizados.
                            </p>
                            <div style="display: flex; align-items: center; gap: 10px;">
                                <div class="control-label" style="margin-bottom: 0;">Número de Barras:</div>
                                <p:spinner id="histogramBins" value="#{dashboardBean.histogramBins}" min="5" max="200" styleClass="aether-input" style="width: 80px;" autocomplete="off">
                                    <p:ajax process="@this" update=":aetherForm:growl" />
                                </p:spinner>
                                <p:commandButton value="Aplicar" action="#{dashboardBean.analyzePair}" update=":aetherForm" styleClass="ui-button-outlined" style="padding: 2px 8px; font-size: 0.9em;"/>
                            </div>
                        </div>
                        <div style="width: 100%; height: 350px;">
                            <p:barChart model="#{dashboardBean.histogramModel}" style="width: 100%; height: 350px;" />
                        </div>
                    </div>

                    <div class="card">
                        <h3>Campana de Gauss (Distribución Teórica Z-Score)</h3>
                        <div style="width: 100%; height: 320px;">
                            <p:lineChart model="#{dashboardBean.gaussianModel}" style="width: 100%; height: 320px;" />
                        </div>
                    </div>
                </div>
            </p:tab>
"""

if '<p:tab title="Distribución Estadística">' not in content:
    idx_macd = content.find('<p:tab title="Indicador iMACD (Media)">')
    if idx_macd != -1:
        content = content[:idx_macd] + distribucion_tab + '\n            ' + content[idx_macd:]
        print("Restored Distribución Estadística tab!")

# 2. Restore Standard Deviation calculation in JS
std_calc_code = """            // -------------------------------------------------------------
            // DESVIACIÓN ESTÁNDAR DE PRECIOS NORMALIZADOS
            // (Par A - Azules / Par B - Naranjas)
            // Se calcula la Media (μ) y Desviación Estándar (σ) de cada serie normalizada
            // Banda Superior (+1σ) y Banda Inferior (-1σ)
            // -------------------------------------------------------------
            const validNormA = priceASeries.map(p => p.y).filter(y => y !== null);
            const validNormB = priceBSeries.map(p => p.y).filter(y => y !== null);

            // Par A
            let meanNormA = null, stdNormA = null, stdAboveA = null, stdBelowA = null;
            let rawStdAboveA = null, rawStdBelowA = null;
            if (validNormA.length > 0) {
                meanNormA = validNormA.reduce((a, b) => a + b, 0) / validNormA.length;
                const varianceA = validNormA.reduce((a, b) => a + Math.pow(b - meanNormA, 2), 0) / validNormA.length;
                stdNormA = Math.sqrt(varianceA);
                stdAboveA = meanNormA + stdNormA;
                stdBelowA = meanNormA - stdNormA;
                rawStdAboveA = minA + (stdAboveA * rangeA);
                rawStdBelowA = minA + (stdBelowA * rangeA);
            }

            // Par B
            let meanNormB = null, stdNormB = null, stdAboveB = null, stdBelowB = null;
            let rawStdAboveB = null, rawStdBelowB = null;
            if (validNormB.length > 0) {
                meanNormB = validNormB.reduce((a, b) => a + b, 0) / validNormB.length;
                const varianceB = validNormB.reduce((a, b) => a + Math.pow(b - meanNormB, 2), 0) / validNormB.length;
                stdNormB = Math.sqrt(varianceB);
                stdAboveB = meanNormB + stdNormB;
                stdBelowB = meanNormB - stdNormB;
                rawStdAboveB = minB + (stdAboveB * rangeB);
                rawStdBelowB = minB + (stdBelowB * rangeB);
            }

            // Series de líneas horizontales continuas para ambos pares (+1σ y -1σ)
            const numStdAboveSeries = rawData.map((d, i) => ({ x: dates[i], y: stdAboveA }));
            const numStdBelowSeries = rawData.map((d, i) => ({ x: dates[i], y: stdBelowA }));
            const denStdAboveSeries = rawData.map((d, i) => ({ x: dates[i], y: stdAboveB }));
            const denStdBelowSeries = rawData.map((d, i) => ({ x: dates[i], y: stdBelowB }));"""

if 'numStdAboveSeries' not in content:
    content = content.replace('// (Desviación estándar removida)', std_calc_code)
    print("Restored std dev JS calculations!")

# 2.1 Restore std dev condition in cross points detection
content = content.replace(
    'if (!isProj && i > 0) {\n                    const currP_A = priceASeries[i] ? priceASeries[i].y : null;',
    'if (!isProj && i > 0 && stdAboveA !== null && stdBelowA !== null && stdAboveB !== null && stdBelowB !== null) {\n                    const currP_A = priceASeries[i] ? priceASeries[i].y : null;'
)
content = content.replace(
    'const isCrossDownHigh_A = (prevP_A >= prevEma_A && currP_A < currEma_A);',
    'const isCrossDownHigh_A = (prevP_A >= prevEma_A && currP_A < currEma_A && currP_A >= stdAboveA);'
)
content = content.replace(
    'const isCrossUpLow_A = (prevP_A <= prevEma_A && currP_A > currEma_A);',
    'const isCrossUpLow_A = (prevP_A <= prevEma_A && currP_A > currEma_A && currP_A <= stdBelowA);'
)
content = content.replace(
    'const isCrossDownHigh_B = (prevP_B >= prevEma_B && currP_B < currEma_B);',
    'const isCrossDownHigh_B = (prevP_B >= prevEma_B && currP_B < currEma_B && currP_B >= stdAboveB);'
)
content = content.replace(
    'const isCrossUpLow_B = (prevP_B <= prevEma_B && currP_B > currEma_B);',
    'const isCrossUpLow_B = (prevP_B <= prevEma_B && currP_B > currEma_B && currP_B <= stdBelowB);'
)

# 2.2 Restore datasets in Chart.js
std_datasets = """                        { label: '+1σ ' + pairA + (rawStdAboveA !== null ? ' [Px: ' + rawStdAboveA.toFixed(5) + ' | Norm: ' + stdAboveA.toFixed(4) + ']' : ''), data: numStdAboveSeries, borderColor: '#38bdf8', fill: false, tension: 0, pointRadius: 0, borderWidth: 2, borderDash: [6, 4], spanGaps: true, yAxisID: 'y-price' },
                        { label: '-1σ ' + pairA + (rawStdBelowA !== null ? ' [Px: ' + rawStdBelowA.toFixed(5) + ' | Norm: ' + stdBelowA.toFixed(4) + ']' : ''), data: numStdBelowSeries, borderColor: '#60a5fa', fill: false, tension: 0, pointRadius: 0, borderWidth: 2, borderDash: [6, 4], spanGaps: true, yAxisID: 'y-price' },
                        { label: '+1σ ' + pairB + (rawStdAboveB !== null ? ' [Px: ' + rawStdAboveB.toFixed(5) + ' | Norm: ' + stdAboveB.toFixed(4) + ']' : ''), data: denStdAboveSeries, borderColor: '#fbbf24', fill: false, tension: 0, pointRadius: 0, borderWidth: 2, borderDash: [6, 4], spanGaps: true, yAxisID: 'y-price' },
                        { label: '-1σ ' + pairB + (rawStdBelowB !== null ? ' [Px: ' + rawStdBelowB.toFixed(5) + ' | Norm: ' + stdBelowB.toFixed(4) + ']' : ''), data: denStdBelowSeries, borderColor: '#f97316', fill: false, tension: 0, pointRadius: 0, borderWidth: 2, borderDash: [6, 4], spanGaps: true, yAxisID: 'y-price' },"""

target_ds = "{ label: 'EMA Lenta ' + emaSlowPeriod + ' ' + pairB, data: smaSlowBSeries, borderColor: '#a35700', fill: false, tension: 0.4, pointRadius: 0, borderWidth: 2, yAxisID: 'y-price', hidden: true },"
if 'numStdAboveSeries' in content and '+1σ' not in content:
    content = content.replace(target_ds, target_ds + '\n' + std_datasets)
    print("Restored std dev datasets in Chart.js!")

# 3. Implement Compact Tooltip:
# Lines 1 & 2 show Par A & Par B with color boxes and signal icons immediately next to them.
# Line 3 shows Media (with circle if cross).
# Footer shows Difference.
# NO 1σ in tooltip!

new_compact_tooltip = """                        tooltip: {
                            enabled: false,
                            mode: 'index',
                            intersect: false,
                            external: function(context) {
                                const chart = context.chart;
                                const tooltipModel = context.tooltip;
                                let tooltipEl = document.getElementById('chartjs-tooltip-box');
                                
                                if (!tooltipEl) {
                                    tooltipEl = document.createElement('div');
                                    tooltipEl.id = 'chartjs-tooltip-box';
                                    tooltipEl.style.position = 'absolute';
                                    tooltipEl.style.left = '20px';
                                    tooltipEl.style.bottom = '40px';
                                    tooltipEl.style.backgroundColor = 'rgba(255, 255, 255, 0.96)';
                                    tooltipEl.style.border = '1px solid rgba(200, 200, 200, 0.5)';
                                    tooltipEl.style.borderRadius = '8px';
                                    tooltipEl.style.padding = '12px 16px';
                                    tooltipEl.style.boxShadow = '0 4px 14px rgba(0,0,0,0.12)';
                                    tooltipEl.style.pointerEvents = 'none';
                                    tooltipEl.style.zIndex = '10';
                                    tooltipEl.style.minWidth = '240px';
                                    tooltipEl.innerHTML = '<table style="margin:0; border-collapse: collapse; width:100%;"></table>';
                                    
                                    const container = document.getElementById('chart-container');
                                    if (container) {
                                        container.style.position = 'relative';
                                        container.appendChild(tooltipEl);
                                    }
                                }

                                if (tooltipModel.opacity === 0) {
                                    tooltipEl.style.opacity = 0;
                                    return;
                                }

                                const dataPoints = tooltipModel.dataPoints || [];
                                const titleLines = tooltipModel.title || [];
                                if (dataPoints.length === 0) return;

                                const index = dataPoints[0].dataIndex;
                                const rawItem = rawData[index];

                                let innerHtml = '<thead>';
                                titleLines.forEach(function(title) {
                                    innerHtml += '<tr><th colspan="2" style="text-align:left; border-bottom:1px solid #e2e8f0; padding-bottom:6px; font-family:\\'Inter\\',sans-serif; color:#1e293b; font-size:13px; font-weight:700;">' + title + '</th></tr>';
                                });
                                innerHtml += '</thead><tbody>';

                                // 1. Par A (Numerador)
                                const normValA = (priceASeries[index] && priceASeries[index].y !== null) ? priceASeries[index].y : null;
                                const realPxA = (rawItem && rawItem.priceA !== null && rawItem.priceA !== undefined) ? rawItem.priceA.toFixed(5) : 'N/A';
                                const crossA = crossPointsASeries[index];
                                
                                const parBoxA = '<span style="background:#007bff; border:1px solid #007bff; display:inline-block; width:10px; height:10px; margin-right:4px; border-radius:2px; vertical-align:middle;" title="Par ' + pairA + '"></span>';
                                
                                let signalTagA = '';
                                if (crossA && crossA.crossType && crossA.y !== null) {
                                    const isCoinc = crossA.crossType.includes('coincident');
                                    const isVenta = crossA.crossType.startsWith('high_cross_down');
                                    const sigColor = isVenta ? '#ef4444' : '#22c55e';
                                    const sigName = isVenta ? 'Venta' : 'Compra';
                                    
                                    let shapeIcon = '';
                                    if (isCoinc) {
                                        if (isVenta) {
                                            shapeIcon = '<span style="display:inline-block; width:0; height:0; border-left:5px solid transparent; border-right:5px solid transparent; border-top:7px solid #ef4444; margin-right:5px; vertical-align:middle;" title="Venta Coincidente"></span>';
                                        } else {
                                            shapeIcon = '<span style="display:inline-block; width:0; height:0; border-left:5px solid transparent; border-right:5px solid transparent; border-bottom:7px solid #22c55e; margin-right:5px; vertical-align:middle;" title="Compra Coincidente"></span>';
                                        }
                                    } else {
                                        shapeIcon = '<span style="background:' + sigColor + '; border:1px solid ' + sigColor + '; display:inline-block; width:9px; height:9px; margin-right:5px; border-radius:2px; vertical-align:middle;" title="' + sigName + '"></span>';
                                    }
                                    signalTagA = shapeIcon + '<span style="color:' + sigColor + '; font-weight:700; margin-left:2px;">[' + sigName + (isCoinc ? ' ▲' : ' ■') + ']</span>';
                                }

                                if (normValA !== null) {
                                    innerHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#334155;">' +
                                                 parBoxA + (signalTagA ? signalTagA + ' ' : '') + '<strong style="color:#007bff;">' + pairA + '</strong>: ' + realPxA + ' <span style="color:#64748b; font-size:11px;">(' + normValA.toFixed(4) + ')</span></td></tr>';
                                }

                                // 2. Par B (Denominador)
                                const normValB = (priceBSeries[index] && priceBSeries[index].y !== null) ? priceBSeries[index].y : null;
                                const realPxB = (rawItem && rawItem.priceB !== null && rawItem.priceB !== undefined) ? rawItem.priceB.toFixed(5) : 'N/A';
                                const crossB = crossPointsBSeries[index];
                                
                                const parBoxB = '<span style="background:#ff8c00; border:1px solid #ff8c00; display:inline-block; width:10px; height:10px; margin-right:4px; border-radius:2px; vertical-align:middle;" title="Par ' + pairB + '"></span>';
                                
                                let signalTagB = '';
                                if (crossB && crossB.crossType && crossB.y !== null) {
                                    const isCoinc = crossB.crossType.includes('coincident');
                                    const isVenta = crossB.crossType.startsWith('high_cross_down');
                                    const sigColor = isVenta ? '#ef4444' : '#22c55e';
                                    const sigName = isVenta ? 'Venta' : 'Compra';
                                    
                                    let shapeIcon = '';
                                    if (isCoinc) {
                                        if (isVenta) {
                                            shapeIcon = '<span style="display:inline-block; width:0; height:0; border-left:5px solid transparent; border-right:5px solid transparent; border-top:7px solid #ef4444; margin-right:5px; vertical-align:middle;" title="Venta Coincidente"></span>';
                                        } else {
                                            shapeIcon = '<span style="display:inline-block; width:0; height:0; border-left:5px solid transparent; border-right:5px solid transparent; border-bottom:7px solid #22c55e; margin-right:5px; vertical-align:middle;" title="Compra Coincidente"></span>';
                                        }
                                    } else {
                                        shapeIcon = '<span style="background:' + sigColor + '; border:1px solid ' + sigColor + '; display:inline-block; width:9px; height:9px; margin-right:5px; border-radius:2px; vertical-align:middle;" title="' + sigName + '"></span>';
                                    }
                                    signalTagB = shapeIcon + '<span style="color:' + sigColor + '; font-weight:700; margin-left:2px;">[' + sigName + (isCoinc ? ' ▲' : ' ■') + ']</span>';
                                }

                                if (normValB !== null) {
                                    innerHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#334155;">' +
                                                 parBoxB + (signalTagB ? signalTagB + ' ' : '') + '<strong style="color:#ff8c00;">' + pairB + '</strong>: ' + realPxB + ' <span style="color:#64748b; font-size:11px;">(' + normValB.toFixed(4) + ')</span></td></tr>';
                                }

                                // 3. Media
                                const normMean = (priceMeanSeries[index] && priceMeanSeries[index].y !== null) ? priceMeanSeries[index].y : null;
                                const isMeanCross = (meanCrossSeries[index] && meanCrossSeries[index].y !== null);
                                
                                if (normMean !== null) {
                                    const meanBox = '<span style="background:#000000; border:1px solid #000000; display:inline-block; width:10px; height:10px; margin-right:4px; border-radius:2px; vertical-align:middle;"></span>';
                                    let meanCrossTag = '';
                                    if (isMeanCross) {
                                        const redCircle = '<span style="background:#ef4444; border:1px solid #ef4444; display:inline-block; width:9px; height:9px; margin-right:4px; border-radius:50%; vertical-align:middle;" title="Cruce con Media"></span>';
                                        meanCrossTag = ' ' + redCircle + '<span style="color:#ef4444; font-weight:700;">[🔴 Cruce Media]</span>';
                                    }
                                    innerHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#334155;">' +
                                                 meanBox + '<strong>Media (0-1)</strong>: ' + normMean.toFixed(4) + meanCrossTag + '</td></tr>';
                                }

                                // Footer: Diferencia
                                if (normValA !== null && normValB !== null) {
                                    const diff = Math.abs(normValA - normValB);
                                    innerHtml += '</tbody><tfoot><tr><td style="padding-top:8px; border-top:1px solid #e2e8f0; margin-top:6px; display:block; font-family:\\'Inter\\',sans-serif; font-size:12px; font-weight:700; color:#0284c7;">' +
                                                 'Diferencia (|A - B|): ' + diff.toFixed(4) + '</td></tr></tfoot>';
                                } else {
                                    innerHtml += '</tbody>';
                                }

                                let tableRoot = tooltipEl.querySelector('table');
                                tableRoot.innerHTML = innerHtml;
                                tooltipEl.style.opacity = 1;
                            }
                        }"""

idx_tt_start = content.find('tooltip: {')
idx_tt_end = content.find('annotation: {', idx_tt_start)

if idx_tt_start != -1 and idx_tt_end != -1:
    content = content[:idx_tt_start] + new_compact_tooltip + ',\n                        ' + content[idx_tt_end:]
    print("Compact tooltip applied!")

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("dashboard.xhtml saved successfully!")

# 4. Re-enable Gauss and Histogram in DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    bcontent = f.read()

bcontent = bcontent.replace(
    '// createGaussianModel(statsNode.get("bellCurve"));',
    'createGaussianModel(statsNode.get("bellCurve"));'
)
bcontent = bcontent.replace(
    '// createHistogramModel(statsNode.get("histogram"));',
    'createHistogramModel(statsNode.get("histogram"));'
)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(bcontent)

print("DashboardBean.java re-enabled Gauss/Histogram models!")
