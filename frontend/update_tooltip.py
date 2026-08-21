import re

dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    content = f.read()

new_tooltip_code = """                        tooltip: {
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

                                let innerHtml = '<thead>';
                                titleLines.forEach(function(title) {
                                    innerHtml += '<tr><th colspan="2" style="text-align:left; border-bottom:1px solid #e2e8f0; padding-bottom:6px; font-family:\\'Inter\\',sans-serif; color:#1e293b; font-size:13px; font-weight:700;">' + title + '</th></tr>';
                                });
                                innerHtml += '</thead><tbody>';

                                let rowsHtml = '';
                                let normA = null;
                                let normB = null;

                                dataPoints.forEach(function(dp) {
                                    const dataset = dp.dataset;
                                    const label = dataset.label || '';
                                    const index = dp.dataIndex;
                                    const rawVal = dp.raw;
                                    const parsedY = (dp.parsed && dp.parsed.y !== null && dp.parsed.y !== undefined) ? dp.parsed.y : (rawVal && rawVal.y !== undefined ? rawVal.y : null);
                                    const rawItem = rawData[index];

                                    if (parsedY === null || parsedY === undefined) return;

                                    // 1. Norm Pair A (Azul #007bff)
                                    if (label.includes('Norm.') && label.includes(pairA)) {
                                        normA = parsedY;
                                        const realPx = (rawItem && rawItem.priceA !== null && rawItem.priceA !== undefined) ? rawItem.priceA.toFixed(5) : 'N/A';
                                        const colorBox = '<span style="background:#007bff; border:1px solid #007bff; display:inline-block; width:10px; height:10px; margin-right:8px; border-radius:2px; vertical-align:middle;"></span>';
                                        rowsHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#334155;">' +
                                                    colorBox + '<strong style="color:#007bff;">' + pairA + '</strong>: ' + realPx + ' <span style="color:#64748b; font-size:11px;">(' + parsedY.toFixed(4) + ')</span></td></tr>';
                                    }
                                    // 2. Norm Pair B (Naranja #ff8c00)
                                    else if (label.includes('Norm.') && label.includes(pairB)) {
                                        normB = parsedY;
                                        const realPx = (rawItem && rawItem.priceB !== null && rawItem.priceB !== undefined) ? rawItem.priceB.toFixed(5) : 'N/A';
                                        const colorBox = '<span style="background:#ff8c00; border:1px solid #ff8c00; display:inline-block; width:10px; height:10px; margin-right:8px; border-radius:2px; vertical-align:middle;"></span>';
                                        rowsHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#334155;">' +
                                                    colorBox + '<strong style="color:#ff8c00;">' + pairB + '</strong>: ' + realPx + ' <span style="color:#64748b; font-size:11px;">(' + parsedY.toFixed(4) + ')</span></td></tr>';
                                    }
                                    // 3. Puntos Cruce EMA Par A
                                    else if (label.includes('Puntos Cruce EMA') && label.includes(pairA)) {
                                        if (rawVal && rawVal.crossType) {
                                            const isCoincident = rawVal.crossType.includes('coincident');
                                            const isVenta = rawVal.crossType.startsWith('high_cross_down');
                                            const signalColor = isVenta ? '#ef4444' : '#22c55e';
                                            const signalText = isVenta ? 'Venta' : 'Compra';
                                            
                                            // Primer cuadro: Color del Par A (Azul #007bff)
                                            const parBox = '<span style="background:#007bff; border:1px solid #007bff; display:inline-block; width:10px; height:10px; margin-right:5px; border-radius:2px; vertical-align:middle;" title="Par ' + pairA + '"></span>';
                                            
                                            // Segundo icono: Triángulo si es coincidente, Cuadro si no lo es (con color verde/rojo)
                                            let shapeIcon = '';
                                            if (isCoincident) {
                                                if (isVenta) {
                                                    shapeIcon = '<span style="display:inline-block; width:0; height:0; border-left:5px solid transparent; border-right:5px solid transparent; border-top:7px solid #ef4444; margin-right:8px; vertical-align:middle;" title="Venta Coincidente"></span>';
                                                } else {
                                                    shapeIcon = '<span style="display:inline-block; width:0; height:0; border-left:5px solid transparent; border-right:5px solid transparent; border-bottom:7px solid #22c55e; margin-right:8px; vertical-align:middle;" title="Compra Coincidente"></span>';
                                                }
                                            } else {
                                                shapeIcon = '<span style="background:' + signalColor + '; border:1px solid ' + signalColor + '; display:inline-block; width:9px; height:9px; margin-right:8px; border-radius:2px; vertical-align:middle;" title="' + signalText + '"></span>';
                                            }

                                            const shapeLabel = isCoincident ? '▲ Coincidente' : '■';
                                            rowsHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#334155;">' +
                                                        parBox + shapeIcon + '<strong style="color:#007bff;">' + pairA + '</strong> Cruce EMA: <span style="color:' + signalColor + '; font-weight:700;">' + signalText + ' (' + shapeLabel + ')</span> - ' + parsedY.toFixed(4) + '</td></tr>';
                                        }
                                    }
                                    // 4. Puntos Cruce EMA Par B
                                    else if (label.includes('Puntos Cruce EMA') && label.includes(pairB)) {
                                        if (rawVal && rawVal.crossType) {
                                            const isCoincident = rawVal.crossType.includes('coincident');
                                            const isVenta = rawVal.crossType.startsWith('high_cross_down');
                                            const signalColor = isVenta ? '#ef4444' : '#22c55e';
                                            const signalText = isVenta ? 'Venta' : 'Compra';
                                            
                                            // Primer cuadro: Color del Par B (Naranja #ff8c00)
                                            const parBox = '<span style="background:#ff8c00; border:1px solid #ff8c00; display:inline-block; width:10px; height:10px; margin-right:5px; border-radius:2px; vertical-align:middle;" title="Par ' + pairB + '"></span>';
                                            
                                            // Segundo icono: Triángulo si es coincidente, Cuadro si no lo es (con color verde/rojo)
                                            let shapeIcon = '';
                                            if (isCoincident) {
                                                if (isVenta) {
                                                    shapeIcon = '<span style="display:inline-block; width:0; height:0; border-left:5px solid transparent; border-right:5px solid transparent; border-top:7px solid #ef4444; margin-right:8px; vertical-align:middle;" title="Venta Coincidente"></span>';
                                                } else {
                                                    shapeIcon = '<span style="display:inline-block; width:0; height:0; border-left:5px solid transparent; border-right:5px solid transparent; border-bottom:7px solid #22c55e; margin-right:8px; vertical-align:middle;" title="Compra Coincidente"></span>';
                                                }
                                            } else {
                                                shapeIcon = '<span style="background:' + signalColor + '; border:1px solid ' + signalColor + '; display:inline-block; width:9px; height:9px; margin-right:8px; border-radius:2px; vertical-align:middle;" title="' + signalText + '"></span>';
                                            }

                                            const shapeLabel = isCoincident ? '▲ Coincidente' : '■';
                                            rowsHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#334155;">' +
                                                        parBox + shapeIcon + '<strong style="color:#ff8c00;">' + pairB + '</strong> Cruce EMA: <span style="color:' + signalColor + '; font-weight:700;">' + signalText + ' (' + shapeLabel + ')</span> - ' + parsedY.toFixed(4) + '</td></tr>';
                                        }
                                    }
                                    // 5. Media (Negro #000000)
                                    else if (label === 'Media') {
                                        const colorBox = '<span style="background:#000000; border:1px solid #000000; display:inline-block; width:10px; height:10px; margin-right:8px; border-radius:2px; vertical-align:middle;"></span>';
                                        rowsHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#334155;">' +
                                                    colorBox + '<strong>Media (0-1)</strong>: ' + parsedY.toFixed(4) + '</td></tr>';
                                    }
                                    // 6. Cruce con Media (Rojo #ef4444)
                                    else if (label.includes('Cruce con Media')) {
                                        const circleIcon = '<span style="background:#ef4444; border:1px solid #ef4444; display:inline-block; width:10px; height:10px; margin-right:8px; border-radius:50%; vertical-align:middle;"></span>';
                                        rowsHtml += '<tr><td style="padding:4px 0; font-family:\\'Inter\\',sans-serif; font-size:12px; color:#ef4444; font-weight:600;">' +
                                                    circleIcon + 'Cruce de Precios con Media: ' + parsedY.toFixed(4) + '</td></tr>';
                                    }
                                    // 7. Bandas de desviación (+1σ, -1σ)
                                    else if (label.includes('1σ')) {
                                        const borderColor = dataset.borderColor || '#38bdf8';
                                        const colorBox = '<span style="background:' + borderColor + '; border:1px solid ' + borderColor + '; display:inline-block; width:10px; height:10px; margin-right:8px; border-radius:2px; vertical-align:middle;"></span>';
                                        rowsHtml += '<tr><td style="padding:3px 0; font-family:\\'Inter\\',sans-serif; font-size:11px; color:#64748b;">' +
                                                    colorBox + label + ': ' + parsedY.toFixed(4) + '</td></tr>';
                                    }
                                });

                                innerHtml += rowsHtml;

                                if (normA !== null && normB !== null) {
                                    const diff = Math.abs(normA - normB);
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

idx_tooltip = content.find('tooltip: {')
idx_annotation = content.find('annotation: {', idx_tooltip)

if idx_tooltip != -1 and idx_annotation != -1:
    # Find the end of tooltip before annotation
    content = content[:idx_tooltip] + new_tooltip_code + ',\n                        ' + content[idx_annotation:]
    print("Updated tooltip successfully!")
else:
    print(f"Indices: tooltip={idx_tooltip}, annotation={idx_annotation}")

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("dashboard.xhtml saved!")
