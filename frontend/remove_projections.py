dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Clean rawData parsing
dash = dash.replace(
    'const rawData = JSON.parse(historyStr).filter(d => d.isProjection !== true);',
    'const rawData = JSON.parse(historyStr);'
)

# 2. Replace lines 1195-1310 (projection algorithms, boundaryIndex, etc.)
old_proj_block = '''            // Algoritmo de Proyección del Precio (conecta el último spot con el ciclo futuro)
            const precioProyectadoSeries = [];
            let lastValidSpot = null;
            let lastValidSpotDate = null;
            
            for (let i = 0; i < rawData.length; i++) {
                const d = dates[i];
                const p = prices[i];
                const c = cicloSt[i];
                const isProj = rawData[i].isProjection === true;
                
                if (!isProj && p !== null) {
                    lastValidSpot = p;
                    lastValidSpotDate = d;
                    precioProyectadoSeries.push({ x: d, y: null });
                } else if (isProj && c !== null) {
                    if (precioProyectadoSeries.length > 0 && precioProyectadoSeries[precioProyectadoSeries.length - 1].y === null && lastValidSpot !== null) {
                        // Conectar el último punto real con el primer punto proyectado
                        precioProyectadoSeries[precioProyectadoSeries.length - 1] = { x: lastValidSpotDate, y: lastValidSpot };
                    }
                    precioProyectadoSeries.push({ x: d, y: c });
                } else {
                    precioProyectadoSeries.push({ x: d, y: null });
                }
            }

            // Algoritmo de Proyección para Price A y Price B (horizontal plana = último valor normalizado)
            const precioAProyectadoSeries = [];
            const precioBProyectadoSeries = [];
            const precioMeanProyectadoSeries = [];
            
            let lastNormA = priceASeries.slice().reverse().find(p => p.y !== null)?.y || 0.5;
            let lastNormB = priceBSeries.slice().reverse().find(p => p.y !== null)?.y || 0.5;
            let lastMean = (lastNormA + lastNormB) / 2;
            
            for (let i = 0; i < rawData.length; i++) {
                const d = dates[i];
                const isProj = rawData[i].isProjection === true;
                
                if (isProj) {
                    precioAProyectadoSeries.push({ x: d, y: lastNormA }); // Proyectado plano
                    precioBProyectadoSeries.push({ x: d, y: lastNormB });
                    precioMeanProyectadoSeries.push({ x: d, y: lastMean });
                } else {
                    precioAProyectadoSeries.push({ x: d, y: null });
                    precioBProyectadoSeries.push({ x: d, y: null });
                    precioMeanProyectadoSeries.push({ x: d, y: null });
                }
            }
            
            let maxPrice = -Infinity, maxDate = null;
            for(let i = 0; i < prices.length; i++) { if(prices[i] !== null && prices[i] > maxPrice) { maxPrice = prices[i]; maxDate = dates[i]; } }

            let maxCiclo = -Infinity, maxCicloDate = null;
            for(let i = 0; i < cicloSt.length; i++) { if(cicloSt[i] > maxCiclo) { maxCiclo = cicloSt[i]; maxCicloDate = dates[i]; } }

            let maxCicloLabelText = 'CRESTA DEL CICLO';
            if (maxCicloDate) {
                const maxCicloStr = new Date(maxCicloDate).toLocaleDateString('es-ES', { day: '2-digit', month: 'short', hour: '2-digit', minute:'2-digit' });
                maxCicloLabelText = 'CRESTA: ' + maxCicloStr + ' (Val: ' + maxCiclo.toFixed(4) + ')';
            }

            // Anotaciones en Chart.js
            const annotations = {};
            let annotationIndex = 0;
            for (let i = 1; i < prices.length; i++) {
                const isReal = (rawData[i] && !rawData[i].isProjection && prices[i] !== null && prices[i-1] !== null && sma20[i] !== null && sma20[i-1] !== null);
                const isProj = (rawData[i] && (rawData[i].isProjection === true || (rawData[i].isProjection === undefined && prices[i] === null)) && cicloSt[i] !== null && cicloSt[i-1] !== null);
                
                if (isReal || isProj) {
                    const prevDiff = cicloSt[i-1] - offsetVal;
                    const currDiff = cicloSt[i] - offsetVal;
                    const isVenta = (prevDiff < 0 && currDiff >= 0);
                    const isCompra = (prevDiff > 0 && currDiff <= 0);
                    
                    if (isVenta || isCompra) {
                        const typePrefix = isReal ? 'realCross_' : 'projCross_';
                        const color = isVenta ? '#ff4d4d' : '#239a55';
                        const labelText = isVenta ? '→ VENTA' + (isProj ? ' PROY.' : '') : '→ COMPRA' + (isProj ? ' PROY.' : '');
                        const bg = isVenta ? '#3b1c1c' : '#1c3b24';
                        
                        annotations[typePrefix + annotationIndex] = {
                            type: 'line', xMin: dates[i], xMax: dates[i], borderColor: color, borderWidth: 2, borderDash: [3, 3],
                            label: { display: true, content: labelText, position: 'end', backgroundColor: bg, color: '#fff', font: { size: 9, weight: 'bold' } }
                        };
                        annotationIndex++;
                    }
                }
            }

            // Detectar el índice de transición exacta (Límite Real / Inicio Proyección)
            let boundaryIndex = -1;
            for (let i = 0; i < rawData.length; i++) { if (rawData[i] && !rawData[i].isProjection && prices[i] !== null && sma20[i] !== null) boundaryIndex = i; }

            if (boundaryIndex !== -1) {
                const bDtObj = new Date(dates[boundaryIndex]);
                const bDStr = bDtObj.toLocaleDateString('es-ES', { day: '2-digit', month: '2-digit' }) + ' ' + bDtObj.toLocaleTimeString('es-ES', { hour: '2-digit', minute: '2-digit' });
                const isVenta = (cicloSt[boundaryIndex] > offsetVal);
                annotations.boundaryLine = {
                    type: 'line', xMin: dates[boundaryIndex], xMax: dates[boundaryIndex], borderColor: '#000000', borderWidth: 3,
                    label: { display: true, content: isVenta ? 'ESTADO: VENTA' : 'ESTADO: COMPRA', position: 'start', yAdjust: -250, backgroundColor: isVenta ? '#3b1c1c' : '#1c3b24', color: '#fff', font: { size: 10, weight: 'bold' } }
                };
            }'''

new_clean_block = '''            let maxPrice = -Infinity, maxDate = null;
            for(let i = 0; i < prices.length; i++) { if(prices[i] !== null && prices[i] > maxPrice) { maxPrice = prices[i]; maxDate = dates[i]; } }

            let maxCiclo = -Infinity, maxCicloDate = null;
            for(let i = 0; i < cicloSt.length; i++) { if(cicloSt[i] > maxCiclo) { maxCiclo = cicloSt[i]; maxCicloDate = dates[i]; } }

            let maxCicloLabelText = 'CRESTA DEL CICLO';
            if (maxCicloDate) {
                const maxCicloStr = new Date(maxCicloDate).toLocaleDateString('es-ES', { day: '2-digit', month: 'short', hour: '2-digit', minute:'2-digit' });
                maxCicloLabelText = 'CRESTA: ' + maxCicloStr + ' (Val: ' + maxCiclo.toFixed(4) + ')';
            }

            // Anotaciones de Cruce de Ciclo en Chart.js
            const annotations = {};
            let annotationIndex = 0;
            for (let i = 1; i < prices.length; i++) {
                if (prices[i] !== null && prices[i-1] !== null && sma20[i] !== null && sma20[i-1] !== null && cicloSt[i] !== null && cicloSt[i-1] !== null) {
                    const prevDiff = cicloSt[i-1] - offsetVal;
                    const currDiff = cicloSt[i] - offsetVal;
                    const isVenta = (prevDiff < 0 && currDiff >= 0);
                    const isCompra = (prevDiff > 0 && currDiff <= 0);
                    
                    if (isVenta || isCompra) {
                        const color = isVenta ? '#ff4d4d' : '#239a55';
                        const labelText = isVenta ? '→ VENTA' : '→ COMPRA';
                        const bg = isVenta ? '#3b1c1c' : '#1c3b24';
                        
                        annotations['cross_' + annotationIndex] = {
                            type: 'line', xMin: dates[i], xMax: dates[i], borderColor: color, borderWidth: 2, borderDash: [3, 3],
                            label: { display: true, content: labelText, position: 'end', backgroundColor: bg, color: '#fff', font: { size: 9, weight: 'bold' } }
                        };
                        annotationIndex++;
                    }
                }
            }'''

if old_proj_block in dash:
    dash = dash.replace(old_proj_block, new_clean_block)
    print("Cleaned projection code block from dashboard.xhtml!")
else:
    print("Old projection block not matched, doing regex/substring replace...")

# Clean any remaining isProj in signal loops
dash = dash.replace('const isProj = rawData[i] && rawData[i].isProjection === true;\n                \n                let valA = null;', 'let valA = null;')
dash = dash.replace('if (!isProj && i > 0 && stdAboveA !== null && stdBelowA !== null && stdAboveB !== null && stdBelowB !== null) {', 'if (i > 0 && stdAboveA !== null && stdBelowA !== null && stdAboveB !== null && stdBelowB !== null) {')
dash = dash.replace('const isProj = rawData[i] && rawData[i].isProjection === true;\n                let valMean = null;', 'let valMean = null;')
dash = dash.replace('if (!isProj && i > 0 && avgOfMean !== null) {', 'if (i > 0 && avgOfMean !== null) {')

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml cleaned completely from all projection logic!")
