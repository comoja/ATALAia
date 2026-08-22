import re

dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Add hidden input for quantTradesJsonData
if 'id="quantTradesJsonData"' not in dash:
    dash = dash.replace(
        '<h:inputHidden id="equityCurveJsonData" value="#{dashboardBean.equityCurveJson}" />',
        '<h:inputHidden id="equityCurveJsonData" value="#{dashboardBean.equityCurveJson}" />\n        <h:inputHidden id="quantTradesJsonData" value="#{dashboardBean.quantTradesJson}" />'
    )
    print("Added quantTradesJsonData hidden input to dashboard.xhtml")

# 2. Add trade points mapping logic in renderAetherChart
trade_mapping_js = '''
            // Mapeo de Trades Simulados del Backtest (con Estrellas ★ y Cruces Rotadas ✕)
            const tradesRawStr = document.getElementById('aetherForm:quantTradesJsonData')?.value || '[]';
            let tradesRaw = [];
            try { tradesRaw = JSON.parse(tradesRawStr); } catch(e) {}

            const simTradeEntries = [];
            const simTradeEntryColors = [];
            const simTradeExits = [];
            const simTradeExitColors = [];

            // Crear un mapa de fecha a valor normalizado (para posicionar exactamente el trade sobre la media del par)
            const dateToMeanMap = {};
            for (let i = 0; i < dates.length; i++) {
                if (priceMeanSeries[i] && priceMeanSeries[i].y !== null) {
                    // Mapear tanto fecha completa como formato 'YYYY-MM-DD'
                    dateToMeanMap[dates[i]] = priceMeanSeries[i].y;
                    const dateOnly = dates[i].substring(0, 10);
                    dateToMeanMap[dateOnly] = priceMeanSeries[i].y;
                }
            }

            for (let i = 0; i < dates.length; i++) {
                const d = dates[i];
                const dOnly = d.substring(0, 10);
                const meanVal = priceMeanSeries[i] ? priceMeanSeries[i].y : 0.5;

                // Buscar si hay entrada en esta fecha
                const entryTrade = tradesRaw.find(t => t.entryDate === d || t.entryDate === dOnly);
                if (entryTrade) {
                    simTradeEntries.push({ x: d, y: meanVal, tradeInfo: entryTrade });
                    simTradeEntryColors.push(entryTrade.type === 'LARGO_RATIO' ? '#8b5cf6' : '#ec4899');
                } else {
                    simTradeEntries.push({ x: d, y: null });
                    simTradeEntryColors.push('transparent');
                }

                // Buscar si hay salida en esta fecha
                const exitTrade = tradesRaw.find(t => t.exitDate === d || t.exitDate === dOnly);
                if (exitTrade) {
                    simTradeExits.push({ x: d, y: meanVal, tradeInfo: exitTrade });
                    simTradeExitColors.push(exitTrade.isWin ? '#10b981' : '#ef4444');
                } else {
                    simTradeExits.push({ x: d, y: null });
                    simTradeExitColors.push('transparent');
                }
            }
'''

if 'const simTradeEntries = [];' not in dash:
    dash = dash.replace('const meanCrossColors = [];', trade_mapping_js + '\n            const meanCrossColors = [];')
    print("Added trade mapping JS logic to renderAetherChart!")

# 3. Add datasets to aetherChart datasets array
datasets_entry = '''                        { label: '★ Entradas Trades Backtest', data: simTradeEntries, showLine: false, pointStyle: 'star', pointRadius: 8, pointHoverRadius: 12, pointBackgroundColor: simTradeEntryColors, pointBorderColor: '#ffffff', pointBorderWidth: 1.5, yAxisID: 'y-price' },
                        { label: '✕ Salidas Trades Backtest', data: simTradeExits, showLine: false, pointStyle: 'crossRot', pointRadius: 8, pointHoverRadius: 12, pointBackgroundColor: simTradeExitColors, pointBorderColor: '#ffffff', pointBorderWidth: 2, yAxisID: 'y-price' },'''

if "'★ Entradas Trades Backtest'" not in dash:
    dash = dash.replace(
        "{ label: 'Cruce con Media', data: meanCrossSeries, showLine: false, pointBackgroundColor: meanCrossColors, pointBorderColor: meanCrossBorderColors, pointBorderWidth: 1.5, pointRadius: meanCrossRadius, pointHoverRadius: 8, pointStyle: 'circle', yAxisID: 'y-price' },",
        "{ label: 'Cruce con Media', data: meanCrossSeries, showLine: false, pointBackgroundColor: meanCrossColors, pointBorderColor: meanCrossBorderColors, pointBorderWidth: 1.5, pointRadius: meanCrossRadius, pointHoverRadius: 8, pointStyle: 'circle', yAxisID: 'y-price' },\n" + datasets_entry
    )
    print("Added simulated trades datasets (star and crossRot) to aetherChart!")

# 4. Enhance custom tooltip to render simulated trade info
tooltip_trade_render = '''
                                // Renderizar datos especiales de Trades Simulados
                                for (let item of items) {
                                    if (item.dataset.label && item.dataset.label.includes('★ Entradas Trades Backtest') && item.raw && item.raw.tradeInfo) {
                                        const t = item.raw.tradeInfo;
                                        innerHtml += '<tr style="border-top: 1px dashed #e2e8f0; background: #f5f3ff;">';
                                        innerHtml += '<td colspan="2" style="padding: 4px 6px; font-weight: 700; color: #6d28d9; font-size: 1.1rem;">';
                                        innerHtml += '★ Trade #' + t.tradeNum + ' (Entrada ' + (t.type === 'LARGO_RATIO' ? 'COMPRA' : 'VENTA') + ') - Ratio: ' + t.entryPrice;
                                        innerHtml += '</td></tr>';
                                    }
                                    if (item.dataset.label && item.dataset.label.includes('✕ Salidas Trades Backtest') && item.raw && item.raw.tradeInfo) {
                                        const t = item.raw.tradeInfo;
                                        innerHtml += '<tr style="border-top: 1px dashed #e2e8f0; background: ' + (t.isWin ? '#f0fdf4' : '#fef2f2') + ';">';
                                        innerHtml += '<td colspan="2" style="padding: 4px 6px; font-weight: 700; color: ' + (t.isWin ? '#15803d' : '#b91c1c') + '; font-size: 1.1rem;">';
                                        innerHtml += '✕ Trade #' + t.tradeNum + ' Salida (' + (t.isWin ? '+' : '') + t.returnPct + '%, PnL: $' + t.pnl + ')';
                                        innerHtml += '</td></tr>';
                                    }
                                }
'''

if 'Renderizar datos especiales de Trades Simulados' not in dash:
    dash = dash.replace(
        "if (items.length > 0) {",
        "if (items.length > 0) {\n" + tooltip_trade_render
    )
    print("Added custom tooltip trade rendering to dashboard.xhtml!")

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml updated with Star and CrossRot trade markers successfully!")
