dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Fix the trade mapping block
old_mapping = '''            // Mapeo de Trades Simulados del Backtest (con Estrellas ★ y Cruces Rotadas ✕)
            const tradesInput = document.querySelector("[id$='quantTradesJsonData']");
            const tradesRawStr = tradesInput ? tradesInput.value : '[]';
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
            }'''

new_mapping = '''            // Mapeo de Trades Simulados del Backtest (con Estrellas ★ y Cruces Rotadas ✕)
            const tradesInput = document.querySelector("[id$='quantTradesJsonData']");
            const tradesRawStr = tradesInput ? tradesInput.value : '[]';
            let tradesRaw = [];
            try { tradesRaw = JSON.parse(tradesRawStr); } catch(e) {}

            const simTradeEntries = [];
            const simTradeEntryColors = [];
            const simTradeExits = [];
            const simTradeExitColors = [];

            for (let i = 0; i < rawData.length; i++) {
                const ts = dates[i];
                const rawDt = (rawData[i] && rawData[i].datetime) ? String(rawData[i].datetime) : '';
                const dtOnly = rawDt.substring(0, 10);
                const meanVal = (priceMeanSeries[i] && priceMeanSeries[i].y !== null) ? priceMeanSeries[i].y : 0.5;

                // Buscar si hay entrada en esta fecha
                const entryTrade = tradesRaw.find(function(t) {
                    if (!t || !t.entryDate) return false;
                    const tDate = String(t.entryDate).substring(0, 10);
                    return tDate === dtOnly;
                });

                if (entryTrade) {
                    simTradeEntries.push({ x: ts, y: meanVal, tradeInfo: entryTrade });
                    simTradeEntryColors.push(entryTrade.type === 'LARGO_RATIO' ? '#8b5cf6' : '#ec4899');
                } else {
                    simTradeEntries.push({ x: ts, y: null });
                    simTradeEntryColors.push('transparent');
                }

                // Buscar si hay salida en esta fecha
                const exitTrade = tradesRaw.find(function(t) {
                    if (!t || !t.exitDate) return false;
                    const tDate = String(t.exitDate).substring(0, 10);
                    return tDate === dtOnly;
                });

                if (exitTrade) {
                    simTradeExits.push({ x: ts, y: meanVal, tradeInfo: exitTrade });
                    simTradeExitColors.push(exitTrade.isWin ? '#10b981' : '#ef4444');
                } else {
                    simTradeExits.push({ x: ts, y: null });
                    simTradeExitColors.push('transparent');
                }
            }'''

if old_mapping in dash:
    dash = dash.replace(old_mapping, new_mapping)
    print("Fixed trade mapping timestamp matching bug in dashboard.xhtml!")
else:
    print("Old mapping block not found directly, performing regex replacement...")
    # fallback with regex
    import re
    dash = re.sub(
        r'// Mapeo de Trades Simulados del Backtest.*?(?=const meanCrossColors = \[\];)',
        new_mapping + '\n\n            ',
        dash,
        flags=re.DOTALL
    )
    print("Regex replacement completed!")

# 2. Also ensure execution on jQuery ready and window load
old_listener = 'document.addEventListener("DOMContentLoaded", function() { renderAetherChart(); });'
new_listener = '''document.addEventListener("DOMContentLoaded", function() { setTimeout(renderAetherChart, 100); });
        window.addEventListener("load", function() { setTimeout(renderAetherChart, 100); });
        if (window.jQuery) {
            jQuery(function() { setTimeout(renderAetherChart, 100); });
        }'''
if old_listener in dash:
    dash = dash.replace(old_listener, new_listener)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml successfully fixed and saved!")
