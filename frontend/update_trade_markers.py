dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Replace trade mapping logic with explicit radius and border color arrays
old_trade_block = '''            // Mapeo de Trades Simulados del Backtest (con Estrellas ★ y Cruces Rotadas ✕)
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

new_trade_block = '''            // Mapeo de Trades Simulados del Backtest (con Estrellas ★ y Cruces Rotadas ✕)
            const tradesInput = document.querySelector("[id$='quantTradesJsonData']");
            const tradesRawStr = tradesInput ? tradesInput.value : '[]';
            let tradesRaw = [];
            try { tradesRaw = JSON.parse(tradesRawStr); } catch(e) {}
            console.log("Simulated Trades for Chart Rendering:", tradesRaw.length, tradesRaw);

            const simTradeEntries = [];
            const simTradeEntryColors = [];
            const simTradeEntryBorders = [];
            const simTradeEntryRadius = [];

            const simTradeExits = [];
            const simTradeExitColors = [];
            const simTradeExitBorders = [];
            const simTradeExitRadius = [];

            // Conjuntos para evitar duplicar marcas en datos horarios del mismo día
            const matchedEntryTrades = new Set();
            const matchedExitTrades = new Set();

            for (let i = 0; i < rawData.length; i++) {
                const ts = dates[i];
                const rawDt = (rawData[i] && rawData[i].datetime) ? String(rawData[i].datetime) : '';
                const dtOnly = rawDt.substring(0, 10);
                const meanVal = (priceMeanSeries[i] && priceMeanSeries[i].y !== null) ? priceMeanSeries[i].y : 0.5;

                // Buscar si hay entrada en esta fecha
                let foundEntry = null;
                for (let t of tradesRaw) {
                    if (t && t.entryDate) {
                        const tDate = String(t.entryDate).substring(0, 10);
                        if (tDate === dtOnly && !matchedEntryTrades.has(t.tradeNum)) {
                            foundEntry = t;
                            matchedEntryTrades.add(t.tradeNum);
                            break;
                        }
                    }
                }

                if (foundEntry) {
                    simTradeEntries.push({ x: ts, y: meanVal, tradeInfo: foundEntry });
                    simTradeEntryColors.push(foundEntry.type === 'LARGO_RATIO' ? '#8b5cf6' : '#ec4899');
                    simTradeEntryBorders.push('#ffffff');
                    simTradeEntryRadius.push(9.0);
                } else {
                    simTradeEntries.push({ x: ts, y: null });
                    simTradeEntryColors.push('transparent');
                    simTradeEntryBorders.push('transparent');
                    simTradeEntryRadius.push(0);
                }

                // Buscar si hay salida en esta fecha
                let foundExit = null;
                for (let t of tradesRaw) {
                    if (t && t.exitDate) {
                        const tDate = String(t.exitDate).substring(0, 10);
                        if (tDate === dtOnly && !matchedExitTrades.has(t.tradeNum)) {
                            foundExit = t;
                            matchedExitTrades.add(t.tradeNum);
                            break;
                        }
                    }
                }

                if (foundExit) {
                    simTradeExits.push({ x: ts, y: meanVal, tradeInfo: foundExit });
                    simTradeExitColors.push(foundExit.isWin ? '#10b981' : '#ef4444');
                    simTradeExitBorders.push('#ffffff');
                    simTradeExitRadius.push(9.0);
                } else {
                    simTradeExits.push({ x: ts, y: null });
                    simTradeExitColors.push('transparent');
                    simTradeExitBorders.push('transparent');
                    simTradeExitRadius.push(0);
                }
            }'''

dash = dash.replace(old_trade_block, new_trade_block)

# Update the dataset definitions in datasets array
old_ds_entries = "{ label: '★ Entradas Trades Backtest', data: simTradeEntries, showLine: false, pointStyle: 'star', pointRadius: 8, pointHoverRadius: 12, pointBackgroundColor: simTradeEntryColors, pointBorderColor: '#ffffff', pointBorderWidth: 1.5, yAxisID: 'y-price' },"
old_ds_exits = "{ label: '✕ Salidas Trades Backtest', data: simTradeExits, showLine: false, pointStyle: 'crossRot', pointRadius: 8, pointHoverRadius: 12, pointBackgroundColor: simTradeExitColors, pointBorderColor: '#ffffff', pointBorderWidth: 2, yAxisID: 'y-price' },"

new_ds_entries = "{ label: '★ Entradas Trades Backtest', data: simTradeEntries, showLine: false, pointStyle: 'star', pointRadius: simTradeEntryRadius, pointHoverRadius: 13, pointBackgroundColor: simTradeEntryColors, pointBorderColor: simTradeEntryBorders, pointBorderWidth: 2.0, yAxisID: 'y-price' },"
new_ds_exits = "{ label: '✕ Salidas Trades Backtest', data: simTradeExits, showLine: false, pointStyle: 'crossRot', pointRadius: simTradeExitRadius, pointHoverRadius: 13, pointBackgroundColor: simTradeExitColors, pointBorderColor: simTradeExitBorders, pointBorderWidth: 2.5, yAxisID: 'y-price' },"

dash = dash.replace(old_ds_entries, new_ds_entries)
dash = dash.replace(old_ds_exits, new_ds_exits)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with robust pointRadius and pointBorderColor arrays for trades!")
