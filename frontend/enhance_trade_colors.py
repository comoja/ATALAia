dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_color_logic = '''                if (foundEntry) {
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
                }'''

new_color_logic = '''                if (foundEntry) {
                    simTradeEntries.push({ x: ts, y: meanVal, tradeInfo: foundEntry });
                    // Colores oscuros de alto contraste para las estrellas de entrada
                    const isLong = (foundEntry.type === 'LARGO_RATIO');
                    simTradeEntryColors.push(isLong ? '#0f172a' : '#3b0764'); // Negro pizarra (Largo) o Púrpura muy oscuro (Corto)
                    simTradeEntryBorders.push(isLong ? '#38bdf8' : '#e879f9'); // Borde cian o fucsia brillante para destacar la estrella
                    simTradeEntryRadius.push(11.5);
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
                    // Colores oscuros de alto impacto para las cruces de salida
                    simTradeExitColors.push(foundExit.isWin ? '#064e3b' : '#7f1d1d'); // Verde bosque profundo (Win) o Rojo borgoña oscuro (Loss)
                    simTradeExitBorders.push(foundExit.isWin ? '#34d399' : '#f87171'); // Borde de refuerzo
                    simTradeExitRadius.push(11.5);
                } else {
                    simTradeExits.push({ x: ts, y: null });
                    simTradeExitColors.push('transparent');
                    simTradeExitBorders.push('transparent');
                    simTradeExitRadius.push(0);
                }'''

dash = dash.replace(old_color_logic, new_color_logic)

old_ds_exits = "{ label: '✕ Salidas Trades Backtest', data: simTradeExits, showLine: false, pointStyle: 'crossRot', pointRadius: simTradeExitRadius, pointHoverRadius: 13, pointBackgroundColor: simTradeExitColors, pointBorderColor: simTradeExitBorders, pointBorderWidth: 2.5, yAxisID: 'y-price' },"
new_ds_exits = "{ label: '✕ Salidas Trades Backtest', data: simTradeExits, showLine: false, pointStyle: 'crossRot', pointRadius: simTradeExitRadius, pointHoverRadius: 15, pointBackgroundColor: simTradeExitColors, pointBorderColor: simTradeExitBorders, pointBorderWidth: 3.5, yAxisID: 'y-price' },"
dash = dash.replace(old_ds_exits, new_ds_exits)

old_ds_entries = "{ label: '★ Entradas Trades Backtest', data: simTradeEntries, showLine: false, pointStyle: 'star', pointRadius: simTradeEntryRadius, pointHoverRadius: 13, pointBackgroundColor: simTradeEntryColors, pointBorderColor: simTradeEntryBorders, pointBorderWidth: 2.0, yAxisID: 'y-price' },"
new_ds_entries = "{ label: '★ Entradas Trades Backtest', data: simTradeEntries, showLine: false, pointStyle: 'star', pointRadius: simTradeEntryRadius, pointHoverRadius: 15, pointBackgroundColor: simTradeEntryColors, pointBorderColor: simTradeEntryBorders, pointBorderWidth: 2.5, yAxisID: 'y-price' },"
dash = dash.replace(old_ds_entries, new_ds_entries)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with dark, high-contrast, larger trade markers!")
