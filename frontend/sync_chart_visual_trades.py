dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_trade_loading = '''            // Mapeo de Trades Simulados del Backtest (con Estrellas ★ y Cruces Rotadas ✕)
            const tradesInput = document.querySelector("[id$='quantTradesJsonData']");
            const tradesRawStr = tradesInput ? tradesInput.value : '[]';
            let tradesRaw = [];
            try { tradesRaw = JSON.parse(tradesRawStr); } catch(e) {}
            console.log("Simulated Trades for Chart Rendering:", tradesRaw.length, tradesRaw);'''

new_trade_loading = '''            // Mapeo de TODOS los Trades de Señales Visuales (Triángulos y Cuadros con salida en Círculos)
            const sigBtInput = document.querySelector("[id$='signalBtComparisonCurveJsonData']");
            const quantTradesInput = document.querySelector("[id$='quantTradesJsonData']");
            let tradesRaw = [];
            
            if (sigBtInput && sigBtInput.value) {
                try {
                    const sbData = JSON.parse(sigBtInput.value);
                    if (sbData.combined && sbData.combined.trades && sbData.combined.trades.length > 0) {
                        tradesRaw = sbData.combined.trades;
                    } else if (sbData.trianglesOnly && sbData.trianglesOnly.trades && sbData.trianglesOnly.trades.length > 0) {
                        tradesRaw = sbData.trianglesOnly.trades;
                    }
                } catch(e) { console.warn("Error parsing signalBt trades for chart:", e); }
            }
            
            // Fallback a quantTrades si no hay señales de backtest
            if (tradesRaw.length === 0 && quantTradesInput && quantTradesInput.value) {
                try { tradesRaw = JSON.parse(quantTradesInput.value); } catch(e) {}
            }
            console.log("Visual Signal Trades for Chart Rendering:", tradesRaw.length, tradesRaw);'''

dash = dash.replace(old_trade_loading, new_trade_loading)

# Also update the matching logic to support all trades in multiple entries/exits
old_match_logic = '''                if (foundEntry) {
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

new_match_logic = '''                if (foundEntry) {
                    simTradeEntries.push({ x: ts, y: meanVal, tradeInfo: foundEntry });
                    // Colores oscuros de alto contraste para las estrellas de entrada
                    const isLong = (foundEntry.direction ? foundEntry.direction.startsWith('LONG A') : (foundEntry.type === 'LARGO_RATIO'));
                    simTradeEntryColors.push(isLong ? '#0f172a' : '#3b0764'); // Negro pizarra (Largo A / Compra) o Púrpura muy oscuro (Short A / Venta)
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

dash = dash.replace(old_match_logic, new_match_logic)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Synchronized all visual trades with chart stars and crosses!")
