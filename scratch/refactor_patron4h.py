import re

with open("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_patron4h_optimization.py", "r") as f:
    content = f.read()

# We want to replace from "        symbolBestCombo = None" to the end of the simulation block.
# Let's find the exact block using string splitting.
parts = content.split("        symbolBestCombo = None")
if len(parts) == 2:
    start_str = parts[0]
    rest = parts[1]
    # The end of the simulation block is before "        if symbolBestCombo:"
    end_parts = rest.split("        if symbolBestCombo:")
    if len(end_parts) == 2:
        end_str = end_parts[1]
        
        new_block = """        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        # 1. Precalcular todas las señales crudas (Structural Simulation)
        raw_signals = {} # key: (displacementPct, fvgMinPct), value: list of raw signals
        
        for displacementPct in displacementPctCombos:
            for fvgMinPct in fvgMinPctCombos:
                current_config = {
                    'fvgMinPct': fvgMinPct,
                    'displacementPct': displacementPct,
                    'rrRatioMin': 0.0, # Para obtener TP estructural puro
                    'maxMinutosFvg': 240.0,
                    'minConfidence': 0.0, # Para obtener todas las señales viables
                    'lookback': 50
                }
                
                combo_signals = []
                idx = 100
                n = len(df_15m_global)
                
                while idx < n:
                    t_current = df_15m_global.index[idx]
                    
                    # Obtener índices posicionales
                    i1h = idx1h_map[idx]
                    i4h = idx4h_map[idx]
                    i1d = idx1d_map[idx]
                    
                    if i1d < 2:
                        idx += 1
                        continue
                        
                    t_4h_closed = df_4h_global.index[i4h]
                    t_1h_closed = df_1h_global.index[i1h]
                    
                    # Filtro rápido
                    if t_4h_closed not in catalizadores and t_1h_closed not in catalizadores:
                        idx += 1
                        continue
                        
                    df_15m_sliced = df_15m_global.iloc[:idx+1]
                    df_1h_sliced = df_1h_global.iloc[:i1h+1]
                    df_4h_sliced = df_4h_global.iloc[:i4h+1]
                    df_1d_sliced = df_1d_global.iloc[:i1d+1]
                    
                    symbolInfo = {
                        'symbol': symbol,
                        'intervalo': '15min',
                        'momentum': '☁️ SIN DATOS',
                        'weekly_trend': 'NEUTRAL',
                        'refCapital': 10000.0,
                        'refRiskPct': 1.0
                    }
                    
                    preloaded = {
                        symbol: {
                            '15min': df_15m_sliced,
                            '1h': df_1h_sliced,
                            '4h': df_4h_sliced,
                            '1d': df_1d_sliced
                        }
                    }
                    
                    try:
                        bot.currentTime = t_current
                        # Importamos asincronía aquí si es necesario
                        import asyncio
                        signals = await bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData=preloaded)
                        if signals and len(signals) > 0:
                            sig = signals[0]
                            combo_signals.append({
                                'idx': idx, # índice de la vela actual
                                'direction': sig.direction,
                                'entry': sig.entry_price,
                                'sl': sig.stop_loss,
                                'original_tp': sig.take_profit,
                                'confidence': sig.confidence
                            })
                    except Exception as e:
                        pass
                        
                    idx += 1
                    
                raw_signals[(displacementPct, fvgMinPct)] = combo_signals
                print(f"    - Precalculadas {len(combo_signals)} señales para Disp={displacementPct}, FVG={fvgMinPct}")

        # 2. Evaluación Rápida (RR y Confianza) sobre las señales crudas
        for displacementPct in displacementPctCombos:
            for fvgMinPct in fvgMinPctCombos:
                signals = raw_signals[(displacementPct, fvgMinPct)]
                
                for minRr in minRrCombos:
                    for minConfidence in minConfidenceCombos:
                        
                        trades = []
                        activeTrade = None
                        
                        # Loop muy rápido sobre los índices 15m
                        idx = 100
                        n = len(df_15m_global)
                        
                        # Convertir a generador o lista para procesar las señales secuencialmente
                        signal_queue = [s for s in signals if s['confidence'] >= minConfidence]
                        sig_idx = 0
                        num_sigs = len(signal_queue)
                        
                        while idx < n:
                            if activeTrade:
                                row = df_15m_global.iloc[idx]
                                vHigh = row['high']
                                vLow = row['low']
                                
                                if activeTrade['direction'] == 'LARGO':
                                    lowAdj = vLow - (spreadPrice / 2.0)
                                    highAdj = vHigh + (spreadPrice / 2.0)
                                    if lowAdj <= activeTrade['sl']:
                                        trades.append(-100.0)
                                        activeTrade = None
                                    elif highAdj >= activeTrade['tp']:
                                        trades.append(100.0 * activeTrade['rr'])
                                        activeTrade = None
                                else:
                                    highAdj = vHigh + (spreadPrice / 2.0)
                                    lowAdj = vLow - (spreadPrice / 2.0)
                                    if highAdj >= activeTrade['sl']:
                                        trades.append(-100.0)
                                        activeTrade = None
                                    elif lowAdj <= activeTrade['tp']:
                                        trades.append(100.0 * activeTrade['rr'])
                                        activeTrade = None
                                        
                                idx += 1
                                continue
                            
                            # Si no hay trade activo, ver si hay una señal en este índice
                            if sig_idx < num_sigs and signal_queue[sig_idx]['idx'] == idx:
                                s = signal_queue[sig_idx]
                                # Ajustar el TP según el minRr
                                riesgo = abs(s['entry'] - s['sl'])
                                if riesgo > 0:
                                    if s['direction'] == 'LARGO':
                                        min_tp = s['entry'] + (riesgo * minRr)
                                        adjusted_tp = max(s['original_tp'], min_tp)
                                    else:
                                        min_tp = s['entry'] - (riesgo * minRr)
                                        adjusted_tp = min(s['original_tp'], min_tp)
                                        
                                    adjusted_rr = abs(adjusted_tp - s['entry']) / riesgo
                                    activeTrade = {
                                        'direction': s['direction'],
                                        'entry': s['entry'],
                                        'sl': s['sl'],
                                        'tp': adjusted_tp,
                                        'rr': adjusted_rr
                                    }
                                sig_idx += 1
                            
                            # Optimizador: saltar directo al siguiente idx de señal o vela
                            if not activeTrade:
                                if sig_idx < num_sigs:
                                    idx = signal_queue[sig_idx]['idx']
                                else:
                                    break # Ya no hay más señales
                            else:
                                idx += 1
                                
                        # Métricas finales del combo
                        tCount = len(trades)
                        if tCount >= 2:
                            wCount = len([t for t in trades if t > 0])
                            wRate = (wCount / tCount) * 100
                            pnlNet = sum(trades)
                            
                            profitCount = sum([t for t in trades if t > 0])
                            lossCount = abs(sum([t for t in trades if t <= 0]))
                            profFactor = profitCount / lossCount if lossCount > 0 else float('inf')
                            
                            comboData = {
                                'Símbolo': symbol,
                                'Min RR': minRr,
                                'Min Conf': minConfidence,
                                'Displacement Pct': displacementPct,
                                'FVG Min Pct': fvgMinPct,
                                'Trades': tCount,
                                'Win Rate': f"{wRate:.1f}%",
                                'Profit Factor': round(profFactor, 2),
                                'PnL USD': pnlNet
                            }
                            allResultsRaw.append(comboData)
                            
                            if pnlNet > symbolBestProfit and profFactor >= 1.25 and wRate >= 42.0:
                                symbolBestProfit = pnlNet
                                symbolBestCombo = comboData

"""
        
        with open("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_patron4h_optimization.py", "w") as fout:
            fout.write(start_str + new_block + "        if symbolBestCombo:" + end_str)
        print("Patched Patron4h successfully.")
    else:
        print("Failed to find 'if symbolBestCombo:' block.")
else:
    print("Failed to find 'symbolBestCombo = None' block.")
