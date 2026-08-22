import urllib.request, json
import numpy as np, pandas as pd

url = 'http://127.0.0.1:8004/api/v1/ratio/EUR%2FUSD?pairB=USD%2FMXN&tf=1d'
req = urllib.request.urlopen(url)
data = json.loads(req.read().decode('utf-8'))['history']
df = pd.DataFrame(data).tail(180).reset_index(drop=True)

minA, maxA = df['priceA'].min(), df['priceA'].max()
minB, maxB = df['priceB'].min(), df['priceB'].max()
normA = (df['priceA'] - minA) / (maxA - minA)
normB = (df['priceB'] - minB) / (maxB - minB)

emaA = normA.ewm(span=3, adjust=False).mean()
emaB = normB.ewm(span=3, adjust=False).mean()
priceMean = (normA + normB) / 2.0
avgOfMean = priceMean.mean()

stdA = emaA.rolling(window=30, min_periods=3).std().fillna(0.0)
stdB = emaB.rolling(window=30, min_periods=3).std().fillna(0.0)
stdAboveA, stdBelowA = avgOfMean + stdA, avgOfMean - stdA
stdAboveB, stdBelowB = avgOfMean + stdB, avgOfMean - stdB

# Simulación con Capital $800 USD, Asignación 3%
initialCapital = 800.0
allocationPct = 3.0
commissionBps = 2.0
slippageBps = 1.0

# Par A: EUR/USD
minLotsA = 1000.0
margenPctA = 0.25 # 0.25%
pipA = 0.00010
quoteA = "USD"

# Par B: USD/MXN
minLotsB = 1000.0
margenPctB = 1.00 # 1.00%
pipB = 0.01000
quoteB = "MXN"

equity = initialCapital
cycle_start_equity = equity
active_trades = []
finished_trades = []
raw_trades_count = 0
cycle_counter = 0

margenRateA = margenPctA / 100.0
margenRateB = margenPctB / 100.0

for i in range(1, len(df)):
    currP_A, prevP_A = normA.iloc[i], normA.iloc[i-1]
    currE_A, prevE_A = emaA.iloc[i], emaA.iloc[i-1]
    currP_B, prevP_B = normB.iloc[i], normB.iloc[i-1]
    currE_B, prevE_B = emaB.iloc[i], emaB.iloc[i-1]
    
    isCrossDownHigh_A = (prevP_A >= prevE_A and currP_A < currE_A and currP_A >= stdAboveA.iloc[i])
    isCrossUpLow_A = (prevP_A <= prevE_A and currP_A > currE_A and currP_A <= stdBelowA.iloc[i])
    hasSignal_A = (isCrossDownHigh_A or isCrossUpLow_A)
    
    isCrossDownHigh_B = (prevP_B >= prevE_B and currP_B < currE_B and currP_B >= stdAboveB.iloc[i])
    isCrossUpLow_B = (prevP_B <= prevE_B and currP_B > currE_B and currP_B <= stdBelowB.iloc[i])
    hasSignal_B = (isCrossDownHigh_B or isCrossUpLow_B)
    
    prevDiff = normA.iloc[i-1] - normB.iloc[i-1]
    currDiff = normA.iloc[i] - normB.iloc[i]
    isMeanCross = (prevDiff > 0 and currDiff <= 0) or (prevDiff < 0 and currDiff >= 0)
    
    dt = df['datetime'].iloc[i]
    pxA = df['priceA'].iloc[i]
    pxB = df['priceB'].iloc[i]
    
    # 1. CIERRE DE CICLO EN MEDIA (●) -> CÁLCULO EXACTO POR PIPS Y REINVERSIÓN
    if isMeanCross and active_trades:
        cycle_counter += 1
        cycle_trades = []
        cycle_pnl = 0.0
        cycle_margin = sum(t['margin'] for t in active_trades)
        cycle_units_a = sum(t['unitsA'] for t in active_trades)
        cycle_units_b = sum(t['unitsB'] for t in active_trades)
        
        for t in active_trades:
            raw_trades_count += 1
            
            # Pips y PnL Par A
            if t['direction'] == 'LONG A / SHORT B':
                deltaA = (pxA - t['entryPxA'])
                deltaB = (t['entryPxB'] - pxB)
            else:
                deltaA = (t['entryPxA'] - pxA)
                deltaB = (pxB - t['entryPxB'])
                
            pipsA = deltaA / pipA if pipA > 0 else 0.0
            pipsB = deltaB / pipB if pipB > 0 else 0.0
            
            if quoteA == "USD":
                pipValA = t['unitsA'] * pipA
            else:
                pipValA = (t['unitsA'] * pipA) / pxA if pxA > 0 else (t['unitsA'] * pipA)
                
            if quoteB == "USD":
                pipValB = t['unitsB'] * pipB
            else:
                pipValB = (t['unitsB'] * pipB) / pxB if pxB > 0 else (t['unitsB'] * pipB)
                
            pnlA = pipsA * pipValA
            pnlB = pipsB * pipValB
            
            nominalA = t['unitsA'] * pxA if quoteA == "USD" else t['unitsA']
            nominalB = t['unitsB'] if quoteB != "USD" else t['unitsB'] * pxB
            costs = (nominalA + nominalB) * ((commissionBps + slippageBps) / 10000.0)
            
            tradePnl = pnlA + pnlB - costs
            tradeNetRet = (tradePnl / t['margin']) if t['margin'] > 0 else 0.0
            
            equity += tradePnl
            cycle_pnl += tradePnl
            
            cycle_trades.append({
                'tradeNum': raw_trades_count,
                'isSubtotal': False,
                'cycleNum': cycle_counter,
                'signalType': t['signalType'],
                'direction': t['direction'],
                'entryDate': t['entryDate'],
                'exitDate': dt,
                'margin': round(t['margin'], 2),
                'multA': t['multA'],
                'multB': t['multB'],
                'unitsA': int(t['unitsA']),
                'unitsB': int(t['unitsB']),
                'pipsA': round(pipsA, 1),
                'pipsB': round(pipsB, 1),
                'pnlA': round(pnlA, 2),
                'pnlB': round(pnlB, 2),
                'costs': round(costs, 2),
                'returnPct': round(tradeNetRet * 100, 2),
                'pnl': round(tradePnl, 2),
                'isWin': bool(tradePnl > 0)
            })
            
        cycleAvgRet = round((cycle_pnl / cycle_margin) * 100, 2) if cycle_margin > 0 else 0.0
        for ct in cycle_trades:
            finished_trades.append(ct)
            
        finished_trades.append({
            'tradeNum': None,
            'isSubtotal': True,
            'cycleNum': cycle_counter,
            'signalType': f'SUBTOTAL CIERRE #{cycle_counter}',
            'direction': f'● Salida Media ({len(cycle_trades)} ops)',
            'entryDate': f'{cycle_trades[0]["entryDate"][:10]} ... {cycle_trades[-1]["entryDate"][:10]}',
            'exitDate': dt,
            'margin': round(cycle_margin, 2),
            'multA': None,
            'multB': None,
            'unitsA': int(cycle_units_a),
            'unitsB': int(cycle_units_b),
            'returnPct': cycleAvgRet,
            'pnl': round(cycle_pnl, 2),
            'isWin': bool(cycle_pnl > 0)
        })
        
        # Reinversión al cierre de todas las posiciones
        cycle_start_equity = equity
        active_trades = []
        
    sigType, direction = None, None
    if hasSignal_A and hasSignal_B:
        if isCrossUpLow_A and isCrossDownHigh_B:
            sigType = 'TRIANGULO_VERDE_COINCIDENTE'
            direction = 'LONG A / SHORT B'
        elif isCrossDownHigh_A and isCrossUpLow_B:
            sigType = 'TRIANGULO_ROJO_COINCIDENTE'
            direction = 'SHORT A / LONG B'
    elif hasSignal_A or hasSignal_B:
        if hasSignal_A:
            if isCrossUpLow_A:
                sigType = 'CUADRO_VERDE_PAR_A'
                direction = 'LONG A / SHORT B'
            elif isCrossDownHigh_A:
                sigType = 'CUADRO_ROJO_PAR_A'
                direction = 'SHORT A / LONG B'
        elif hasSignal_B:
            if isCrossUpLow_B:
                sigType = 'CUADRO_VERDE_PAR_B'
                direction = 'SHORT A / LONG B'
            elif isCrossDownHigh_B:
                sigType = 'CUADRO_ROJO_PAR_B'
                direction = 'LONG A / SHORT B'
                
    if sigType and direction:
        # Presupuesto 3% del capital base del ciclo
        totalEntryBudget = cycle_start_equity * (allocationPct / 100.0)
        budgetA = totalEntryBudget / 2.0
        budgetB = totalEntryBudget / 2.0
        
        margen1LotA = (minLotsA * pxA) * margenRateA
        margen1LotB = (minLotsB * (1.0 if quoteB == "MXN" else pxB)) * margenRateB
        
        multA = max(1, int(budgetA // margen1LotA)) if budgetA >= margen1LotA else 1
        multB = max(1, int(budgetB // margen1LotB)) if budgetB >= margen1LotB else 1
        
        realMargenA = multA * margen1LotA
        realMargenB = multB * margen1LotB
        
        unitsA = multA * minLotsA
        unitsB = multB * minLotsB
        
        active_trades.append({
            'signalType': sigType,
            'direction': direction,
            'entryDate': dt,
            'entryIndex': i,
            'entryPxA': pxA,
            'entryPxB': pxB,
            'margin': realMargenA + realMargenB,
            'multA': multA,
            'multB': multB,
            'unitsA': unitsA,
            'unitsB': unitsB
        })

print(f"=== RESULTADOS DEL BACKTEST CON PIPS REALES Y MULTIPLOS ===")
print(f"Capital Inicial: ${initialCapital:.2f} USD -> Capital Final: ${equity:.2f} USD (+${equity - initialCapital:.2f} USD, {(equity - initialCapital)/initialCapital*100:.2f}%)")
print(f"\nBitácora de Trades:")
for t in finished_trades[:8]:
    if t['isSubtotal']:
        print(f"==> {t['signalType']} [Cierre {t['exitDate']}] Margen: ${t['margin']} | Total Lotes: {t['unitsA']} A / {t['unitsB']} B | Ret: {t['returnPct']}% | PnL: ${t['pnl']}")
    else:
        print(f"    Trade #{t['tradeNum']} [{t['signalType']}] Margen: ${t['margin']} [A: {t['unitsA']} lotes ({t['multA']}x) | B: {t['unitsB']} lotes ({t['multB']}x)] | Pips A: {t['pipsA']} | Pips B: {t['pipsB']} | PnL: ${t['pnl']}")
