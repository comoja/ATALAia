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

# Capital de la cuenta del usuario: $800.00 USD
initialCapital = 800.0
allocationPct = 3.0 # 3% por entrada total (1.5% Par A + 1.5% Par B)
costMultiplier = 0.0003

# symbols.min_lots y symbols.margen
minLotsA = 1000.0 # EUR/USD
margenPctA = 0.25 # symbols.margen: 0.25%

minLotsB = 1000.0 # USD/MXN
margenPctB = 1.00 # symbols.margen: 1.00%

equity = initialCapital
cycle_start_equity = equity # Capital al inicio del ciclo (se reinvierte al cerrar el ciclo)
active_trades = []
finished_trades = []
raw_trades_count = 0
cycle_counter = 0

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
    
    # 1. CIERRE DE CICLO EN LA MEDIA (●) -> SE REINVIERTE EL CAPITAL PARA EL SIGUIENTE CICLO
    if isMeanCross and active_trades:
        cycle_counter += 1
        cycle_trades = []
        cycle_pnl = 0.0
        cycle_margin = sum(t['margin'] for t in active_trades)
        
        for t in active_trades:
            raw_trades_count += 1
            if t['direction'] == 'LONG A / SHORT B':
                rA = (pxA - t['entryPxA']) / t['entryPxA']
                rB = (t['entryPxB'] - pxB) / t['entryPxB']
            else:
                rA = (t['entryPxA'] - pxA) / t['entryPxA']
                rB = (pxB - t['entryPxB']) / t['entryPxB']
            
            pnlA = t['nominalA'] * rA
            pnlB = t['nominalB'] * rB
            costs = (t['nominalA'] + t['nominalB']) * costMultiplier
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
                'nominal': round(t['nominalA'] + t['nominalB'], 2),
                'multA': t['multA'],
                'multB': t['multB'],
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
            'direction': f'● Salida en Media ({len(cycle_trades)} ops)',
            'entryDate': f'{cycle_trades[0]["entryDate"][:10]} ... {cycle_trades[-1]["entryDate"][:10]}',
            'exitDate': dt,
            'margin': round(cycle_margin, 2),
            'nominal': sum(ct['nominal'] for ct in cycle_trades),
            'multA': None,
            'multB': None,
            'returnPct': cycleAvgRet,
            'pnl': round(cycle_pnl, 2),
            'isWin': bool(cycle_pnl > 0)
        })
        
        # Reinvierte el capital para el siguiente ciclo
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
        # 3% del capital de la cuenta (repartido entre las 2 patas)
        totalEntryBudget = cycle_start_equity * (allocationPct / 100.0) # Ej. 800 * 3% = $24.00
        budgetA = totalEntryBudget / 2.0 # $12.00
        budgetB = totalEntryBudget / 2.0 # $12.00
        
        # Margen requerido por 1 min_lots
        margenRateA = margenPctA / 100.0 if margenPctA > 0 else 0.01
        margenRateB = margenPctB / 100.0 if margenPctB > 0 else 0.01
        
        margen1LotA = (minLotsA * pxA) * margenRateA # Ej. 1000 * 1.17 * 0.25% = $2.92
        margen1LotB = (minLotsB * 1.0) * margenRateB # Ej. 1000 * 1.00% = $10.00
        
        # Múltiplos enteros de min_lots basados en el presupuesto del 3%
        multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA) else 1
        multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB) else 1
        
        realMargenA = multA * margen1LotA
        realMargenB = multB * margen1LotB
        
        realNominalA = multA * minLotsA * pxA
        realNominalB = multB * minLotsB * 1.0
        
        active_trades.append({
            'signalType': sigType,
            'direction': direction,
            'entryDate': dt,
            'entryIndex': i,
            'entryPxA': pxA,
            'entryPxB': pxB,
            'margin': realMargenA + realMargenB,
            'nominalA': realNominalA,
            'nominalB': realNominalB,
            'multA': multA,
            'multB': multB
        })

print(f"=== REGLA DE ASIGNACIÓN 3% CON REINVERSIÓN AL CIERRE ===")
print(f"Capital Inicial: ${initialCapital:.2f} USD")
print(f"Capital Final Reinvertido: ${equity:.2f} USD (Ganancia Neta: +${equity - initialCapital:.2f} USD, {(equity - initialCapital)/initialCapital*100:.2f}%)")
print(f"Margen 1 Lot A (EUR/USD): ${(minLotsA * 1.17 * (margenPctA/100)):.2f} USD | Margen 1 Lot B (USD/MXN): ${(minLotsB * (margenPctB/100)):.2f} USD")
print(f"Margen Requerido por 1k lotes de entrada (ambas patas): ${(minLotsA * 1.17 * (margenPctA/100) + minLotsB * (margenPctB/100)):.2f} USD")
print("\nPrimeras operaciones del Backtest:")
for t in finished_trades[:8]:
    if t['isSubtotal']:
        print(f"==> {t['signalType']} [Cierre {t['exitDate']}] | Margen Total: ${t['margin']} | Retorno: {t['returnPct']}% | Subtotal PnL: ${t['pnl']}")
    else:
        print(f"    Trade #{t['tradeNum']} [{t['signalType']}] Margen: ${t['margin']} (Nominal: ${t['nominal']}) [{t['multA']}x minLotsA + {t['multB']}x minLotsB] PnL: ${t['pnl']}")
