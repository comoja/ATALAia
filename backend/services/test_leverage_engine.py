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

# Simulamos con cuenta.capital = $1,000.00 USD y Apalancamiento 1:100
initialCapital = 1000.0
leverage = 100.0
allocationRate = 0.20 # 20% de margen de la cuenta
costMultiplier = 0.0003

minLotsA = 1000.0 # EUR/USD
minLotsB = 1000.0 # USD/MXN

equity = initialCapital
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
    
    if isMeanCross and active_trades:
        cycle_counter += 1
        cycle_trades = []
        cycle_pnl = 0.0
        cycle_invested = sum(t['margin'] for t in active_trades)
        
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
            netRet = (tradePnl / t['margin']) if t['margin'] > 0 else 0.0
            
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
                'returnPct': round(netRet * 100, 2),
                'pnl': round(tradePnl, 2),
                'isWin': bool(tradePnl > 0)
            })
            
        cycleAvgRet = round((cycle_pnl / cycle_invested) * 100, 2) if cycle_invested > 0 else 0.0
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
            'margin': round(cycle_invested, 2),
            'nominal': sum(ct['nominal'] for ct in cycle_trades),
            'multA': None,
            'multB': None,
            'returnPct': cycleAvgRet,
            'pnl': round(cycle_pnl, 2),
            'isWin': bool(cycle_pnl > 0)
        })
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
        targetMargenA = (equity * allocationRate) / 2.0
        targetMargenB = (equity * allocationRate) / 2.0
        
        # Margen requerido por 1 min_lots con apalancamiento 1:100
        margen1LotA = (minLotsA * pxA) / leverage
        margen1LotB = (minLotsB * 1.0) / leverage
        
        multA = max(1, int(round(targetMargenA / margen1LotA)))
        multB = max(1, int(round(targetMargenB / margen1LotB)))
        
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

print(f"Cuenta Capital: ${initialCapital:.2f} USD | Apalancamiento: 1:{int(leverage)}")
print(f"Capital Final: ${equity:.2f} USD | Ganancia Neta: +${equity - initialCapital:.2f} USD ({(equity - initialCapital)/initialCapital*100:.2f}%)")
print("Trades con Apalancamiento 1:100:")
for t in finished_trades[:8]:
    if t['isSubtotal']:
        print(f"==> {t['signalType']} [Cierre {t['exitDate']}] | Margen Invertido: ${t['margin']} | Retorno: {t['returnPct']}% | PnL: ${t['pnl']}")
    else:
        print(f"    Trade #{t['tradeNum']} [{t['signalType']}] Margen: ${t['margin']} (Nominal: ${t['nominal']}) [{t['multA']}x minLotsA + {t['multB']}x minLotsB] PnL: ${t['pnl']}")
