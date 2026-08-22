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

initialCapital = 10000.0
equity = initialCapital
allocationRate = 0.20
costMultiplier = 0.0003

active_trades = []
finished_trades = []
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
        cycle_invested = sum(t['allocatedCapital'] for t in active_trades)
        
        for t in active_trades:
            if t['direction'] == 'LONG A / SHORT B':
                rA = (pxA - t['entryPxA']) / t['entryPxA']
                rB = (t['entryPxB'] - pxB) / t['entryPxB']
            else:
                rA = (t['entryPxA'] - pxA) / t['entryPxA']
                rB = (pxB - t['entryPxB']) / t['entryPxB']
            netRet = ((rA + rB) / 2.0) - costMultiplier
            tradePnl = t['allocatedCapital'] * netRet
            equity += tradePnl
            cycle_pnl += tradePnl
            cycle_trades.append({
                'tradeNum': len(finished_trades) + len(cycle_trades) + 1,
                'cycleNum': cycle_counter,
                'signalType': t['signalType'],
                'direction': t['direction'],
                'entryDate': t['entryDate'],
                'exitDate': dt,
                'allocatedCapital': round(t['allocatedCapital'], 2),
                'entryPriceA': round(t['entryPxA'], 5),
                'exitPriceA': round(pxA, 5),
                'entryPriceB': round(t['entryPxB'], 5),
                'exitPriceB': round(pxB, 5),
                'durationBars': i - t['entryIndex'],
                'returnPct': round(netRet * 100, 2),
                'pnl': round(tradePnl, 2),
                'isWin': bool(tradePnl > 0)
            })
            
        cycleAvgRet = round((cycle_pnl / cycle_invested) * 100, 2) if cycle_invested > 0 else 0.0
        for ct in cycle_trades:
            ct['cycleTotalTrades'] = len(cycle_trades)
            ct['cycleTotalInvested'] = round(cycle_invested, 2)
            ct['cycleTotalPnl'] = round(cycle_pnl, 2)
            ct['cycleAvgReturnPct'] = cycleAvgRet
            finished_trades.append(ct)
            
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
        tradeCap = equity * allocationRate
        active_trades.append({
            'signalType': sigType,
            'direction': direction,
            'entryDate': dt,
            'entryIndex': i,
            'entryPxA': pxA,
            'entryPxB': pxB,
            'allocatedCapital': tradeCap
        })

cycles = {}
for t in finished_trades:
    c = t['cycleNum']
    if c not in cycles:
        cycles[c] = {
            'exitDate': t['exitDate'],
            'trades': t['cycleTotalTrades'],
            'invested': t['cycleTotalInvested'],
            'pnl': t['cycleTotalPnl'],
            'ret': t['cycleAvgReturnPct']
        }

print('Total closing cycles:', len(cycles))
for c_id, c_data in cycles.items():
    print(f" Ciclo #{c_id} [Salida {c_data['exitDate']}]: {c_data['trades']} ops | Subtotal Invertido: ${c_data['invested']} | Retorno Subtotal: {c_data['ret']}% | Subtotal PnL: ${c_data['pnl']}")
