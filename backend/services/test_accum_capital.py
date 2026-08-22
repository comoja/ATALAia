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

dates = [str(d)[:10] for d in df['datetime']]
pricesA = df['priceA'].values
pricesB = df['priceB'].values
normA_vals = normA.values
normB_vals = normB.values
emaA_vals = emaA.values
emaB_vals = emaB.values
stdAboveA_vals = stdAboveA.values
stdBelowA_vals = stdBelowA.values
stdAboveB_vals = stdAboveB.values
stdBelowB_vals = stdBelowB.values
n = len(dates)

initialCapital = 800.0
allocationPct = 3.0
minLotsA, minLotsB = 1000.0, 1000.0
margenPctA, margenPctB = 0.25, 1.00
pipA, pipB = 0.00010, 0.01000
quoteA, quoteB = "USD", "MXN"
nameA, nameB = "EUR/USD", "USD/MXN"
commissionBps, slippageBps = 2.0, 1.0
costRate = (commissionBps + slippageBps) / 10000.0
allocationRate = allocationPct / 100.0
margenRateA = margenPctA / 100.0
margenRateB = margenPctB / 100.0

active_trades = []
finished_trades = []
raw_trades_count = 0
cycle_counter = 0
equity = initialCapital
cycle_start_equity = equity

for i in range(1, n):
    d = dates[i]
    pxA = pricesA[i]
    pxB = pricesB[i]

    currP_A, prevP_A = normA_vals[i], normA_vals[i - 1]
    currE_A, prevE_A = emaA_vals[i], emaA_vals[i - 1]
    currP_B, prevP_B = normB_vals[i], normB_vals[i - 1]
    currE_B, prevE_B = emaB_vals[i], emaB_vals[i - 1]

    isCrossDownHigh_A = (prevP_A >= prevE_A and currP_A < currE_A and currP_A >= stdAboveA_vals[i])
    isCrossUpLow_A = (prevP_A <= prevE_A and currP_A > currE_A and currP_A <= stdBelowA_vals[i])
    hasSignal_A = (isCrossDownHigh_A or isCrossUpLow_A)

    isCrossDownHigh_B = (prevP_B >= prevE_B and currP_B < currE_B and currP_B >= stdAboveB_vals[i])
    isCrossUpLow_B = (prevP_B <= prevE_B and currP_B > currE_B and currP_B <= stdBelowB_vals[i])
    hasSignal_B = (isCrossDownHigh_B or isCrossUpLow_B)

    prevDiffReal = normA_vals[i - 1] - normB_vals[i - 1]
    currDiff = normA_vals[i] - normB_vals[i]
    isMeanCross = (prevDiffReal > 0 and currDiff <= 0) or (prevDiffReal < 0 and currDiff >= 0)

    if isMeanCross and active_trades:
        cycle_counter += 1
        cycle_trades = []
        cycle_pnl = 0.0
        cycle_margin = sum(t["margin"] for t in active_trades)
        cycle_units_a = sum(t["unitsA"] for t in active_trades)
        cycle_units_b = sum(t["unitsB"] for t in active_trades)

        for t in active_trades:
            raw_trades_count += 1
            if "LONG " + nameA in t["direction"]:
                deltaA = (pxA - t["entryPxA"])
                deltaB = (t["entryPxB"] - pxB)
            else:
                deltaA = (t["entryPxA"] - pxA)
                deltaB = (pxB - t["entryPxB"])

            pipsA = deltaA / pipA if pipA > 0 else 0.0
            pipsB = deltaB / pipB if pipB > 0 else 0.0

            pipValA = t["unitsA"] * pipA if quoteA == "USD" else ((t["unitsA"] * pipA) / pxA if pxA > 0 else t["unitsA"] * pipA)
            pipValB = t["unitsB"] * pipB if quoteB == "USD" else ((t["unitsB"] * pipB) / pxB if pxB > 0 else t["unitsB"] * pipB)

            pnlA = pipsA * pipValA
            pnlB = pipsB * pipValB

            nominalA = t["unitsA"] * pxA if quoteA == "USD" else t["unitsA"]
            nominalB = t["unitsB"] if quoteB != "USD" else t["unitsB"] * pxB
            costs = (nominalA + nominalB) * (costRate * 2.0)

            tradePnl = pnlA + pnlB - costs
            tradeNetRet = (tradePnl / t["margin"]) if t["margin"] > 0 else 0.0

            equity += tradePnl
            cycle_pnl += tradePnl

            cycle_trades.append({
                "tradeNum": raw_trades_count,
                "isSubtotal": False,
                "isOpen": False,
                "cycleNum": cycle_counter,
                "signalType": t["signalType"],
                "direction": t["direction"],
                "entryDate": t["entryDate"],
                "exitDate": d,
                "allocatedCapital": round(float(t["margin"]), 2),
                "accumCapital": round(float(equity), 2),
                "returnPct": round(float(tradeNetRet * 100.0), 2),
                "pnl": round(float(tradePnl), 2),
                "isWin": bool(tradePnl > 0)
            })

        for ct in cycle_trades:
            finished_trades.append(ct)

        entrySpan = f"{cycle_trades[0]['entryDate']} a {cycle_trades[-1]['entryDate']}" if len(cycle_trades) > 1 else cycle_trades[0]['entryDate']
        finished_trades.append({
            "tradeNum": None,
            "isSubtotal": True,
            "isOpen": False,
            "cycleNum": cycle_counter,
            "signalType": f"SUBTOTAL CIERRE #{cycle_counter}",
            "direction": f"● Salida Media ({len(cycle_trades)} ops)",
            "entryDate": f"Entradas: {entrySpan}",
            "exitDate": d,
            "allocatedCapital": round(float(cycle_margin), 2),
            "accumCapital": round(float(equity), 2),
            "returnPct": round((cycle_pnl / cycle_margin) * 100.0, 2) if cycle_margin > 0 else 0.0,
            "pnl": round(float(cycle_pnl), 2),
            "isWin": bool(cycle_pnl > 0)
        })

        cycle_start_equity = equity
        active_trades = []

    # Entrada
    sigType, direction = None, None
    if hasSignal_A and hasSignal_B:
        if isCrossUpLow_A and isCrossDownHigh_B:
            sigType = f"TRIANGULO_VERDE ({nameA} + {nameB})"
            direction = f"LONG {nameA} / SHORT {nameB}"
        elif isCrossDownHigh_A and isCrossUpLow_B:
            sigType = f"TRIANGULO_ROJO ({nameA} + {nameB})"
            direction = f"SHORT {nameA} / LONG {nameB}"
    elif hasSignal_A or hasSignal_B:
        if hasSignal_A:
            if isCrossUpLow_A:
                sigType = f"CUADRO_VERDE {nameA}"
                direction = f"LONG {nameA} / SHORT {nameB}"
            elif isCrossDownHigh_A:
                sigType = f"CUADRO_ROJO {nameA}"
                direction = f"SHORT {nameA} / LONG {nameB}"
        elif hasSignal_B:
            if isCrossUpLow_B:
                sigType = f"CUADRO_VERDE {nameB}"
                direction = f"SHORT {nameA} / LONG {nameB}"
            elif isCrossDownHigh_B:
                sigType = f"CUADRO_ROJO {nameB}"
                direction = f"LONG {nameA} / SHORT {nameB}"

    if sigType and direction:
        totalEntryBudget = cycle_start_equity * allocationRate
        budgetA = totalEntryBudget / 2.0
        budgetB = totalEntryBudget / 2.0

        margen1LotA = (minLotsA * pxA) * margenRateA
        margen1LotB = (minLotsB * (1.0 if quoteB == "MXN" else pxB)) * margenRateB

        multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA) else 1
        multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB) else 1

        active_trades.append({
            "signalType": sigType,
            "direction": direction,
            "entryDate": d,
            "entryIndex": i,
            "entryPxA": pxA,
            "entryPxB": pxB,
            "margin": (multA * margen1LotA) + (multB * margen1LotB),
            "multA": multA,
            "multB": multB,
            "unitsA": multA * minLotsA,
            "unitsB": multB * minLotsB
        })

print("First 6 trades with accumCapital:")
for t in finished_trades[:6]:
    print(f"Trade #{t['tradeNum']} | PnL: ${t['pnl']} | Capital: ${t['accumCapital']}")
