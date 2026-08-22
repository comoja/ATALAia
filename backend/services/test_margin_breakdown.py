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
quoteA, quoteB = "USD", "MXN"
nameA, nameB = "EUR/USD", "USD/MXN"
allocationRate = allocationPct / 100.0
margenRateA = margenPctA / 100.0
margenRateB = margenPctB / 100.0

totalEntryBudget = initialCapital * allocationRate # $24.00
budgetA = totalEntryBudget / 2.0 # $12.00
budgetB = totalEntryBudget / 2.0 # $12.00

pxA = pricesA[10]
pxB = pricesB[10]
margen1LotA = (minLotsA * pxA) * margenRateA # 1000 * 1.17 * 0.0025 = $2.93
margen1LotB = (minLotsB * (1.0 if quoteB == "MXN" else pxB)) * margenRateB # 1000 * 1.0 * 0.01 = $10.00

multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA) else 1
multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB) else 1

realMargenA = multA * margen1LotA
realMargenB = multB * margen1LotB
totalMargen = realMargenA + realMargenB

print(f"Capital: ${initialCapital}")
print(f"Total Budget (3%): ${totalEntryBudget}")
print(f"Total Margin Sum: ${totalMargen:.2f}")
print(f"  {nameA}: Margin ${realMargenA:.2f} ({multA * minLotsA} lotes, {multA}x)")
print(f"  {nameB}: Margin ${realMargenB:.2f} ({multB * minLotsB} lotes, {multB}x)")
