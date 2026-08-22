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

# Check signals in last 15 bars
for i in range(len(df)-15, len(df)):
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
    
    dt = df['datetime'].iloc[i][:10]
    print(f"Date: {dt} | Signal A: {hasSignal_A} (UpLow: {isCrossUpLow_A}, DownHigh: {isCrossDownHigh_A}) | Signal B: {hasSignal_B} (UpLow: {isCrossUpLow_B}, DownHigh: {isCrossDownHigh_B})")
