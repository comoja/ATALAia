import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import json
from datetime import datetime

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

ALL_SYMBOLS = [
    'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
    'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
    'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
]

PIP_MULTIPLIERS = {
    'EUR/USD': 10000.0,
    'GBP/USD': 10000.0,
    'AUD/USD': 10000.0,
    'NZD/USD': 10000.0,
    'USD/CAD': 10000.0,
    'USD/CHF': 10000.0,
    'EUR/GBP': 10000.0,
    'GBP/CAD': 10000.0,
    'GBP/JPY': 100.0,
    'USD/JPY': 100.0,
    'USD/MXN': 10000.0,
    'XAU/USD': 1.0,
    'BTC/USD': 1.0,
}

SPREADS = {
    'EUR/USD': 1.0,
    'GBP/USD': 1.5,
    'AUD/USD': 1.2,
    'NZD/USD': 1.5,
    'USD/CAD': 1.5,
    'USD/CHF': 1.6,
    'EUR/GBP': 1.5,
    'GBP/CAD': 2.2,
    'GBP/JPY': 2.0,
    'USD/JPY': 1.2,
    'USD/MXN': 25.0,
    'XAU/USD': 0.35,
    'BTC/USD': 30.0,
}

def loadCandles(symbol: str, startDate: str, endDate: str) -> pd.DataFrame:
    try:
        connection = dbConnection.getConnection()
        if connection is None:
            return pd.DataFrame()
        query = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = '5min' AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, connection, params=(symbol, startDate, endDate))
        connection.close()
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
        return df
    except Exception as e:
        print(f"Error cargando velas: {e}")
        return pd.DataFrame()

def calculateIchimokuIndicators(df: pd.DataFrame, tenkan: int, kijun: int, senkou: int, displacement: int) -> pd.DataFrame:
    dfResult = df.copy()
    
    # BB (20, 2)
    dfResult['bb_upper'], dfResult['bb_middle'], dfResult['bb_lower'] = ta.BBANDS(
        dfResult['close'].values, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0
    )
    
    # Ichimoku
    highT = dfResult['high'].rolling(window=tenkan).max()
    lowT = dfResult['low'].rolling(window=tenkan).min()
    dfResult['tenkan_sen'] = (highT + lowT) / 2
    
    highK = dfResult['high'].rolling(window=kijun).max()
    lowK = dfResult['low'].rolling(window=kijun).min()
    dfResult['kijun_sen'] = (highK + lowK) / 2
    
    dfResult['senkou_span_a'] = ((dfResult['tenkan_sen'] + dfResult['kijun_sen']) / 2).shift(displacement)
    
    highS = dfResult['high'].rolling(window=senkou).max()
    lowS = dfResult['low'].rolling(window=senkou).min()
    dfResult['senkou_span_b'] = ((highS + lowS) / 2).shift(displacement)
    
    # SMC Volume MA (14)
    dfResult['volumeMa'] = dfResult['volume'].rolling(window=14).mean()
    
    # Sweep localHigh/localLow
    dfResult['localHigh'] = dfResult['high'].shift(1).rolling(window=20).max()
    dfResult['localLow'] = dfResult['low'].shift(1).rolling(window=20).min()
    
    return dfResult

def calculateHtfTrendSeries(dfHtf: pd.DataFrame, tenkan: int, kijun: int, senkou: int, displacement: int) -> pd.Series:
    highT = dfHtf['high'].rolling(window=tenkan).max()
    lowT = dfHtf['low'].rolling(window=tenkan).min()
    tenkanH = (highT + lowT) / 2
    
    highK = dfHtf['high'].rolling(window=kijun).max()
    lowK = dfHtf['low'].rolling(window=kijun).min()
    kijunH = (highK + lowK) / 2
    
    spanA = ((tenkanH + kijunH) / 2).shift(displacement)
    
    highS = dfHtf['high'].rolling(window=senkou).max()
    lowS = dfHtf['low'].rolling(window=senkou).min()
    spanB = ((highS + lowS) / 2).shift(displacement)
    
    trend = pd.Series("NEUTRAL", index=dfHtf.index)
    for idx in range(len(dfHtf)):
        closeVal = dfHtf['close'].iloc[idx]
        spanAVal = spanA.iloc[idx]
        spanBVal = spanB.iloc[idx]
        if pd.isna(spanAVal) or pd.isna(spanBVal):
            continue
        if closeVal > max(spanAVal, spanBVal):
            trend.iloc[idx] = "ALCISTA"
        elif closeVal < min(spanAVal, spanBVal):
            trend.iloc[idx] = "BAJISTA"
    return trend

def runIchimokuGridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (ICHIMOKU)        ")
    print("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros Ichimoku
    # (tenkan, kijun, senkou, displacement)
    ichimokuCombos = [
        (6, 18, 36, 18),
        (7, 22, 44, 22),
        (9, 26, 52, 26),
        (10, 30, 60, 30),
        (12, 36, 72, 36),
        (15, 45, 90, 45)
    ]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 300:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        # Resamplear timeframes necesarios
        df15mRaw = df5m.resample('15min').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
        df30mRaw = df5m.resample('30min').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
        df1hRaw = df5m.resample('1h').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
        
        if len(df15mRaw) < 150 or len(df30mRaw) < 100:
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for tenkan, kijun, senkou, displacement in ichimokuCombos:
            # Calcular indicadores en 15m y 30m
            df15m = calculateIchimokuIndicators(df15mRaw, tenkan, kijun, senkou, displacement)
            df30m = calculateIchimokuIndicators(df30mRaw, tenkan, kijun, senkou, displacement)
            
            # Calcular tendencias HTF
            trend30m = calculateHtfTrendSeries(df30mRaw, tenkan, kijun, senkou, displacement) # HTF para 15m
            trend1h = calculateHtfTrendSeries(df1hRaw, tenkan, kijun, senkou, displacement) # HTF para 30m
            
            for minRr in minRrCombos:
                # Simular la cascada: iterar sobre los timestamps de 15 minutos
                trades = []
                activeTrade = None # {'direction': str, 'entry': float, 'sl': float, 'tp': float}
                
                # Iterar barra por barra en 15min
                # Empezamos en un offset seguro para tener indicadores calculados
                idx15m = 80
                while idx15m < len(df15m):
                    rowTime = df15m.index[idx15m]
                    row15m = df15m.iloc[idx15m]
                    
                    # Si ya tenemos un trade activo, evaluamos salidas
                    if activeTrade:
                        vHigh = float(row15m['high'])
                        vLow = float(row15m['low'])
                        
                        if activeTrade['direction'] == 'LARGO':
                            lowAdjusted = vLow - (spreadPrice / 2.0)
                            highAdjusted = vHigh + (spreadPrice / 2.0)
                            if lowAdjusted <= activeTrade['sl']:
                                trades.append(-100.0)
                                activeTrade = None
                            elif highAdjusted >= activeTrade['tp']:
                                trades.append(100.0 * minRr)
                                activeTrade = None
                        else:
                            highAdjusted = vHigh + (spreadPrice / 2.0)
                            lowAdjusted = vLow - (spreadPrice / 2.0)
                            if highAdjusted >= activeTrade['sl']:
                                trades.append(-100.0)
                                activeTrade = None
                            elif lowAdjusted <= activeTrade['tp']:
                                trades.append(100.0 * minRr)
                                activeTrade = None
                                
                        idx15m += 1
                        continue
                    
                    # ── EVALUAR ENTRADAS EN CASCADA ──
                    
                    # 1. Chequear canal de 30min primero
                    # Buscamos la vela de 30min correspondiente que haya cerrado antes o en este timestamp
                    idx30mList = df30m.index[df30m.index <= rowTime]
                    if len(idx30mList) > 0:
                        last30mTime = idx30mList[-1]
                        row30m = df30m.loc[last30mTime]
                        
                        # Buscar tendencia HTF para 30m (que es 1h)
                        idx1hList = trend1h.index[trend1h.index <= last30mTime]
                        htfTrend30 = trend1h.loc[idx1hList[-1]] if len(idx1hList) > 0 else "NEUTRAL"
                        
                        # Condiciones 30min
                        close30 = float(row30m['close'])
                        open30 = float(row30m['open'])
                        color30 = "Verde" if close30 > open30 else ("Roja" if close30 < open30 else "Neutro")
                        
                        tenkan30 = float(row30m['tenkan_sen'])
                        kijun30 = float(row30m['kijun_sen'])
                        spanA30 = float(row30m['senkou_span_a'])
                        spanB30 = float(row30m['senkou_span_b'])
                        bbMiddle30 = float(row30m['bb_middle'])
                        bbUpper30 = float(row30m['bb_upper'])
                        bbLower30 = float(row30m['bb_lower'])
                        
                        kumoMax30 = max(spanA30, spanB30)
                        kumoMin30 = min(spanA30, spanB30)
                        
                        # Para simplificar, asumimos volumen y sweeps estables en backtest,
                        # evaluando las reglas básicas del Kumo + BB + color de vela
                        # Largo 30min
                        if (close30 > kumoMax30 and tenkan30 > kijun30 and close30 > bbMiddle30 
                                and spanA30 > spanB30 and color30 == "Verde" and htfTrend30 in ("ALCISTA", "NEUTRAL")):
                            slPrice = float(row30m['kijun_sen'])
                            if slPrice < close30:
                                activeTrade = {
                                    'direction': 'LARGO',
                                    'entry': close30 + (spreadPrice / 2.0),
                                    'sl': slPrice,
                                    'tp': close30 + (abs(close30 - slPrice) * minRr)
                                }
                        # Corto 30min
                        elif (close30 < kumoMin30 and tenkan30 < kijun30 and close30 < bbMiddle30 
                                and spanA30 < spanB30 and color30 == "Roja" and htfTrend30 in ("BAJISTA", "NEUTRAL")):
                            slPrice = float(row30m['kijun_sen'])
                            if slPrice > close30:
                                activeTrade = {
                                    'direction': 'CORTO',
                                    'entry': close30 - (spreadPrice / 2.0),
                                    'sl': slPrice,
                                    'tp': close30 - (abs(slPrice - close30) * minRr)
                                }
                                
                    # 2. Si no hay trade abierto en 30min, chequear canal de 15min
                    if not activeTrade:
                        idx30mListFor15 = trend30m.index[trend30m.index <= rowTime]
                        htfTrend15 = trend30m.loc[idx30mListFor15[-1]] if len(idx30mListFor15) > 0 else "NEUTRAL"
                        
                        close15 = float(row15m['close'])
                        open15 = float(row15m['open'])
                        color15 = "Verde" if close15 > open15 else ("Roja" if close15 < open15 else "Neutro")
                        
                        tenkan15 = float(row15m['tenkan_sen'])
                        kijun15 = float(row15m['kijun_sen'])
                        spanA15 = float(row15m['senkou_span_a'])
                        spanB15 = float(row15m['senkou_span_b'])
                        bbMiddle15 = float(row15m['bb_middle'])
                        
                        kumoMax15 = max(spanA15, spanB15)
                        kumoMin15 = min(spanA15, spanB15)
                        
                        # Largo 15min
                        if (close15 > kumoMax15 and tenkan15 > kijun15 and close15 > bbMiddle15 
                                and spanA15 > spanB15 and color15 == "Verde" and htfTrend15 in ("ALCISTA", "NEUTRAL")):
                            slPrice = float(row15m['kijun_sen'])
                            if slPrice < close15:
                                activeTrade = {
                                    'direction': 'LARGO',
                                    'entry': close15 + (spreadPrice / 2.0),
                                    'sl': slPrice,
                                    'tp': close15 + (abs(close15 - slPrice) * minRr)
                                }
                        # Corto 15min
                        elif (close15 < kumoMin15 and tenkan15 < kijun15 and close15 < bbMiddle15 
                                and spanA15 < spanB15 and color15 == "Roja" and htfTrend15 in ("BAJISTA", "NEUTRAL")):
                            slPrice = float(row15m['kijun_sen'])
                            if slPrice > close15:
                                activeTrade = {
                                    'direction': 'CORTO',
                                    'entry': close15 - (spreadPrice / 2.0),
                                    'sl': slPrice,
                                    'tp': close15 - (abs(slPrice - close15) * minRr)
                                }
                    idx15m += 1
                    
                # Evaluar métricas del combo
                tCount = len(trades)
                if tCount > 3:
                    wCount = len([t for t in trades if t > 0])
                    wRate = (wCount / tCount) * 100
                    pnlNet = sum(trades)
                    
                    profitCount = sum([t for t in trades if t > 0])
                    lossCount = abs(sum([t for t in trades if t <= 0]))
                    profFactor = profitCount / lossCount if lossCount > 0 else float('inf')
                    
                    comboData = {
                        'Símbolo': symbol,
                        'Tenkan': tenkan,
                        'Kijun': kijun,
                        'Senkou': senkou,
                        'Displacement': displacement,
                        'Min RR': minRr,
                        'Trades': tCount,
                        'Win Rate': f"{wRate:.1f}%",
                        'Profit Factor': round(profFactor, 2),
                        'PnL USD': pnlNet
                    }
                    allResultsRaw.append(comboData)
                    
                    if pnlNet > symbolBestProfit and profFactor >= 1.0:
                        symbolBestProfit = pnlNet
                        symbolBestCombo = comboData
                        
        if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            print(f"  🏆 Mejor combo para {symbol}: Tenkan={symbolBestCombo['Tenkan']} | Kijun={symbolBestCombo['Kijun']} | Senkou={symbolBestCombo['Senkou']} | RR={symbolBestCombo['Min RR']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}.")

    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/ichimoku_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/ichimoku_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runIchimokuGridSearch()
