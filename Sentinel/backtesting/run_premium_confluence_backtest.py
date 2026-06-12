import sys
import os
import pandas as pd
import numpy as np
import talib as ta
from datetime import datetime, timedelta

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from Sentinel.analysis import technical

# Multiplicadores de pip por símbolo para cálculo de pips y spreads
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
}

# Spreads típicos estimados en pips/puntos por activo
SPREADS = {
    'EUR/USD': 1.0,    # 1.0 pip
    'GBP/USD': 1.5,    # 1.5 pips
    'AUD/USD': 1.2,    # 1.2 pips
    'NZD/USD': 1.5,    # 1.5 pips
    'USD/CAD': 1.5,    # 1.5 pips
    'USD/CHF': 1.6,    # 1.6 pips
    'EUR/GBP': 1.5,    # 1.5 pips
    'GBP/CAD': 2.2,    # 2.2 pips
    'GBP/JPY': 2.0,    # 2.0 pips (jpy pips)
    'USD/JPY': 1.2,    # 1.2 pips
    'USD/MXN': 25.0,   # 25.0 pips de spread (forex exótico)
    'XAU/USD': 0.35,   # 35 centavos de spread en Oro
}

def loadActiveSymbols() -> list:
    """Obtiene de forma dinámica la lista de símbolos activos desde la tabla SentinelSymbol."""
    try:
        connection = dbConnection.getConnection()
        if connection is None:
            return []
        cursor = connection.cursor()
        cursor.execute("SELECT symbol FROM SentinelSymbol WHERE Activo = 1")
        symbols = [row[0] for row in cursor.fetchall()]
        cursor.close()
        connection.close()
        return symbols
    except Exception as e:
        print(f"❌ Error al cargar símbolos activos de la BD: {e}")
        return []

def loadCandles(symbol: str, startDate: str, endDate: str) -> pd.DataFrame:
    """Carga velas de 5min desde la base de datos MySQL para un simbolo en el rango de fechas."""
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
        
        if df.empty:
            return pd.DataFrame()
            
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        return df
    except Exception as e:
        print(f"❌ Error al cargar velas para {symbol}: {e}")
        return pd.DataFrame()

def calculateSmoothedHeikinAshi(df: pd.DataFrame, period1: int = 10, period2: int = 10) -> tuple:
    """Calcula velas Heikin Ashi Suavizadas (Smoothed HA) usando EMAs."""
    openSmooth = df['open'].ewm(span=period1, adjust=False).mean()
    highSmooth = df['high'].ewm(span=period1, adjust=False).mean()
    lowSmooth = df['low'].ewm(span=period1, adjust=False).mean()
    closeSmooth = df['close'].ewm(span=period1, adjust=False).mean()
    
    haClose = (openSmooth + highSmooth + lowSmooth + closeSmooth) / 4.0
    
    haOpen = np.zeros(len(df))
    haOpen[0] = (openSmooth.iloc[0] + closeSmooth.iloc[0]) / 2.0
    
    for i in range(1, len(df)):
        haOpen[i] = (haOpen[i - 1] + haClose.iloc[i - 1]) / 2.0
        
    haOpenSeries = pd.Series(haOpen, index=df.index)
    
    haCloseSmooth = haClose.ewm(span=period2, adjust=False).mean()
    haOpenSmooth = haOpenSeries.ewm(span=period2, adjust=False).mean()
    
    return haCloseSmooth, haOpenSmooth

def runBacktest() -> None:
    print("==========================================================")
    print("     BACKTEST REALISTA: ESTRATEGIA PREMIUM CONFLUENCE     ")
    print("==========================================================")
    
    activeSymbols = loadActiveSymbols()
    if not activeSymbols:
        print("❌ No se encontraron símbolos activos en SentinelSymbol. Abortando.")
        return
        
    print(f"Símbolos activos cargados dinámicamente: {activeSymbols}")
    
    # 3 semanas de simulación
    startDateStr = '2026-05-21 00:00:00'
    endDateStr = '2026-06-11 14:00:00'
    
    print(f"Rango de simulación: del {startDateStr} al {endDateStr}")
    print(f"Capital Inicial: $10,000 USD | Riesgo por trade: $100 USD (1.0%)\n")
    
    initialCapital = 10000.0
    
    allTrades = []
    
    for symbol in activeSymbols:
        print(f"⚙️ Procesando {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 150:
            print(f"  ⚠️ Datos insuficientes en BD para {symbol}. Saltando.")
            continue
            
        # Resamplear a 15min (timeframe operacional de la estrategia)
        df15m = df5m.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        df15m.ffill(inplace=True)
        df15m.bfill(inplace=True)
        
        if len(df15m) < 50:
            print(f"  ⚠️ Datos insuficientes tras resampling a 15min para {symbol}.")
            continue
            
        # --- Cálculo de Indicadores ---
        closePrices = df15m['close'].values.astype(float)
        highPrices = df15m['high'].values.astype(float)
        lowPrices = df15m['low'].values.astype(float)
        
        # 1. EMA 200
        ema200 = ta.EMA(closePrices, timeperiod=200)
        
        # 2. Supertrend (10, 1.5)
        stTrend, stTrail = technical.calculateAtrStop(df15m, period=10, multiplier=1.5)
        
        # 3. Velas Heikin Ashi Suavizadas (10, 10)
        haCloseSmooth, haOpenSmooth = calculateSmoothedHeikinAshi(df15m, 10, 10)
        
        # 4. Impulse MACD (20, 9)
        impulseMacd, impulseSignal = technical.calculateImpulseMacd(df15m, lengthMa=20, lengthSignal=9)
        
        # 5. ATR de 14 para filtros de riesgo
        atr14 = ta.ATR(highPrices, lowPrices, closePrices, timeperiod=14)
        
        # Pip multiplier y spread para este activo
        # Fallback a 10000.0 si no se encuentra en el diccionario
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPips = SPREADS.get(symbol, 1.0)
        spreadPrice = spreadPips / pipMult
        
        # Bucle de simulación histórica
        # Empezamos en 200 para tener EMAs y osciladores totalmente cargados
        i = 200
        while i < len(df15m):
            dt = df15m.index[i]
            cPrice = float(df15m['close'].iloc[i])
            cEma200 = ema200[i]
            
            if pd.isna(cEma200) or pd.isna(atr14[i]):
                i += 1
                continue
                
            # Estados de tendencia e indicadores
            macroBullish = cPrice > cEma200
            macroBearish = cPrice < cEma200
            
            stBullish = stTrend[i] == 1
            stBearish = stTrend[i] == -1
            
            haBullish = haCloseSmooth.iloc[i] > haOpenSmooth.iloc[i]
            haBearish = haCloseSmooth.iloc[i] < haOpenSmooth.iloc[i]
            
            macdBullish = impulseMacd.iloc[i] > impulseSignal.iloc[i]
            macdBearish = impulseMacd.iloc[i] < impulseSignal.iloc[i]
            
            # 5. MSS / Ruptura Estructural (máximo/mínimo estructural de las últimas 8 velas anteriores)
            maxHighPrev = float(df15m['high'].iloc[i-8:i].max())
            minLowPrev = float(df15m['low'].iloc[i-8:i].min())
            
            mssBullish = cPrice > maxHighPrev
            mssBearish = cPrice < minLowPrev
            
            direction = None
            if macroBullish and stBullish and haBullish and macdBullish and mssBullish:
                direction = 'LARGO'
            elif macroBearish and stBearish and haBearish and macdBearish and mssBearish:
                direction = 'CORTO'
                
            if direction:
                # --- Filtros de Riesgo de Disparo ---
                slPrice = float(stTrail[i])
                slDist = abs(cPrice - slPrice)
                
                # Buffer del Stop Loss
                atrVal = float(atr14[i])
                
                # Filtro de Spike de disparo y SL sobre-extendido
                triggerRange = float(df15m['high'].iloc[i] - df15m['low'].iloc[i])
                if triggerRange > 2.0 * atrVal or slDist > 2.0 * atrVal or slDist <= 0.0:
                    i += 1
                    continue
                    
                # Precio de Entrada Real ajustando spread
                entryPrice = cPrice + (spreadPrice / 2.0) if direction == 'LARGO' else cPrice - (spreadPrice / 2.0)
                
                # Calcular niveles de trade
                if direction == 'LARGO':
                    takeProfit = entryPrice + (1.5 * slDist)
                else:
                    takeProfit = entryPrice - (1.5 * slDist)
                    
                # Simular ciclo de vida del trade en las velas posteriores
                tradeClosed = False
                exitPrice = 0.0
                exitDt = None
                pipsResult = 0.0
                
                for j in range(i + 1, len(df15m)):
                    vHigh = float(df15m['high'].iloc[j])
                    vLow = float(df15m['low'].iloc[j])
                    vClose = float(df15m['close'].iloc[j])
                    jDt = df15m.index[j]
                    
                    if direction == 'LARGO':
                        lowAdjusted = vLow - (spreadPrice / 2.0)
                        highAdjusted = vHigh + (spreadPrice / 2.0)
                        
                        if lowAdjusted <= slPrice:
                            exitPrice = slPrice
                            pipsResult = -abs(entryPrice - slPrice) * pipMult
                            tradeClosed = True
                        elif highAdjusted >= takeProfit:
                            exitPrice = takeProfit
                            pipsResult = abs(takeProfit - entryPrice) * pipMult
                            tradeClosed = True
                    else:
                        highAdjusted = vHigh + (spreadPrice / 2.0)
                        lowAdjusted = vLow - (spreadPrice / 2.0)
                        
                        if highAdjusted >= slPrice:
                            exitPrice = slPrice
                            pipsResult = -abs(slPrice - entryPrice) * pipMult
                            tradeClosed = True
                        elif lowAdjusted <= takeProfit:
                            exitPrice = takeProfit
                            pipsResult = abs(entryPrice - takeProfit) * pipMult
                            tradeClosed = True
                            
                    if tradeClosed:
                        pnlUsd = 150.0 if pipsResult > 0 else -100.0
                        exitDt = jDt
                        i = j
                        break
                        
                if tradeClosed:
                    allTrades.append({
                        'Símbolo': symbol,
                        'Fecha Entrada': dt,
                        'Fecha Salida': exitDt,
                        'Dirección': direction,
                        'Entrada': round(entryPrice, 5),
                        'Stop Loss': round(slPrice, 5),
                        'Take Profit': round(takeProfit, 5),
                        'Resultado Pips': round(pipsResult, 1),
                        'PnL USD': pnlUsd
                    })
            i += 1
            
    # --- PROCESAMIENTO Y METRICAS DEL BACKTEST ---
    print("\n==========================================================")
    print("             ESTADÍSTICAS GLOBALES DEL BACKTEST           ")
    print("==========================================================")
    
    if not allTrades:
        print("❌ No se generaron trades durante las últimas 3 semanas para esta estrategia.")
        return
        
    dfTrades = pd.DataFrame(allTrades)
    
    totalTrades = len(dfTrades)
    winningTrades = len(dfTrades[dfTrades['PnL USD'] > 0])
    losingTrades = len(dfTrades[dfTrades['PnL USD'] <= 0])
    winRate = (winningTrades / totalTrades) * 100
    
    totalProfit = dfTrades[dfTrades['PnL USD'] > 0]['PnL USD'].sum()
    totalLoss = abs(dfTrades[dfTrades['PnL USD'] <= 0]['PnL USD'].sum())
    profitFactor = totalProfit / totalLoss if totalLoss > 0 else float('inf')
    
    netPnL = dfTrades['PnL USD'].sum()
    pctReturn = (netPnL / initialCapital) * 100
    
    # Calcular Máximo Drawdown
    dfTrades = dfTrades.sort_values(by='Fecha Entrada')
    capitalHistory = [initialCapital]
    currentCap = initialCapital
    for pnl in dfTrades['PnL USD']:
        currentCap += pnl
        capitalHistory.append(currentCap)
        
    peak = initialCapital
    maxDrawdown = 0.0
    for cap in capitalHistory:
        if cap > peak:
            peak = cap
        dd = (peak - cap) / peak * 100
        if dd > maxDrawdown:
            maxDrawdown = dd
            
    print(f"Total Operaciones  : {totalTrades}")
    print(f"Ganadoras          : {winningTrades}")
    print(f"Perdedoras         : {losingTrades}")
    print(f"Win Rate (Efect.)  : {winRate:.2f}%")
    print(f"Profit Factor      : {profitFactor:.2f}")
    print(f"PnL Neto Total     : ${netPnL:.2f} USD")
    print(f"Retorno %          : {pctReturn:.2f}%")
    print(f"Máximo Drawdown %  : {maxDrawdown:.2f}%")
    print("==========================================================\n")
    
    # Mostrar desglose por símbolo
    print("----------------------------------------------------------")
    print("            DESGLOSE DE RENDIMIENTO POR ACTIVO            ")
    print("----------------------------------------------------------")
    
    grouped = dfTrades.groupby('Símbolo')
    breakdown = []
    for name, group in grouped:
        tCount = len(group)
        wCount = len(group[group['PnL USD'] > 0])
        wRate = (wCount / tCount) * 100
        pnlAct = group['PnL USD'].sum()
        pipsAct = group['Resultado Pips'].sum()
        breakdown.append({
            'Símbolo': name,
            'Trades': tCount,
            'Win Rate': f"{wRate:.1f}%",
            'Pips': round(pipsAct, 1),
            'PnL USD': f"${pnlAct:.2f}"
        })
    dfBreakdown = pd.DataFrame(breakdown)
    print(dfBreakdown.to_string(index=False))
    print("----------------------------------------------------------\n")
    
    # Guardar resultados
    rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/backtest_premium_confluence_trades.csv"
    dfTrades.to_csv(rawPath, index=False)
    print(f"💾 Reporte completo de trades guardado en: {rawPath}")

if __name__ == '__main__':
    runBacktest()
