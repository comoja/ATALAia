import sys
import os
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta, time as dt_time
import pytz

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection


def getActiveSymbols():
    try:
        from middleware.database import dbConnection
        connection = dbConnection.getConnection()
        if connection is None:
            return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']
        cursor = connection.cursor()
        cursor.execute("SELECT symbol FROM SentinelSymbol WHERE Activo = 1")
        rows = cursor.fetchall()
        cursor.close()
        connection.close()
        symbolsList = [row[0] for row in rows]
        if not symbolsList:
            return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']
        return symbolsList
    except Exception as e:
        print(f"Error fetching active symbols: {e}")
        return ['EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD']

ALL_SYMBOLS = getActiveSymbols()

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

def runBreakoutNYGridSearch() -> None:
    print("==========================================================")
    print("       INICIANDO GRID SEARCH OPTIMIZER (BREAKOUTNY)       ")
    print("==========================================================")
    
    startDateStr = '2026-05-21 00:00:00'
    endDateStr = '2026-06-11 14:00:00'
    
    # Grid de Parámetros de BreakoutNY
    rangeDurations = [15, 20, 25, 30, 40, 45, 50, 60]
    tradingWindows = [90, 120, 150, 180, 210, 240]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        print(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 200:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        # Agrupar velas por fecha local (asumiendo que los timestamps están en UTC o timezone local consistente)
        # Haremos una agrupación por día de mercado
        groupedDays = df5m.groupby(df5m.index.date)
        
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        for rangeDuration in rangeDurations:
            for tradingWindow in tradingWindows:
                for minRr in minRrCombos:
                    
                    trades = []
                    
                    for dayDate, dfDay in groupedDays:
                        if len(dfDay) < 20:
                            continue
                            
                        # Determinar startTs (09:00) y endTs (09:00 + rangeDuration) para este día
                        startTs = pd.Timestamp(datetime.combine(dayDate, dt_time(9, 0)))
                        # Localizar timezone si el index de dfDay lo tiene
                        if dfDay.index.tz is not None:
                            startTs = startTs.tz_localize(dfDay.index.tz)
                            
                        endTs = startTs + timedelta(minutes=rangeDuration)
                        endTimeTs = endTs + timedelta(minutes=tradingWindow)
                        
                        # Extraer velas de rango
                        dfRange = dfDay[(dfDay.index >= startTs) & (dfDay.index < endTs)]
                        if dfRange.empty:
                            continue
                            
                        rangeHigh = float(dfRange['high'].max())
                        rangeLow = float(dfRange['low'].min())
                        
                        # Velas de la ventana de trading
                        dfWindow = dfDay[(dfDay.index >= endTs) & (dfDay.index <= endTimeTs)]
                        if dfWindow.empty:
                            continue
                            
                        # Simular iteración en la ventana de trading
                        openArr = dfWindow['open'].values
                        highArr = dfWindow['high'].values
                        lowArr = dfWindow['low'].values
                        closeArr = dfWindow['close'].values
                        timeArr = dfWindow.index
                        
                        # Para evaluar la condición de vela anterior dentro del rango,
                        # necesitamos el historial continuo de este día
                        # Buscaremos la vela anterior a la primera de la ventana en dfDay
                        firstWindowTime = timeArr[0]
                        dfPrior = dfDay[dfDay.index < firstWindowTime]
                        if dfPrior.empty:
                            continue
                        prevCloseVal = float(dfPrior['close'].iloc[-1])
                        
                        tradeClosed = False
                        pnlUsd = 0.0
                        
                        for idx in range(len(dfWindow)):
                            closePrice = float(closeArr[idx])
                            openPrice = float(openArr[idx])
                            
                            # Filtro Seguridad: vela anterior dentro del rango
                            isExplosiveLong = closePrice > rangeHigh and prevCloseVal <= rangeHigh
                            isExplosiveShort = closePrice < rangeLow and prevCloseVal >= rangeLow
                            
                            direction = None
                            if isExplosiveLong:
                                direction = "LARGO"
                            elif isExplosiveShort:
                                direction = "CORTO"
                                
                            if direction:
                                # Entrar
                                entryPrice = closePrice + (spreadPrice / 2.0) if direction == "LARGO" else closePrice - (spreadPrice / 2.0)
                                sl = rangeLow if direction == "LARGO" else rangeHigh
                                riskDist = abs(entryPrice - sl)
                                if riskDist <= 0:
                                    prevCloseVal = closePrice
                                    continue
                                    
                                tp = entryPrice + (riskDist * minRr) if direction == "LARGO" else entryPrice - (riskDist * minRr)
                                
                                # Seguir holding period en las velas siguientes de este día
                                for j in range(idx + 1, len(dfWindow)):
                                    vHigh = float(highArr[j])
                                    vLow = float(lowArr[j])
                                    
                                    if direction == "LARGO":
                                        lowAdjusted = vLow - (spreadPrice / 2.0)
                                        highAdjusted = vHigh + (spreadPrice / 2.0)
                                        if lowAdjusted <= sl:
                                            pnlUsd = -100.0
                                            tradeClosed = True
                                        elif highAdjusted >= tp:
                                            pnlUsd = 100.0 * minRr
                                            tradeClosed = True
                                    else:
                                        highAdjusted = vHigh + (spreadPrice / 2.0)
                                        lowAdjusted = vLow - (spreadPrice / 2.0)
                                        if highAdjusted >= sl:
                                            pnlUsd = -100.0
                                            tradeClosed = True
                                        elif lowAdjusted <= tp:
                                            pnlUsd = 100.0 * minRr
                                            tradeClosed = True
                                            
                                    if tradeClosed:
                                        trades.append(pnlUsd)
                                        break
                                        
                            if tradeClosed:
                                # Máximo 1 trade por día
                                break
                            prevCloseVal = closePrice
                            
                    # Métricas de este combo
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
                            'Range Duration': rangeDuration,
                            'Trading Window': tradingWindow,
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
            try:
                import json
                from middleware.database import dbConnection
                conn = dbConnection.getConnection()
                cursor = conn.cursor()
                combo = symbolBestCombo
                params = {"min_rr": combo["Min RR"]}
                paramsJson = json.dumps(params)
                
                sql = """
                    INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                    VALUES ('BreakoutNY', %s, TRUE, %s)
                    ON DUPLICATE KEY UPDATE parametersJson = VALUES(parametersJson), enabled = TRUE
                """
                cursor.execute(sql, (symbol, paramsJson))
                conn.commit()
                print(f"✅ DB: Guardado {symbol} (TRUE)")
            except Exception as e:
                print(f"❌ Error DB {symbol}: {e}")
            finally:
                if 'cursor' in locals(): cursor.close()
                if 'conn' in locals() and hasattr(conn, 'close'): conn.close()

            print(f"  🏆 Mejor combo para {symbol}: Range={symbolBestCombo['Range Duration']}m | Window={symbolBestCombo['Trading Window']}m | RR={symbolBestCombo['Min RR']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}.")

    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/breakoutny_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/breakoutny_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")

if __name__ == '__main__':
    runBreakoutNYGridSearch()
