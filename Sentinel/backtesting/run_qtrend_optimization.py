import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import asyncio
import json
import warnings
from datetime import datetime, timedelta

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.utils.alertBuilder import adjustTPForMinRR
from Sentinel.analysis import technical
from Sentinel.backtesting import opt_db_helper

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

def runBacktestForCombo(df15m: pd.DataFrame, symbol: str, superPeriod: int, superMult: float, fastPeriod: int, slowPeriod: int, tpParam: float) -> dict:
    stTrend, stTrail = technical.calculateAtrStop(df15m, superPeriod, superMult)
    closePrices = df15m['close'].values.astype(float)
    highPrices = df15m['high'].values.astype(float)
    lowPrices = df15m['low'].values.astype(float)
    
    emaFast = ta.EMA(closePrices, timeperiod=fastPeriod)
    emaSlow = ta.EMA(closePrices, timeperiod=slowPeriod)
    atrSeries = ta.ATR(highPrices, lowPrices, closePrices, timeperiod=14)
    
    if len(df15m) < max(superPeriod, slowPeriod) + 10:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    trades = []
    activeTrade = None
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    symbolType = 'CRYPTO' if symbol == 'BTC/USD' else 'METAL' if 'XAU' in symbol or 'XAG' in symbol else 'MONEDA'

    for i in range(max(superPeriod, slowPeriod) + 5, len(df15m)):
        currentPrice = float(closePrices[i])
        currentTime = df15m.index[i]
        
        if activeTrade:
            velaHigh = float(highPrices[i])
            velaLow = float(lowPrices[i])
            direction = activeTrade['direction']
            sl = activeTrade['sl']
            tp = activeTrade['tp']
            
            closed = False
            exitPrice = 0.0
            pnlPips = 0.0
            
            if direction == "LARGO":
                if velaLow <= sl:
                    closed = True
                    exitPrice = sl
                    pnlPips = (sl - activeTrade['entry']) * pipMult
                elif velaHigh >= tp:
                    closed = True
                    exitPrice = tp
                    pnlPips = (tp - activeTrade['entry']) * pipMult
            else:
                if velaHigh >= sl:
                    closed = True
                    exitPrice = sl
                    pnlPips = (activeTrade['entry'] - sl) * pipMult
                elif velaLow <= tp:
                    closed = True
                    exitPrice = tp
                    pnlPips = (activeTrade['entry'] - tp) * pipMult
            
            if closed:
                pnlPips -= opt_db_helper.getSpread(symbol)
                trades.append({
                    "direction": direction,
                    "entryTime": activeTrade['entryTime'],
                    "exitTime": currentTime,
                    "pnl": pnlPips,
                    "result": "WIN" if pnlPips > 0 else "LOSS"
                })
                activeTrade = None
            continue
            
        stBullish = stTrend[i] == 1
        stBearish = stTrend[i] == -1
        qBullish = emaFast[i] > emaSlow[i]
        qBearish = emaFast[i] < emaSlow[i]
        
        stBullishAge = -1
        stBearishAge = -1
        qBullishAge = -1
        qBearishAge = -1
        
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if stTrend[idx-1] == -1 and stTrend[idx] == 1:
                    stBullishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if stTrend[idx-1] == 1 and stTrend[idx] == -1:
                    stBearishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if emaFast[idx-1] <= emaSlow[idx-1] and emaFast[idx] > emaSlow[idx]:
                    qBullishAge = age
                    break
        for age in range(0, 4):
            idx = i - age
            if idx > 0:
                if emaFast[idx-1] >= emaSlow[idx-1] and emaFast[idx] < emaSlow[idx]:
                    qBearishAge = age
                    break
                    
        direction = None
        if stBullish and qBullish and 0 <= stBullishAge <= 3 and 0 <= qBullishAge <= 3:
            direction = "LARGO"
        elif stBearish and qBearish and 0 <= stBearishAge <= 3 and 0 <= qBearishAge <= 3:
            direction = "CORTO"
            
        if direction:
            slPrice = float(stTrail[i])
            slDist = abs(currentPrice - slPrice)
            if slDist <= 0:
                continue
                
            atrVal = atrSeries[i] if not np.isnan(atrSeries[i]) else 0.0
            if atrVal > 0 and slDist > 2.5 * atrVal:
                continue
                
            if symbolType == 'MONEDA':
                if direction == "LARGO":
                    calculatedTp = currentPrice + (tpParam * slDist)
                else:
                    calculatedTp = currentPrice - (tpParam * slDist)
            else:
                tpPctDecimal = tpParam / 100.0
                if direction == "LARGO":
                    calculatedTp = currentPrice * (1.0 + tpPctDecimal)
                else:
                    calculatedTp = currentPrice * (1.0 - tpPctDecimal)
                    
            tpPrice = adjustTPForMinRR(currentPrice, slPrice, calculatedTp, direction, minRR=1.5 if symbolType != 'MONEDA' else tpParam)
            
            activeTrade = {
                "direction": direction,
                "entry": currentPrice,
                "sl": slPrice,
                "tp": tpPrice,
                "entryTime": currentTime
            }

    if not trades:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    
    totalProfit = sum(t['pnl'] for t in wins)
    totalLoss = abs(sum(t['pnl'] for t in losses))
    
    winRate = (len(wins) / len(trades)) * 100.0
    profitFactor = totalProfit / totalLoss if totalLoss > 0 else 999.0 if totalProfit > 0 else 0.0
    pnlTotal = sum(t['pnl'] for t in trades)
    
    return {
        "trades": trades,
        "winRate": round(winRate, 2),
        "profitFactor": round(profitFactor, 2),
        "pnl": round(pnlTotal, 2)
    }

async def runQTrendGridSearch():
    logger.info("==========================================================")
    logger.info("  INICIANDO GRID SEARCH OPTIMIZER (QTREND - 15MIN DATA)  ")
    logger.info("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    supertrendPeriodCombos = [8, 10, 12, 14, 16]
    supertrendMultiplierCombos = [1.5, 2.0, 2.5, 3.0, 3.5]
    qtrendFastCombos = [7, 9, 11, 12, 14]
    qtrendSlowCombos = [18, 21, 24, 26, 30]
    
    tpParamCombosMoneda = [1.2, 1.5, 1.8, 2.0, 2.5, 3.0]
    tpParamCombosExotic = [1.0, 1.5, 2.0, 2.5, 3.0]
    
    allResultsRaw = []
    bestResults = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos de 5m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"min_rr": 1.5, "supertrendPeriod": 10, "supertrendMultiplier": 3.0, "qtrendFast": 12, "qtrendSlow": 26, "tpParam": 2.0}
            opt_db_helper.saveSymbolStrategyConfig('QTrend', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 100:
            logger.warning(f"  ⚠️ Datos resampleados a 15m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"min_rr": 1.5, "supertrendPeriod": 10, "supertrendMultiplier": 3.0, "qtrendFast": 12, "qtrendSlow": 26, "tpParam": 2.0}
            opt_db_helper.saveSymbolStrategyConfig('QTrend', symbol, False, fallbackParams)
            continue
            
        isMoneda = symbol not in ['XAU/USD', 'BTC/USD', 'XAG/USD', 'NAS100']
        tpCombos = tpParamCombosMoneda if isMoneda else tpParamCombosExotic
        
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for sPeriod in supertrendPeriodCombos:
            for sMult in supertrendMultiplierCombos:
                for qFast in qtrendFastCombos:
                    for qSlow in qtrendSlowCombos:
                        for tpParam in tpCombos:
                            metrics = runBacktestForCombo(df15m, symbol, sPeriod, sMult, qFast, qSlow, tpParam)
                            nTrades = len(metrics['trades'])
                            
                            resRow = {
                                "symbol": symbol,
                                "supertrendPeriod": sPeriod,
                                "supertrendMultiplier": sMult,
                                "qtrendFast": qFast,
                                "qtrendSlow": qSlow,
                                "tpParam": tpParam,
                                "nTrades": nTrades,
                                "winRate": metrics['winRate'],
                                "profitFactor": metrics['profitFactor'],
                                "pnl": metrics['pnl']
                            }
                            allResultsRaw.append(resRow)
                            
                            if nTrades > 0 and metrics['profitFactor'] >= 1.00 and metrics['winRate'] >= 35.0:
                                if metrics['profitFactor'] > bestPf or (metrics['profitFactor'] == bestPf and metrics['winRate'] > bestWr):
                                    bestPf = metrics['profitFactor']
                                    bestWr = metrics['winRate']
                                    bestCombo = resRow
        
        if bestCombo:
            bestResults.append(bestCombo)
            params = {
                "min_rr": bestCombo["tpParam"],
                "supertrendPeriod": bestCombo["supertrendPeriod"],
                "supertrendMultiplier": bestCombo["supertrendMultiplier"],
                "qtrendFast": bestCombo["qtrendFast"],
                "qtrendSlow": bestCombo["qtrendSlow"],
                "tpParam": bestCombo["tpParam"]
            }
            opt_db_helper.saveSymbolStrategyConfig('QTrend', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            logger.info(f"  🏆 Mejor combo viable para {symbol}: ST_P={bestCombo['supertrendPeriod']}, ST_M={bestCombo['supertrendMultiplier']}, QF={bestCombo['qtrendFast']}, QS={bestCombo['qtrendSlow']}, TP={bestCombo['tpParam']} | PF={bestCombo['profitFactor']}, WR={bestCombo['winRate']}%, PnL={bestCombo['pnl']:.2f}")
        else:
            logger.info(f"  ❌ Ningún combo cumplió con el umbral de viabilidad para {symbol}.")
            fallbackParams = {"min_rr": 1.5, "supertrendPeriod": 10, "supertrendMultiplier": 3.0, "qtrendFast": 12, "qtrendSlow": 26, "tpParam": 2.0}
            opt_db_helper.saveSymbolStrategyConfig('QTrend', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró combo viable para {symbol}. Guardado en DB (FALSE).")

    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: dfAll.to_csv(opt_db_helper.getOutputPath("qtrend_grid_results_all.csv"), index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty: dfBest.to_csv(opt_db_helper.getOutputPath("qtrend_grid_results_best.csv"), index=False)

if __name__ == "__main__":
    asyncio.run(runQTrendGridSearch())
