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
from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot
from Sentinel.backtesting import opt_db_helper

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("sentinel").setLevel(logging.WARNING)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

async def runBacktestForCombo(df15m: pd.DataFrame, symbol: str, swingLookback: int, mssLookback: int, minRr: float) -> dict:
    bot = SesgoBiasHTFBot()
    bot.swing_lookback = swingLookback
    bot.swingLookback = swingLookback
    bot.mss_lookback = mssLookback
    bot.mssLookback = mssLookback
    bot.minRr = minRr
    
    df_daily = bot.resample_ohlcv(df15m, '1d')
    if len(df_daily) >= 14:
        df_daily['sma14'] = df_daily['close'].rolling(14).mean()
        last_sma = df_daily['sma14'].iloc[-1]
        last_close = df_daily['close'].iloc[-1]
        weekly_trend = "ALCISTA" if last_close > last_sma else "BAJISTA"
    else:
        weekly_trend = "NEUTRAL"
        
    symbol_info = {
        'symbol': symbol,
        'weekly_trend': weekly_trend
    }
    
    trades = []
    activeTrade = None
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    
    startIdx = max(swingLookback, mssLookback, 50) + 10
    if len(df15m) < startIdx:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    for i in range(startIdx, len(df15m)):
        currentTime = df15m.index[i]
        
        if activeTrade:
            velaHigh = float(df15m['high'].iloc[i])
            velaLow = float(df15m['low'].iloc[i])
            direction = activeTrade['direction']
            sl = activeTrade['sl']
            tp = activeTrade['tp']
            
            closed = False
            pnlPips = 0.0
            
            if direction == "LARGO":
                if velaLow <= sl:
                    closed = True
                    pnlPips = (sl - activeTrade['entry']) * pipMult
                elif velaHigh >= tp:
                    closed = True
                    pnlPips = (tp - activeTrade['entry']) * pipMult
            else:
                if velaHigh >= sl:
                    closed = True
                    pnlPips = (activeTrade['entry'] - sl) * pipMult
                elif velaLow <= tp:
                    closed = True
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
            
        df_slice = df15m.iloc[:i+1]
        try:
            signal = await bot.analyze(df_slice, symbol_info)
            if signal and signal.action in ["COMPRA", "VENTA"]:
                direction = "LARGO" if signal.action == "COMPRA" else "CORTO"
                currentClose = float(df15m['close'].iloc[i])
                slPrice = signal.stopLoss
                tpPrice = signal.takeProfit
                
                slDist = abs(currentClose - slPrice)
                if slDist > 0:
                    activeTrade = {
                        "direction": direction,
                        "entry": currentClose,
                        "sl": slPrice,
                        "tp": tpPrice,
                        "entryTime": currentTime
                    }
        except Exception:
            continue
            
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

async def runSesgoBiasHTFGridSearch():
    logger.info("==========================================================")
    logger.info("  INICIANDO GRID SEARCH OPTIMIZER (SESGO BIAS HTF)       ")
    logger.info("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    swingLookbackCombos = [15, 20, 25, 30]
    mssLookbackCombos = [8, 10, 12, 15]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos de 5m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"swingLookback": 20, "mssLookback": 10, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('SesgoBiasHTF', symbol, False, fallbackParams)
            continue
            
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        if len(df15m) < 150:
            logger.warning(f"  ⚠️ Datos de 15m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"swingLookback": 20, "mssLookback": 10, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('SesgoBiasHTF', symbol, False, fallbackParams)
            continue
            
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for swingLookback in swingLookbackCombos:
            for mssLookback in mssLookbackCombos:
                for minRr in minRrCombos:
                    res = await runBacktestForCombo(df15m, symbol, swingLookback, mssLookback, minRr)
                    numTrades = len(res['trades'])
                    
                    row = {
                        "symbol": symbol,
                        "swingLookback": swingLookback,
                        "mssLookback": mssLookback,
                        "minRr": minRr,
                        "totalTrades": numTrades,
                        "winRate": round(res['winRate'], 2),
                        "profitFactor": round(res['profitFactor'], 2),
                        "pnl": round(res['pnl'], 2)
                    }
                    allResultsRaw.append(row)
                    
                    if numTrades >= 1 and res['winRate'] >= 35.0 and res['profitFactor'] >= 1.00:
                        if res['profitFactor'] > bestPf or (res['profitFactor'] == bestPf and res['winRate'] > bestWr):
                            bestPf = res['profitFactor']
                            bestWr = res['winRate']
                            bestCombo = row
                            
        if bestCombo:
            logger.info(f"  ✨ Mejor combo viable para {symbol}: Swing={bestCombo['swingLookback']}, MSS={bestCombo['mssLookback']}, Min R:R={bestCombo['minRr']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
            params = {
                "swingLookback": int(bestCombo['swingLookback']),
                "mssLookback": int(bestCombo['mssLookback']),
                "minRr": float(bestCombo['minRr'])
            }
            opt_db_helper.saveSymbolStrategyConfig('SesgoBiasHTF', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0 y WR >= 35%) para {symbol}.")
            fallbackParams = {"swingLookback": 20, "mssLookback": 10, "minRr": 1.5}
            opt_db_helper.saveSymbolStrategyConfig('SesgoBiasHTF', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró combo viable para {symbol}. Guardado en DB (FALSE).")
            
    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: dfAll.to_csv(opt_db_helper.getOutputPath("sesgobiashtf_grid_results_all.csv"), index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty: dfBest.to_csv(opt_db_helper.getOutputPath("sesgobiashtf_grid_results_best.csv"), index=False)

if __name__ == "__main__":
    asyncio.run(runSesgoBiasHTFGridSearch())
