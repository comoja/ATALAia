import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import json
import warnings
from datetime import datetime, time, timedelta
import pytz

warnings.filterwarnings('ignore')

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.utils.alertBuilder import adjustTPForMinRR
from Sentinel.backtesting import opt_db_helper

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles

NY_TZ = pytz.timezone("America/New_York")
SILVER_BULLET_WINDOWS = {
    "london_open": {"start": time(3, 0), "end": time(4, 0)},
    "ny_am":       {"start": time(10, 0), "end": time(11, 0)},
    "ny_pm":       {"start": time(14, 0), "end": time(15, 0)}
}

def runBacktestForCombo(df5m: pd.DataFrame, symbol: str, fvgMinPct: float, minRrVal: float, minAdx: float) -> dict:
    df = df5m.copy()
    
    df["adx"] = pd.Series(ta.ADX(df['high'].values.astype(float), df['low'].values.astype(float), df['close'].values.astype(float), timeperiod=14), index=df.index)
    df["atr"] = pd.Series(ta.ATR(df['high'].values.astype(float), df['low'].values.astype(float), df['close'].values.astype(float), timeperiod=14), index=df.index)
    
    df.dropna(subset=["adx", "atr"], inplace=True)
    if len(df) < 50:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    try:
        idxNy = df.index.tz_convert("America/New_York")
    except Exception:
        df.index = df.index.tz_localize("America/Mexico_City", ambiguous='infer', nonexistent='shift_forward')
        idxNy = df.index.tz_convert("America/New_York")
        
    df["ny_time"] = idxNy.time
    df["ny_date"] = idxNy.date
    
    trades = []
    pipMult = opt_db_helper.getPipMultiplier(symbol)
    spread = opt_db_helper.getSpread(symbol) / pipMult
    
    dates = df["ny_date"].unique()
    
    for d in dates:
        df_day = df[df["ny_date"] == d]
        if len(df_day) < 12:
            continue
            
        for w_name, w in SILVER_BULLET_WINDOWS.items():
            w_start_ny = NY_TZ.localize(datetime.combine(d, w["start"]))
            w_end_ny = NY_TZ.localize(datetime.combine(d, w["end"]))
            
            idx_ny_day = df_day.index.tz_convert("America/New_York")
            df_w = df_day[(idx_ny_day >= w_start_ny) & (idx_ny_day < w_end_ny)]
            if len(df_w) < 5:
                continue
                
            ref_end = w_start_ny + timedelta(minutes=15)
            df_ref = df_w[df_w.index.tz_convert("America/New_York") < ref_end]
            if df_ref.empty or len(df_ref) < 3:
                continue
                
            ref = {
                "high": float(df_ref["high"].max()),
                "low": float(df_ref["low"].min())
            }
            
            sweep_start = w_start_ny + timedelta(minutes=15)
            df_post = df_w[df_w.index.tz_convert("America/New_York") >= sweep_start]
            if df_post.empty:
                continue
                
            sweep = None
            for idx_v, (time_v, v) in enumerate(df_post.iterrows()):
                adx_v = float(v["adx"])
                if adx_v < minAdx:
                    continue
                    
                if v["low"] < ref["low"] and v["close"] > ref["low"]:
                    sweep = {"type": "LARGO", "swept_level": ref["low"], "sweep_low": v["low"], "idx": idx_v, "time": time_v}
                    break
                elif v["high"] > ref["high"] and v["close"] < ref["high"]:
                    sweep = {"type": "CORTO", "swept_level": ref["high"], "sweep_high": v["high"], "idx": idx_v, "time": time_v}
                    break
                    
            if not sweep:
                continue
                
            df_signals = df_post.iloc[sweep["idx"]+1:]
            if len(df_signals) < 3:
                continue
                
            fvg = None
            for idx_s, (time_s, s) in enumerate(df_signals.iterrows()):
                abs_idx = df_day.index.get_loc(time_s)
                if abs_idx < 2:
                    continue
                    
                h2 = df_day["high"].iloc[abs_idx - 2]
                l2 = df_day["low"].iloc[abs_idx - 2]
                h = df_day["high"].iloc[abs_idx]
                l = df_day["low"].iloc[abs_idx]
                c = df_day["close"].iloc[abs_idx]
                
                h_vals = df_day["high"].iloc[max(0, abs_idx-6):abs_idx].values
                l_vals = df_day["low"].iloc[max(0, abs_idx-6):abs_idx].values
                if sweep["type"] == "LARGO":
                    mss_ok = float(c) > float(np.max(h_vals)) if len(h_vals) > 0 else False
                    if l > h2 and mss_ok:
                        gap_pct = (l - h2) / float(c)
                        if gap_pct >= fvgMinPct:
                            fvg = {
                                "direction": "LARGO",
                                "entry": (h2 + l) / 2.0,
                                "sl": float(sweep["sweep_low"]) - (float(s["atr"]) * 0.1),
                                "time": time_s,
                                "idx_day": abs_idx
                            }
                            break
                else:
                    mss_ok = float(c) < float(np.min(l_vals)) if len(l_vals) > 0 else False
                    if h < l2 and mss_ok:
                        gap_pct = (l2 - h) / float(c)
                        if gap_pct >= fvgMinPct:
                            fvg = {
                                "direction": "CORTO",
                                "entry": (l2 + h) / 2.0,
                                "sl": float(sweep["sweep_high"]) + (float(s["atr"]) * 0.1),
                                "time": time_s,
                                "idx_day": abs_idx
                            }
                            break
                            
            if not fvg:
                continue
                
            entry_p = fvg["entry"]
            sl_p = fvg["sl"]
            sl_dist = abs(entry_p - sl_p)
            if sl_dist <= 0:
                continue
                
            tp_p = entry_p + (sl_dist * minRrVal) if fvg["direction"] == "LARGO" else entry_p - (sl_dist * minRrVal)
            
            df_exec = df_day.iloc[fvg["idx_day"]+1:]
            if df_exec.empty:
                continue
                
            trade_active = False
            for time_e, e in df_exec.iterrows():
                high_e = float(e["high"])
                low_e = float(e["low"])
                
                if not trade_active:
                    if fvg["direction"] == "LARGO" and low_e <= entry_p:
                        trade_active = True
                    elif fvg["direction"] == "CORTO" and high_e >= entry_p:
                        trade_active = True
                    else:
                        continue
                        
                if trade_active:
                    if fvg["direction"] == "LARGO":
                        if low_e <= sl_p:
                            trades.append({"pnl": -100.0})
                            break
                        elif high_e >= tp_p:
                            trades.append({"pnl": 100.0 * minRrVal})
                            break
                    else:
                        if high_e >= sl_p:
                            trades.append({"pnl": -100.0})
                            break
                        elif low_e <= tp_p:
                            trades.append({"pnl": 100.0 * minRrVal})
                            break
                            
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

def runSilverBulletGridSearch():
    logger.info("==========================================================")
    logger.info("  INICIANDO GRID SEARCH OPTIMIZER (SILVER BULLET ICT)   ")
    logger.info("==========================================================")
    
    endDate = datetime.now()
    startDate = endDate - timedelta(days=60)
    startDateStr = startDate.strftime("%Y-%m-%d 00:00:00")
    endDateStr = endDate.strftime("%Y-%m-%d %H:%M:%S")
    
    fvgMinPctCombos = [0.00005, 0.0001, 0.00015, 0.0002]
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    minAdxCombos = [15.0, 20.0, 25.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos de 5m insuficientes para {symbol}. Saltando.")
            fallbackParams = {"fvgMinPct": 0.0001, "minRr": 1.5, "minAdx": 20.0}
            opt_db_helper.saveSymbolStrategyConfig('SilverBullet', symbol, False, fallbackParams)
            continue
            
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for fvgMinPct in fvgMinPctCombos:
            for minRr in minRrCombos:
                for minAdx in minAdxCombos:
                    res = runBacktestForCombo(df5m, symbol, fvgMinPct, minRr, minAdx)
                    numTrades = len(res['trades'])
                    
                    row = {
                        "symbol": symbol,
                        "fvgMinPct": fvgMinPct,
                        "minRr": minRr,
                        "minAdx": minAdx,
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
            logger.info(f"  ✨ Mejor combo viable para {symbol}: FVG Min={bestCombo['fvgMinPct']}, Min R:R={bestCombo['minRr']}, Min ADX={bestCombo['minAdx']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
            params = {
                "fvgMinPct": bestCombo["fvgMinPct"],
                "minRr": bestCombo["minRr"],
                "minAdx": bestCombo["minAdx"]
            }
            opt_db_helper.saveSymbolStrategyConfig('SilverBullet', symbol, True, params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0 y WR >= 35%) para {symbol}.")
            fallbackParams = {"fvgMinPct": 0.0001, "minRr": 1.5, "minAdx": 20.0}
            opt_db_helper.saveSymbolStrategyConfig('SilverBullet', symbol, False, fallbackParams)
            print(f"  ❌ No se encontró combo viable para {symbol}. Guardado en DB (FALSE).")
            
    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: dfAll.to_csv(opt_db_helper.getOutputPath("silverbullet_grid_results_all.csv"), index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty: dfBest.to_csv(opt_db_helper.getOutputPath("silverbullet_grid_results_best.csv"), index=False)

if __name__ == "__main__":
    runSilverBulletGridSearch()
