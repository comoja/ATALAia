import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
from datetime import datetime, time, timedelta
import pytz

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR
from Sentinel.analysis import technical

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

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

SILVER_BULLET_WINDOWS = {
    "LONDON_OPEN": {"start": time(3, 0), "end": time(4, 0)},
    "NY_OPENING": {"start": time(8, 30), "end": time(9, 30)},
    "NY_AM": {"start": time(10, 0), "end": time(11, 0)},
    "NY_PM": {"start": time(14, 0), "end": time(15, 0)},
}

NY_TZ = pytz.timezone("America/New_York")

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
        logger.error(f"Error cargando velas para {symbol}: {e}")
        return pd.DataFrame()

def runBacktestForCombo(df5m: pd.DataFrame, symbol: str, fvgMinPct: float, minRrVal: float, minAdx: float) -> dict:
    df = df5m.copy()
    
    # Calcular ADX
    df["adx"] = pd.Series(ta.ADX(df['high'].values.astype(float), df['low'].values.astype(float), df['close'].values.astype(float), timeperiod=14), index=df.index)
    df["atr"] = pd.Series(ta.ATR(df['high'].values.astype(float), df['low'].values.astype(float), df['close'].values.astype(float), timeperiod=14), index=df.index)
    
    df.dropna(subset=["adx", "atr"], inplace=True)
    if len(df) < 50:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    # Convertir el índice a zona horaria de NY para filtrar las ventanas
    idxNy = df.index.tz_convert("America/New_York")
    df["ny_time"] = idxNy.time
    df["ny_date"] = idxNy.date
    
    trades = []
    pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
    spread = SPREADS.get(symbol, 1.0) / pipMult
    
    # Agrupar por fecha en NY
    dates = df["ny_date"].unique()
    
    for d in dates:
        df_day = df[df["ny_date"] == d]
        if len(df_day) < 12:
            continue
            
        for w_name, w in SILVER_BULLET_WINDOWS.items():
            # 1. Definir inicio y fin de la ventana
            w_start_ny = NY_TZ.localize(datetime.combine(d, w["start"]))
            w_end_ny = NY_TZ.localize(datetime.combine(d, w["end"]))
            
            # Obtener datos de la ventana
            idx_ny_day = df_day.index.tz_convert("America/New_York")
            df_w = df_day[(idx_ny_day >= w_start_ny) & (idx_ny_day < w_end_ny)]
            if len(df_w) < 5:
                continue
                
            # 2. Rango de referencia (primeros 15 min de la ventana)
            ref_end = w_start_ny + timedelta(minutes=15)
            df_ref = df_w[df_w.index.tz_convert("America/New_York") < ref_end]
            if df_ref.empty or len(df_ref) < 3:
                continue
                
            ref = {
                "high": float(df_ref["high"].max()),
                "low": float(df_ref["low"].min())
            }
            
            # 3. Barrido de liquidez (sweep) en las velas posteriores
            sweep_start = w_start_ny + timedelta(minutes=15)
            df_post = df_w[df_w.index.tz_convert("America/New_York") >= sweep_start]
            if df_post.empty:
                continue
                
            sweep = None
            for idx_v, (time_v, v) in enumerate(df_post.iterrows()):
                adx_v = float(v["adx"])
                if adx_v < minAdx:
                    continue
                    
                # Sweep de mínimos → bias LARGO
                if v["low"] < ref["low"] and v["close"] > ref["low"]:
                    sweep = {"type": "LARGO", "swept_level": ref["low"], "sweep_low": v["low"], "idx": idx_v, "time": time_v}
                    break
                # Sweep de máximos → bias CORTO
                elif v["high"] > ref["high"] and v["close"] < ref["high"]:
                    sweep = {"type": "CORTO", "swept_level": ref["high"], "sweep_high": v["high"], "idx": idx_v, "time": time_v}
                    break
                    
            if not sweep:
                continue
                
            # 4. Confirmación de MSS y FVG en las velas posteriores al sweep
            df_signals = df_post.iloc[sweep["idx"]+1:]
            if len(df_signals) < 3:
                continue
                
            fvg = None
            for idx_s, (time_s, s) in enumerate(df_signals.iterrows()):
                # Para FVG necesitamos el índice absoluto en df_day
                abs_idx = df_day.index.get_loc(time_s)
                if abs_idx < 2:
                    continue
                    
                h2 = df_day["high"].iloc[abs_idx - 2]
                l2 = df_day["low"].iloc[abs_idx - 2]
                h = df_day["high"].iloc[abs_idx]
                l = df_day["low"].iloc[abs_idx]
                c = df_day["close"].iloc[abs_idx]
                
                # MSS check
                h_vals = df_day["high"].iloc[max(0, abs_idx-6):abs_idx].values
                l_vals = df_day["low"].iloc[max(0, abs_idx-6):abs_idx].values
                if sweep["type"] == "LARGO":
                    mss_ok = float(c) > float(np.max(h_vals))
                else:
                    mss_ok = float(c) < float(np.min(l_vals))
                    
                if not mss_ok:
                    continue
                    
                # FVG check
                if sweep["type"] == "LARGO" and l > h2 and (l - h2) / c >= fvgMinPct:
                    fvg = {"type": "LARGO_FVG", "mid": (h2 + l) / 2, "time": time_s, "abs_idx": abs_idx}
                    break
                elif sweep["type"] == "CORTO" and h < l2 and (l2 - h) / c >= fvgMinPct:
                    fvg = {"type": "CORTO_FVG", "mid": (h + l2) / 2, "time": time_s, "abs_idx": abs_idx}
                    break
                    
            if not fvg:
                continue
                
            # 5. Ejecutar trade
            entry_idx = fvg["abs_idx"]
            entry_price = float(df_day["close"].iloc[entry_idx])
            atr_v = float(df_day["atr"].iloc[entry_idx])
            
            # Niveles estructurales
            df_prior = df_day.iloc[:entry_idx + 1]
            levels = technical.get_structural_levels(df_prior, lookback=20)
            
            if sweep["type"] == "LARGO":
                slPrice = min(entry_price - (atr_v * 1.2), levels['swing_low'] - atr_v * 0.2)
                tp_ref = levels['swing_high']
                tp_ref = min(tp_ref, entry_price + atr_v * 3.0) # Cap de TP
            else:
                slPrice = max(entry_price + (atr_v * 1.2), levels['swing_high'] + atr_v * 0.2)
                tp_ref = levels['swing_low']
                tp_ref = max(tp_ref, entry_price - atr_v * 3.0) # Cap de TP
                
            slDist = abs(entry_price - slPrice)
            if slDist <= 0:
                continue
                
            tpPrice = adjustTPForMinRR(entry_price, slPrice, tp_ref, sweep["type"], minRR=minRrVal)
            
            # Monitorear velas siguientes hasta tocar SL o TP o el fin de la ventana
            closed = False
            pnlPips = 0.0
            
            for k in range(entry_idx + 1, len(df_day)):
                time_k = df_day.index[k]
                if time_k > w_end_ny + timedelta(hours=1): # Cierre forzado por tiempo fuera de la sesión extendida
                    closed = True
                    exit_price = float(df_day["close"].iloc[k])
                    pnlPips = (exit_price - entry_price) * pipMult if sweep["type"] == "LARGO" else (entry_price - exit_price) * pipMult
                    break
                    
                v_k_low = float(df_day["low"].iloc[k])
                v_k_high = float(df_day["high"].iloc[k])
                
                if sweep["type"] == "LARGO":
                    if v_k_low <= slPrice:
                        closed = True
                        pnlPips = (slPrice - entry_price) * pipMult
                        break
                    elif v_k_high >= tpPrice:
                        closed = True
                        pnlPips = (tpPrice - entry_price) * pipMult
                        break
                else: # CORTO
                    if v_k_high >= slPrice:
                        closed = True
                        pnlPips = (entry_price - slPrice) * pipMult
                        break
                    elif v_k_low <= tpPrice:
                        closed = True
                        pnlPips = (entry_price - tpPrice) * pipMult
                        break
                        
            if closed:
                pnlPips -= SPREADS.get(symbol, 1.0)
                trades.append({
                    "direction": sweep["type"],
                    "entryTime": fvg["time"],
                    "pnl": pnlPips,
                    "result": "WIN" if pnlPips > 0 else "LOSS"
                })
                # Permitir solo una operación por ventana Silver Bullet para este símbolo y día
                break

    # Resumen de métricas
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
        "winRate": winRate,
        "profitFactor": profitFactor,
        "pnl": pnlTotal
    }

def runSilverBulletGridSearch():
    logger.info("==========================================================")
    logger.info(" INICIANDO GRID SEARCH OPTIMIZER (SILVERBULLET - 5MIN) ")
    logger.info("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros Extendido (Deep Grid Search)
    fvgMinPctCombos = [0.00005, 0.0001, 0.00015]
    minRrCombos = [1.0, 1.2, 1.5, 2.0, 2.5]
    minAdxCombos = [10.0, 15.0, 20.0, 25.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        from middleware.config.constants import TIMEZONE
        df5m.index = df5m.index.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward')
        
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
                    
                    # Criterio de viabilidad: WR >= 35% y PF >= 1.00, al menos 1 trade
                    if numTrades >= 1 and res['winRate'] >= 35.0 and res['profitFactor'] >= 1.00:
                        if res['profitFactor'] > bestPf or (res['profitFactor'] == bestPf and res['winRate'] > bestWr):
                            bestPf = res['profitFactor']
                            bestWr = res['winRate']
                            bestCombo = row
                            
        if bestCombo:
            logger.info(f"  ✨ Mejor combo viable para {symbol}: FVG Min={bestCombo['fvgMinPct']}, Min R:R={bestCombo['minRr']}, Min ADX={bestCombo['minAdx']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0 y WR >= 35%) para {symbol}.")
            
    # Guardar resultados en CSV
    dfAll = pd.DataFrame(allResultsRaw)
    dfAll.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/silverbullet_grid_results_all.csv", index=False)
    
    dfBest = pd.DataFrame(bestResults)
    dfBest.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/silverbullet_grid_results_best.csv", index=False)
    
    logger.info("==========================================================")
    logger.info(" GRID SEARCH COMPLETADO. Archivos CSV generados con éxito.")
    logger.info("==========================================================")

if __name__ == "__main__":
    runSilverBulletGridSearch()
