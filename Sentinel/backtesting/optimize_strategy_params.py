import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import asyncio
import itertools
from datetime import datetime, timedelta
import pytz
import logging

# Ocultar logs que llenan la pantalla
logging.getLogger('sentinel').setLevel(logging.CRITICAL)

sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from middleware.database import dbManager
from Sentinel.core.models import Signal
from Sentinel.analysis import technical
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval

import datetime as real_datetime

simulated_now = None

class MockDatetime(real_datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        if simulated_now is not None:
            if tz:
                return simulated_now.astimezone(tz)
            return simulated_now.replace(tzinfo=None)
        return real_datetime.datetime.now(tz)

import Sentinel.core.GenericFVG as GFVG
import dataSymbol.mainOrchestrator as mainOrch

GFVG.datetime = MockDatetime
mainOrch.datetime = MockDatetime

# ----- PARAMETERS GRID -----
param_grid = {
    'min_rr': [1.0, 1.5, 2.0],
    'fvg_min_pct': [0.0001, 0.0005, 0.001],
    'require_htf_sweep': [False, True]
}

keys, values = zip(*param_grid.items())
combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]

original_get_config = dbManager.getSymbolStrategyConfig
current_combo = {}

def patched_getSymbolStrategyConfig(strategy, symbol):
    return current_combo

dbManager.getSymbolStrategyConfig = patched_getSymbolStrategyConfig

def load_historical_data(symbol, start_dt, end_dt):
    hist_start = start_dt - timedelta(days=7)
    conn = dbConnection.getConnection()
    df = None
    try:
        q = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = '5min' AND timestamp BETWEEN %s AND %s
            ORDER BY timestamp
        """
        df = pd.read_sql(q, conn, params=(symbol, hist_start, end_dt))
    finally:
        if conn: conn.close()
    
    if df is not None and not df.empty:
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        for col in ['open', 'high', 'low', 'close', 'volume']:
            df[col] = df[col].astype(float)
        df = df[~df.index.duplicated(keep='last')]
    return df

async def run_optimization():
    strategy_name = "GenericFVG"
    
    symbols = [
        "BTC/USD", "EUR/USD", "GBP/USD", "AUD/USD", "USD/JPY", 
        "USD/CAD", "USD/CHF", "NZD/USD", "EUR/GBP", "GBP/JPY", 
        "XAU/USD", "GBP/CAD", "USD/MXN"
    ]
    
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=14)
    
    print("⏳ Cargando datos históricos...")
    data_cache = {}
    for sym in symbols:
        df = load_historical_data(sym, start_dt, end_dt)
        if df is not None and not df.empty:
            data_cache[sym] = df
            
    print(f"✅ Datos cargados. Probando {len(combinations)} combinaciones.")
    
    best_results = {}
    
    for idx, combo in enumerate(combinations):
        global current_combo
        current_combo = combo
        print(f"--- Probando Configuración {idx+1}/{len(combinations)}: {combo} ---", flush=True)
        
        bot = GFVG.GenericFVGBot()
        combo_pnl = 0
        combo_trades = 0
        
        for symbol in symbols:
            df5m = data_cache.get(symbol)
            if df5m is None or len(df5m) < 100: continue
            
            symbolInfo = {'symbol': symbol, 'refCapital': 10000.0, 'refRiskPct': 1.0, 'weekly_trend': 'NEUTRAL'}
            
            df5m_feat = calculateFeatures(df5m)
            df15m = resample_to_interval(df5m, '15min')
            df30m = resample_to_interval(df5m, '30min')
            df1h  = resample_to_interval(df5m, '1h')
            df4h  = resample_to_interval(df5m, '4h')
            df1d  = resample_to_interval(df5m, '1d')
            
            test_slice = df5m_feat.loc[start_dt:end_dt]
            test_timestamps = test_slice.index
            
            active_trade_until = None
            
            for t in test_timestamps:
                global simulated_now
                simulated_now = t
                
                if active_trade_until is not None and t < active_trade_until:
                    continue
                
                slice5m = df5m_feat.loc[:t]
                if len(slice5m) < 40: continue
                
                preloaded_master = {
                    '5min': slice5m,
                    '15min': df15m.loc[:t],
                    '30min': df30m.loc[:t],
                    '1h': df1h.loc[:t],
                    '4h': df4h.loc[:t],
                    '1d': df1d.loc[:t]
                }
                
                try:
                    signals = await bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master})
                except Exception:
                    continue
                    
                if signals:
                    sig = signals[0]
                    future_df = df5m.loc[t:]
                    
                    entry = sig.entry_price
                    sl = sig.stop_loss
                    tp = sig.take_profit
                    direction = sig.direction
                    
                    trade_result = None
                    exit_time = None
                    
                    highs = future_df['high'].values
                    lows = future_df['low'].values
                    
                    if direction == "LARGO":
                        hit_sl = lows <= sl
                        hit_tp = highs >= tp
                        
                        sl_idx = np.argmax(hit_sl) if hit_sl.any() else -1
                        tp_idx = np.argmax(hit_tp) if hit_tp.any() else -1
                        
                        if sl_idx != -1 and tp_idx != -1:
                            if sl_idx <= tp_idx:
                                trade_result = -1.0
                                exit_time = future_df.index[sl_idx]
                            else:
                                trade_result = sig.rr_ratio
                                exit_time = future_df.index[tp_idx]
                        elif sl_idx != -1:
                            trade_result = -1.0
                            exit_time = future_df.index[sl_idx]
                        elif tp_idx != -1:
                            trade_result = sig.rr_ratio
                            exit_time = future_df.index[tp_idx]
                    else:
                        hit_sl = highs >= sl
                        hit_tp = lows <= tp
                        
                        sl_idx = np.argmax(hit_sl) if hit_sl.any() else -1
                        tp_idx = np.argmax(hit_tp) if hit_tp.any() else -1
                        
                        if sl_idx != -1 and tp_idx != -1:
                            if sl_idx <= tp_idx:
                                trade_result = -1.0
                                exit_time = future_df.index[sl_idx]
                            else:
                                trade_result = sig.rr_ratio
                                exit_time = future_df.index[tp_idx]
                        elif sl_idx != -1:
                            trade_result = -1.0
                            exit_time = future_df.index[sl_idx]
                        elif tp_idx != -1:
                            trade_result = sig.rr_ratio
                            exit_time = future_df.index[tp_idx]
                                
                    if trade_result is not None:
                        combo_trades += 1
                        combo_pnl += trade_result
                        active_trade_until = exit_time
                        
        print(f"📊 Resultado Combo: Trades={combo_trades}, PNL={combo_pnl:.2f} R", flush=True)
        
        if combo_pnl > best_results.get('pnl', -999):
            best_results = {
                'combo': combo,
                'pnl': combo_pnl,
                'trades': combo_trades
            }
            
    print("\n===============================")
    print("🏆 MEJOR CONFIGURACIÓN ENCONTRADA:")
    print(best_results)
    
if __name__ == "__main__":
    asyncio.run(run_optimization())
