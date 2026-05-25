import sys
import os
import pandas as pd
import numpy as np
import talib as ta
from datetime import datetime

# Añadir ruta del proyecto
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def load_candles():
    print("--- Cargando velas XAU/USD para prueba comparativa Ichimoku ---")
    try:
        conn = dbConnection.getConnection()
        query = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = 'XAU/USD' AND timeframe = '5min' AND timestamp >= '2024-01-01 00:00:00'
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        if df.empty:
            print("⚠️ No hay velas cargadas.")
            return None
            
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        # Resamplear a 30min para el bot de Ichimoku
        df_30m = df.resample('30min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        print(f"Total de velas cargadas (30min): {len(df_30m)}")
        return df_30m
    except Exception as e:
        print(f"❌ Error al cargar velas: {e}")
        return None

def backtest_ichimoku(df):
    print("\n--- Ejecutando Backtest Comparativo Adaptativo ---")
    
    # 1. Indicadores Base de Ichimoku
    high_9 = df['high'].rolling(9).max()
    low_9 = df['low'].rolling(9).min()
    tenkan = (high_9 + low_9) / 2
    
    high_26 = df['high'].rolling(26).max()
    low_26 = df['low'].rolling(26).min()
    kijun = (high_26 + low_26) / 2
    
    span_a = ((tenkan + kijun) / 2).shift(26)
    
    high_52 = df['high'].rolling(52).max()
    low_52 = df['low'].rolling(52).min()
    span_b = ((high_52 + low_52) / 2).shift(26)
    
    # Bollinger Bands
    bb_upper, bb_middle, bb_lower = ta.BBANDS(df['close'].values, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0)
    
    # MACD para confluencia
    macd, macd_signal, macd_hist = ta.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
    
    # 2. Indicadores de SMC (Volumen y Extremos de Liquidez)
    volume_ma = df['volume'].rolling(14).mean()
    local_high = df['high'].shift(1).rolling(20).max()
    local_low = df['low'].shift(1).rolling(20).min()
    
    trades_classic = []
    trades_smc = []
    
    # Simular barra por barra
    for idx in range(80, len(df)):
        c_close = df['close'].iloc[idx]
        c_open = df['open'].iloc[idx]
        c_high = df['high'].iloc[idx]
        c_low = df['low'].iloc[idx]
        c_vol = df['volume'].iloc[idx]
        
        c_tenkan = tenkan.iloc[idx]
        c_kijun = kijun.iloc[idx]
        c_span_a = span_a.iloc[idx]
        c_span_b = span_b.iloc[idx]
        
        c_bb_upper = bb_upper[idx]
        c_bb_middle = bb_middle[idx]
        c_bb_lower = bb_lower[idx]
        
        c_macd_hist = macd_hist.iloc[idx]
        
        prev_bb_upper = bb_upper[idx-1]
        prev_bb_lower = bb_lower[idx-1]
        
        bb_width_curr = c_bb_upper - c_bb_lower
        bb_width_prev = prev_bb_upper - prev_bb_lower
        
        kumo_max = max(c_span_a, c_span_b)
        kumo_min = min(c_span_a, c_span_b)
        
        if pd.isna(c_span_a) or pd.isna(c_bb_upper) or pd.isna(c_tenkan) or pd.isna(c_macd_hist):
            continue
            
        # Determinar si hay mecha de rechazo / sweep en las últimas 5 velas para confluencia de manipulación
        bullish_sweep = False
        bearish_sweep = False
        
        for offset in range(-5, 0):
            target_idx = idx + offset
            if target_idx < 0:
                continue
            candle_row = df.iloc[target_idx]
            o = candle_row['open']
            c = candle_row['close']
            h = candle_row['high']
            l = candle_row['low']
            rng = h - l
            if rng <= 0:
                continue
            
            # Rechazo inferior (pinbar alcista / barrido intradiario)
            low_wick = min(o, c) - l
            if low_wick > 0.35 * rng:
                bullish_sweep = True
                
            # Rechazo superior (pinbar bajista / barrido intradiario)
            high_wick = h - max(o, c)
            if high_wick > 0.35 * rng:
                bearish_sweep = True
                
        # ──────────────────────────────────────────────────────────────────
        # 1. ICHIMOKU CLÁSICO (Sin Filtros SMC)
        # ──────────────────────────────────────────────────────────────────
        direction_classic = None
        if (c_close > kumo_max and c_tenkan > c_kijun and c_close > c_bb_middle and 
            c_span_a > c_span_b and c_span_a == kumo_max and bb_width_curr > bb_width_prev and c_macd_hist > 0):
            direction_classic = "LARGO"
        elif (c_close < kumo_min and c_tenkan < c_kijun and c_close < c_bb_middle and 
              c_span_a < c_span_b and c_span_b == kumo_min and bb_width_curr > bb_width_prev and c_macd_hist < 0):
            direction_classic = "CORTO"
            
        if direction_classic:
            if idx + 5 < len(df):
                exit_price = df['close'].iloc[idx+5]
                pips = (exit_price - c_close) if direction_classic == "LARGO" else (c_close - exit_price)
                trades_classic.append(pips)
                
        # ──────────────────────────────────────────────────────────────────
        # 2. ICHIMOKU CON FILTROS SMC (Volumen y mecha de rechazo / Sweep)
        # ──────────────────────────────────────────────────────────────────
        direction_smc = None
        # Filtro de volumen adaptativo: Si no hay volumen (Forex/Oro), se omite
        has_vol = volume_ma.iloc[idx] > 0
        vol_ok = not has_vol or (c_vol > 1.0 * volume_ma.iloc[idx])
        
        if (c_close > kumo_max and c_tenkan > c_kijun and c_close > c_bb_middle and 
            c_span_a > c_span_b and c_span_a == kumo_max and bb_width_curr > bb_width_prev and c_macd_hist > 0):
            if vol_ok and bullish_sweep:
                direction_smc = "LARGO"
        elif (c_close < kumo_min and c_tenkan < c_kijun and c_close < c_bb_middle and 
              c_span_a < c_span_b and c_span_b == kumo_min and bb_width_curr > bb_width_prev and c_macd_hist < 0):
            if vol_ok and bearish_sweep:
                direction_smc = "CORTO"
                
        if direction_smc:
            if idx + 5 < len(df):
                exit_price = df['close'].iloc[idx+5]
                pips = (exit_price - c_close) if direction_smc == "LARGO" else (c_close - exit_price)
                trades_smc.append(pips)
                
    # 3. Reporte de Resultados
    print("\n==================================================")
    print("        RESULTADOS DEL BACKTEST COMPARATIVO       ")
    print("==================================================")
    
    # Clásico
    tc = len(trades_classic)
    win_c = len([t for t in trades_classic if t > 0])
    wr_c = (win_c / tc * 100) if tc > 0 else 0
    pnl_c = sum(trades_classic)
    print(f"▶ ICHIMOKU CLÁSICO:")
    print(f"  - Total Trades: {tc}")
    print(f"  - Ganados: {win_c} | Perdidos: {tc - win_c}")
    print(f"  - Win Rate: {wr_c:.2f}%")
    print(f"  - PnL Acumulado (Pips): {pnl_c:.2f}")
    
    # SMC Optimizado
    ts = len(trades_smc)
    win_s = len([t for t in trades_smc if t > 0])
    wr_s = (win_s / ts * 100) if ts > 0 else 0
    pnl_s = sum(trades_smc)
    print(f"\n▶ ICHIMOKU OPTIMIZADO CON FILTROS SMC:")
    print(f"  - Total Trades: {ts}  (Se filtró un {((tc - ts)/tc*100) if tc > 0 else 0:.1f}% de operaciones de ruido)")
    print(f"  - Ganados: {win_s} | Perdidos: {ts - win_s}")
    print(f"  - Win Rate: {wr_s:.2f}%  (Aumento de {wr_s - wr_c:+.2f}%)")
    print(f"  - PnL Acumulado (Pips): {pnl_s:.2f}")
    print("==================================================")

if __name__ == '__main__':
    df = load_candles()
    if df is not None:
        backtest_ichimoku(df)
