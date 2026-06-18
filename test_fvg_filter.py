import sys
import pandas as pd
from datetime import datetime, timedelta

sys.path.append('/Volumes/TimeMachine/ATALAia')
from Sentinel.backtesting.run_weekly_backtest_compounding_v6 import loadCandlesRange
from Sentinel.analysis import technical as _tech

def run_test(df, symbol, use_macd):
    fvgs = _tech.detect_fvgs(df, apply_high_prob_filters=True, use_impulse_macd_filter=use_macd)
    return len(fvgs)

def main():
    symbol = "XAU/USD"
    start_date = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d 00:00:00')
    print(f"Buscando datos de {symbol} en 15m (últimos 60 días)...")
    
    df5m = loadCandlesRange(symbol, start_date)
    if df5m is None or df5m.empty:
        print("No hay datos suficientes.")
        return
        
    df15m = df5m.resample('15min').agg({'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}).dropna()
    df15m = _tech.calculateFeatures(df15m)
    
    print(f"Total velas analizadas (15m): {len(df15m)}")
    count_off = run_test(df15m, symbol, False)
    count_on = run_test(df15m, symbol, True)
    
    print("\n--- RESULTADOS COMPARATIVOS (XAU/USD - 15 Min) ---")
    print(f" FVGs Alta Probabilidad (Filtro MACD OFF): {count_off}")
    print(f" FVGs Alta Probabilidad (Filtro MACD ON) : {count_on}")
    print(f" -> Señales eliminadas por inercia débil:  {count_off - count_on}")

if __name__ == "__main__":
    main()
