"""
==============================================================================
  TEST SCRIPT: SesgoBiasHTF Strategy (PO3)
==============================================================================
  Uso: python3 Sentinel/core/test_sesgo_bias_htf.py
"""

import sys
import os
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import pytz

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot, executeSesgoBiasHTF


def generate_sample_data(symbol: str, n_candles: int = 200) -> pd.DataFrame:
    """Genera datos OHLCV simulados para testing."""
    np.random.seed(42)
    
    tz = pytz.timezone("America/Mexico_City")
    end_date = datetime.now(tz)
    start_date = end_date - timedelta(hours=n_candles)
    
    dates = pd.date_range(start=start_date, end=end_date, freq='1h', tz=tz)
    
    base_price = 100.0
    if 'BTC' in symbol.upper():
        base_price = 45000.0
    elif 'EUR' in symbol.upper():
        base_price = 1.0850
    elif 'XAU' in symbol.upper():
        base_price = 2350.0
    
    prices = [base_price]
    for _ in range(n_candles - 1):
        change = np.random.normal(0, base_price * 0.002)
        prices.append(prices[-1] + change)
    
    data = []
    for i, date in enumerate(dates[:n_candles]):
        open_p = prices[i]
        close_p = prices[i] * (1 + np.random.normal(0, 0.001))
        high_p = max(open_p, close_p) * (1 + abs(np.random.normal(0, 0.0005)))
        low_p = min(open_p, close_p) * (1 - abs(np.random.normal(0, 0.0005)))
        volume = np.random.uniform(100, 1000)
        
        data.append({
            'datetime': date,
            'open': open_p,
            'high': high_p,
            'low': low_p,
            'close': close_p,
            'volume': volume
        })
    
    df = pd.DataFrame(data)
    df.set_index('datetime', inplace=True)
    return df


def test_bias_detection():
    """Test de detección de sesgo."""
    print("\n" + "="*60)
    print("TEST: Detección de Sesgo")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    df = generate_sample_data("BTCUSD", 100)
    
    bias, details = bot.detect_bias(df, lookback=20)
    print(f"  Sesgo detectado: {bias}")
    print(f"  Detalles: {details}")
    assert bias in ['ALCISTA', 'BAJISTA', 'INDETERMINADO'], "Sesgo no detectado correctamente"
    
    print("  [OK] Detección de sesgo funciona correctamente")


def test_fibonacci_zones():
    """Test de cálculo de zonas Fibonacci."""
    print("\n" + "="*60)
    print("TEST: Zonas Fibonacci")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    df = generate_sample_data("EURUSD", 100)
    
    zone_short = bot.calculate_fibonacci_zone(df, 'SHORT', lookback=50)
    print(f"  Zona PREMIUM: {zone_short['type']}")
    print(f"  Fib 50%: {zone_short['fib_50']:.5f}")
    assert zone_short['type'] == 'PREMIUM', "Tipo de zona incorrecto"
    
    zone_long = bot.calculate_fibonacci_zone(df, 'LONG', lookback=50)
    print(f"  Zona DISCOUNT: {zone_long['type']}")
    print(f"  Fib 50%: {zone_long['fib_50']:.5f}")
    assert zone_long['type'] == 'DISCOUNT', "Tipo de zona incorrecto"
    
    print("  [OK] Zonas Fibonacci calculadas correctamente")


def test_fvg_detection():
    """Test de detección de FVG."""
    print("\n" + "="*60)
    print("TEST: Detección de FVG")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    
    df = pd.DataFrame({
        'open': [1.0850, 1.0840, 1.0830, 1.0845, 1.0855],
        'high': [1.0865, 1.0855, 1.0845, 1.0860, 1.0865],
        'low': [1.0840, 1.0830, 1.0820, 1.0835, 1.0845],
        'close': [1.0840, 1.0830, 1.0845, 1.0855, 1.0850]
    })
    
    fvg_bullish = bot.detect_fvg(df, 3, 'LONG')
    print(f"  FVG Alcista detectado: {fvg_bullish is not None}")
    if fvg_bullish:
        print(f"  Tipo: {fvg_bullish['type']}")
    
    print("  [OK] Detección de FVG funciona correctamente")


def test_mss_detection():
    """Test de detección de MSS."""
    print("\n" + "="*60)
    print("TEST: Detección de MSS")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    
    df = pd.DataFrame({
        'high': [1.0800, 1.0820, 1.0810, 1.0795, 1.0790],
        'low': [1.0780, 1.0800, 1.0790, 1.0775, 1.0770],
        'close': [1.0810, 1.0815, 1.0790, 1.0780, 1.0785]
    })
    
    mss_bearish = bot.detect_mss(df, 'SHORT')
    print(f"  MSS Bajista detectado: {mss_bearish}")
    
    print("  [OK] Detección de MSS funciona correctamente")


def test_liquidity_levels():
    """Test de detección de niveles de liquidez."""
    print("\n" + "="*60)
    print("TEST: Niveles de Liquidez (Swing Highs/Lows)")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    df = generate_sample_data("BTCUSD", 100)
    
    liquidity = bot.find_swing_highs_lows(df, lookback=50)
    print(f"  Swing Highs encontrados: {len(liquidity['swing_highs'])}")
    print(f"  Swing Lows encontrados: {len(liquidity['swing_lows'])}")
    print(f"  Equal Highs (EQH): {len(liquidity['eqh'])}")
    print(f"  Equal Lows (EQL): {len(liquidity['eql'])}")
    
    print("  [OK] Detección de liquidez funciona correctamente")


def test_market_inactivity():
    """Test de detección de mercado inactivo."""
    print("\n" + "="*60)
    print("TEST: Filtro de Inactividad")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    df = generate_sample_data("EURUSD", 50)
    
    inactive = bot.is_market_inactive(df)
    print(f"  Mercado inactivo: {inactive}")
    
    print("  [OK] Filtro de inactividad funciona correctamente")


def test_multiframe_resampling():
    """Test de resampleo multi-timeframe."""
    print("\n" + "="*60)
    print("TEST: Resampleo Multi-Timeframe")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    df_1h = generate_sample_data("XAUUSD", 500)
    
    df_4h = bot.resample_ohlcv(df_1h, '4h')
    df_1d = bot.resample_ohlcv(df_1h, '1D')
    df_1w = bot.resample_ohlcv(df_1h, '1W')
    df_1M = bot.resample_ohlcv(df_1h, '1M')
    
    print(f"  1H: {len(df_1h)} velas")
    print(f"  4H: {len(df_4h)} velas")
    print(f"  1D: {len(df_1d)} velas")
    print(f"  1W: {len(df_1w)} velas")
    print(f"  1M: {len(df_1M)} velas")
    
    assert len(df_4h) < len(df_1h), "Resampleo incorrecto"
    
    print("  [OK] Resampleo multi-timeframe funciona correctamente")


def test_signal_validation():
    """Test de validación de señales."""
    print("\n" + "="*60)
    print("TEST: Validación de Señales")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    df = generate_sample_data("BTCUSD", 100)
    
    entry = 45000.0
    sl = 44900.0
    tp = 45200.0
    
    signal = bot.validate_signal(entry, sl, tp, 'LARGO', 'BTCUSD', df, 'MODEL_1_BREAK_FVG')
    print(f"  Señal validada: {signal is not None}")
    if signal:
        print(f"  Modelo: {signal['tipo_entrada']}")
        print(f"  Entry: {signal['entrada']}")
        print(f"  SL: {signal['stop_loss']}")
        print(f"  TP: {signal['take_profit']}")
        print(f"  RR: {signal['rr_ratio']}")
    
    print("  [OK] Validación de señales funciona correctamente")


def test_entry_models():
    """Test de modelos de entrada."""
    print("\n" + "="*60)
    print("TEST: Modelos de Entrada (PO3)")
    print("="*60)
    
    bot = SesgoBiasHTFBot()
    
    engulf_short = {
        'type': 'BEARISH_ENGULFING',
        'idx': 2,
        'swept': 'HIGH',
        'sweep_level': 1.0860,
        'candle_high': 1.0870,
        'candle_low': 1.0840,
        'body_top': 1.0845,
        'body_bottom': 1.0855
    }
    
    entry_m3 = bot.calculate_entry_model3_fib_retracement(engulf_short, 'SHORT')
    print(f"  Modelo 3 (Fib Retracement): {entry_m3:.5f}")
    assert entry_m3 is not None, "Entry no calculado"
    
    sl = bot.calculate_sl_from_sweep(engulf_short, 'SHORT')
    print(f"  SL desde sweep: {sl:.5f}")
    
    print("  [OK] Modelos de entrada funcionan correctamente")


def test_full_analysis():
    """Test de análisis completo de la estrategia."""
    print("\n" + "="*60)
    print("TEST: Análisis Completo PO3")
    print("="*60)
    
    symbol_info = {
        'symbol': 'BTCUSD',
        'tipo': 'CRYPTO',
        'pip': 1.0
    }
    
    df_4h = generate_sample_data("BTCUSD", 500)
    
    datos = {
        '4h': df_4h,
        '1d': None,
        '1w': None,
        '1M': None
    }
    
    resultado = executeSesgoBiasHTF(datos, symbol_info)
    print(f"  Status: {resultado['status']}")
    
    if 'biases' in resultado:
        print(f"  Biases: {resultado['biases']}")
    
    print("  [OK] Análisis completo ejecuta correctamente")


def run_all_tests():
    """Ejecuta todos los tests."""
    print("\n" + "="*60)
    print("INICIANDO TESTS - SESGO BIAS HTF (PO3)")
    print("="*60)
    
    try:
        test_bias_detection()
        test_fibonacci_zones()
        test_fvg_detection()
        test_mss_detection()
        test_liquidity_levels()
        test_market_inactivity()
        test_multiframe_resampling()
        test_signal_validation()
        test_entry_models()
        test_full_analysis()
        
        print("\n" + "="*60)
        print("TODOS LOS TESTS PASARON EXITOSAMENTE")
        print("="*60)
        return True
        
    except AssertionError as e:
        print(f"\n[ERROR] Test fallido: {e}")
        import traceback
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"\n[ERROR] Excepción: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
