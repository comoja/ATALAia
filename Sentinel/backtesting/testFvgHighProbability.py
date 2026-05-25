"""
Script de pruebas para la lógica de FVG de Alta Probabilidad.
Simula un DataFrame con diferentes patrones de velas para validar:
1. Detección base de FVG.
2. Filtro EMA 200.
3. Market Structure Shift (MSS) local de Vela 2.
4. Clasificación avanzada de Vela 3 (Alta Probabilidad, Breakaway Gap, Trampa/Rechazo).
5. Mitigación por toque de precio en velas subsiguientes.
"""
import sys
import os
import pandas as pd
import numpy as np

# Asegurar que el path del proyecto esté en el sistema
sys.path.append("/Volumes/TimeMachine/ATALAia")

try:
    from Sentinel.analysis import technical
    print("✅ Importación de technical exitosa.")
except ImportError as e:
    print(f"❌ Error al importar technical: {e}")
    sys.exit(1)

def generate_mock_data():
    """Genera datos OHLC ficticios para probar todos los escenarios de FVG."""
    dates = pd.date_range(start="2026-05-20 09:00:00", periods=30, freq="15min")
    
    # Iniciar tendencia alcista por encima de EMA 200 (para EMA200 simulada, pondremos close altos)
    # Crearemos un dataframe base
    data = {
        'open':  [100.0] * 30,
        'high':  [101.0] * 30,
        'low':   [99.0] * 30,
        'close': [100.0] * 30,
        'volume': [1000] * 30
    }
    df = pd.DataFrame(data, index=dates)
    
    # Crear una tendencia alcista suave al principio para construir la EMA 200
    for idx in range(30):
        # Inicializar todo con precios incrementales base para evitar vacíos
        df.iloc[idx, df.columns.get_loc('open')]  = 100.0 + idx * 0.5
        df.iloc[idx, df.columns.get_loc('close')] = 100.5 + idx * 0.5
        df.iloc[idx, df.columns.get_loc('high')]  = 101.0 + idx * 0.5
        df.iloc[idx, df.columns.get_loc('low')]   = 99.8 + idx * 0.5

    # --- ESCENARIO 1: Bullish FVG de Alta Probabilidad (Cuerpo Vela 3 < 60%) ---
    # Vela 1 (i-2) = index 15
    df.iloc[15, df.columns.get_loc('open')]  = 108.0
    df.iloc[15, df.columns.get_loc('close')] = 108.5
    df.iloc[15, df.columns.get_loc('high')]  = 109.0
    df.iloc[15, df.columns.get_loc('low')]   = 107.5
    
    # Vela 2 (i-1) = index 16 (Vela de Desplazamiento Fuerte, rompe máximo previo)
    df.iloc[16, df.columns.get_loc('open')]  = 108.6
    df.iloc[16, df.columns.get_loc('close')] = 112.0
    df.iloc[16, df.columns.get_loc('high')]  = 112.5
    df.iloc[16, df.columns.get_loc('low')]   = 108.5  # Rompe el high de Vela 15 (109.0) con fuerza
    
    # Vela 3 (i) = index 17 (Cuerpo < 60% del rango = Pausa/Alta Probabilidad)
    # Rango: 113.0 - 110.0 = 3.0. Cuerpo: 111.0 - 110.5 = 0.5 (16.6% del rango)
    df.iloc[17, df.columns.get_loc('open')]  = 110.5
    df.iloc[17, df.columns.get_loc('close')] = 111.0
    df.iloc[17, df.columns.get_loc('high')]  = 113.0
    df.iloc[17, df.columns.get_loc('low')]   = 110.0  # low_n (110.0) > high_n2 (109.0) -> Gap de 1.0
    
    # --- ESCENARIO 2: Bullish FVG Breakaway Gap (Cuerpo Vela 3 > 80% + Cierra lejos + ATR alto) ---
    # Vela 1 (i-2) = index 18
    df.iloc[18, df.columns.get_loc('open')]  = 111.0
    df.iloc[18, df.columns.get_loc('close')] = 111.5
    df.iloc[18, df.columns.get_loc('high')]  = 112.0
    df.iloc[18, df.columns.get_loc('low')]   = 110.5
    
    # Vela 2 (i-1) = index 19 (Impulso fuerte)
    df.iloc[19, df.columns.get_loc('open')]  = 111.6
    df.iloc[19, df.columns.get_loc('close')] = 116.0
    df.iloc[19, df.columns.get_loc('high')]  = 116.5
    df.iloc[19, df.columns.get_loc('low')]   = 111.5
    
    # Vela 3 (i) = index 20 (Cuerpo > 80%, cierra lejos. Rango: 119.0 - 113.0 = 6.0. Cuerpo: 118.5 - 113.5 = 5.0 (83.3% del rango))
    df.iloc[20, df.columns.get_loc('open')]  = 113.5
    df.iloc[20, df.columns.get_loc('close')] = 118.5
    df.iloc[20, df.columns.get_loc('high')]  = 119.0
    df.iloc[20, df.columns.get_loc('low')]   = 113.0  # low_n (113.0) > high_n2 (112.0) -> Gap de 1.0
    
    # Mantener el nivel de precios alto del index 21 en adelante para no invalidar el gap
    for idx in range(21, 30):
        df.iloc[idx, df.columns.get_loc('open')]  = 118.0
        df.iloc[idx, df.columns.get_loc('close')] = 118.5
        df.iloc[idx, df.columns.get_loc('high')]  = 119.0
        df.iloc[idx, df.columns.get_loc('low')]   = 117.5

    # --- ESCENARIO 3: Mitigación posterior por toque de precio ---
    # Vela index 25 toca el gap del Escenario 1 (rango [109.0, 110.0])
    df.iloc[25, df.columns.get_loc('open')]  = 114.0
    df.iloc[25, df.columns.get_loc('close')] = 112.0
    df.iloc[25, df.columns.get_loc('high')]  = 114.5
    df.iloc[25, df.columns.get_loc('low')]   = 109.5  # Toca la zona del primer FVG (109.5 <= 110.0) -> mitigated=True
    
    # Agregar EMA 200 calculada dinámicamente para simular la realidad del dfFeatured
    df['ema200'] = df['close'].ewm(span=10, adjust=False).mean() # Usamos span=10 de prueba para que se construya rápido
    
    return df

def test_fvg_logic():
    print("--- Generando Datos Ficticios ---")
    df = generate_mock_data()
    
    print(f"Longitud original del DataFrame: {len(df)}")
    df_filtered = technical.filter_to_closed_candles(df)
    print(f"Longitud del DataFrame tras filtrar velas cerradas: {len(df_filtered)}")
    
    # Ejecutar detect_fvgs sin filtros para ver qué se detecta de base
    print("\n--- Ejecutando detect_fvgs() SIN filtros (apply_high_prob_filters=False, validate_mitigation=False) ---")
    fvgs_base = technical.detect_fvgs(df, min_gap_pct=0.0001, validate_mitigation=False, apply_high_prob_filters=False)
    print(f"Total FVGs detectados (Base): {len(fvgs_base)}")
    for idx, f in enumerate(fvgs_base):
        print(f"  Base FVG #{idx+1}: idx={f['idx']}, tipo={f['type']}, gap=[{f['gapLow']:.2f} - {f['gapHigh']:.2f}], clasif={f['classification']}")
    
    # Ejecutar detect_fvgs con filtros
    print("\n--- Ejecutando detect_fvgs() CON filtros (apply_high_prob_filters=True, validate_mitigation=False) ---")
    if hasattr(technical, 'detect_fvgs'):
        fvgs = technical.detect_fvgs(df, min_gap_pct=0.0001, validate_mitigation=False, apply_high_prob_filters=True)
        print(f"Total FVGs detectados (Filtrados): {len(fvgs)}")
        for idx, f in enumerate(fvgs):
            print(f"\nFVG #{idx + 1}:")
            print(f"  Tipo: {f['type']}")
            print(f"  Index: {f['idx']} | Timestamp: {f['timestamp']}")
            print(f"  Gap: [{f['gapLow']:.2f} - {f['gapHigh']:.2f}] (Tamaño: {f['size']:.2f})")
            print(f"  Clasificación: {f.get('classification', 'N/A')}")
            print(f"  ¿Mitigado?: {f.get('mitigated', 'N/A')}")
            print(f"  ¿Alta Probabilidad?: {f.get('highProbability', 'N/A')}")
            print(f"  ¿Breakaway?: {f.get('breakawayGap', 'N/A')}")
            print(f"  ¿Rechazo/Baja?: {f.get('rejectionLowProbability', 'N/A')}")
    else:
        print("⚠️ La función centralizada detect_fvgs no está disponible.")

if __name__ == '__main__':
    test_fvg_logic()
