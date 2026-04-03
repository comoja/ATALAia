"""
==============================================================================
  BACKTESTING: PATRÓN 4H - ICT / Smart Money Concepts
==============================================================================
  Backtest para la estrategia Patron4H con fecha específica.
  
  Uso:
    python backtest_patron4h.py [--fecha YYYY-MM-DD] [--symbol SYMBOL]
  
  Ejemplo:
    python backtest_patron4h.py --fecha 2025-03-19 --symbol EURUSD
  
  Fecha por defecto: 2025-03-19
==============================================================================
"""

import argparse
import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from Sentinel.core.Patron4h import Patron4HBot, executePatron4H
from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging

logger = logging.getLogger(__name__)

FEATURE_COLS = ['open', 'high', 'low', 'close', 'volume']


def generar_datos_sinteticos(symbol: str, fecha: datetime, num_velas_15m: int = 500) -> pd.DataFrame:
    """
    Genera datos sintéticos realistas para backtesting.
    Incluye tendencias, FVG, displacement, etc.
    """
    print(f"Generando datos sintéticos para {symbol}...")
    
    dates = pd.date_range(end=fecha, periods=num_velas_15m, freq='15min')
    
    np.random.seed(42)
    
    if '/' in symbol:
        symbol_key = symbol.replace('/', '').upper()
    else:
        symbol_key = symbol.upper()
    
    base_prices = {
        'EURUSD': 1.0850,
        'EURUSDUSD': 1.0850,
        'GBPUSD': 1.2700,
        'USDJPY': 149.50,
        'XAUUSD': 2150.00,
    }
    base_price = base_prices.get(symbol_key, 1.0850)
    
    if 'XAU' in symbol_key:
        volatility = 0.0015
    else:
        volatility = 0.0003
    
    precios = []
    precio = base_price
    
    for i in range(num_velas_15m):
        if i < num_velas_15m * 0.2:
            trend = 0.000005
        elif i < num_velas_15m * 0.4:
            trend = -0.000008
        elif i < num_velas_15m * 0.6:
            trend = 0.000003
        elif i < num_velas_15m * 0.8:
            trend = -0.00001
        else:
            trend = 0.000006
        
        random_move = np.random.randn() * volatility * 0.8
        cambio = trend + random_move
        precio = precio * (1 + cambio)
        precios.append(precio)
    
    df = pd.DataFrame({'close': precios}, index=dates)
    
    df['open'] = df['close'] * (1 + np.random.uniform(-0.0001, 0.0001, num_velas_15m))
    
    rango = volatility * 0.6
    df['high'] = df[['open', 'close']].max(axis=1) * (1 + np.abs(np.random.randn(num_velas_15m) * rango))
    df['low'] = df[['open', 'close']].min(axis=1) * (1 - np.abs(np.random.randn(num_velas_15m) * rango))
    
    df['high'] = df[['open', 'high']].max(axis=1)
    df['low'] = df[['open', 'low']].min(axis=1)
    
    df['volume'] = np.random.randint(1000, 10000, num_velas_15m)
    
    print(f"Datos generados: {len(df)} velas desde {df.index[0]} hasta {df.index[-1]}")
    print(f"Precio inicio: {df['close'].iloc[0]:.5f} | Precio fin: {df['close'].iloc[-1]:.5f}")
    
    return df


def generar_datos_con_patron_bajista(symbol: str, fecha: datetime, num_velas_15m: int = 500) -> pd.DataFrame:
    """
    Genera datos con un patrón Patron4H BAJISTA completo.
    Incluye:
    - Tendencia previa alcista
    - Liquidity raid (toma de máximo)
    - Displacement bearish fuerte
    - FVG bajista
    - MSS (cambio de estructura)
    """
    print(f"Generando datos con PATRÓN BAJISTA para {symbol}...")
    
    dates = pd.date_range(end=fecha, periods=num_velas_15m, freq='15min')
    
    np.random.seed(42)
    
    base_prices = {'EURUSD': 1.0850, 'GBPUSD': 1.2700, 'XAUUSD': 2150.00, 'EUR': 1.0850}
    base_price = base_prices.get(symbol.replace('/', ''), 1.0850)
    volatility = 0.0003
    
    velas = []
    precio = base_price
    
    num_fases = 10
    velas_por_fase = num_velas_15m // num_fases
    
    for i in range(num_velas_15m):
        fase = i // velas_por_fase
        
        if fase == 0:
            cambio = np.random.randn() * volatility * 0.3
            precio = precio * (1 + cambio)
            
        elif fase == 1:
            cambio = volatility * 0.5 + np.random.randn() * volatility * 0.2
            precio = precio * (1 + cambio)
            
        elif fase == 2:
            cambio = volatility * 0.3 + np.random.randn() * volatility * 0.1
            precio = precio * (1 + cambio)
            
        elif fase == 3:
            if i % 3 == 0:
                cambio = volatility * 0.8
            else:
                cambio = np.random.randn() * volatility * 0.15
            precio = precio * (1 + cambio)
            
        elif fase == 4:
            if i % 4 == 0:
                cambio = -volatility * 1.2
            else:
                cambio = np.random.randn() * volatility * 0.1
            precio = precio * (1 + cambio)
            
        elif fase == 5:
            if i % 5 == 0:
                cambio = volatility * 0.4
            else:
                cambio = np.random.randn() * volatility * 0.08
            precio = precio * (1 + cambio)
            
        elif fase == 6:
            if i % 3 == 0:
                cambio = -volatility * 0.6
            else:
                cambio = np.random.randn() * volatility * 0.1
            precio = precio * (1 + cambio)
            
        elif fase == 7:
            cambio = -volatility * 0.4 + np.random.randn() * volatility * 0.2
            precio = precio * (1 + cambio)
            
        elif fase == 8:
            if i % 2 == 0:
                cambio = -volatility * 1.0
            else:
                cambio = np.random.randn() * volatility * 0.15
            precio = precio * (1 + cambio)
            
        else:
            cambio = -volatility * 0.3 + np.random.randn() * volatility * 0.2
            precio = precio * (1 + cambio)
        
        open_p = precio * (1 + np.random.randn() * volatility * 0.05)
        close_p = precio
        
        high_p = max(open_p, close_p) * (1 + abs(np.random.randn()) * volatility * 0.4)
        low_p = min(open_p, close_p) * (1 - abs(np.random.randn()) * volatility * 0.4)
        
        velas.append({
            'open': open_p,
            'high': high_p,
            'low': low_p,
            'close': close_p,
            'volume': np.random.randint(1000, 10000)
        })
    
    df = pd.DataFrame(velas, index=dates)
    print(f"Datos con patrón BAJISTA generados: {len(df)} velas")
    print(f"Precio inicio: {df['close'].iloc[0]:.5f} | Precio fin: {df['close'].iloc[-1]:.5f}")
    return df


def generar_datos_con_patron_alcista(symbol: str, fecha: datetime, num_velas_15m: int = 500) -> pd.DataFrame:
    """
    Genera datos con un patrón Patron4H ALCISTA completo.
    """
    print(f"Generando datos con PATRÓN ALCISTA para {symbol}...")
    
    dates = pd.date_range(end=fecha, periods=num_velas_15m, freq='15min')
    
    np.random.seed(123)
    
    base_prices = {'EURUSD': 1.0850, 'GBPUSD': 1.2700, 'XAUUSD': 2150.00}
    base_price = base_prices.get(symbol.replace('/', ''), 1.0850)
    volatility = 0.0003
    
    velas = []
    precio = base_price
    
    num_fases = 10
    velas_por_fase = num_velas_15m // num_fases
    
    for i in range(num_velas_15m):
        fase = i // velas_por_fase
        
        if fase == 0:
            cambio = np.random.randn() * volatility * 0.3
            precio = precio * (1 + cambio)
            
        elif fase == 1:
            cambio = -volatility * 0.4 + np.random.randn() * volatility * 0.2
            precio = precio * (1 + cambio)
            
        elif fase == 2:
            if i % 4 == 0:
                cambio = -volatility * 0.6
            else:
                cambio = np.random.randn() * volatility * 0.15
            precio = precio * (1 + cambio)
            
        elif fase == 3:
            if i % 3 == 0:
                cambio = volatility * 1.0
            else:
                cambio = np.random.randn() * volatility * 0.1
            precio = precio * (1 + cambio)
            
        elif fase == 4:
            if i % 5 == 0:
                cambio = -volatility * 0.3
            else:
                cambio = np.random.randn() * volatility * 0.12
            precio = precio * (1 + cambio)
            
        elif fase == 5:
            if i % 4 == 0:
                cambio = volatility * 0.8
            else:
                cambio = np.random.randn() * volatility * 0.1
            precio = precio * (1 + cambio)
            
        elif fase == 6:
            cambio = volatility * 0.5 + np.random.randn() * volatility * 0.2
            precio = precio * (1 + cambio)
            
        elif fase == 7:
            if i % 3 == 0:
                cambio = volatility * 1.2
            else:
                cambio = np.random.randn() * volatility * 0.15
            precio = precio * (1 + cambio)
            
        elif fase == 8:
            cambio = volatility * 0.4 + np.random.randn() * volatility * 0.2
            precio = precio * (1 + cambio)
            
        else:
            if i % 2 == 0:
                cambio = volatility * 0.9
            else:
                cambio = np.random.randn() * volatility * 0.2
            precio = precio * (1 + cambio)
        
        open_p = precio * (1 + np.random.randn() * volatility * 0.05)
        close_p = precio
        
        high_p = max(open_p, close_p) * (1 + abs(np.random.randn()) * volatility * 0.4)
        low_p = min(open_p, close_p) * (1 - abs(np.random.randn()) * volatility * 0.4)
        
        velas.append({
            'open': open_p,
            'high': high_p,
            'low': low_p,
            'close': close_p,
            'volume': np.random.randint(1000, 10000)
        })
    
    df = pd.DataFrame(velas, index=dates)
    print(f"Datos con patrón ALCISTA generados: {len(df)} velas")
    print(f"Precio inicio: {df['close'].iloc[0]:.5f} | Precio fin: {df['close'].iloc[-1]:.5f}")
    return df


async def descargar_datos_reales(symbol: str, fecha: datetime, num_dias: int = 30) -> Optional[pd.DataFrame]:
    """Descarga datos reales de la Base de Datos para backtesting."""
    try:
        from middleware.database.dbManager import getCandlesFromDb
        
        symbol_map = {
            'EURUSD': 'EUR/USD',
            'GBPUSD': 'GBP/USD',
            'USDJPY': 'USD/JPY',
            'XAUUSD': 'XAU/USD',
        }
        symbol_db = symbol_map.get(symbol.upper().replace('/', ''), symbol)
        if '/' not in symbol_db and len(symbol_db) == 6:
            symbol_db = f"{symbol_db[:3]}/{symbol_db[3:]}"
        
        print(f"Obteniendo datos de la BD para {symbol_db}...")
        print(f"Buscando velas hasta: {fecha.strftime('%Y-%m-%d')}")
        
        df = await getCandlesFromDb(symbol_db, "15min", limit=10000)
        
        if df is not None and len(df) > 100:
            df = df.dropna(subset=['close', 'high', 'low', 'open'])
            
            fecha_dt = pd.Timestamp(fecha)
            df_filtrado = df[df.index <= fecha_dt].copy()
            
            if len(df_filtrado) > 100:
                print(f"Datos recibidos: {len(df_filtrado)} velas hasta {fecha.strftime('%Y-%m-%d')}")
                print(f"Rango: {df_filtrado.index[0]} → {df_filtrado.index[-1]}")
                print(f"Precio inicio: {df_filtrado['close'].iloc[0]:.2f} | Precio fin: {df_filtrado['close'].iloc[-1]:.2f}")
                return df_filtrado
            else:
                print(f"Datos insuficientes hasta {fecha}: {len(df_filtrado)} velas")
                print(f"Usando últimos datos disponibles: {len(df)} velas")
                if len(df) > 100:
                    return df.tail(5000)
        else:
            print("No se encontraron datos en la BD")
            return None
            
    except Exception as e:
        print(f"Error obteniendo datos de BD: {e}")
        import traceback
        traceback.print_exc()
        return None


def ejecutar_backtest(bot: Patron4HBot, df_15m: pd.DataFrame, symbol: str, fecha: datetime) -> Dict:
    """
    Ejecuta el backtest para una fecha específica.
    """
    print("\n" + "="*70)
    print(f"  BACKTESTING PATRÓN 4H - {symbol} - {fecha.strftime('%Y-%m-%d')}")
    print("="*70)
    
    df_1h = bot.resample_ohlcv(df_15m, '1H')
    df_4h = bot.resample_ohlcv(df_15m, '4H')
    df_1d = bot.resample_ohlcv(df_15m, '1D')
    
    datos = {
        '15m': df_15m,
        '1h': df_1h,
        '4h': df_4h,
        '1d': df_1d
    }
    
    print(f"\n📊 DATOS RESAMPLEADOS:")
    print(f"   15M: {len(df_15m)} velas")
    print(f"   1H:  {len(df_1h)} velas")
    print(f"   4H:  {len(df_4h)} velas")
    print(f"   1D:  {len(df_1d)} velas")
    
    symbolInfo = {
        'symbol': symbol,
        'intervalo': '15min'
    }
    
    resultados_ciclos = []
    
    print(f"\n🔄 EJECUTANDO CICLOS DE ANÁLISIS (4H → 1H → 15M)...")
    print("-" * 70)
    
    for ciclo_num in range(1, 4):
        ciclo_nombre = ['4H', '1H', '15M'][ciclo_num - 1]
        print(f"\n>>> CICLO {ciclo_num}/3: {ciclo_nombre}")
        
        resultado = bot.ejecutar_ciclo(datos, symbolInfo)
        resultados_ciclos.append(resultado)
        
        print(f"    Status: {resultado['status']}")
        print(f"    Timeframe: {resultado.get('timeframe', 'N/A')}")
        
        if resultado['status'] == 'CATALIZADOR_CONFIRMADO':
            catalizador = bot.estado_patron.get(f"fase2_{ciclo_nombre.lower()}")
            if catalizador:
                print(f"    Displacement: {'✓' if catalizador.get('hay_displacement') else '✗'}")
                print(f"    FVG: {'✓' if catalizador.get('hay_fvg') else '✗'}")
                print(f"    MSS: {'✓' if catalizador.get('hay_mss') else '✗'}")
                print(f"    Reacción POI: {'✓' if catalizador.get('hay_reaccion_poi') else '✗'}")
        
        if resultado['status'] == 'SENAL_GENERADA':
            señal = resultado.get('señal')
            if señal:
                fecha_entrada = señal.get('fecha_entrada', 'N/A')
                fvg = señal.get('fvg', {})
                contexto = bot.estado_patron.get('contexto', {})
                liquidity_raid = bot.estado_patron.get('liquidity_raid', {})
                
                print(f"\n    ╔══════════════════════════════════════════════════════════╗")
                print(f"    ║          ✅✅✅ SEÑAL PATRÓN 4H GENERADA ✅✅✅        ║")
                print(f"    ╠══════════════════════════════════════════════════════════╣")
                print(f"    ║  📅 Fecha/Hora Entrada: {fecha_entrada}")
                print(f"    ║  📊 Dirección: {señal['direccion']}")
                print(f"    ╠══════════════════════════════════════════════════════════╣")
                print(f"    ║  🎯 NIVELES DE LA OPERACIÓN:")
                print(f"    ║  ─────────────────────────────────────────────────────")
                print(f"    ║  Entrada:    {señal['entrada']}")
                print(f"    ║  Stop Loss: {señal['stop_loss']}")
                print(f"    ║  Take Profit: {señal['take_profit']}")
                print(f"    ║  R:R: {señal['rr_ratio']} | Riesgo: {señal['riesgo_pips']} pips")
                print(f"    ╠══════════════════════════════════════════════════════════╣")
                print(f"    ║  🏛️ ZONAS ICT IDENTIFICADAS:")
                print(f"    ║  ─────────────────────────────────────────────────────")
                
                if contexto:
                    print(f"    ║  • Tendencia: {contexto.get('tendencia', 'N/A')}")
                    print(f"    ║  • Max día anterior: {contexto.get('max_dia_anterior', 'N/A')}")
                    print(f"    ║  • Min día anterior: {contexto.get('min_dia_anterior', 'N/A')}")
                
                if liquidity_raid:
                    print(f"    ║  • Liquidity Raid: {liquidity_raid.get('tipo', 'N/A')}")
                    print(f"    ║    Nivel: {liquidity_raid.get('nivel', 'N/A')}")
                
                if fvg:
                    print(f"    ║  • FVG Detectado: {fvg.get('type', 'N/A')}")
                    print(f"    ║    Zona: {fvg.get('start', 'N/A'):.5f} - {fvg.get('end', 'N/A'):.5f}")
                    print(f"    ║    Mitad (entrada): {fvg.get('mid', 'N/A'):.5f}")
                
                print(f"    ║  • Tipo Entrada: {señal.get('tipo_entrada', 'N/A')}")
                print(f"    ║  • Confianza: {señal.get('confianza', 'N/A')}%")
                print(f"    ╚══════════════════════════════════════════════════════════╝")
            break
        
        if resultado['status'] == 'TENDENCIA_LATERAL':
            print(f"    Tendencia lateral detectada - sin operaciones")
            break
    
    print("\n" + "="*70)
    print("  RESUMEN DEL BACKTEST")
    print("="*70)
    
    for i, resultado in enumerate(resultados_ciclos):
        ciclo_nombre = ['4H', '1H', '15M'][i]
        print(f"\n  Ciclo {i+1} ({ciclo_nombre}):")
        print(f"    Status: {resultado['status']}")
        
        if resultado['status'] == 'SENAL_GENERADA':
            señal = resultado.get('señal')
            if señal:
                print(f"    → SEÑAL: {señal['direccion']}")
                print(f"    → Entry: {señal['entrada']:.5f}")
                print(f"    → SL: {señal['stop_loss']:.5f}")
                print(f"    → TP: {señal['take_profit']:.5f}")
                print(f"    → R:R: {señal['rr_ratio']}")
    
    return {
        'symbol': symbol,
        'fecha': fecha,
        'ciclos': resultados_ciclos,
        'estado_final': bot.estado_patron,
        'tiene_senal': any(r['status'] == 'SENAL_GENERADA' for r in resultados_ciclos)
    }


def analizar_para_fecha_especifica(bot: Patron4HBot, df_15m: pd.DataFrame, 
                                   fecha_objetivo: datetime, symbol: str) -> pd.DataFrame:
    """
    Analiza velas hasta una fecha específica y muestra el estado en esa fecha.
    """
    print(f"\n📅 Analizando estado en fecha objetivo: {fecha_objetivo}")
    
    df_hasta_fecha = df_15m[df_15m.index <= fecha_objetivo].copy()
    
    if len(df_hasta_fecha) < 100:
        print(f"⚠️ Datos insuficientes hasta {fecha_objetivo}")
        return pd.DataFrame()
    
    print(f"   Velas hasta la fecha: {len(df_hasta_fecha)}")
    print(f"   Rango: {df_hasta_fecha.index[0]} → {df_hasta_fecha.index[-1]}")
    
    return df_hasta_fecha


async def main():
    parser = argparse.ArgumentParser(description='Backtest Patron4H Strategy')
    parser.add_argument('--fecha', type=str, default='2025-03-19',
                       help='Fecha para backtesting (YYYY-MM-DD)')
    parser.add_argument('--symbol', type=str, default='EURUSD',
                       help='Símbolo a analizar (EURUSD, GBPUSD, XAUUSD, etc.)')
    parser.add_argument('--datos', type=str, default='sintetico',
                       choices=['sintetico', 'patron', 'real'],
                       help='Tipo de datos: sintetico, patron (con patrón), real')
    parser.add_argument('--dias', type=int, default=30,
                       help='Número de días de historia')
    
    args = parser.parse_args()
    
    setupLogging()
    logger = logging.getLogger(__name__)
    
    try:
        fecha_objetivo = datetime.strptime(args.fecha, '%Y-%m-%d')
    except ValueError:
        print(f"❌ Fecha inválida: {args.fecha}")
        return
    
    symbol_map = {
        'EURUSD': 'EURUSD',
        'EUR': 'EURUSD',
        'GBPUSD': 'GBPUSD',
        'GBP': 'GBPUSD',
        'XAUUSD': 'XAUUSD',
        'XAU': 'XAUUSD',
        'GOLD': 'XAUUSD',
        'ORO': 'XAUUSD'
    }
    symbol = symbol_map.get(args.symbol.upper(), args.symbol.upper())
    if '/' not in symbol and symbol != 'XAUUSD':
        symbol = symbol.replace('USD', '/USD') if 'USD' in symbol else symbol
    
    if '/' not in symbol:
        symbol = f"{symbol[:3]}/{symbol[3:]}" if len(symbol) == 6 else symbol
    
    print(f"\n🏁 INICIANDO BACKTEST")
    print(f"   Símbolo: {symbol}")
    print(f"   Fecha objetivo: {fecha_objetivo.strftime('%Y-%m-%d')}")
    print(f"   Tipo de datos: {args.datos}")
    
    if args.datos == 'real':
        df_15m = await descargar_datos_reales(symbol, fecha_objetivo, args.dias)
        if df_15m is None:
            print("⚠️ Usando datos sintéticos por error en descarga...")
            df_15m = generar_datos_sinteticos(symbol, fecha_objetivo, args.dias * 96)
    elif args.datos == 'patron':
        df_15m = generar_datos_con_patron(symbol, fecha_objetivo, args.dias * 96)
    else:
        df_15m = generar_datos_sinteticos(symbol, fecha_objetivo, args.dias * 96)
    
    if df_15m is None or len(df_15m) < 100:
        print("❌ No se pudieron generar datos suficientes")
        return
    
    df_15m = df_15m.dropna(subset=['close', 'high', 'low', 'open'])
    df_15m = df_15m[df_15m.index <= fecha_objetivo + timedelta(hours=1)]
    
    print(f"\n📊 DATOS FINALES:")
    print(f"   Total velas: {len(df_15m)}")
    print(f"   Rango temporal: {df_15m.index[0]} → {df_15m.index[-1]}")
    print(f"   Precio actual: {df_15m['close'].iloc[-1]:.5f}")
    
    bot = Patron4HBot()
    
    resultado = ejecutar_backtest(bot, df_15m, symbol, fecha_objetivo)
    
    print("\n" + "="*70)
    if resultado['tiene_senal']:
        print("  ✅ RESULTADO: SEÑAL GENERADA")
    else:
        print("  ⚠️ RESULTADO: SIN SEÑAL EN ESTE PERÍODO")
    print("="*70)
    
    print(f"\n📋 ESTADO FINAL DEL BOT:")
    estado = resultado['estado_final']
    if estado:
        contexto = estado.get('contexto')
        tendencia = contexto.get('tendencia', 'N/A') if contexto else 'N/A'
        print(f"   Contexto: {tendencia}")
        print(f"   Liquidity Raid: {estado.get('liquidity_raid', 'N/A')}")
        print(f"   Ciclo confirmado: {estado.get('ciclo_confirmado', 'N/A')}")
    else:
        print("   Estado no disponible (reseteado)")
    
    return resultado


if __name__ == "__main__":
    asyncio.run(main())
