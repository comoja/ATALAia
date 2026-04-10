"""
==============================================================================
  BACKTESTING: SESGO BIAS HTF - Power of 3 (PO3) v2
==============================================================================
  Backtest con las especificaciones actualizadas:
    - Killzones: Londres (2-5 AM NY), NY (8-11 AM NY)
    - Confirmación por cuerpo (no mechas)
    - TP: EQH/EQL + Alto/Bajo día anterior
    
  Uso:
    python backtest_sesgo_bias_htf.py [--symbol SYMBOL] [--days DIAS]
==============================================================================
"""

import argparse
import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta, time
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot
from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging
from middleware.database import dbManager
from middleware.api import twelvedata as tdApi

logger = logging.getLogger(__name__)


async def obtener_datos_reales(symbol: str, timeframe: str = "1h", dias: int = 30) -> Optional[pd.DataFrame]:
    """Obtiene datos reales de la API 12Data."""
    print(f"Descargando datos de {symbol}...")
    
    params = {
        'symbol': symbol.replace('USD', '/USD') if 'USD' in symbol and '/' not in symbol else symbol,
        'interval': timeframe,
        'outputSize': min(dias * 24, 2000),
        'apikey': '98c13fd2d0714dc984ca2791e9e3d521'
    }
    
    df = await tdApi.getTimeSeries(params)
    
    if df is not None and len(df) >= 100:
        print(f"Datos obtenidos: {len(df)} velas")
        return df
    
    return None


def run_analysis(df: pd.DataFrame, symbol: str) -> Dict:
    """Ejecuta análisis completo de la estrategia."""
    print(f"\n{'='*70}")
    print(f"ANÁLISIS SESGO BIAS HTF - {symbol}")
    print(f"{'='*70}")
    
    bot = SesgoBiasHTFBot()
    
    df_4h = bot.resample_ohlcv(df, '4h')
    df_1d = bot.resample_ohlcv(df, '1D')
    df_1w = bot.resample_ohlcv(df, '1W')
    df_1M = bot.resample_ohlcv(df, '1M')
    
    print(f"\n📊 DATOS:")
    print(f"   1H: {len(df)} velas | 4H: {len(df_4h)} | 1D: {len(df_1d)} | 1W: {len(df_1w)}")
    
    datos = {
        '4h': df_4h,
        '1d': df_1d,
        '1w': df_1w,
        '1M': df_1M
    }
    
    symbol_info = {
        'symbol': symbol,
        'tipo': 'METALES' if 'XAU' in symbol else 'FOREX',
        'pip': 1.0
    }
    
    resultado = bot.analyze_top_down(datos, symbol_info)
    
    return resultado


def display_results(resultado: Dict, df: pd.DataFrame):
    """Muestra los resultados del análisis."""
    print(f"\n{'─'*70}")
    print("RESULTADOS")
    print(f"{'─'*70}")
    
    status = resultado['status']
    
    print(f"\n📋 Status: {status}")
    
    if 'biases' in resultado:
        print(f"\n🔍 BIAS HTF:")
        biases = resultado['biases']
        for tf, bias in biases.items():
            emoji = '🟢' if bias == 'ALCISTA' else ('🔴' if bias == 'BAJISTA' else '⚪')
            print(f"   {tf}: {emoji} {bias}")
    
    if 'zone' in resultado:
        zone = resultado['zone']
        print(f"\n🎯 ZONA:")
        print(f"   Tipo: {zone.get('type', 'N/A')}")
        print(f"   Fib 50%: {zone.get('fib_50', 0):.2f}")
    
    if status == 'SENAL_GENERADA' and 'senal' in resultado:
        senal = resultado['senal']
        print(f"\n{'='*70}")
        print("🔔 SEÑAL GENERADA")
        print(f"{'='*70}")
        
        direccion = '🟢 LARGO' if senal['direccion'] == 'LARGO' else '🔴 CORTO'
        print(f"   Dirección: {direccion}")
        print(f"   Modelo: {senal['tipo_entrada']}")
        print(f"   Entry: {senal['entrada']:.2f}")
        print(f"   SL: {senal['stop_loss']:.2f}")
        print(f"   TP: {senal['take_profit']:.2f}")
        print(f"   RR: {senal['rr_ratio']:.2f}")
        print(f"   MSS: {'✅' if senal.get('mss') else '❌'}")
        print(f"   Killzone: {senal.get('killzone', 'N/A')}")
        
        if 'prev_day_high' in senal and senal['prev_day_high']:
            print(f"   Prev Day H/L: {senal['prev_day_high']:.2f} / {senal.get('prev_day_low', 'N/A')}")
    else:
        print(f"\n⚠️  NO SE GENERÓ SEÑAL")


def run_backtest(symbol: str, dias: int = 30):
    """Ejecuta backtest completo."""
    print(f"\n{'#'*70}")
    print(f"# SESGO BIAS HTF - BACKTEST v2")
    print(f"# Símbolo: {symbol} | Período: {dias} días")
    print(f"# Killzones: Londres (2-5 AM NY), NY (8-11 AM NY)")
    print(f"{'#'*70}")
    
    async def run():
        df = await obtener_datos_reales(symbol, '1h', dias)
        
        if df is None or len(df) < 100:
            print("❌ No se pudieron obtener datos")
            return
        
        start_date = datetime(2026, 3, 1)
        df = df[df.index >= start_date].copy()
        
        if len(df) < 50:
            print("❌ Datos insuficientes desde 01/03/2026")
            return
        
        print(f"\n📅 Período анализ: {df.index[0].strftime('%d/%m/%Y')} - {df.index[-1].strftime('%d/%m/%Y')}")
        print(f"💰 Rango precio: {df['low'].min():.2f} - {df['high'].max():.2f}")
        
        resultado = run_analysis(df, symbol)
        display_results(resultado, df)
        
        print(f"\n{'#'*70}")
        print(f"# ANÁLISIS POR PERÍODOS")
        print(f"{'#'*70}")
        
        bot = SesgoBiasHTFBot()
        
        for i in range(4):
            days_back = 7 * (4 - i)
            period_start = df.index[-1] - timedelta(days=days_back)
            period_end = period_start + timedelta(days=7)
            
            df_period = df[(df.index >= period_start) & (df.index < period_end)]
            
            if len(df_period) < 50:
                continue
            
            df_4h_p = bot.resample_ohlcv(df_period, '4h')
            df_1d_p = bot.resample_ohlcv(df_period, '1D')
            
            datos_p = {
                '4h': df_4h_p,
                '1d': df_1d_p,
                '1w': None,
                '1M': None
            }
            
            symbol_info = {'symbol': symbol, 'tipo': 'METALES', 'pip': 1.0}
            
            result_p = bot.analyze_top_down(datos_p, symbol_info)
            
            period_name = f"{period_start.strftime('%d/%m')} - {period_end.strftime('%d/%m')}"
            status_emoji = '✅' if 'SENAL' in result_p['status'] else ('⚠️' if 'NO_TRADE' in result_p['status'] else '❌')
            
            in_kz, kz_name = bot.is_in_killzone(period_end)
            kz_info = f" | KZ: {kz_name}" if in_kz else ""
            
            print(f"   {period_name}: {status_emoji} {result_p['status']}{kz_info}")
        
        print(f"\n✅ Backtest completado")
    
    asyncio.run(run())


def main():
    parser = argparse.ArgumentParser(description='Backtest SesgoBiasHTF PO3 v2')
    parser.add_argument('--symbol', '-s', type=str, default='XAUUSD',
                       help='Símbolo (default: XAUUSD)')
    parser.add_argument('--days', '-d', type=int, default=30,
                       help='Días de datos (default: 30)')
    
    args = parser.parse_args()
    
    symbol = args.symbol.replace('/', '')
    
    run_backtest(symbol, args.days)


if __name__ == "__main__":
    main()
