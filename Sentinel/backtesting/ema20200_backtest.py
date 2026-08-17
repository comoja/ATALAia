import sys
import os
import types

# Mock the finnhub module to avoid import errors
finnhub_mock = types.ModuleType('finnhub')
class FinnhubClient:
    def __init__(self, *args, **kwargs):
        pass
finnhub_mock.Client = FinnhubClient
sys.modules['finnhub'] = finnhub_mock

# Now we can safely import the modules
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.api import twelvedata
from middleware.database.dbManager import getCandlesFromDb as get_candles_from_db
from Sentinel.data.dataLoader import getParametros as get_parametros
from Sentinel.analysis import technical

import asyncio
import logging

import sys
import types
import os

# Mock finnhub
finnhub_mock = types.ModuleType('finnhub')
class FinnhubClient:
    def __init__(self, *args, **kwargs):
        pass
finnhub_mock.Client = FinnhubClient
sys.modules['finnhub'] = finnhub_mock

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.api import twelvedata
from middleware.database.dbManager import getCandlesFromDb as get_candles_from_db
from Sentinel.data.dataLoader import getParametros as get_parametros
from Sentinel.analysis import technical

from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import talib as ta
import pytz


def adjustDataframeInplace(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["range"] = df["high"] - df["low"]
    df["spread"] = df["range"] * 0.2
    spread_high = df["spread"] * 0.3
    spread_low = df["spread"] * 0.7
    df["high"] = df["high"] + spread_high
    df["low"] = df["low"] - spread_low
    adjustment = (spread_high - spread_low) / 2
    df["open"] = df["open"] + adjustment
    df["close"] = df["close"] + adjustment
    return df


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TIMEZONE = 'America/Mexico_City'


class EMA20200Backtest:
    def __init__(
        self,
        symbol: str = "XAU/USD",
        timeframe: str = "1h",
        ema20_period: int = 20,
        ema200_period: int = 200,
        min_slope: float = 0.5,
        min_separation_pct: float = 0.001,
        atr_period: int = 14
    ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.ema20_period = ema20_period
        self.ema200_period = ema200_period
        self.min_slope = min_slope
        self.min_separation_pct = min_separation_pct
        self.atr_period = atr_period

    def resample_to_1h(self, df):
        df = df.copy()
        df.index = pd.to_datetime(df.index)
        
        df1h = df.resample('1h', label='right', closed='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })
        
        if df.index[-1] < df1h.index[-1]:
            df1h = df1h.iloc[:-1]
        
        return df1h.dropna()

    def calculate_ema(self, df, period):
        return ta.EMA(df['close'], timeperiod=period)

    def calculate_slope(self, series):
        slope_periods = 10
        y = series.dropna().tail(slope_periods).values
        if len(y) < slope_periods:
            return 0
        
        import math
        
        # Calcular pendiente basada en el cambio de valor a lo largo del tiempo
        # Usamos la diferencia entre el primer y ultimo valor de las ultimas 10 velas
        cambio = y[-1] - y[0]
        
        # Calcular como angulo (degrees)
        # Dividimos por el valor medio para obtener un cambio relativo
        media = np.mean(y)
        cambio_pct = (cambio / media) * 100
        
        # Angulo basado en el cambio porcentual
        slope_degrees = math.degrees(math.atan(cambio_pct / 10))
        
        print(f"  EMA20 valores: {y}")
        print(f"  Cambio 10 velas: {cambio:.6f} ({cambio_pct:.4f}%)")
        print(f"  Slope grados: {slope_degrees:.2f}°")
        
        return slope_degrees

    def calculate_atr(self, df):
        return ta.ATR(df['high'], df['low'], df['close'], timeperiod=self.atr_period)

    def detect_cross(self, ema_fast, ema_slow):
        diff = ema_fast - ema_slow
        
        cross_up = (diff > 0) & (diff.shift(1) <= 0)
        cross_down = (diff < 0) & (diff.shift(1) >= 0)
        
        # Buscar cruce en las ultimas 120 horas (5 dias)
        for i in range(len(diff) - 1, max(len(diff) - 120, 0), -1):
            if cross_up.iloc[i]:
                return "LARGO", i
            if cross_down.iloc[i]:
                return "CORTO", i
        
        return None, None

    def valid_separation(self, ema_fast, ema_slow):
        diff = abs(ema_fast.iloc[-1] - ema_slow.iloc[-1])
        return (diff / ema_slow.iloc[-1]) > self.min_separation_pct

    async def run_analysis(self, target_datetime: datetime, symbol: str = None):
        symbol = symbol or self.symbol
        
        logger.info(f"=== Backtesting EMA20_200 para {symbol} en {target_datetime} ===")
        
        apiKey, _, _, nVelas, _ = get_parametros()
        
        df = await get_candles_from_db(symbol, self.timeframe, nVelas)
        
        if df is None or df.empty:
            logger.error("No se pudieron obtener datos")
            return
        
        logger.info(f"Datos obtenidos: {len(df)} velas")
        
        df = technical.calculateFeatures(df)
        
        # Aplicar ajuste de precios
        df = adjustDataframeInplace(df)
        
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            df = df.set_index('datetime')
        df = df.sort_index()
        
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        
        target_dt_aware = target_datetime.replace(tzinfo=pytz.UTC)
        
        df = df[df.index <= target_dt_aware + timedelta(hours=24)]
        
        if len(df) < 300:
            logger.error(f"Datos insuficientes: {len(df)} filas")
            return
        
        logger.info(f"Analizando {len(df)} velas hasta {target_datetime}")
        
        # ===== TODO EN 1H =====
        if len(df) < 250:
            logger.error(f"Datos insuficientes: {len(df)} filas")
            return
        
        logger.info(f"Analizando {len(df)} velas hasta {target_datetime}")
        
        # Calcular EMAs
        ema20 = self.calculate_ema(df, self.ema20_period)
        ema200 = self.calculate_ema(df, self.ema200_period)
        
        print("\n" + "="*50)
        print("=== ANALISIS DE CRUCE EN 1H ===")
        print("="*50)
        
        # Mostrar datos de las ultimas 5 velas
        print("\n--- Ultimas 5 velas ---")
        print(df[['open', 'high', 'low', 'close']].tail(5))
        
        # Mostrar EMAs de las ultimas 5 velas
        print("\n--- EMAs ultimas 5 velas ---")
        ema20_df = pd.DataFrame({'ema20': ema20, 'ema200': ema200})
        print(ema20_df.tail(5))
        
        # Detectar cruce (busca los ultimos 48 dias)
        direction, idx = self.detect_cross(ema20, ema200)
        
        print(f"\n--- Cruce detectado: {direction} ---")
        
        if idx is not None:
            cruce_time = df.index[idx]
            horas_desde_cruce = (target_dt_aware - cruce_time).total_seconds() / 3600
            print(f"Fecha cruce: {cruce_time}")
            print(f"Horas desde cruce: {horas_desde_cruce:.1f}")
        
        if not direction:
            print("X NO HAY CRUCE")
            print("\n=== RESUMEN: SENAL RECHAZADA ===")
            print("Razon: No hay cruce EMA20/EMA200")
            return
        
        print("V CRUCE DETECTADO")
        
        # Verificar separacion minima
        print(f"\n--- FILTRO 2: Separacion minima {self.min_separation_pct*100}% ---")
        diff = abs(ema20.iloc[-1] - ema200.iloc[-1])
        sep_pct = (diff / ema200.iloc[-1]) * 100
        print(f"Separacion actual: {sep_pct:.3f}%")
        
        if not self.valid_separation(ema20, ema200):
            print(f"X SEPARACION INSUFICIENTE")
            print("\n=== RESUMEN: SENAL RECHAZADA ===")
            print("Razon: Separacion entre EMAs insuficiente")
            return
        print("V SEPARACION OK")
        
        # Verificar slope
        print(f"\n--- FILTRO 3: Slope minimo {self.min_slope}° ---")
        slope = self.calculate_slope(ema20)
        
        print(f"Slope: {slope:.2f}°")
        
        if abs(slope) < self.min_slope:
            print(f"X SLOPE INSUFICIENTE ({slope:.2f}° < {self.min_slope}°)")
            print("\n=== RESUMEN: SENAL RECHAZADA ===")
            print("Razon: Pendiente EMA20 insuficiente")
            return
        print(f"V SLOPE OK ({slope:.2f}°)")
        
        # Verificar ATR
        print(f"\n--- FILTRO 4: ATR debe ser mayor al promedio ---")
        atr_series = self.calculate_atr(df)
        atr = atr_series.iloc[-1]
        atr_avg = atr_series.tail(20).mean()
        print(f"ATR actual: {atr:.5f}")
        print(f"ATR promedio (20): {atr_avg:.5f}")
        
        # Reducido: ahora permite ATR hasta 20% menor que el promedio
        if atr < atr_avg * 0.8:
            print("X ATR MUY BAJO")
            print("\n=== RESUMEN: SENAL RECHAZADA ===")
            print("Razon: ATR muy bajo")
            return
        print("V ATR OK")
        
        # Si llego hasta aqui, hay senal
        print("\n" + "="*50)
        print("=== SENAL GENERADA ===")
        print("="*50)
        print(f"Direccion: {direction}")
        print(f"Precio entrada: {df['close'].iloc[-1]:.5f}")
        
        price = df['close'].iloc[-1]
        if direction == "LARGO":
            sl = price - atr * 1.5
            tp = price + atr * 2.0
        else:
            sl = price + atr * 1.5
            tp = price - atr * 2.0
        
        print(f"Stop Loss: {sl:.5f}")
        print(f"Take Profit: {tp:.5f}")
        print(f"ATR: {atr:.5f}")


async def main():
    # Configura la fecha y simbolo que quieres analizar
    target_datetime = datetime(2026, 3, 26, 8, 0, 0)  # 26 de marzo 2026, 08:00 UTC = 02:00 Mexico
    target_datetime = target_datetime.replace(tzinfo=pytz.UTC)
    
    symbols = [
        "AUD/USD", "EUR/GBP", "EUR/USD", "GBP/CAD", "GBP/JPY", 
        "GBP/USD", "NZD/USD", "USD/CAD", "USD/CHF", "USD/JPY", 
        "USD/MXN", "XAU/USD"
    ]
    
    backtest = EMA20200Backtest()
    
    for symbol in symbols:
        print(f"\n\n{'#'*60}")
        print(f"# ANALIZANDO: {symbol}")
        print(f"# FECHA: {target_datetime}")
        print(f"{'#'*60}")
        
        try:
            await backtest.run_analysis(target_datetime, symbol)
        except Exception as e:
            logger.error(f"Error analisando {symbol}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())