"""
Backtesting script for DEMA20_200 strategy on XAU/USD
"""
import asyncio
import logging
import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import talib as ta
import pytz

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.api import twelvedata
from middleware.database.dbManager import getCandlesFromDb as get_candles_from_db
from Sentinel.data.dataLoader import getParametros as get_parametros
from Sentinel.analysis import technical

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

TIMEZONE = 'America/Mexico_City'


class DEMA20200Backtest:
    def __init__(
        self,
        symbol: str = "XAU/USD",
        timeframe: str = "1h",
        dema20_period: int = 20,
        dema200_period: int = 200,
        min_slope_degrees: float = 10.0,
        slope_periods: int = 50,
        atr_period: int = 14,
        tp_multiplier_alta: float = 2.0,
        tp_multiplier_media_baja: float = 1.5,
        lookback_candles: int = 20,
        confirmation_search: int = 20
    ):
        self.symbol = symbol
        self.timeframe = timeframe
        self.dema20_period = dema20_period
        self.dema200_period = dema200_period
        self.min_slope_degrees = min_slope_degrees
        self.slope_periods = slope_periods
        self.atr_period = atr_period
        self.tp_multiplier_alta = tp_multiplier_alta
        self.tp_multiplier_media_baja = tp_multiplier_media_baja
        self.lookback_candles = lookback_candles
        self.confirmation_search = confirmation_search

    def calculate_dema(self, df: pd.DataFrame) -> tuple:
        dema20 = ta.DEMA(df['close'], timeperiod=self.dema20_period)
        dema200 = ta.DEMA(df['close'], timeperiod=self.dema200_period)
        return dema20, dema200
    
    def calculate_slope(self, dema_values: pd.Series) -> float:
        if len(dema_values) < self.slope_periods:
            return 0
        
        recent_values = dema_values.dropna().tail(self.slope_periods)
        if len(recent_values) < 2:
            return 0
        
        x = np.arange(len(recent_values))
        y = recent_values.values
        
        slope, _ = np.polyfit(x, y, 1)
        
        avg_price = recent_values.mean()
        
        angle_degrees = np.degrees(np.arctan2(slope, avg_price))
        
        return angle_degrees
        
        return angle_degrees
    
    def calculate_slope_from_crossover(self, dema_values: pd.Series, crossover_idx: int, df: pd.DataFrame) -> float:
        start_idx = max(0, crossover_idx - 5)
        end_idx = min(len(df), crossover_idx + 5)
        
        values = dema_values.iloc[start_idx:end_idx].dropna()
        
        if len(values) < 2:
            return 0
        
        start_price = values.iloc[0]
        end_price = values.iloc[-1]
        
        price_change = end_price - start_price
        
        angle_degrees = np.degrees(np.arctan(price_change / start_price * 100))
        
        return angle_degrees

    def calculate_volatility(self, df: pd.DataFrame) -> str:
        atr = ta.ATR(df['high'], df['low'], df['close'], 14)
        current_atr = atr.iloc[-1]
        avg_atr = atr.tail(20).mean()
        
        if current_atr > avg_atr * 1.5:
            return "alta"
        elif current_atr > avg_atr * 1.0:
            return "media"
        else:
            return "baja"

    def detect_crossover(self, dema20: pd.Series, dema200: pd.Series) -> tuple:
        if len(dema20) < 2 or len(dema200) < 2:
            return None, None
        
        cruce_alcista = (dema20 > dema200) & (dema20.shift(1) <= dema200.shift(1))
        cruce_bajista = (dema20 < dema200) & (dema20.shift(1) >= dema200.shift(1))
        
        posiciones_alcistas = np.where(cruce_alcista)[0]
        posiciones_bajistas = np.where(cruce_bajista)[0]
        
        if len(posiciones_alcistas) == 0 and len(posiciones_bajistas) == 0:
            return None, None
        
        ultima_pos_alcista = posiciones_alcistas[-1] if len(posiciones_alcistas) > 0 else -1
        ultima_pos_bajista = posiciones_bajistas[-1] if len(posiciones_bajistas) > 0 else -1
        
        if ultima_pos_alcista > ultima_pos_bajista:
            return "LARGO", int(ultima_pos_alcista)
        elif ultima_pos_bajista > ultima_pos_alcista:
            return "CORTO", int(ultima_pos_bajista)
        
        return None, None
    
    def get_opposite_candle_extreme(self, df: pd.DataFrame, crossover_idx: int, direction: str) -> float:
        lookback = min(20, crossover_idx)
        
        recent_df = df.iloc[crossover_idx - lookback:crossover_idx]
        
        if direction == "LARGO":
            bearish_candles = recent_df[recent_df['close'] < recent_df['open']]
            if not bearish_candles.empty:
                return float(bearish_candles['high'].max())
        else:
            bullish_candles = recent_df[recent_df['close'] > recent_df['open']]
            if not bullish_candles.empty:
                return float(bullish_candles['low'].min())
        
        return None

    async def run_analysis(self, target_datetime: datetime):
        symbol = "XAU/USD"
        
        logger.info(f"=== Backtesting DEMA20_200 para {symbol} en {target_datetime} ===")
        
        apiKey, _, _, nVelas, _ = get_parametros()
        
        start_date = target_datetime - timedelta(days=30)
        end_date = target_datetime + timedelta(days=1)
        
        logger.info(f"Obteniendo datos desde DB (CON ajuste adjustDataframeInplace)...")
        
        df = await get_candles_from_db(symbol, self.timeframe, nVelas)
        df = twelvedata.adjustDataframeInplace(df)
        print(f"\n*** Ajuste: adjustDataframeInplace aplicado ***")
        
        ADJUST_OFFSET = 0
        print(f"\n*** Ajuste aplicado: {ADJUST_OFFSET} ***")
        
        if df is None or df.empty:
            logger.error("No se pudieron obtener datos")
            return
        
        logger.info(f"Datos obtenidos (sin ajuste): {len(df)} velas")
        
        df = technical.calculateFeatures(df)
        
        if 'datetime' in df.columns:
            df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
            df = df.set_index('datetime')
        df = df.sort_index()
        
        if df.index.tz is None:
            df.index = df.index.tz_localize('UTC')
        
        target_dt_aware = target_datetime.replace(tzinfo=pytz.UTC)
        
        if target_dt_aware not in df.index:
            closest_idx = df.index.get_indexer([target_dt_aware], method='nearest')[0]
            logger.info(f"Fecha exacta no encontrada, usando índice más cercano: {df.index[closest_idx]}")
        
        df_analysis = df[df.index <= target_dt_aware]
        
        df = df[df.index <= target_dt_aware + timedelta(hours=24)]
        
        if len(df) < 250:
            logger.error(f"Insufficient data: {len(df)} rows (need at least 250)")
            return
        
        logger.info(f"Analizando {len(df)} velas hasta {target_datetime}")
        
        dema20, dema200 = self.calculate_dema(df)
        
        direction, crossover_idx = self.detect_crossover(dema20, dema200)
        
        if direction is None:
            print(f"\n=== RESULTADO ===")
            print(f"Fecha: {target_datetime}")
            print(f"Símbolo: {symbol}")
            print(f"Señal: SIN CRUCE DEMA20/DEMA200")
            return
        
        print(f"DEBUG: crossover_idx = {crossover_idx}, len(df) = {len(df)}")
        
        candle_time = df.index[crossover_idx]
        
        minutos_desde_cruce = (target_dt_aware - candle_time).total_seconds() / 60
        
        print(f"\n=== CRUCE DETECTADO ===")
        print(f"Fecha del cruce: {candle_time}")
        print(f"Dirección: {direction}")
        print(f"Minutos desde el cruce: {minutos_desde_cruce:.1f}")
        
        slope = self.calculate_slope(dema20)
        volatility = self.calculate_volatility(df)
        
        print(f"\n=== DATOS DEMA20 (últimos 15 valores) ===")
        print(dema20.tail(15))
        
        print(f"\n=== DATOS DEMA200 (últimos 15 valores) ===")
        print(dema200.tail(15))
        
        candle_time = df.index[crossover_idx]
        minutos_desde_cruce = (target_dt_aware - candle_time).total_seconds() / 60
        
        print(f"\n=== CRUCE DETECTADO ===")
        print(f"Fecha del cruce: {candle_time}")
        print(f"Dirección: {direction}")
        print(f"Minutos desde el cruce: {minutos_desde_cruce:.1f}")
        
        slope_from_crossover = self.calculate_slope_from_crossover(dema20, crossover_idx, df)
        slope200_from_crossover = self.calculate_slope_from_crossover(dema200, crossover_idx, df)
        
        print(f"Slope DEMA20 (desde cruce): {slope_from_crossover:.4f}°")
        print(f"Slope DEMA200 (desde cruce): {slope200_from_crossover:.4f}°")
        
        slope = slope_from_crossover
        print(f"Volatilidad: {volatility}")
        
        min_slope = 10
        valid_volatility = True
        
        filtro_pasa = (slope >= min_slope or slope <= -min_slope) and valid_volatility
        
        print(f"Filtro slope (min {min_slope}): {'✓ PASA' if abs(slope) >= min_slope else '✗ FALLA'}")
        print(f"Filtro volatilidad: {volatility} (siempre pasa)")
        
        confirmation_candle_idx = None
        print(f"Buscando vela de confirmación desde índice {crossover_idx + 1}...")
        search_range = min(crossover_idx + 20, len(df))
        print(f"Rango de búsqueda: {crossover_idx + 1} a {search_range}")
        
        for i in range(crossover_idx + 1, search_range):
            close_i = df['close'].iloc[i]
            open_i = df['open'].iloc[i]
            is_bearish = close_i < open_i
            is_bullish = close_i > open_i
            
            if direction == "LARGO":
                if is_bullish:
                    confirmation_candle_idx = i
                    print(f"Vela confirmación LARGO encontrada en índice {i}: O={open_i:.2f} C={close_i:.2f}")
                    break
            else:
                if is_bearish:
                    confirmation_candle_idx = i
                    print(f"Vela confirmación CORTO encontrada en índice {i}: O={open_i:.2f} C={close_i:.2f}")
                    break
        
        if confirmation_candle_idx is None:
            print("No se encontró vela de confirmación")
            print(f"Dirección: {direction}")
            for i in range(crossover_idx + 1, min(crossover_idx + 10, len(df))):
                close_i = df['close'].iloc[i]
                open_i = df['open'].iloc[i]
                candle_dir = "BEARISH" if close_i < open_i else "BULLISH"
                print(f"  Índice {i}: {df.index[i]} O={open_i:.2f} C={close_i:.2f} {candle_dir}")
            return
        
        next_candle_idx = confirmation_candle_idx
        
        next_close = df['close'].iloc[next_candle_idx]
        next_open = df['open'].iloc[next_candle_idx]
        
        print(f"\n=== CONFIRMACIÓN DE TENDENCIA ===")
        print(f"Vela de confirmación #{next_candle_idx - crossover_idx}: Open={next_open}, Close={next_close}")
        
        trend_confirmed = True
        
        entryPrice = float(next_close)
        
        opposite_candle_idx = None
        for i in range(crossover_idx + 1, next_candle_idx + 1):
            if direction == "LARGO":
                if df['close'].iloc[i] < df['open'].iloc[i]:
                    opposite_candle_idx = i
                    break
            else:
                if df['close'].iloc[i] > df['open'].iloc[i]:
                    opposite_candle_idx = i
                    break
        
        if opposite_candle_idx is not None:
            if direction == "LARGO":
                slPrice = float(df['high'].iloc[opposite_candle_idx])
            else:
                slPrice = float(df['low'].iloc[opposite_candle_idx])
        else:
            slPrice = None
        
        if slPrice is None:
            if direction == "LARGO":
                slPrice = float(df['low'].iloc[next_candle_idx])
            else:
                slPrice = float(df['high'].iloc[next_candle_idx])
        
        slDistance = abs(entryPrice - slPrice)
        
        if volatility == "alta":
            tpMultiplier = 2.0
        else:
            tpMultiplier = 1.5
        
        if direction == "LARGO":
            tpPrice = entryPrice + (slDistance * tpMultiplier)
        else:
            tpPrice = entryPrice - (slDistance * tpMultiplier)
        
        print(f"\n=== SEÑAL GENERADA ===")
        print(f"Dirección: {direction}")
        print(f"Entry Price: {entryPrice}")
        print(f"Stop Loss: {slPrice}")
        print(f"Take Profit: {tpPrice}")
        print(f"SL Distance: {slDistance}")
        print(f"Risk/Reward: 1:{tpMultiplier}")
        
        print(f"\n=== RESUMEN FINAL ===")
        print(f"Fecha análisis: {target_datetime}")
        print(f"Símbolo: {symbol}")
        
        direction_str = "COMPRA" if direction == "LARGO" else "VENTA"
        signal = direction_str if filtro_pasa and trend_confirmed else "NO GENERADA"
        print(f"Señal: {signal}")
        
        if filtro_pasa and trend_confirmed:
            print(f"✓ SEÑAL VÁLIDA - Proceder con ejecución")
        else:
            print(f"✗ SEÑAL NO VÁLIDA - No cumple filtros")

async def main():
    target_datetime = datetime(2026, 3, 3, 4, 0, 0)
    
    backtest = DEMA20200Backtest(
        symbol="XAU/USD",
        timeframe="1h",
        dema20_period=20,
        dema200_period=200,
        min_slope_degrees=10.0,
        slope_periods=50,
        atr_period=14,
        tp_multiplier_alta=2.0,
        tp_multiplier_media_baja=1.5,
        lookback_candles=20,
        confirmation_search=20
    )
    await backtest.run_analysis(target_datetime)

if __name__ == "__main__":
    asyncio.run(main())
