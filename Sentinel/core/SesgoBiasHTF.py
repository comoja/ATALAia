"""
==============================================================================
  ESTRATEGIA DE TRADING: SESGO BIAS HTF - Power of 3 (PO3)
==============================================================================
  Implementa análisis Multi-Timeframe (HTF) con:
    - Análisis de sesgo en Mensual, Semanal, Diario, H4
    - Confirmación por cuerpo (no mechas) para rompimientos de estructura
    - Mapeo de liquidez (Swing Highs/Lows, EQH, EQL)
    - Detección de Fair Value Gaps (FVG) - 3 velas
    - Killzones: Londres (2-5 AM NY), NY (8-11 AM NY)
    - Modelo Power of 3 (PO3): Acumulación -> Manipulación -> Distribución
    - Tres modelos de entrada: Break+FVG, IFVG, Retroceso Fibonacci
    - Gestión adaptativa enfocada en win rate 70-80%
    
==============================================================================
"""

import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
import numpy as np
import talib as ta
import pytz

import os
import sys
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.config import constants
from middleware.database import dbManager
from Sentinel.analysis import risk
from Sentinel.analysis import technical
from middleware.utils.communications import sendTelegramAlert
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.analysis.technical import is_in_ote_zone, calculate_ote_zone
from Sentinel.analysis.orderblocks import detect_order_blocks, detect_breaker_blocks, ob_confluence_score

logger = logging.getLogger("sentinel")


class SesgoBiasHTFBot:
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    NY_TZ = pytz.timezone("America/New_York")
    
    KILLZONES = {
        'LONDON': {'start': time(2, 0), 'end': time(5, 0)},
        'NY': {'start': time(8, 0), 'end': time(11, 0)}
    }
    
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}
        
        strategyConfig = dbManager.getStrategyConfig("SesgoBiasHTF")
        
        self.fibonacci_level      = strategyConfig.get('fibonacci_level', 0.50)      if strategyConfig else 0.50
        self.entry_fib_min        = strategyConfig.get('entry_fib_min', 0.25)         if strategyConfig else 0.25
        self.entry_fib_max        = strategyConfig.get('entry_fib_max', 0.50)         if strategyConfig else 0.50
        self.swing_lookback       = strategyConfig.get('swing_lookback', 50)          if strategyConfig else 50
        self.fvg_min_pct          = strategyConfig.get('fvg_min_pct', 0.0001)        if strategyConfig else 0.0001
        self.min_distance_pips    = strategyConfig.get('min_distance_pips', 10)       if strategyConfig else 10
        self.max_signal_age_minutes = strategyConfig.get('max_signal_age_minutes', 60) if strategyConfig else 60
        self.mss_lookback         = strategyConfig.get('mss_lookback', 5)             if strategyConfig else 5
        self.use_killzones        = strategyConfig.get('use_killzones', True)         if strategyConfig else True
        self.volatility_threshold = strategyConfig.get('volatility_threshold', 0.5)  if strategyConfig else 0.5
        # OTE — Optimal Trade Entry (ICT Fibonacci 62-79%)
        self.use_ote_filter   = strategyConfig.get('use_ote_filter', True)   if strategyConfig else True
        self.ote_fib_min      = strategyConfig.get('ote_fib_min', 0.62)      if strategyConfig else 0.62
        self.ote_fib_max      = strategyConfig.get('ote_fib_max', 0.79)      if strategyConfig else 0.79
        self.ote_reduce_conf  = strategyConfig.get('ote_reduce_conf', 15)    if strategyConfig else 15
        
        self.signalsGeneradas = {}
        self.timestamps_signals = {}
        
        logger.info("Bot SesgoBiasHTF iniciado - Power of 3 (PO3) Strategy v2")

    def getMexicoTime(self) -> datetime:
        return datetime.now(self.MEXICO_TZ)

    def getNYTime(self) -> datetime:
        return datetime.now(self.NY_TZ)

    def is_in_killzone(self, dt: Optional[datetime] = None) -> Tuple[bool, str]:
        """
        Verifica si la hora actual está dentro de una killzone.
        Londres: 2:00 AM - 5:00 AM NY
        Nueva York: 8:00 AM - 11:00 AM NY
        """
        if dt is None:
            dt = self.getNYTime()
        
        current_time = dt.time()
        
        for name, kz in self.KILLZONES.items():
            if kz['start'] <= current_time < kz['end']:
                return True, name
        
        return False, 'NONE'

    def resample_ohlcv(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        return technical.resample_to_interval(df, timeframe)
                
        return df_resampled.dropna()

    def _get_pip_multiplier(self, symbol: str) -> float:
        symbol_up = symbol.upper()
        if "XAU" in symbol_up or "GOLD" in symbol_up:
            return 1.0
        if any(pair in symbol_up for pair in ["JPY", "HUF"]):
            return 100.0
        if any(crypto in symbol_up for crypto in ["BTC", "ETH", "SOL", "BNB"]):
            return 1.0
        return 10000.0

    def detect_bias(self, df: pd.DataFrame, lookback: int = 20) -> Tuple[str, Dict]:
        """
        Detecta el sesgo del mercado analizando swings de precio.
        Bullish: HH + HL (Altos más altos, Bajos más altos)
        Bearish: LH + LL (Altos más bajos, Bajos más bajos)
        """
        if len(df) < lookback + 1:
            return 'INDETERMINADO', {}
        
        highs = df['high'].iloc[-lookback:].values
        lows = df['low'].iloc[-lookback:].values
        closes = df['close'].iloc[-lookback:].values
        opens = df['open'].iloc[-lookback:].values
        
        hh_count, hl_count, lh_count, ll_count = 0, 0, 0, 0
        
        for i in range(2, len(highs)):
            if highs[i] > highs[i-1]:
                hh_count += 1
            if lows[i] > lows[i-1]:
                hl_count += 1
            if highs[i] < highs[i-1]:
                lh_count += 1
            if lows[i] < lows[i-1]:
                ll_count += 1
        
        total_swings = hh_count + hl_count + lh_count + ll_count
        if total_swings == 0:
            return 'INDETERMINADO', {'counts': {'hh': 0, 'hl': 0, 'lh': 0, 'll': 0}}
        
        hh_ratio = hh_count / total_swings
        hl_ratio = hl_count / total_swings
        lh_ratio = lh_count / total_swings
        ll_ratio = ll_count / total_swings
        
        counts = {'hh': hh_count, 'hl': hl_count, 'lh': lh_count, 'll': ll_count}
        
        if hh_ratio > 0.4 and hl_ratio > 0.3:
            return 'ALCISTA', {'counts': counts, 'ratios': {'hh': hh_ratio, 'hl': hl_ratio}}
        elif ll_ratio > 0.4 and lh_ratio > 0.3:
            return 'BAJISTA', {'counts': counts, 'ratios': {'ll': ll_ratio, 'lh': lh_ratio}}
        elif hh_ratio > lh_ratio and hl_ratio > ll_ratio:
            return 'ALCISTA', {'counts': counts, 'ratios': {'hh': hh_ratio, 'hl': hl_ratio}}
        elif lh_ratio > hh_ratio and ll_ratio > hl_ratio:
            return 'BAJISTA', {'counts': counts, 'ratios': {'ll': ll_ratio, 'lh': lh_ratio}}
        
        return 'INDETERMINADO', {'counts': counts}

    def is_market_inactive(self, df: pd.DataFrame, lookback: int = 20) -> bool:
        """
        Filtro de rango: Si el precio está en rango sin ineficiencias,
        el bot debe emitir NO_TRADE.
        """
        if len(df) < lookback + 1:
            return True
        
        relevant = df.iloc[-lookback:]
        range_size = (relevant['high'].max() - relevant['low'].min()) / relevant['close'].iloc[-1]
        
        atr = ta.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
        atr_pct = atr / df['close'].iloc[-1]
        
        if range_size < atr_pct * 2:
            return True
        
        return False

    def find_swing_highs_lows(self, df: pd.DataFrame, lookback: int = 50) -> Dict:
        """
        Encuentra Swing Highs/Lows y niveles de Equal Highs (EQH) y Equal Lows (EQL).
        Los EQH/EQL son los principales imanes de dinero (objetivos de TP).
        """
        if len(df) < lookback + 2:
            return {'swing_highs': [], 'swing_lows': [], 'eqh': [], 'eql': []}
        
        highs = df['high'].iloc[-lookback:].values
        lows = df['low'].iloc[-lookback:].values
        
        swing_highs = []
        swing_lows = []
        
        for i in range(2, len(highs) - 2):
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_highs.append(float(highs[i]))
        
        for i in range(2, len(lows) - 2):
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append(float(lows[i]))
        
        eqh = []
        eqh_values = {}
        for sh in swing_highs:
            rounded = round(sh, 5)
            if rounded in eqh_values:
                eqh_values[rounded] += 1
            else:
                eqh_values[rounded] = 1
        
        for val, count in eqh_values.items():
            if count >= 2:
                eqh.append(val)
        
        eql = []
        eql_values = {}
        for sl in swing_lows:
            rounded = round(sl, 5)
            if rounded in eql_values:
                eql_values[rounded] += 1
            else:
                eql_values[rounded] = 1
        
        for val, count in eql_values.items():
            if count >= 2:
                eql.append(val)
        
        return {
            'swing_highs': sorted(swing_highs, reverse=True),
            'swing_lows': sorted(swing_lows),
            'eqh': sorted(eqh, reverse=True),
            'eql': sorted(eql)
        }

    def get_prev_day_high_low(self, df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
        """
        Obtiene el alto y bajo del día anterior para TP.
        """
        if df is None or len(df) < 2:
            return None, None
        
        df_1d = self.resample_ohlcv(df, '1d')
        if df_1d is not None and len(df_1d) >= 2:
            prev_day = df_1d.iloc[-2]
            return float(prev_day['high']), float(prev_day['low'])
        
        return None, None

    def detect_fvg(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[Dict]:
        """
        Detecta Fair Value Gap (FVG).
        Regla: 3 velas donde el máximo de la 1ra no coincide con el mínimo de la 3ra.
       Solo usa velas terminadas (idx <= len(df) - 2).
        """
        if idx < 2 or idx >= len(df) - 1:
            return None
        
        last_closed_idx = len(df) - 2
        if idx > last_closed_idx:
            return None
        
        if direction == 'LARGO':
            low_n = float(df['low'].iloc[idx])
            high_n2 = float(df['high'].iloc[idx - 2])
            if low_n > high_n2:
                gap = low_n - high_n2
                if gap / float(df['close'].iloc[idx]) >= self.fvg_min_pct:
                    return {
                        'type': 'Bullish_FVG',
                        'top': low_n,
                        'bottom': high_n2,
                        'mid': (high_n2 + low_n) / 2,
                        'size': gap,
                        'idx': idx,
                        'vela_idx': idx
                    }
        else:
            high_n = float(df['high'].iloc[idx])
            low_n2 = float(df['low'].iloc[idx - 2])
            if high_n < low_n2:
                gap = low_n2 - high_n
                if gap / float(df['close'].iloc[idx]) >= self.fvg_min_pct:
                    return {
                        'type': 'Bearish_FVG',
                        'top': low_n2,
                        'bottom': high_n,
                        'mid': (low_n2 + high_n) / 2,
                        'size': gap,
                        'idx': idx,
                        'vela_idx': idx
                    }
        return None

    def detect_mss(self, df: pd.DataFrame, direction: str) -> bool:
        """
        Market Structure Shift (MSS): Cambio de estructura con CIERRE DE CUERPO.
        IMPORTANTE: Se confirma por cuerpo, no por mecha.
        """
        if len(df) < self.mss_lookback + 1:
            return False
        
        closes = df['close'].values
        opens = df['open'].values
        
        if direction == 'CORTO':
            for i in range(len(df) - self.mss_lookback, len(df) - 1):
                prev_high = max(closes[max(0, i-self.mss_lookback):i])
                if closes[-1] < prev_high and closes[-1] < opens[-1]:
                    return True
        else:
            for i in range(len(df) - self.mss_lookback, len(df) - 1):
                prev_low = min(closes[max(0, i-self.mss_lookback):i])
                if closes[-1] > prev_low and closes[-1] > opens[-1]:
                    return True
        
        return False

    def detect_displacement(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[Dict]:
        """
        Displacement: Movimiento de ruptura fuerte que deja FVG.
        La vela debe tener cuerpo dominante (>60%) y mecha pequeña (<25%).
        """
        if idx < 1 or idx >= len(df):
            return None
        
        vela = df.iloc[idx]
        open_p = float(vela['open'])
        close_p = float(vela['close'])
        high_p = float(vela['high'])
        low_p = float(vela['low'])
        
        body = abs(close_p - open_p)
        range_v = high_p - low_p
        
        if range_v == 0 or body / range_v < 0.6:
            return None
        
        if direction == 'CORTO':
            if close_p < open_p and body / range_v >= 0.6:
                if ((close_p - low_p) / range_v) <= 0.25:
                    return {
                        'idx': idx,
                        'type': 'Bearish_Displacement',
                        'body_pct': (body / range_v) * 100,
                        'wick_pct': ((close_p - low_p) / range_v) * 100
                    }
        else:
            if close_p > open_p and body / range_v >= 0.6:
                if ((high_p - close_p) / range_v) <= 0.25:
                    return {
                        'idx': idx,
                        'type': 'Bullish_Displacement',
                        'body_pct': (body / range_v) * 100,
                        'wick_pct': ((high_p - close_p) / range_v) * 100
                    }
        return None

    def check_ote(self, df: pd.DataFrame, direction: str, lookback: int = 50) -> Dict:
        """
        Verifica si el precio actual está en la zona OTE (62-79% de retroceso Fibonacci).
        Usa el último swing completo del período `lookback` como referencia.

        Returns dict con:
            - in_ote    : bool
            - ote_zone  : dict con ote_low, ote_high, sweet_spot
            - swing_high: float
            - swing_low : float
        """
        if len(df) < lookback + 1:
            return {'in_ote': True, 'ote_zone': None}  # Sin datos suficientes, no filtrar

        relevant = df.iloc[-lookback:]
        swing_high = float(relevant['high'].max())
        swing_low  = float(relevant['low'].min())
        price      = float(df['close'].iloc[-1])

        if direction in ('LARGO'):
            # Impulso previo: precio cayó desde swing_high hasta swing_low
            # Ahora buscamos entrada en retroceso alcista (62-79% desde swing_low)
            in_ote, zone = is_in_ote_zone(
                price, swing_high, swing_low, 'LARGO',
                fib_min=self.ote_fib_min, fib_max=self.ote_fib_max
            )
        else:  #  CORTO
            # Impulso previo: precio subió desde swing_low hasta swing_high
            # Ahora buscamos entrada en retroceso bajista (62-79% desde swing_high)
            in_ote, zone = is_in_ote_zone(
                price, swing_low, swing_high, 'CORTO',
                fib_min=self.ote_fib_min, fib_max=self.ote_fib_max
            )

        logger.info(
            f"[SesgoBiasHTF] OTE check '{direction}': price={price:.4f} "
            f"zona=[{zone['ote_low']:.4f}, {zone['ote_high']:.4f}] "
            f"sweet={zone['sweet_spot']:.4f} → {'✅ DENTRO' if in_ote else '⚠️ FUERA'}"
        )
        return {
            'in_ote':     in_ote,
            'ote_zone':   zone,
            'swing_high': swing_high,
            'swing_low':  swing_low,
        }

    def get_ob_analysis(self, df: pd.DataFrame, direction: str, price: float, atr: float) -> Dict:
        """
        Detecta Order Blocks y Breaker Blocks en `df` y retorna
        un dict de análisis con score de confluencia.
        Integrable en cualquier punto de la cadena de señal.
        """
        obs  = detect_order_blocks(df, direction, lookback=80)
        bbs  = detect_breaker_blocks(df, obs)
        conf = ob_confluence_score(price, obs, direction, atr=atr)

        if obs:
            nearest = conf.get('nearest_ob')
            if nearest:
                ob_pos_label = "DENTRO OB" if conf['in_ob_zone'] else ("dist~" + f"{abs(price - nearest['mid']):.4f}")
                logger.info(
                    f"[SesgoBiasHTF] OB mas cercano: {nearest['type']} "
                    f"zona=[{nearest['bottom']:.4f}, {nearest['top']:.4f}] "
                    f"| {ob_pos_label} "
                    f"| Score OB: {conf['score']}"
                )
        if bbs:
            logger.info(f"[SesgoBiasHTF] Breaker Blocks detectados: {len(bbs)} (usable como TP/SL ref)")

        return {
            'order_blocks':   obs,
            'breaker_blocks': bbs,
            'ob_score':       conf['score'],
            'in_ob_zone':     conf['in_ob_zone'],
            'nearest_ob':     conf.get('nearest_ob'),
            'ob_count':       conf['ob_count'],
        }

    def calculate_fibonacci_zone(self, df: pd.DataFrame, direction: str, lookback: int = 50) -> Optional[Dict]:
        """
        Traza Fibonacci del último impulso para definir zonas Premium/Discount.
        Zona Discount: <50% (Solo compras)
        Zona Premium: >50% (Solo ventas)
        """
        if len(df) < lookback + 1:
            return None
        
        relevant_data = df.iloc[-lookback:]
        
        if direction == 'CORTO':
            swing_low = float(relevant_data['low'].min())
            swing_high = float(relevant_data['high'].max())
            
            if swing_high <= swing_low:
                return None
            
            fib_50 = swing_low + (swing_high - swing_low) * self.fibonacci_level
            
            return {
                'type': 'PREMIUM',
                'swing_low': swing_low,
                'swing_high': swing_high,
                'fib_50': fib_50
            }
        else:
            swing_high = float(relevant_data['high'].max())
            swing_low = float(relevant_data['low'].min())
            
            if swing_high <= swing_low:
                return None
            
            fib_50 = swing_low + (swing_high - swing_low) * self.fibonacci_level
            
            return {
                'type': 'DISCOUNT',
                'swing_low': swing_low,
                'swing_high': swing_high,
                'fib_50': fib_50
            }

    def detect_engulfing(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[Dict]:
        """
        Detecta patrón Engulfing (vela de reacción).
        Confirmación: Cuerpo actual envuelve cuerpo anterior.
        """
        if idx < 1 or idx >= len(df):
            return None
        
        prev = df.iloc[idx - 1]
        curr = df.iloc[idx]
        
        prev_open = float(prev['open'])
        prev_close = float(prev['close'])
        prev_high = float(prev['high'])
        prev_low = float(prev['low'])
        
        curr_open = float(curr['open'])
        curr_close = float(curr['close'])
        curr_high = float(curr['high'])
        curr_low = float(curr['low'])
        
        prev_body_top = max(prev_open, prev_close)
        prev_body_bottom = min(prev_open, prev_close)
        
        curr_body_top = max(curr_open, curr_close)
        curr_body_bottom = min(curr_open, curr_close)
        
        if direction == 'CORTO':
            if prev_close > prev_open:
                return None
            if curr_close >= curr_open:
                return None
            
            swept_high = curr_high > prev_high
            engulf_complete = curr_body_top > prev_body_top and curr_body_bottom < prev_body_bottom
            
            if swept_high and engulf_complete:
                return {
                    'type': 'BEARISH_ENGULFING',
                    'idx': idx,
                    'swept': 'HIGH',
                    'sweep_level': prev_high,
                    'candle_high': curr_high,
                    'candle_low': curr_low,
                    'body_top': curr_body_top,
                    'body_bottom': curr_body_bottom,
                    'candle_range': curr_high - curr_low
                }
        else:
            if prev_close < prev_open:
                return None
            if curr_close <= curr_open:
                return None
            
            swept_low = curr_low < prev_low
            engulf_complete = curr_body_top > prev_body_top and curr_body_bottom < prev_body_bottom
            
            if swept_low and engulf_complete:
                return {
                    'type': 'BULLISH_ENGULFING',
                    'idx': idx,
                    'swept': 'LOW',
                    'sweep_level': prev_low,
                    'candle_high': curr_high,
                    'candle_low': curr_low,
                    'body_top': curr_body_top,
                    'body_bottom': curr_body_bottom,
                    'candle_range': curr_high - curr_low
                }
        
        return None

    def detect_liquidity_sweep(self, df: pd.DataFrame, direction: str, liquidity_levels: List[float]) -> Optional[Dict]:
        """
        Liquidity Sweep (Manipulación): Detecta barrido de altos/bajos importantes.
        """
        for level in liquidity_levels:
            for i in range(len(df) - 1, max(0, len(df) - 10), -1):
                vela = df.iloc[i]
                high = float(vela['high'])
                low = float(vela['low'])
                
                if direction == 'CORTO':
                    if high > level:
                        return {
                            'idx': i,
                            'type': 'LIQUIDITY_SWEEP_HIGH',
                            'level': level,
                            'swept_price': high
                        }
                else:
                    if low < level:
                        return {
                            'idx': i,
                            'type': 'LIQUIDITY_SWEEP_LOW',
                            'level': level,
                            'swept_price': low
                        }
        return None

    def detect_ifvg(self, df: pd.DataFrame, direction: str, fvg: Dict) -> bool:
        """
        Inversion FVG: Detecta si el precio atraviesa con cuerpo un FVG contrario.
        """
        for i in range(len(df) - 1, max(0, len(df) - 30), -1):
            vela = df.iloc[i]
            high = float(vela['high'])
            low = float(vela['low'])
            close = float(vela['close'])
            open_p = float(vela['open'])
            
            body_top = max(open_p, close)
            body_bottom = min(open_p, close)
            
            fvg_top = fvg['top']
            fvg_bottom = fvg['bottom']
            
            if direction == 'CORTO':
                if low < fvg_bottom and body_top > fvg_bottom:
                    return True
            else:
                if high > fvg_top and body_bottom < fvg_top:
                    return True
        
        return False

    def get_htf_bias(self, df_h4: pd.DataFrame, df_1d: pd.DataFrame, 
                     df_1w: pd.DataFrame, df_1M: pd.DataFrame) -> Dict[str, str]:
        """
        Obtiene sesgo de cada timeframe: H4, D, W, M.
        """
        biases = {}
        
        for name, df_tf in [('H4', df_h4), ('D', df_1d), ('W', df_1w), ('M', df_1M)]:
            if df_tf is not None and len(df_tf) >= 20:
                bias, _ = self.detect_bias(df_tf, lookback=20)
                biases[name] = bias
            else:
                biases[name] = 'INDETERMINADO'
        
        return biases

    def get_consensus_bias(self, biases: Dict[str, str]) -> Tuple[str, float]:
        """
        Calcula consenso de sesgo ponderado entre timeframes.
        """
        weights = {'H4': 0.35, 'D': 0.35, 'W': 0.20, 'M': 0.10}
        
        bullish_score = sum(weights.get(tf, 0) for tf, bias in biases.items() if bias == 'ALCISTA')
        bearish_score = sum(weights.get(tf, 0) for tf, bias in biases.items() if bias == 'BAJISTA')
        
        if bullish_score > bearish_score:
            return 'LARGO', bullish_score
        elif bearish_score > bullish_score:
            return 'CORTO', bearish_score
        
        return 'NO_TRADE', 0.0

    def calculate_entry_model1_break_fvg(self, fvg: Dict) -> Optional[float]:
        """
        Modelo 1: Entrada en el retroceso al FVG generado tras MSS.
        """
        return float(fvg['mid'])

    def calculate_entry_model2_ifvg(self, fvg: Dict) -> Optional[float]:
        """
        Modelo 2: Inversion FVG - Entrada cuando precio atraviesa FVG contrario.
        """
        return float(fvg['mid'])

    def calculate_entry_model3_fib_retracement(self, engulf_info: Dict, direction: str) -> Optional[float]:
        """
        Modelo 3: Fibonacci interno de la vela de barrido (Engulfing).
        Orden límite entre 25% y 50% de la vela.
        """
        candle_high = engulf_info['candle_high']
        candle_low = engulf_info['candle_low']
        candle_range = candle_high - candle_low
        
        if candle_range == 0:
            return None
        
        if direction == 'CORTO':
            entry_min = candle_high - (candle_range * self.entry_fib_max)
            entry_max = candle_high - (candle_range * self.entry_fib_min)
        else:
            entry_min = candle_low + (candle_range * self.entry_fib_min)
            entry_max = candle_low + (candle_range * self.entry_fib_max)
        
        return (entry_min + entry_max) / 2

    def calculate_sl_from_sweep(self, sweep_info: Dict, direction: str) -> float:
        """
        Stop Loss: Siempre por debajo/encima de la mecha del sweep.
        """
        if direction == 'CORTO':
            return sweep_info['swept_price'] + (sweep_info.get('candle_range', 0.001) * 0.1)
        else:
            return sweep_info['swept_price'] - (sweep_info.get('candle_range', 0.001) * 0.1)

    def calculate_tp_structural(self, df: pd.DataFrame, direction: str, 
                                liquidity: Dict, prev_day: Tuple[Optional[float], Optional[float]]) -> List[float]:
        """
        Take Profit: Objetivos hacia zonas de liquidez externa.
        Prioridad: EQH/EQL > Alto/Bajo día anterior > Swing Highs/Lows.
        """
        tp_levels = []
        
        if direction == 'CORTO':
            if liquidity.get('eqh'):
                for eqh in liquidity['eqh'][:2]:
                    if float(df['close'].iloc[-1]) > eqh:
                        tp_levels.append(eqh)
            
            if prev_day[0] and float(df['close'].iloc[-1]) > prev_day[0]:
                tp_levels.append(prev_day[0])
            
            if liquidity.get('swing_lows'):
                for sl in liquidity['swing_lows'][:2]:
                    if float(df['close'].iloc[-1]) > sl:
                        tp_levels.append(sl)
        else:
            if liquidity.get('eql'):
                for eql in liquidity['eql'][:2]:
                    if float(df['close'].iloc[-1]) < eql:
                        tp_levels.append(eql)
            
            if prev_day[1] and float(df['close'].iloc[-1]) < prev_day[1]:
                tp_levels.append(prev_day[1])
            
            if liquidity.get('swing_highs'):
                for sh in liquidity['swing_highs'][:2]:
                    if float(df['close'].iloc[-1]) < sh:
                        tp_levels.append(sh)
        
        return sorted(tp_levels, reverse=(direction == 'CORTO'))

    def analyze_po3_cycle(self, df: pd.DataFrame, direction: str, 
                          zone: Dict, liquidity: Dict) -> Optional[Dict]:
        """
        Analiza ciclo PO3: Acumulación -> Manipulación -> Distribución.
        """
        if df is None or len(df) < 30:
            return None
        
        price = float(df['close'].iloc[-1])
        
        if direction == 'LARGO':
            if price > zone['fib_50']:
                logger.debug(f"[PO3] Precio {price} por encima de zona Discount {zone['fib_50']}")
                return None
        else:
            if price < zone['fib_50']:
                logger.debug(f"[PO3] Precio {price} por debajo de zona Premium {zone['fib_50']}")
                return None
        
        liquidity_levels = liquidity.get('swing_highs', [])[:3] + liquidity.get('eqh', [])[:2]
        liquidity_levels.extend(liquidity.get('swing_lows', [])[:3] + liquidity.get('eql', [])[:2])
        
        sweep = self.detect_liquidity_sweep(df, direction, liquidity_levels)
        
        if not sweep:
            return None
        
        for i in range(sweep['idx'] - 1, max(0, sweep['idx'] - 20), -1):
            engulf = self.detect_engulfing(df, i, direction)
            if engulf:
                ahora = self.getMexicoTime().replace(tzinfo=None)
                vela_time = df.index[i]
                if hasattr(vela_time, 'to_pydatetime'):
                    vela_time = vela_time.to_pydatetime()
                if vela_time.tzinfo is not None:
                    vela_time = vela_time.replace(tzinfo=None)
                
                minutos_antiguedad = (ahora - vela_time).total_seconds() / 60
                if minutos_antiguedad > self.max_signal_age_minutes:
                    continue
                
                mss = self.detect_mss(df, direction)
                
                displacement = None
                for j in range(i - 1, max(0, i - 10), -1):
                    disp = self.detect_displacement(df, j, direction)
                    if disp:
                        displacement = disp
                        break
                
                fvgs = []
                for k in range(max(1, i - 15), i):
                    fvg = self.detect_fvg(df, k, direction)
                    if fvg:
                        fvgs.append(fvg)
                
                sweep['mss'] = mss
                sweep['displacement'] = displacement is not None
                sweep['fvgs'] = fvgs
                sweep['engulf_info'] = engulf
                sweep['engulf_idx'] = i
                sweep['vela_time'] = vela_time
                sweep['antiguedad_min'] = minutos_antiguedad
                
                return sweep
        
        return None

    def analyze_entry_models(self, df: pd.DataFrame, po3_data: Dict, 
                            direction: str) -> List[Dict]:
        """
        Analiza los 3 modelos de entrada PO3.
        """
        models = []
        
        if not po3_data or not po3_data.get('mss'):
            return models
        
        fvgs = po3_data.get('fvgs', [])
        engulf = po3_data.get('engulf_info')
        
        if not engulf:
            return models
        
        for fvg in fvgs:
            ifvg = self.detect_ifvg(df, direction, fvg)
            
            model_1_entry = self.calculate_entry_model1_break_fvg(fvg)
            models.append({
                'model': 'MODEL_1_BREAK_FVG',
                'entry': model_1_entry,
                'fvg': fvg,
                'ifvg': False
            })
            
            if ifvg:
                model_2_entry = self.calculate_entry_model2_ifvg(fvg)
                models.append({
                    'model': 'MODEL_2_IFVG',
                    'entry': model_2_entry,
                    'fvg': fvg,
                    'ifvg': True
                })
        
        model_3_entry = self.calculate_entry_model3_fib_retracement(engulf, direction)
        models.append({
            'model': 'MODEL_3_FIB_RETRACEMENT',
            'entry': model_3_entry,
            'engulf': engulf,
            'ifvg': False
        })
        
        return models

    def validate_signal(self, entry: float, sl: float, tp: float, 
                       direction: str, symbol: str, df: pd.DataFrame, 
                       model: str) -> Optional[Dict]:
        """
        Valida que la señal cumpla con los criterios mínimos.
        """
        riesgo = abs(entry - sl)
        if riesgo == 0:
            return None
        
        multiplier = self._get_pip_multiplier(symbol)
        
        if riesgo < (self.min_distance_pips / multiplier):
            logger.info(f"[{symbol}] Señal descartada: distancia SL muy pequeña")
            return None
        
        if df is not None and len(df) >= 14:
            atr = ta.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
            if not pd.isna(atr) and atr > 0:
                atr_min_distance = atr * 0.3
                if riesgo < atr_min_distance:
                    logger.info(f"[{symbol}] Señal descartada: distancia SL < 0.3*ATR")
                    return None
        
        distancia_tp = abs(tp - entry)
        if distancia_tp < (self.min_distance_pips / multiplier):
            logger.info(f"[{symbol}] Señal descartada: distancia TP muy pequeña")
            return None
        
        rr_ratio = distancia_tp / riesgo if riesgo > 0 else 0
        
        # --- SEMÁFORO DE ENTRADA (Price Action) ---
        # Al ser el momento de la detección el progreso es 0%
        status_msg = "EN ZONA ✅"

        return {
            'tipo_entrada': model,
            'direccion': direction,
            'entrada': round(entry, 5),
            'stop_loss': round(sl, 5),
            'take_profit': round(tp, 5),
            'status': status_msg,
            'riesgo_pips': round(riesgo * multiplier, 1),
            'rr_ratio': round(rr_ratio, 2),
            'timeframe_entrada': 'H4',
            'timeframe_confirmacion': 'D',
            'confianza': 75
        }

    def analyze_top_down(self, datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> Dict:
        """
        Análisis Top-Down: Desde HTF hasta entrada.
        """
        df_4h = datos.get('4h')
        df_1d = datos.get('1d')
        df_1w = datos.get('1w')
        df_1M = datos.get('1M')
        
        if df_4h is None or len(df_4h) < 50:
            return {'status': 'DATOS_INSUFICIENTES'}
        
        biases = self.get_htf_bias(df_4h, df_1d, df_1w, df_1M)
        logger.info(f"[{symbolInfo['symbol']}] Bias HTF: {biases}")
        
        bias, confidence = self.get_consensus_bias(biases)
        
        if bias == 'NO_TRADE':
            logger.info(f"[{symbolInfo['symbol']}] Sesgo indeterminado o mercado inactivo")
            return {'status': 'NO_TRADE', 'biases': biases}
        
        if self.is_market_inactive(df_4h):
            logger.info(f"[{symbolInfo['symbol']}] Mercado inactivo detectado")
            return {'status': 'MERCADO_INACTIVO', 'biases': biases}
        
        in_killzone, zone_name = self.is_in_killzone()
        logger.info(f"[{symbolInfo['symbol']}] Killzone: {zone_name} ({in_killzone})")
        
        # ── Veto Estricto de Killzone (ICT) - COMENTADO PARA PRUEBAS ─────────
        # if self.use_killzones and not in_killzone:
        #     logger.info(f"[{symbolInfo['symbol']}] ⛔ VETO: Fuera de ventana horaria Killzone")
        #     return {'status': 'FUERA_DE_KILLZONE', 'biases': biases}
        # ─────────────────────────────────────────────────────────────────────
        
        direction = 'LARGO' if bias == 'LARGO' else 'CORTO'
        
        if df_1d is not None and len(df_1d) >= 20:
            zone = self.calculate_fibonacci_zone(df_1d, direction, lookback=self.swing_lookback)
        else:
            zone = self.calculate_fibonacci_zone(df_4h, direction, lookback=self.swing_lookback)
        
        if zone is None:
            return {'status': 'ZONA_NO_DETECTADA'}
        
        logger.info(f"[{symbolInfo['symbol']}] Zona {zone['type']}: Fib50={zone['fib_50']:.5f}")
        
        liquidity_4h = self.find_swing_highs_lows(df_4h, lookback=50)
        liquidity_1d = self.find_swing_highs_lows(df_1d, lookback=20) if df_1d is not None else liquidity_4h
        
        from Sentinel.analysis import technical
        prev_day_data = technical.get_prev_day_high_low(df_4h)
        prev_day = (prev_day_data['pdh'], prev_day_data['pdl'])
        logger.info(f"[{symbolInfo['symbol']}] Prev Day H/L: {prev_day[0]:.4f}/{prev_day[1]:.4f}" if prev_day[0] else "N/A")
        
        po3_daily = self.analyze_po3_cycle(df_1d, direction, zone, liquidity_1d)
        
        if not po3_daily:
            logger.info(f"[{symbolInfo['symbol']}] Sin ciclo PO3 válido en diario")
            return {'status': 'SIN_CONFIRMACION_DIARIA', 'zone': zone, 'biases': biases}
        
        logger.info(f"[{symbolInfo['symbol']}] PO3 detectado en diario: Sweep={po3_daily['type']}, MSS={po3_daily['mss']}")
        
        po3_4h = self.analyze_po3_cycle(df_4h, direction, zone, liquidity_4h)
        
        po3_final = po3_4h if po3_4h else po3_daily
        df_refinement = df_4h if po3_4h else df_1d
        liquidity_refinement = liquidity_4h if po3_4h else liquidity_1d
        
        entry_models = self.analyze_entry_models(df_refinement, po3_final, direction)
        
        if not entry_models:
            return {'status': 'SIN_MODELOS_VALIDOS', 'po3': po3_final, 'zone': zone}

        # ── OTE Filter (Fibonacci 62-79%) ─────────────────────────────────────
        ote_data = {'in_ote': True, 'ote_zone': None}
        if self.use_ote_filter:
            ote_data = self.check_ote(df_refinement, direction, lookback=self.swing_lookback)
            if not ote_data['in_ote']:
                logger.info(
                    f"[{symbolInfo['symbol']}] Precio fuera de OTE "
                    f"(zona=[{ote_data['ote_zone']['ote_low']:.4f}, "
                    f"{ote_data['ote_zone']['ote_high']:.4f}]). "
                    f"Confianza reducida en -{self.ote_reduce_conf}%."
                )
        # ─────────────────────────────────────────────────────────────────────

        best_signal = None
        best_rr = 0
        
        for model_data in entry_models:
            entry = model_data['entry']
            if entry is None:
                continue
            
            sl = self.calculate_sl_from_sweep(po3_final, direction)
            
            tp_levels = self.calculate_tp_structural(df_refinement, direction, liquidity_refinement, prev_day)
            
            if not tp_levels:
                continue
            
            tp = tp_levels[0]
            
            validation = self.validate_signal(
                entry, sl, tp, direction,
                symbolInfo['symbol'], df_refinement, model_data['model']
            )
            
            if validation and validation['rr_ratio'] > best_rr:
                best_rr = validation['rr_ratio']
                best_signal = validation
        
        if best_signal is None:
            return {'status': 'SENAL_INVALIDA', 'zone': zone, 'biases': biases}
        
        # Ajustar confianza según OTE
        confianza_base = best_signal.get('confianza', 75)
        if not ote_data['in_ote']:
            best_signal['confianza'] = max(40, confianza_base - self.ote_reduce_conf)
        else:
            best_signal['confianza'] = min(95, confianza_base + 5)  # Bonus OTE confirmado

        # ── Killzone activa: bonus de confianza ────────────────────────────────
        if in_killzone:
            best_signal['confianza'] = min(95, best_signal['confianza'] + 10)
            logger.info(
                f"[{symbolInfo['symbol']}] Killzone {zone_name} activa ✅ "
                f"+10% confianza → {best_signal['confianza']}%"
            )
        else:
            logger.info(
                f"[{symbolInfo['symbol']}] Fuera de killzone — "
                f"señal válida (HTF), sin bonus temporal"
            )
        # ────────────────────────────────────────────────────────────

        # ── Order Blocks & Breaker Blocks ──────────────────────────────────
        entry_price = best_signal.get('entrada', float(df_refinement['close'].iloc[-1]))
        atr_for_ob  = float(ta.ATR(df_refinement['high'], df_refinement['low'], df_refinement['close'], 14).dropna().iloc[-1]) if len(df_refinement) >= 14 else 0.001
        ob_analysis  = self.get_ob_analysis(df_refinement, direction, entry_price, atr_for_ob)
        # Bonus si el precio está dentro de un Order Block alineado
        if ob_analysis['in_ob_zone']:
            best_signal['confianza'] = min(95, best_signal['confianza'] + 10)
            logger.info(
                f"[{symbolInfo['symbol']}] Precio en zona OB ✅ "
                f"+10% confianza → {best_signal['confianza']}%"
            )
        # ────────────────────────────────────────────────────────────

        best_signal['biases'] = biases
        best_signal['zone'] = zone
        best_signal['po3_type'] = po3_final.get('type', 'UNKNOWN')
        best_signal['mss'] = po3_final.get('mss', False)
        best_signal['killzone'] = zone_name if in_killzone else 'NONE'
        best_signal['prev_day_high'] = prev_day[0]
        best_signal['prev_day_low'] = prev_day[1]
        best_signal['ote_in_zone'] = ote_data['in_ote']
        best_signal['ote_zone'] = ote_data.get('ote_zone')
        best_signal['order_blocks']   = ob_analysis.get('order_blocks', [])
        best_signal['breaker_blocks'] = ob_analysis.get('breaker_blocks', [])
        best_signal['ob_score']       = ob_analysis.get('ob_score', 0)
        best_signal['nearest_ob']     = ob_analysis.get('nearest_ob')
        
        now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
        last_closed = get_last_closed_candle(now_cdmx, interval=5)
        best_signal['candle_time'] = last_closed.strftime("%Y-%m-%d %H:%M:%S")
        
        return {'status': 'SENAL_GENERADA', 'senal': best_signal}

    async def _executeTrades(self, signal: Dict, symbolInfo: Dict, df_used: pd.DataFrame = None):
        if not signal:
            return

        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                return

        for account in self.accounts:
            # Excluir cuenta maestra de señales (SENTINEL)
            if account['idCuenta'] == 1: continue
            if not dbManager.isEstrategiaHabilitadaParaCuenta(account['idCuenta'], 'SesgoBiasHTF'):
                continue
            
            entry_price = signal['entrada']
            sl_price = signal['stop_loss']
            sl_distance = abs(entry_price - sl_price)
            
            posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=sl_distance,
                symbolInfo=symbolInfo,
                entryPrice=entry_price
            )
            
            if posSize is None or posSize == 0:
                continue
            
            signal['profit'] = riskUsd
            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbolInfo['symbol'],
                "direction": signal['direccion'],
                "entryPrice": entry_price,
                "openTime": self.getMexicoTime().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": sl_price,
                "takeProfit": signal['take_profit'],
                "size": posSize,
                "intervalo": "4h",
                "status": "OPEN",
                "strategy": "SesgoBiasHTF",
                "margin_used": marginUsed,
            }
            
            from middleware.execution.broker_gateway import gateway
            
            signal_norm = {
                **signal,
                "direction": signal.get("direccion"),
                "entryPrice": signal.get("entrada"),
                "confidence": signal.get("confianza", 75),
                "setup": signal.get("tipo_entrada", "SESGO_BIAS_HTF_PO3"),
                "candle_time": signal.get("candle_time", self.getMexicoTime().strftime("%Y-%m-%d %H:%M:%S"))
            }
            
            success, msgId = await gateway.execute_trade(trade, signal_norm, account, "SesgoBiasHTF", df=df_used)
            if success and msgId:
                self.lastMessageIds[symbolInfo['symbol']] = msgId
        
        self.signalsGeneradas[symbolInfo['symbol']] = True

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, 
                                        preloadedData: Dict = None, 
                                        apiKey: str = None):
        symbol = symbolInfo['symbol']
        logger.info(f"▶ ENTRANDO análisis SesgoBiasHTF PO3 para {symbol}")
        
        df_4h = preloadedData.get('4h') if preloadedData else None
        
        if df_4h is None or len(df_4h) < 100:
            logger.info(f"◀ SALIENDO análisis para {symbol} (datos insuficientes 4h)")
            return
        
        df_1d = self.resample_ohlcv(df_4h, '1D')
        df_1w = self.resample_ohlcv(df_4h, '1W')
        df_1M = self.resample_ohlcv(df_4h, '1M')
        
        datos = {
            '4h': df_4h, 
            '1d': df_1d, 
            '1w': df_1w, 
            '1M': df_1M
        }
        
        if self.signalsGeneradas.get(symbol, False) and self.timestamps_signals.get(symbol):
            ahora = self.getMexicoTime().replace(tzinfo=None)
            minutos_desde = (ahora - self.timestamps_signals[symbol]).total_seconds() / 60
            if minutos_desde > 30:
                self.signalsGeneradas[symbol] = False
            else:
                logger.info(f"[{symbol}] Cooldown: Señal generada hace {minutos_desde:.1f}m")
                return
        
        resultado = self.analyze_top_down(datos, symbolInfo)
        
        # Verificar en DB si ya existe trade abierto para este símbolo
        existing_trade = dbManager.getOpenTradeBySymbol(symbol)
        if existing_trade:
            logger.info(f"[SesgoBiasHTF] Trade ya abierto para {symbol} - omitiendo")
            return
        
        if resultado['status'] == 'SENAL_GENERADA' and resultado.get('senal'):
            señal = resultado['senal']
            self.timestamps_signals[symbol] = self.getMexicoTime().replace(tzinfo=None)
            
            signal_telegram = {
                **señal,
                "strategy": "SesgoBiasHTF",
                "direction": señal['direccion']
            }
            await self._executeTrades(signal_telegram, symbolInfo, df_4h)

        logger.info(f"◀ SALIENDO análisis SesgoBiasHTF PO3 para {symbol} - Status: {resultado['status']}")


def executeSesgoBiasHTF(datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> Optional[Dict]:
    bot = SesgoBiasHTFBot()
    df_4h = datos.get('4h')
    if df_4h is None:
        return None
    
    df_1d = bot.resample_ohlcv(df_4h, '1D')
    df_1w = bot.resample_ohlcv(df_4h, '1W')
    df_1M = bot.resample_ohlcv(df_4h, '1M')
    
    datos_completos = {
        '4h': df_4h,
        '1d': df_1d,
        '1w': df_1w,
        '1M': df_1M
    }
    
    return bot.analyze_top_down(datos_completos, symbolInfo)


if __name__ == "__main__":
    print("SesgoBiasHTF Strategy Module - Power of 3 (PO3) v2")
