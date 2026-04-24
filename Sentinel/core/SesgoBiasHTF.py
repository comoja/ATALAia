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

from middleware.config import constants
from middleware.database import dbManager
from Sentinel.analysis import technical
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.analysis.technical import is_in_ote_zone, calculate_ote_zone, check_tp_exhaustion, check_signal_health
from middleware.utils.alertBuilder import adjustTPForMinRR, getPipMultiplier
from Sentinel.analysis.orderblocks import detect_order_blocks, detect_breaker_blocks, ob_confluence_score

from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class SesgoBiasHTFBot:
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    NY_TZ = pytz.timezone("America/New_York")
    
    KILLZONES = {
        'LONDON': {'start': time(2, 0), 'end': time(5, 0)},
        'NY': {'start': time(8, 0), 'end': time(11, 0)}
    }
    
    def __init__(self):
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
        if dt is None:
            dt = self.getNYTime()
        current_time = dt.time()
        for name, kz in self.KILLZONES.items():
            if kz['start'] <= current_time < kz['end']:
                return True, name
        return False, 'NONE'

    def resample_ohlcv(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        return technical.resample_to_interval(df, timeframe)

    def detect_bias(self, df: pd.DataFrame, lookback: int = 20) -> Tuple[str, Dict]:
        if len(df) < lookback + 1:
            return 'INDETERMINADO', {}
        highs = df['high'].iloc[-lookback:].values
        lows = df['low'].iloc[-lookback:].values
        hh_count, hl_count, lh_count, ll_count = 0, 0, 0, 0
        for i in range(2, len(highs)):
            if highs[i] > highs[i-1]: hh_count += 1
            if lows[i] > lows[i-1]: hl_count += 1
            if highs[i] < highs[i-1]: lh_count += 1
            if lows[i] < lows[i-1]: ll_count += 1
        total_swings = hh_count + hl_count + lh_count + ll_count
        if total_swings == 0: return 'INDETERMINADO', {'counts': {'hh': 0, 'hl': 0, 'lh': 0, 'll': 0}}
        hh_ratio = hh_count / total_swings
        hl_ratio = hl_count / total_swings
        lh_ratio = lh_count / total_swings
        ll_ratio = ll_count / total_swings
        counts = {'hh': hh_count, 'hl': hl_count, 'lh': lh_count, 'll': ll_count}
        if hh_ratio > 0.4 and hl_ratio > 0.3: return 'LARGO', {'counts': counts, 'ratios': {'hh': hh_ratio, 'hl': hl_ratio}}
        elif ll_ratio > 0.4 and lh_ratio > 0.3: return 'CORTO', {'counts': counts, 'ratios': {'ll': ll_ratio, 'lh': lh_ratio}}
        elif hh_ratio > lh_ratio and hl_ratio > ll_ratio: return 'LARGO', {'counts': counts, 'ratios': {'hh': hh_ratio, 'hl': hl_ratio}}
        elif lh_ratio > hh_ratio and ll_ratio > hl_ratio: return 'CORTO', {'counts': counts, 'ratios': {'ll': ll_ratio, 'lh': lh_ratio}}
        return 'INDETERMINADO', {'counts': counts}

    def is_market_inactive(self, df: pd.DataFrame, lookback: int = 20) -> bool:
        if len(df) < lookback + 1: return True
        relevant = df.iloc[-lookback:]
        range_size = (relevant['high'].max() - relevant['low'].min()) / relevant['close'].iloc[-1]
        atr = ta.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
        atr_pct = atr / df['close'].iloc[-1]
        return range_size < atr_pct * 2

    def find_swing_highs_lows(self, df: pd.DataFrame, lookback: int = 50) -> Dict:
        if len(df) < lookback + 2: return {'swing_highs': [], 'swing_lows': [], 'eqh': [], 'eql': []}
        highs = df['high'].iloc[-lookback:].values
        lows = df['low'].iloc[-lookback:].values
        swing_highs, swing_lows = [], []
        for i in range(2, len(highs) - 2):
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_highs.append(float(highs[i]))
        for i in range(2, len(lows) - 2):
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append(float(lows[i]))
        eqh, eqh_values = [], {}
        for sh in swing_highs:
            rounded = round(sh, 5)
            eqh_values[rounded] = eqh_values.get(rounded, 0) + 1
        for val, count in eqh_values.items():
            if count >= 2: eqh.append(val)
        eql, eql_values = [], {}
        for sl in swing_lows:
            rounded = round(sl, 5)
            eql_values[rounded] = eql_values.get(rounded, 0) + 1
        for val, count in eql_values.items():
            if count >= 2: eql.append(val)
        return {'swing_highs': sorted(swing_highs, reverse=True), 'swing_lows': sorted(swing_lows), 'eqh': sorted(eqh, reverse=True), 'eql': sorted(eql)}

    def get_prev_day_high_low(self, df: pd.DataFrame) -> Tuple[Optional[float], Optional[float]]:
        if df is None or len(df) < 2: return None, None
        df_1d = self.resample_ohlcv(df, '1d')
        if df_1d is not None and len(df_1d) >= 2:
            prev_day = df_1d.iloc[-2]
            return float(prev_day['high']), float(prev_day['low'])
        return None, None

    def detect_fvg(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[Dict]:
        if idx < 2 or idx >= len(df) - 1: return None
        if direction == 'LARGO':
            low_n = float(df['low'].iloc[idx])
            high_n2 = float(df['high'].iloc[idx - 2])
            if low_n > high_n2:
                gap = low_n - high_n2
                if gap / float(df['close'].iloc[idx]) >= self.fvg_min_pct:
                    return {'type': 'Bullish_FVG', 'top': low_n, 'bottom': high_n2, 'mid': (high_n2 + low_n) / 2, 'size': gap, 'idx': idx, 'vela_idx': idx}
        else:
            high_n = float(df['high'].iloc[idx])
            low_n2 = float(df['low'].iloc[idx - 2])
            if high_n < low_n2:
                gap = low_n2 - high_n
                if gap / float(df['close'].iloc[idx]) >= self.fvg_min_pct:
                    return {'type': 'Bearish_FVG', 'top': low_n2, 'bottom': high_n, 'mid': (low_n2 + high_n) / 2, 'size': gap, 'idx': idx, 'vela_idx': idx}
        return None

    def detect_mss(self, df: pd.DataFrame, direction: str) -> bool:
        if len(df) < self.mss_lookback + 1: return False
        closes, opens = df['close'].values, df['open'].values
        if direction == 'CORTO':
            for i in range(len(df) - self.mss_lookback, len(df) - 1):
                prev_high = max(closes[max(0, i-self.mss_lookback):i])
                if closes[-1] < prev_high and closes[-1] < opens[-1]: return True
        else:
            for i in range(len(df) - self.mss_lookback, len(df) - 1):
                prev_low = min(closes[max(0, i-self.mss_lookback):i])
                if closes[-1] > prev_low and closes[-1] > opens[-1]: return True
        return False

    def detect_displacement(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[Dict]:
        if idx < 1 or idx >= len(df): return None
        vela = df.iloc[idx]
        open_p, close_p, high_p, low_p = float(vela['open']), float(vela['close']), float(vela['high']), float(vela['low'])
        body, range_v = abs(close_p - open_p), high_p - low_p
        if range_v == 0 or body / range_v < 0.6: return None
        if direction == 'CORTO':
            if close_p < open_p and body / range_v >= 0.6:
                if ((close_p - low_p) / range_v) <= 0.25: return {'idx': idx, 'type': 'Bearish_Displacement', 'body_pct': (body / range_v) * 100, 'wick_pct': ((close_p - low_p) / range_v) * 100}
        else:
            if close_p > open_p and body / range_v >= 0.6:
                if ((high_p - close_p) / range_v) <= 0.25: return {'idx': idx, 'type': 'Bullish_Displacement', 'body_pct': (body / range_v) * 100, 'wick_pct': ((high_p - close_p) / range_v) * 100}
        return None

    def check_ote(self, df: pd.DataFrame, direction: str, lookback: int = 50) -> Dict:
        if len(df) < lookback + 1: return {'in_ote': True, 'ote_zone': None}
        relevant = df.iloc[-lookback:]
        swing_high, swing_low = float(relevant['high'].max()), float(relevant['low'].min())
        price = float(df['close'].iloc[-1])
        if direction in ('LARGO'): in_ote, zone = is_in_ote_zone(price, swing_high, swing_low, 'LARGO', fib_min=self.ote_fib_min, fib_max=self.ote_fib_max)
        else: in_ote, zone = is_in_ote_zone(price, swing_low, swing_high, 'CORTO', fib_min=self.ote_fib_min, fib_max=self.ote_fib_max)
        return {'in_ote': in_ote, 'ote_zone': zone, 'swing_high': swing_high, 'swing_low': swing_low}

    def get_ob_analysis(self, df: pd.DataFrame, direction: str, price: float, atr: float) -> Dict:
        obs = detect_order_blocks(df, direction, lookback=80)
        bbs = detect_breaker_blocks(df, obs)
        conf = ob_confluence_score(price, obs, direction, atr=atr)
        return {'order_blocks': obs, 'breaker_blocks': bbs, 'ob_score': conf['score'], 'in_ob_zone': conf['in_ob_zone'], 'nearest_ob': conf.get('nearest_ob'), 'ob_count': conf['ob_count']}

    def calculate_fibonacci_zone(self, df: pd.DataFrame, direction: str, lookback: int = 50) -> Optional[Dict]:
        if len(df) < lookback + 1: return None
        relevant_data = df.iloc[-lookback:]
        if direction == 'CORTO':
            swing_low, swing_high = float(relevant_data['low'].min()), float(relevant_data['high'].max())
            if swing_high <= swing_low: return None
            fib_50 = swing_low + (swing_high - swing_low) * self.fibonacci_level
            return {'type': 'PREMIUM', 'swing_low': swing_low, 'swing_high': swing_high, 'fib_50': fib_50}
        else:
            swing_high, swing_low = float(relevant_data['high'].max()), float(relevant_data['low'].min())
            if swing_high <= swing_low: return None
            fib_50 = swing_low + (swing_high - swing_low) * self.fibonacci_level
            return {'type': 'DISCOUNT', 'swing_low': swing_low, 'swing_high': swing_high, 'fib_50': fib_50}

    def detect_engulfing(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[Dict]:
        if idx < 1 or idx >= len(df): return None
        prev, curr = df.iloc[idx - 1], df.iloc[idx]
        prev_open, prev_close, prev_high, prev_low = float(prev['open']), float(prev['close']), float(prev['high']), float(prev['low'])
        curr_open, curr_close, curr_high, curr_low = float(curr['open']), float(curr['close']), float(curr['high']), float(curr['low'])
        prev_body_top, prev_body_bottom = max(prev_open, prev_close), min(prev_open, prev_close)
        curr_body_top, curr_body_bottom = max(curr_open, curr_close), min(curr_open, curr_close)
        if direction == 'CORTO':
            if prev_close > prev_open or curr_close >= curr_open: return None
            if curr_high > prev_high and curr_body_top > prev_body_top and curr_body_bottom < prev_body_bottom:
                return {'type': 'BEARISH_ENGULFING', 'idx': idx, 'swept': 'HIGH', 'sweep_level': prev_high, 'candle_high': curr_high, 'candle_low': curr_low, 'body_top': curr_body_top, 'body_bottom': curr_body_bottom, 'candle_range': curr_high - curr_low}
        else:
            if prev_close < prev_open or curr_close <= curr_open: return None
            if curr_low < prev_low and curr_body_top > prev_body_top and curr_body_bottom < prev_body_bottom:
                return {'type': 'BULLISH_ENGULFING', 'idx': idx, 'swept': 'LOW', 'sweep_level': prev_low, 'candle_high': curr_high, 'candle_low': curr_low, 'body_top': curr_body_top, 'body_bottom': curr_body_bottom, 'candle_range': curr_high - curr_low}
        return None

    def detect_liquidity_sweep(self, df: pd.DataFrame, direction: str, liquidity_levels: List[float]) -> Optional[Dict]:
        for level in liquidity_levels:
            for i in range(len(df) - 1, max(0, len(df) - 10), -1):
                vela = df.iloc[i]
                high, low = float(vela['high']), float(vela['low'])
                if direction == 'CORTO' and high > level: return {'idx': i, 'type': 'LIQUIDITY_SWEEP_HIGH', 'level': level, 'swept_price': high}
                elif direction == 'LARGO' and low < level: return {'idx': i, 'type': 'LIQUIDITY_SWEEP_LOW', 'level': level, 'swept_price': low}
        return None

    def get_htf_bias(self, df_h4: pd.DataFrame, df_1d: pd.DataFrame, df_1w: pd.DataFrame, df_1M: pd.DataFrame) -> Dict[str, str]:
        biases = {}
        for name, df_tf in [('H4', df_h4), ('D', df_1d), ('W', df_1w), ('M', df_1M)]:
            if df_tf is not None and len(df_tf) >= 20:
                bias, _ = self.detect_bias(df_tf, lookback=20)
                biases[name] = bias
            else: biases[name] = 'INDETERMINADO'
        return biases

    def get_consensus_bias(self, biases: Dict[str, str]) -> Tuple[str, float]:
        weights = {'H4': 0.35, 'D': 0.35, 'W': 0.20, 'M': 0.10}
        bullish_score = sum(weights.get(tf, 0) for tf, bias in biases.items() if bias == 'LARGO')
        bearish_score = sum(weights.get(tf, 0) for tf, bias in biases.items() if bias == 'CORTO')
        if bullish_score > bearish_score: return 'LARGO', bullish_score
        elif bearish_score > bullish_score: return 'CORTO', bearish_score
        return 'NO_TRADE', 0.0

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f"▶ SesgoBiasHTF: Iniciando análisis para {symbol}")
        
        master = preloadedData.get(symbol) if preloadedData else None
        
        # Punto 3: Master Dictionary integration
        if isinstance(master, dict):
            df_15m = master.get('15min')
            df_h4 = master.get('4h')
            df_1d = master.get('1d')
            df_1w = master.get('1w')
            df_1M = master.get('1m')
        else:
            df_15m = master
            df_h4, df_1d, df_1w, df_1M = None, None, None, None

        if df_15m is None or len(df_15m) < 100: return None
        
        # Fallback de resampleo si no vienen en el master (Punto 3)
        if df_h4 is None: df_h4 = self.resample_ohlcv(df_15m, '4h')
        if df_1d is None: df_1d = self.resample_ohlcv(df_15m, '1d')
        if df_1w is None: df_1w = self.resample_ohlcv(df_15m, '1w')
        if df_1M is None: df_1M = self.resample_ohlcv(df_15m, '1M')

        
        biases = self.get_htf_bias(df_h4, df_1d, df_1w, df_1M)
        consensus_direction, consensus_score = self.get_consensus_bias(biases)
        
        if consensus_direction == 'NO_TRADE' or consensus_score < 0.6: return None
        
        fib_zone = self.calculate_fibonacci_zone(df_15m, consensus_direction)
        if not fib_zone: return None
        
        liquidity = self.find_swing_highs_lows(df_15m)
        prev_day = self.get_prev_day_high_low(df_15m)
        
        po3_data = self.analyze_po3_cycle(df_15m, consensus_direction, fib_zone, liquidity)
        if not po3_data or not po3_data.get('mss'): return None
        
        price = float(df_15m['close'].iloc[-1])
        atr = float(ta.ATR(df_15m['high'], df_15m['low'], df_15m['close'], 14).iloc[-1])
        
        ob_analysis = self.get_ob_analysis(df_15m, consensus_direction, price, atr)
        ote_analysis = self.check_ote(df_15m, consensus_direction)
        
        from middleware.database import dbManager
        strat_config = dbManager.getStrategyConfig("SesgoBiasHTF") or {}
        
        confidence = int(consensus_score * 100)
        if ob_analysis['ob_score'] >= 10: confidence += 5
        if ote_analysis['in_ote']: confidence += 5
        
        if self.use_ote_filter and not ote_analysis['in_ote']: confidence -= self.ote_reduce_conf
        
        min_conf = int(strat_config.get('min_confidence', 70))
        if confidence < min_conf: return None
        
        entry_price = price
        sl_price = self.calculate_sl_from_sweep(po3_data, consensus_direction)
        tp_levels = self.calculate_tp_structural(df_15m, consensus_direction, liquidity, prev_day)
        tp_price = tp_levels[0] if tp_levels else (entry_price + abs(entry_price-sl_price)*2 if consensus_direction=='LARGO' else entry_price-abs(entry_price-sl_price)*2)
        min_rr = float(strat_config.get('min_rr', 1.5))
        tp_price = adjustTPForMinRR(entry_price, sl_price, tp_price, consensus_direction, minRR=min_rr)
        
        multiplier = getPipMultiplier(symbol)
        sl_dist = abs(entry_price - sl_price)
        
        is_valid, _, mensaje = check_tp_exhaustion(df_15m, len(df_15m)-5, entry_price, tp_price, sl_price, consensus_direction, threshold=0.60, timeframe="15M")
        if not is_valid: return None
        
        current_price = float(df_15m['close'].iloc[-1])
        now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
        last_closed = get_last_closed_candle(now_cdmx, interval=15)
        candle_time = last_closed.strftime("%Y-%m-%d %H:%M:%S")
        is_valid, _, mensaje = check_signal_health(entry_price, tp_price, sl_price, consensus_direction, current_price, threshold=0.65, candle_time=candle_time)
        if not is_valid: return None
        
        # ── FILTRO: Tendencia mensual ──
        monthly_trend = symbolInfo.get('weekly_trend', 'NEUTRAL')
        logger.debug(f"[{symbol}] SesgoBiasHTF: consensus_direction={consensus_direction}, monthly_trend={monthly_trend}")
        
        if monthly_trend == "BAJISTA" and consensus_direction == "LARGO":
            logger.info(f"[{symbol}] Señal LARGO descartada - tendencia mensual BAJISTA")
            return None
        elif monthly_trend == "ALCISTA" and consensus_direction == "CORTO":
            logger.info(f"[{symbol}] Señal CORTO descartada - tendencia mensual ALCISTA")
            return None
        
        return Signal(
            strategy="SesgoBiasHTF",
            symbol=symbol,
            direction=consensus_direction,
            entry_price=entry_price,
            stop_loss=sl_price,
            take_profit=tp_price,
            sl_distance=sl_dist,
            confidence=confidence,
            setup="PO3 + MSS + FVG",
            status="EN ZONA ✅",
            candleTime=last_closed.strftime("%Y-%m-%d %H:%M:%S"),
            intervalo="15min",
            riesgo_pips=round(sl_dist * multiplier, 1),
            rr_ratio=round(abs(tp_price - entry_price) / sl_dist, 2),
            metadata={
                "biases": biases,
                "consensus_score": consensus_score,
                "ob_score": ob_analysis['ob_score'],
                "in_ote": ote_analysis['in_ote']
            }
        )

    def analyze_po3_cycle(self, df: pd.DataFrame, direction: str, zone: Dict, liquidity: Dict) -> Optional[Dict]:
        if df is None or len(df) < 30: return None
        price = float(df['close'].iloc[-1])
        if direction == 'LARGO' and price > zone['fib_50']: return None
        if direction == 'CORTO' and price < zone['fib_50']: return None
        liquidity_levels = liquidity.get('swing_highs', [])[:3] + liquidity.get('eqh', [])[:2]
        liquidity_levels.extend(liquidity.get('swing_lows', [])[:3] + liquidity.get('eql', [])[:2])
        sweep = self.detect_liquidity_sweep(df, direction, liquidity_levels)
        if not sweep: return None
        for i in range(sweep['idx'] - 1, max(0, sweep['idx'] - 20), -1):
            engulf = self.detect_engulfing(df, i, direction)
            if engulf:
                ahora = self.getMexicoTime().replace(tzinfo=None)
                vela_time = df.index[i]
                if hasattr(vela_time, 'to_pydatetime'): vela_time = vela_time.to_pydatetime()
                if vela_time.tzinfo is not None: vela_time = vela_time.replace(tzinfo=None)
                if (ahora - vela_time).total_seconds() / 60 > self.max_signal_age_minutes: continue
                sweep.update({'mss': self.detect_mss(df, direction), 'engulf_info': engulf, 'vela_time': vela_time})
                return sweep
        return None
