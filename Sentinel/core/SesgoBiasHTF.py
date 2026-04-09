"""
==============================================================================
  ESTRATEGIA DE TRADING: SESGO BIAS HTF - Market Bias + Institutional Price Action
==============================================================================
  Implementa análisis Multi-Timeframe (HTF) con:
    - Lectura de Sesgo en H4, Diario, Semanal, Mensual
    - Zonas Premium/Discount basadas en Fibonacci 50%
    - Confirmación con Sweep + Engulfing Pattern
    - Entrada refinada en H4 con Fibonacci de la vela de confirmación
    
  CARACTERÍSTICAS:
    - Sesgo Bajista: HH + HL
    - Sesgo Alcista: LL + LH
    - Gestión adaptativa enfocada en win rate 70-80%
==============================================================================
"""

import logging
from datetime import datetime
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
from middleware.utils.alertBuilder import buildAlertMessage
from middleware.config.constants import TIMEZONE

logger = logging.getLogger(__name__)


class SesgoBiasHTFBot:
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}
        
        strategyConfig = dbManager.getStrategyConfig("SesgoBiasHTF")
        
        self.fibonacci_level = strategyConfig.get('fibonacci_level', 0.50) if strategyConfig else 0.50
        self.entry_fib_min = strategyConfig.get('entry_fib_min', 0.25) if strategyConfig else 0.25
        self.entry_fib_max = strategyConfig.get('entry_fib_max', 0.50) if strategyConfig else 0.50
        self.swing_lookback = strategyConfig.get('swing_lookback', 50) if strategyConfig else 50
        self.confirmation_lookback = strategyConfig.get('confirmation_lookback', 5) if strategyConfig else 5
        self.min_distance_pips = strategyConfig.get('min_distance_pips', 10) if strategyConfig else 10
        self.max_signal_age_minutes = strategyConfig.get('max_signal_age_minutes', 60) if strategyConfig else 60
        
        self.signalsGeneradas = {}
        self.timestamps_signals = {}
        
        self.timeframes_htf = ['1d', '1w', '1M']
        
        logger.info("Bot SesgoBiasHTF iniciado - Análisis Multi-Timeframe (H4, D, W, M)")

    def getMexicoTime(self) -> datetime:
        return datetime.now(self.MEXICO_TZ)

    def resample_ohlcv(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        if df is None or df.empty:
            return df
        
        df = df.copy()
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        rule_map = {
            '1h': '1h', '4h': '4h', '1d': '1d', '1w': '1W',
            '1M': '1ME', '1H': '1h', '4H': '4h', '1D': '1d',
            '1W': '1W', '1M': '1ME'
        }
        rule = rule_map.get(timeframe, timeframe)
        
        agg_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        }
        if 'volume' in df.columns:
            agg_dict['volume'] = 'sum'
            
        df_resampled = df.resample(rule).agg(agg_dict)
        if len(df) > 0 and len(df_resampled) > 0:
            if df.index[-1] < df_resampled.index[-1]:
                df_resampled = df_resampled.iloc[:-1]
                
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
        if len(df) < lookback + 1:
            return 'INDETERMINADO', {}
        
        highs = df['high'].iloc[-lookback:].values
        lows = df['low'].iloc[-lookback:].values
        closes = df['close'].iloc[-lookback:].values
        
        hh_count = 0
        hl_count = 0
        lh_count = 0
        ll_count = 0
        
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

    def calculate_fibonacci_zone(self, df: pd.DataFrame, direction: str, lookback: int = 50) -> Optional[Dict]:
        if len(df) < lookback + 1:
            return None
        
        relevant_data = df.iloc[-lookback:]
        
        if direction == 'SHORT':
            swing_low = float(relevant_data['low'].min())
            swing_high = float(relevant_data['high'].max())
            
            if swing_high <= swing_low:
                return None
            
            fib_50 = swing_low + (swing_high - swing_low) * self.fibonacci_level
            
            return {
                'type': 'PREMIUM',
                'swing_low': swing_low,
                'swing_high': swing_high,
                'fib_50': fib_50,
                'zone_start': fib_50,
                'zone_end': swing_high
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
                'fib_50': fib_50,
                'zone_start': swing_low,
                'zone_end': fib_50
            }

    def detect_engulfing(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[Dict]:
        if idx < 1 or idx >= len(df):
            return None
        
        prev_candle = df.iloc[idx - 1]
        curr_candle = df.iloc[idx]
        
        prev_open = float(prev_candle['open'])
        prev_close = float(prev_candle['close'])
        prev_high = float(prev_candle['high'])
        prev_low = float(prev_candle['low'])
        
        curr_open = float(curr_candle['open'])
        curr_close = float(curr_candle['close'])
        curr_high = float(curr_candle['high'])
        curr_low = float(curr_candle['low'])
        
        prev_body_top = max(prev_open, prev_close)
        prev_body_bottom = min(prev_open, prev_close)
        prev_body_size = abs(prev_close - prev_open)
        
        curr_body_top = max(curr_open, curr_close)
        curr_body_bottom = min(curr_open, curr_close)
        curr_body_size = abs(curr_close - curr_open)
        
        if direction == 'SHORT':
            if prev_close > prev_open:
                return None
            
            if curr_close < curr_open:
                return None
            
            if curr_body_top > prev_body_top and curr_body_bottom < prev_body_bottom:
                sweep_high = curr_high > prev_high
                engulf_complete = curr_body_size >= prev_body_size * 0.8
                
                if sweep_high and engulf_complete:
                    return {
                        'type': 'BEARISH_ENGULFING',
                        'idx': idx,
                        'swept': 'HIGH',
                        'sweep_level': prev_high,
                        'engulf_body_size': curr_body_size,
                        'prev_body_size': prev_body_size,
                        'candle_high': curr_high,
                        'candle_low': curr_low,
                        'candle_open': curr_open,
                        'candle_close': curr_close
                    }
        
        else:
            if prev_close < prev_open:
                return None
            
            if curr_close > curr_open:
                return None
            
            if curr_body_top > prev_body_top and curr_body_bottom < prev_body_bottom:
                sweep_low = curr_low < prev_low
                engulf_complete = curr_body_size >= prev_body_size * 0.8
                
                if sweep_low and engulf_complete:
                    return {
                        'type': 'BULLISH_ENGULFING',
                        'idx': idx,
                        'swept': 'LOW',
                        'sweep_level': prev_low,
                        'engulf_body_size': curr_body_size,
                        'prev_body_size': prev_body_size,
                        'candle_high': curr_high,
                        'candle_low': curr_low,
                        'candle_open': curr_open,
                        'candle_close': curr_close
                    }
        
        return None

    def calculate_entry_from_fib(self, engulf_info: Dict, df: pd.DataFrame, direction: str) -> Optional[float]:
        candle_high = engulf_info['candle_high']
        candle_low = engulf_info['candle_low']
        
        if direction == 'SHORT':
            entry_range = candle_high - candle_low
            entry_min = candle_high - (entry_range * self.entry_fib_max)
            entry_max = candle_high - (entry_range * self.entry_fib_min)
        else:
            entry_range = candle_high - candle_low
            entry_min = candle_low + (entry_range * self.entry_fib_min)
            entry_max = candle_low + (entry_range * self.entry_fib_max)
        
        return (entry_min + entry_max) / 2

    def calculate_sl_from_fib(self, engulf_info: Dict, direction: str) -> float:
        if direction == 'SHORT':
            return engulf_info['candle_high'] + (engulf_info['candle_high'] - engulf_info['candle_low']) * 0.1
        else:
            return engulf_info['candle_low'] - (engulf_info['candle_high'] - engulf_info['candle_low']) * 0.1

    def calculate_tp_structural(self, df: pd.DataFrame, direction: str, lookback: int = 20) -> float:
        if len(df) < lookback:
            return 0.0
        
        relevant_data = df.iloc[-lookback:]
        
        if direction == 'SHORT':
            target_low = float(relevant_data['low'].min())
            return target_low
        else:
            target_high = float(relevant_data['high'].max())
            return target_high

    def get_htf_bias(self, df_h4: pd.DataFrame, df_1d: pd.DataFrame, 
                     df_1w: pd.DataFrame, df_1M: pd.DataFrame) -> Dict[str, str]:
        biases = {}
        
        if df_h4 is not None and len(df_h4) >= 20:
            bias_h4, _ = self.detect_bias(df_h4, lookback=20)
            biases['H4'] = bias_h4
        else:
            biases['H4'] = 'INDETERMINADO'
        
        if df_1d is not None and len(df_1d) >= 20:
            bias_1d, _ = self.detect_bias(df_1d, lookback=20)
            biases['D'] = bias_1d
        else:
            biases['D'] = 'INDETERMINADO'
        
        if df_1w is not None and len(df_1w) >= 10:
            bias_1w, _ = self.detect_bias(df_1w, lookback=10)
            biases['W'] = bias_1w
        else:
            biases['W'] = 'INDETERMINADO'
        
        if df_1M is not None and len(df_1M) >= 5:
            bias_1M, _ = self.detect_bias(df_1M, lookback=5)
            biases['M'] = bias_1M
        else:
            biases['M'] = 'INDETERMINADO'
        
        return biases

    def get_consensus_bias(self, biases: Dict[str, str]) -> Tuple[str, float]:
        bias_weights = {
            'H4': 0.35,
            'D': 0.35,
            'W': 0.20,
            'M': 0.10
        }
        
        bullish_score = 0.0
        bearish_score = 0.0
        
        for tf, bias in biases.items():
            weight = bias_weights.get(tf, 0.0)
            if bias == 'ALCISTA':
                bullish_score += weight
            elif bias == 'BAJISTA':
                bearish_score += weight
        
        if bullish_score > bearish_score:
            return 'LARGO', bullish_score
        elif bearish_score > bullish_score:
            return 'CORTO', bearish_score
        
        return 'INDETERMINADO', 0.0

    def analyze_daily_confirmation(self, df_1d: pd.DataFrame, bias: str, 
                                   zone: Dict) -> Optional[Dict]:
        if df_1d is None or len(df_1d) < 10:
            return None
        
        direction = 'LONG' if bias == 'LARGO' else 'SHORT'
        price = float(df_1d['close'].iloc[-1])
        
        if bias == 'LARGO':
            if price > zone['fib_50']:
                logger.debug(f"[D] Precio {price} por encima de zona Discount {zone['fib_50']}")
                return None
        else:
            if price < zone['fib_50']:
                logger.debug(f"[D] Precio {price} por debajo de zona Premium {zone['fib_50']}")
                return None
        
        for i in range(len(df_1d) - 2, max(0, len(df_1d) - 10), -1):
            engulf = self.detect_engulfing(df_1d, i, direction)
            if engulf:
                ahora = self.getMexicoTime().replace(tzinfo=None)
                vela_time = df_1d.index[i]
                if hasattr(vela_time, 'to_pydatetime'):
                    vela_time = vela_time.to_pydatetime()
                if vela_time.tzinfo is not None:
                    vela_time = vela_time.replace(tzinfo=None)
                
                minutos_antiguedad = (ahora - vela_time).total_seconds() / 60
                
                if minutos_antiguedad > self.max_signal_age_minutes:
                    logger.debug(f"[D] Engulfing descartado por antigüedad: {minutos_antiguedad:.1f}m")
                    continue
                
                return {
                    'confirmed': True,
                    'engulf_info': engulf,
                    'idx': i,
                    'timeframe': 'D',
                    'vela_time': vela_time,
                    'antiguedad_min': minutos_antiguedad
                }
        
        return None

    def analyze_h4_refinement(self, df_4h: pd.DataFrame, bias: str, 
                              daily_confirm: Dict) -> Optional[Dict]:
        if df_4h is None or len(df_4h) < 10:
            return None
        
        direction = 'LONG' if bias == 'LARGO' else 'SHORT'
        
        for i in range(len(df_4h) - 2, max(0, len(df_4h) - 30), -1):
            engulf = self.detect_engulfing(df_4h, i, direction)
            if engulf:
                ahora = self.getMexicoTime().replace(tzinfo=None)
                vela_time = df_4h.index[i]
                if hasattr(vela_time, 'to_pydatetime'):
                    vela_time = vela_time.to_pydatetime()
                if vela_time.tzinfo is not None:
                    vela_time = vela_time.replace(tzinfo=None)
                
                minutos_antiguedad = (ahora - vela_time).total_seconds() / 60
                
                if minutos_antiguedad > self.max_signal_age_minutes:
                    continue
                
                return {
                    'confirmed': True,
                    'engulf_info': engulf,
                    'idx': i,
                    'timeframe': 'H4',
                    'vela_time': vela_time,
                    'antiguedad_min': minutos_antiguedad
                }
        
        return None

    def validate_signal(self, entry: float, sl: float, tp: float, 
                       direction: str, symbol: str, df: pd.DataFrame) -> Optional[Dict]:
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
        
        return {
            'tipo_entrada': 'SESGO_BIAS_HTF',
            'direccion': direction,
            'entrada': round(entry, 5),
            'stop_loss': round(sl, 5),
            'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * multiplier, 1),
            'rr_ratio': round(rr_ratio, 2),
            'timeframe_entrada': 'H4',
            'timeframe_confirmacion': 'D',
            'confianza': 75
        }

    def analyze_top_down(self, datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> Dict:
        df_4h = datos.get('4h')
        df_1d = datos.get('1d')
        df_1w = datos.get('1w')
        df_1M = datos.get('1M')
        
        if df_4h is None or len(df_4h) < 50:
            return {'status': 'DATOS_INSUFICIENTES'}
        
        biases = self.get_htf_bias(df_4h, df_1d, df_1w, df_1M)
        logger.info(f"[{symbolInfo['symbol']}] Bias HTF: {biases}")
        
        bias, confidence = self.get_consensus_bias(biases)
        
        if bias == 'INDETERMINADO':
            logger.info(f"[{symbolInfo['symbol']}] Sesgo indeterminado en todos los timeframes")
            return {'status': 'SESGO_INDETERMINADO', 'biases': biases}
        
        direction = 'LONG' if bias == 'LARGO' else 'SHORT'
        
        if df_1d is not None and len(df_1d) >= 20:
            zone = self.calculate_fibonacci_zone(df_1d, direction, lookback=self.swing_lookback)
        elif df_4h is not None and len(df_4h) >= 50:
            zone = self.calculate_fibonacci_zone(df_4h, direction, lookback=self.swing_lookback)
        else:
            return {'status': 'DATOS_INSUFICIENTES_PARA_FIB'}
        
        if zone is None:
            return {'status': 'ZONA_NO_DETECTADA'}
        
        logger.info(f"[{symbolInfo['symbol']}] Zona {zone['type']}: Fib50={zone['fib_50']:.5f}")
        
        daily_confirm = self.analyze_daily_confirmation(df_1d, bias, zone)
        
        if not daily_confirm:
            logger.info(f"[{symbolInfo['symbol']}] Sin confirmación diaria válida")
            return {'status': 'SIN_CONFIRMACION_DIARIA', 'zone': zone, 'biases': biases}
        
        logger.info(f"[{symbolInfo['symbol']}] Confirmación diaria: Engulfing en vela {daily_confirm['idx']}")
        
        h4_refinement = self.analyze_h4_refinement(df_4h, bias, daily_confirm)
        
        engulf_info = None
        confirm_timeframe = 'D'
        confirm_idx = daily_confirm['idx']
        
        if h4_refinement:
            engulf_info = h4_refinement['engulf_info']
            confirm_timeframe = 'H4'
            confirm_idx = h4_refinement['idx']
            logger.info(f"[{symbolInfo['symbol']}] Refinamiento H4: Engulfing en vela {confirm_idx}")
        else:
            engulf_info = daily_confirm['engulf_info']
        
        if engulf_info is None:
            return {'status': 'SIN_ENGULFING_VALIDO'}
        
        entry_price = self.calculate_entry_from_fib(engulf_info, df_4h if h4_refinement else df_1d, direction)
        
        if entry_price is None:
            return {'status': 'ENTRADA_NO_CALCULADA'}
        
        sl_price = self.calculate_sl_with_fib(df_4h, engulf_info, direction, confirm_idx)
        
        if df_4h is not None:
            tp_price = self.calculate_tp_structural(df_4h, direction, lookback=30)
        elif df_1d is not None:
            tp_price = self.calculate_tp_structural(df_1d, direction, lookback=20)
        else:
            return {'status': 'TP_NO_CALCULADO'}
        
        if tp_price == 0.0:
            return {'status': 'TP_NO_CALCULADO'}
        
        validation = self.validate_signal(
            entry_price, sl_price, tp_price, direction, 
            symbolInfo['symbol'], df_4h if h4_refinement else df_1d
        )
        
        if validation is None:
            return {'status': 'SENAL_INVALIDA'}
        
        validation['biases'] = biases
        validation['zone'] = zone
        validation['candle_time'] = (h4_refinement['vela_time'] if h4_refinement else daily_confirm['vela_time']).strftime("%Y-%m-%d %H:%M:%S")
        
        return {'status': 'SENAL_GENERADA', 'senal': validation}

    def calculate_sl_with_fib(self, df: pd.DataFrame, engulf_info: Dict, 
                               direction: str, idx: int) -> float:
        candle_high = engulf_info['candle_high']
        candle_low = engulf_info['candle_low']
        candle_range = candle_high - candle_low
        
        if len(df) > idx + 1:
            next_high = float(df['high'].iloc[idx + 1:min(idx + 4, len(df))].max())
            next_low = float(df['low'].iloc[idx + 1:min(idx + 4, len(df))].min())
            
            if direction == 'SHORT':
                return next_high + (candle_range * 0.2)
            else:
                return next_low - (candle_range * 0.2)
        
        if direction == 'SHORT':
            return candle_high + (candle_range * 0.15)
        else:
            return candle_low - (candle_range * 0.15)

    async def _executeTrades(self, signal: Dict, symbolInfo: Dict, df_used: pd.DataFrame = None):
        if not signal:
            return

        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                return

        for account in self.accounts:
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
                "setup": signal.get("tipo_entrada", "SESGO_BIAS_HTF"),
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
        logger.info(f"▶ ENTRANDO análisis SesgoBiasHTF para {symbol}")
        
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
        
        if resultado['status'] == 'SENAL_GENERADA' and resultado.get('senal'):
            señal = resultado['senal']
            self.timestamps_signals[symbol] = self.getMexicoTime().replace(tzinfo=None)
            
            signal_telegram = {
                **señal,
                "strategy": "SesgoBiasHTF",
                "direction": señal['direccion']
            }
            await self._executeTrades(signal_telegram, symbolInfo, df_4h)

        logger.info(f"◀ SALIENDO análisis SesgoBiasHTF para {symbol} - Status: {resultado['status']}")


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
    print("SesgoBiasHTF Strategy Module - Multi-Timeframe Bias Analysis")
