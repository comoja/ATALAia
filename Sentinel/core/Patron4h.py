import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd
import numpy as np
import talib as ta
import pytz
from zoneinfo import ZoneInfo

from middleware.config import constants
from middleware.database import dbManager
from middleware.utils import momentum
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.analysis import technical
from Sentinel.analysis.technical import is_in_ote_zone, calculate_ote_zone, resample_to_interval, check_tp_exhaustion, check_signal_health
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateBEPrice

from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class Patron4HBot:
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    
    def __init__(self):
        self.strategy_name = "Patron4h"
        strategyConfig = dbManager.getStrategyConfig(self.strategy_name)
        self.fvg_min_pct         = strategyConfig.get('fvg_min_pct',        0.00005) if strategyConfig else 0.00005
        self.displacement_pct     = strategyConfig.get('displacement_pct',   0.0005)  if strategyConfig else 0.0005
        self.rr_ratio_min         = strategyConfig.get('rr_ratio_min',       1.5)     if strategyConfig else 1.5
        self.max_minutos_fvg      = strategyConfig.get('max_minutos_fvg',    240)     if strategyConfig else 240
        self.usar_filtro_fibonacci = True
        self.ote_fib_min          = strategyConfig.get('ote_fib_min', 0.62) if strategyConfig else 0.62
        self.ote_fib_max          = strategyConfig.get('ote_fib_max', 0.79) if strategyConfig else 0.79
        self.modo_flexible        = True
        
        logger.info("Bot iniciado con sistema Top-Down (1D -> 4H -> 1H -> 15M)")

    def getMexicoTime(self) -> datetime:
        return datetime.now(self.MEXICO_TZ)

    def resample_ohlcv(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        return resample_to_interval(df, timeframe)

    def detectar_fvg(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[dict]:
        if idx < 2 or idx >= len(df) - 1: return None
        if direction == 'LARGO':
            low_n = df['low'].iloc[idx]
            high_n2 = df['high'].iloc[idx - 2]
            low_n2 = df['low'].iloc[idx - 2]
            if low_n > high_n2:
                gap = low_n - high_n2
                if gap / df['close'].iloc[idx] >= self.fvg_min_pct:
                    fvg = {
                        'type': 'Bullish_FVG',
                        'top': float(low_n),
                        'bottom': float(high_n2),
                        'mid': float((high_n2 + low_n) / 2),
                        'size': float(gap),
                        'v1_low': float(low_n2),
                        'v1_high': float(high_n2),
                        'idx': idx,
                        'timestamp': str(df.index[idx])
                    }
                    # Regla ICT 50%: descartar si el gap ya fue mitigado
                    if technical._is_fvg_mitigated(df, idx, fvg):
                        return None
                    return fvg
        else:
            high_n = df['high'].iloc[idx]
            low_n2 = df['low'].iloc[idx - 2]
            high_n2 = df['high'].iloc[idx - 2]
            if high_n < low_n2:
                gap = low_n2 - high_n
                if gap / df['close'].iloc[idx] >= self.fvg_min_pct:
                    fvg = {
                        'type': 'Bearish_FVG',
                        'top': float(low_n2),
                        'bottom': float(high_n),
                        'mid': float((low_n2 + high_n) / 2),
                        'size': float(gap),
                        'v1_low': float(low_n2),
                        'v1_high': float(high_n2),
                        'idx': idx,
                        'timestamp': str(df.index[idx])
                    }
                    # Regla ICT 50%: descartar si el gap ya fue mitigado
                    if technical._is_fvg_mitigated(df, idx, fvg):
                        return None
                    return fvg
        return None

    def detectar_displacement(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[dict]:
        if idx < 1: return None
        vela = df.iloc[idx]
        open_p, close_p, high_p, low_p = vela['open'], vela['close'], vela['high'], vela['low']
        cuerpo, rango = abs(close_p - open_p), high_p - low_p
        if rango == 0 or (cuerpo / close_p) < self.displacement_pct: return None
        if direction == 'CORTO':
            if close_p < open_p and (cuerpo / rango) > 0.6 and ((close_p - low_p) / rango) <= 0.25:
                return {'idx': idx, 'type': 'Bearish_Displacement', 'cuerpo_pct': (cuerpo/close_p)*100, 'vela_open': open_p, 'vela_close': close_p, 'vela_high': high_p, 'vela_low': low_p}
        else:
            if close_p > open_p and (cuerpo / rango) > 0.6 and ((high_p - close_p) / rango) <= 0.25:
                return {'idx': idx, 'type': 'Bullish_Displacement', 'cuerpo_pct': (cuerpo/close_p)*100, 'vela_open': open_p, 'vela_close': close_p, 'vela_high': high_p, 'vela_low': low_p}
        return None

    def detectar_mss(self, df: pd.DataFrame, direction: str) -> bool:
        # Delegamos a la función centralizada en technical.py con un lookback mayor
        return technical.detect_mss(df, direction, lookback=15)

    def obtener_contexto_diario(self, df_1d: pd.DataFrame) -> dict:
        if len(df_1d) < 3: return {'tendencia': 'LATERAL', 'max_dia_anterior': None, 'min_dia_anterior': None, 'fvgs_diarios': []}
        max_prev, min_prev = float(df_1d['high'].iloc[-2]), float(df_1d['low'].iloc[-2])
        closes = df_1d['close'].iloc[-10:].values
        cambio = (closes[-1] - closes[0]) / closes[0]
        if cambio > 0.005: tendencia = 'ALCISTA'
        elif cambio < -0.005: tendencia = 'BAJISTA'
        else: tendencia = 'ALCISTA' if closes[-1] > closes[0] else 'BAJISTA' if self.modo_flexible else 'LATERAL'
        fvgs = []
        for i in range(2, len(df_1d)):
            f_l = self.detectar_fvg(df_1d, i, 'LARGO')
            if f_l: fvgs.append(f_l)
            f_c = self.detectar_fvg(df_1d, i, 'CORTO')
            if f_c: fvgs.append(f_c)
        return {'tendencia': tendencia, 'max_dia_anterior': max_prev, 'min_dia_anterior': min_prev, 'fvgs_diarios': fvgs}

    def detectarLiquidityRaid(self, df_ltf: pd.DataFrame, maxPrev: float, minPrev: float) -> Optional[dict]:
        """
        Detecta un barrido de liquidez HTF usando la función centralizada.
        Requiere que el precio SUPERE el nivel Y CIERRE de vuelta dentro del rango
        (sweep real, no simple ruptura). Usa technical.detectLiquiditySweep.
        """
        return technical.detectLiquiditySweep(df_ltf, htfHigh=maxPrev, htfLow=minPrev, lookback=10)

    def analizar_catalizador(self, df_tf: pd.DataFrame, contexto: dict, raid: Optional[dict], name: str) -> dict:
        res = {'timeframe': name, 'hay_displacement': False, 'fvgs': [], 'hay_mss': False, 'confirmado': False, 'vela_origen_idx': None}
        if len(df_tf) < 20: return res
        trend = contexto['tendencia']
        direction = 'CORTO' if trend == 'BAJISTA' else 'LARGO'
        ahora = self.getMexicoTime().replace(tzinfo=None)
        for i in range(len(df_tf)-1, max(len(df_tf)-10, 0), -1):
            disp = self.detectar_displacement(df_tf, i, direction)
            if disp:
                v_t = df_tf.index[i].to_pydatetime().replace(tzinfo=None) if hasattr(df_tf.index[i], 'to_pydatetime') else df_tf.index[i].replace(tzinfo=None)
                if (ahora - v_t).total_seconds() / 60 <= 120:
                    res.update({'hay_displacement': True, 'displacement_info': disp, 'vela_origen_idx': i})
                    break
        for i in range(max(1, len(df_tf)-30), len(df_tf)-1):
            fvg = self.detectar_fvg(df_tf, i, direction)
            if fvg: res['fvgs'].append(fvg)
        res['hay_mss'] = self.detectar_mss(df_tf, direction)
        res['confirmado'] = (res['hay_displacement'] or (len(res['fvgs'])>0 and res['hay_mss'])) if self.modo_flexible else (res['hay_displacement'] and len(res['fvgs'])>0)
        return res

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[List[Signal]]:
        symbol = symbolInfo['symbol']
        logger.info(f"[{symbol}] Analizando...")
        master = preloadedData.get(symbol) if preloadedData else None
        
        if isinstance(master, dict):
            df_15m = master.get('15min')
            df_1h = master.get('1h')
            df_4h = master.get('4h')
            df_1d = master.get('1d')
        else:
            df_15m = master
            df_1h, df_4h, df_1d = None, None, None

        if df_15m is None or len(df_15m) < 100: return None
        
        if df_1h is None: df_1h = resample_to_interval(df_15m, '1h')
        if df_4h is None: df_4h = resample_to_interval(df_15m, '4h')
        if df_1d is None: df_1d = resample_to_interval(df_15m, '1d')
        
        ctx = self.obtener_contexto_diario(df_1d)
        if ctx['tendencia'] == 'LATERAL': return None

        # Detectar sweep HTF en LTF (15m): precio superó PDH/PDL y cerró de vuelta al rango
        htfSweep = self.detectarLiquidityRaid(df_15m, ctx['max_dia_anterior'], ctx['min_dia_anterior'])
        
        c4h, c1h = self.analizar_catalizador(df_4h, ctx, htfSweep, '4H'), self.analizar_catalizador(df_1h, ctx, htfSweep, '1h')
        catalizador = c4h if c4h['confirmado'] else c1h if c1h['confirmado'] else None
        if not catalizador: return None
        
        trend = ctx['tendencia']
        direction = 'CORTO' if trend == 'BAJISTA' else 'LARGO'
        fvg = next((f for f in catalizador['fvgs'] if (trend == 'BAJISTA' and f['type'] == 'Bearish_FVG') or (trend == 'ALCISTA' and f['type'] == 'Bullish_FVG')), catalizador['fvgs'][0] if catalizador['fvgs'] else None)
        if not fvg: return None
        
        v_origen_idx = catalizador['vela_origen_idx'] if catalizador['vela_origen_idx'] is not None else fvg['idx']
        v_origen_time = str(df_15m.index[v_origen_idx]) if v_origen_idx < len(df_15m) else "N/A"
        
        # --- Cálculo de Niveles Centralizado (Maura SMC) ---
        current_price = float(df_15m['close'].iloc[-1])
        atr_series = ta.ATR(df_15m['high'], df_15m['low'], df_15m['close'], 14).dropna()
        atr = atr_series.iloc[-1] if not atr_series.empty else 0
        
        setup = technical.calculate_fvg_setup(fvg, current_price, atr)
        entry = setup['entry']
        sl = setup['sl']
        direction = setup['direction']
        
        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        lookback_val = int(strat_config.get('lookback', 50))
        levels = technical.get_structural_levels(df_15m, lookback=lookback_val)
        atr_series = ta.ATR(df_15m['high'], df_15m['low'], df_15m['close'], 14).dropna()
        atr = atr_series.iloc[-1] if not atr_series.empty else 0
        
        # SL Estructural (Toni Maura style): Por debajo/encima del Swing previo
        if direction == 'CORTO':
            structural_sl = max(levels['swing_high'], entry * 1.001)
            sl = max(structural_sl, entry + atr*1.0) if atr > 0 else structural_sl
        else:
            structural_sl = min(levels['swing_low'], entry * 0.999)
            sl = min(structural_sl, entry - atr*1.0) if atr > 0 else structural_sl
            
        sl_dist = abs(entry - sl)
        
        tp_final_val = levels['low_zone'] if direction == 'CORTO' else levels['high_zone']
        tp_final_val = adjustTPForMinRR(entry, sl, tp_final_val, direction, minRR=1.5)
        
        # Cap de TP: máximo 4.0 ATR desde la entrada (Patron4h maneja TF 4H, permite más espacio)
        from Sentinel.analysis.technical import capTpByAtr
        tp_final_val = capTpByAtr(tp_final_val, entry, float(atr), direction, maxAtrMult=4.0)
        
        if direction == "LARGO":
            tp1_val = entry + (sl_dist * 1.25)
            tp2_val = (tp1_val + tp_final_val) / 2
        else:
            tp1_val = entry - (sl_dist * 1.25)
            tp2_val = (tp1_val + tp_final_val) / 2

        multiplier = getPipMultiplier(symbol)
        
        is_valid, _, _ = check_tp_exhaustion(df_15m, v_origen_idx, entry, tp_final_val, sl, direction, threshold=0.60, timeframe="15min")
        if not is_valid: return None
        
        current_price = float(df_15m['close'].iloc[-1])
        last_v = get_last_closed_candle(datetime.now(ZoneInfo(TIMEZONE)), 15, df=df_15m)
        candle_time = (last_v.name if hasattr(last_v, 'name') else last_v).strftime("%Y-%m-%d %H:%M:%S")

        is_valid, _, _ = check_signal_health(entry, tp_final_val, sl, direction, current_price, threshold=0.65, candle_time=candle_time)
        if not is_valid: return None
        
        mom_state = symbolInfo.get('momentum', '☁️ SIN DATOS')
        mom_bonus, _ = momentum.getMomentumBonus(mom_state, direction)
        
        # ── FILTRO: Tendencia mensual ──
        monthly_trend = symbolInfo.get('weekly_trend', 'NEUTRAL')
        logger.debug(f"[{symbol}] Patron4h: ctx_trend={ctx['tendencia']}, direction={direction}, monthly_trend={monthly_trend}")
        
        if monthly_trend == "BAJISTA" and direction == "LARGO":
            logger.info(f"[{symbol}] Señal LARGO descartada - tendencia mensual BAJISTA")
            return None
        elif monthly_trend == "ALCISTA" and direction == "CORTO":
            logger.info(f"[{symbol}] Señal CORTO descartada - tendencia mensual ALCISTA")
            return None
        
        candleTime = candle_time
        
        base_confidence = 70 + mom_bonus
        # --- Cálculo de Tamaño de Posición Real (Centralizado) ---
        from Sentinel.analysis import risk
        refCapital = symbolInfo.get('refCapital', 10000.0)
        refRiskPct = symbolInfo.get('refRiskPct', 1.0)
        
        # Usar precio actual como entrada real
        realEntry = current_price
        realRiskDist = abs(realEntry - sl)
        
        totalSize, riskUsdActual, marginUsed = risk.calculatePositionSize(
            refCapital, refRiskPct, realRiskDist, symbolInfo, entryPrice=realEntry
        )
        
        if totalSize is None or totalSize <= 0:
            logger.info(f"[{symbol}] Patron4h: Tamaño de posición inválido o margen insuficiente - descartando")
            return None
            
        rrRatio = round(abs(tp_final_val - realEntry) / realRiskDist, 2) if realRiskDist > 0 else 0
        expectedProfit = riskUsdActual * rrRatio
        
        minUsdProfit = float(strat_config.get('min_usd_profit', 10.0))
        if expectedProfit < minUsdProfit:
            logger.info(f"[{symbol}] Patron4h: Beneficio Est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - descartando")
            return None

        min_confidence = float(strat_config.get('min_confidence', 70))
        if base_confidence < min_confidence:
            logger.info(f"[{symbol}] Patron4h: confidence={base_confidence} < min_confidence={min_confidence} - descartando")
            return None
        
        import math
        sizePerTp = math.floor((totalSize / 3) / 1000) * 1000 if "JPY" not in symbol else math.floor(totalSize / 3)
        if sizePerTp < 1000 and "JPY" not in symbol: sizePerTp = 1000


        signals = []
        tp_configs = [
            ("TP1", tp1_val, "TP1 [1.25 RR]"),
            ("TP2", tp2_val, "TP2 [Intermedio]"),
            ("TP3", tp_final_val, "TP3 [Estructural]")
        ]
        
        for suffix, tp_val, setup_label in tp_configs:
            signals.append(Signal(
                strategy=f"Patron4h_{suffix}",
                symbol=symbol,
                direction=direction,
                entry_price=realEntry,
                stop_loss=sl,
                take_profit=tp_val,
                sl_distance=realRiskDist,
                risk_factor=0.33,
                confidence=base_confidence,
                setup=setup_label,
                status="EN ZONA ✅",
                candleTime=candleTime,
                intervalo="15min",
                riesgo_pips=round(realRiskDist * multiplier, 1),
                rr_ratio=round(abs(tp_val - realEntry) / realRiskDist, 2) if realRiskDist > 0 else 0,
                break_even=calculateBEPrice(realEntry, sl, tp_val, direction),
                size=sizePerTp,
                metadata={
                    "riskUsd": round(riskUsdActual / 3, 2),
                    "expectedProfit": round(expectedProfit / 3, 2),
                    "marginUsed": round(marginUsed / 3, 2),
                    "velaOrigen": v_origen_time
                }
            ))
            
        return signals

