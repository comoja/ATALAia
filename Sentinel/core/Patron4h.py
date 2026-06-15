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
from Sentinel.analysis.technical import is_in_ote_zone, calculate_ote_zone, resample_to_interval, check_tp_exhaustion, check_signal_health, detect_fvgs
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateBEPrice

from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class Patron4HBot:
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    
    def __init__(self):
        self.strategy_name = "Patron4h"
        self.usar_filtro_fibonacci = True
        self.modo_flexible = True
        self.currentTime = None
        logger.info("Bot iniciado con sistema Top-Down (1D -> 4H -> 1H -> 15M)")

    def getMexicoTime(self) -> datetime:
        if hasattr(self, 'currentTime') and self.currentTime is not None:
            if self.currentTime.tzinfo is None:
                return self.MEXICO_TZ.localize(self.currentTime)
            return self.currentTime.astimezone(self.MEXICO_TZ)
        return datetime.now(self.MEXICO_TZ)

    def resample_ohlcv(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        return resample_to_interval(df, timeframe)

    def detectar_fvg(self, df: pd.DataFrame, idx: int, direction: str, fvgMinPct: float) -> Optional[dict]:
        """
        Detecta un Fair Value Gap aislado de alta probabilidad delegando en la función centralizada de technical.py.
        """
        fvgs = detect_fvgs(df, min_gap_pct=fvgMinPct, validate_mitigation=True, apply_high_prob_filters=True)
        target_type = 'Bullish_FVG' if direction == 'LARGO' else 'Bearish_FVG'
        for fvg in fvgs:
            if fvg['idx'] == idx and fvg['type'] == target_type:
                if not fvg.get('rejectionLowProbability', False):
                    return fvg
        return None

    def detectar_displacement(self, df: pd.DataFrame, idx: int, direction: str, displacementPct: float) -> Optional[dict]:
        if idx < 1: return None
        vela = df.iloc[idx]
        open_p, close_p, high_p, low_p = vela['open'], vela['close'], vela['high'], vela['low']
        cuerpo, rango = abs(close_p - open_p), high_p - low_p
        if rango == 0 or (cuerpo / close_p) < displacementPct: return None
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

    def obtener_contexto_diario(self, df_1d: pd.DataFrame, fvgMinPct: float) -> dict:
        if len(df_1d) < 3: return {'tendencia': 'LATERAL', 'max_dia_anterior': None, 'min_dia_anterior': None, 'fvgs_diarios': []}
        max_prev, min_prev = float(df_1d['high'].iloc[-2]), float(df_1d['low'].iloc[-2])
        closes = df_1d['close'].iloc[-10:].values
        cambio = (closes[-1] - closes[0]) / closes[0]
        if cambio > 0.005: tendencia = 'ALCISTA'
        elif cambio < -0.005: tendencia = 'BAJISTA'
        else: tendencia = 'ALCISTA' if closes[-1] > closes[0] else 'BAJISTA' if self.modo_flexible else 'LATERAL'
        
        # Para el diario no aplicamos EMA200 rígida inicial para no sesgar de más, pero sí la lógica de alta probabilidad
        fvgs_detectados = detect_fvgs(df_1d, min_gap_pct=fvgMinPct, validate_mitigation=True, apply_high_prob_filters=False)
        fvgs = [f for f in fvgs_detectados if not f.get('rejectionLowProbability', False)]
        
        return {'tendencia': tendencia, 'max_dia_anterior': max_prev, 'min_dia_anterior': min_prev, 'fvgs_diarios': fvgs}

    def detectarLiquidityRaid(self, df_ltf: pd.DataFrame, maxPrev: float, minPrev: float) -> Optional[dict]:
        """
        Detecta un barrido de liquidez HTF usando la función centralizada.
        Requiere que el precio SUPERE el nivel Y CIERRE de vuelta dentro del rango
        (sweep real, no simple ruptura). Usa technical.detectLiquiditySweep.
        """
        return technical.detectLiquiditySweep(df_ltf, htfHigh=maxPrev, htfLow=minPrev, lookback=10)

    def analizar_catalizador(self, df_tf: pd.DataFrame, contexto: dict, raid: Optional[dict], name: str, fvgMinPct: float, displacementPct: float) -> dict:
        res = {'timeframe': name, 'hay_displacement': False, 'fvgs': [], 'hay_mss': False, 'confirmado': False, 'vela_origen_idx': None}
        if len(df_tf) < 20: return res
        trend = contexto['tendencia']
        direction = 'CORTO' if trend == 'BAJISTA' else 'LARGO'
        ahora = self.getMexicoTime().replace(tzinfo=None)
        
        # 1. Desplazamientos en las últimas 10 velas
        for i in range(len(df_tf)-1, max(len(df_tf)-10, 0), -1):
            disp = self.detectar_displacement(df_tf, i, direction, displacementPct)
            if disp:
                v_t = df_tf.index[i].to_pydatetime().replace(tzinfo=None) if hasattr(df_tf.index[i], 'to_pydatetime') else df_tf.index[i].replace(tzinfo=None)
                if (ahora - v_t).total_seconds() / 60 <= 120:
                    res.update({'hay_displacement': True, 'displacement_info': disp, 'vela_origen_idx': i})
                    break
                    
        # 2. Detección centralizada de FVG en catalizador una sola vez (Alto Rendimiento)
        fvgs_detectados = detect_fvgs(df_tf, min_gap_pct=fvgMinPct, validate_mitigation=True, apply_high_prob_filters=True)
        start_idx = max(1, len(df_tf) - 30)
        target_type = 'Bullish_FVG' if direction == 'LARGO' else 'Bearish_FVG'
        
        for fvg in fvgs_detectados:
            if fvg['idx'] >= start_idx and fvg['idx'] < len(df_tf) - 1:
                if fvg['type'] == target_type and not fvg.get('rejectionLowProbability', False):
                    res['fvgs'].append(fvg)
                    
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

        if df_15m is None or len(df_15m) < 100: 
            logger.info(f"[{symbol}] Datos insuficientes en {self.strategy_name}")
            return None
        
        if df_1h is None: df_1h = resample_to_interval(df_15m, '1h')
        if df_4h is None: df_4h = resample_to_interval(df_15m, '4h')
        if df_1d is None: df_1d = resample_to_interval(df_15m, '1d')
        
        # Carga dinámica de parámetros por símbolo
        stratConfig = dbManager.getSymbolStrategyConfig(self.strategy_name, symbol) or {}
        fvgMinPct = float(stratConfig.get('fvgMinPct') or stratConfig.get('fvg_min_pct') or 0.00005)
        displacementPct = float(stratConfig.get('displacementPct') or stratConfig.get('displacement_pct') or 0.0005)
        rrRatioMin = float(stratConfig.get('rrRatioMin') or stratConfig.get('rr_ratio_min') or 1.5)
        maxMinutosFvg = float(stratConfig.get('maxMinutosFvg') or stratConfig.get('max_minutos_fvg') or 240.0)
        oteFibMin = float(stratConfig.get('oteFibMin') or stratConfig.get('ote_fib_min') or 0.62)
        oteFibMax = float(stratConfig.get('oteFibMax') or stratConfig.get('ote_fib_max') or 0.79)
        lookbackVal = int(stratConfig.get('lookback') or 50)
        
        ctx = self.obtener_contexto_diario(df_1d, fvgMinPct)
        if ctx['tendencia'] == 'LATERAL': 
            logger.info(f"[{symbol}] No se detecto tendencia") 
            return None

        # Detectar sweep HTF en LTF (15m): precio superó PDH/PDL y cerró de vuelta al rango
        htfSweep = self.detectarLiquidityRaid(df_15m, ctx['max_dia_anterior'], ctx['min_dia_anterior'])
        
        c4h = self.analizar_catalizador(df_4h, ctx, htfSweep, '4H', fvgMinPct, displacementPct)
        c1h = self.analizar_catalizador(df_1h, ctx, htfSweep, '1h', fvgMinPct, displacementPct)
        catalizador = c4h if c4h['confirmado'] else c1h if c1h['confirmado'] else None
        if not catalizador: 
            logger.info(f"[{symbol}] No se detecto catalizador") 
            return None
        
        trend = ctx['tendencia']
        direction = 'CORTO' if trend == 'BAJISTA' else 'LARGO'
        fvg = next((f for f in catalizador['fvgs'] if (trend == 'BAJISTA' and f['type'] == 'Bearish_FVG') or (trend == 'ALCISTA' and f['type'] == 'Bullish_FVG')), catalizador['fvgs'][0] if catalizador['fvgs'] else None)
        if not fvg:
            logger.info(f"[{symbol}] No se detecto FVG") 
            return None
        
        df_catalizador = df_4h if c4h['confirmado'] else df_1h
        fvg_time = df_catalizador.index[fvg['idx']]
        
        # Mapear el índice al dataframe de 15min (LTF) por timestamp
        try:
            v_origen_idx_ltf = int(df_15m.index.get_indexer([fvg_time], method='pad')[0])
            if v_origen_idx_ltf < 0:
                v_origen_idx_ltf = len(df_15m) - 5
        except Exception:
            v_origen_idx_ltf = len(df_15m) - 5
            
        v_origen_time = fvg_time.strftime("%Y-%m-%d %H:%M:%S")
        
        # --- Cálculo de Niveles Centralizado (Maura SMC) ---
        currentPrice = float(df_15m['close'].iloc[-1])
        atr_series = ta.ATR(df_15m['high'], df_15m['low'], df_15m['close'], 14).dropna()
        atr = atr_series.iloc[-1] if not atr_series.empty else 0
        
        setup = technical.calculate_fvg_setup(fvg, currentPrice, atr)
        entry = setup['entry']
        sl = setup['sl']
        direction = setup['direction']
        
        levels = technical.get_structural_levels(df_15m, lookback=lookbackVal)
        
        # SL Estructural (Toni Maura style): Por debajo/encima del Swing previo
        if direction == 'CORTO':
            structural_sl = max(levels['swing_high'], entry * 1.001)
            sl = max(structural_sl, entry + atr*1.0) if atr > 0 else structural_sl
        else:
            structural_sl = min(levels['swing_low'], entry * 0.999)
            sl = min(structural_sl, entry - atr*1.0) if atr > 0 else structural_sl
            
        slDist = abs(entry - sl)
        
        tpFinalVal = levels['low_zone'] if direction == 'CORTO' else levels['high_zone']
        tpFinalVal = adjustTPForMinRR(entry, sl, tpFinalVal, direction, minRR=rrRatioMin)
        
        # Cap de TP: máximo 4.0 ATR desde la entrada (Patron4h maneja TF 4H, permite más espacio)
        from Sentinel.analysis.technical import capTpByAtr
        tpFinalVal = capTpByAtr(tpFinalVal, entry, float(atr), direction, maxAtrMult=4.0)
        
        multiplier = getPipMultiplier(symbol)
        
        isValid, _, _ = check_tp_exhaustion(df_15m, v_origen_idx_ltf, entry, tpFinalVal, sl, direction, threshold=0.75, timeframe="15min")
        if not isValid: return None
        
        current_dt = self.currentTime if (hasattr(self, 'currentTime') and self.currentTime is not None) else datetime.now(ZoneInfo(TIMEZONE))
        if df_15m is not None and not df_15m.empty:
            is_df_naive = (df_15m.index.tzinfo is None)
            if is_df_naive and current_dt.tzinfo is not None:
                current_dt = current_dt.replace(tzinfo=None)
            elif not is_df_naive and current_dt.tzinfo is None:
                current_dt = pytz.timezone(TIMEZONE).localize(current_dt)
                
        lastV = get_last_closed_candle(current_dt, 15, df=df_15m)
        candleTimeStr = (lastV.name if hasattr(lastV, 'name') else lastV).strftime("%Y-%m-%d %H:%M:%S")

        isValid, _, _ = check_signal_health(entry, tpFinalVal, sl, direction, currentPrice, threshold=0.65, candle_time=candleTimeStr)
        if not isValid: return None
        
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
        
        baseConfidence = 70 + mom_bonus
        # --- Cálculo de Tamaño de Posición Real (Centralizado) ---
        from Sentinel.analysis import risk
        refCapital = symbolInfo.get('refCapital', 10000.0)
        refRiskPct = symbolInfo.get('refRiskPct', 1.0)
        
        # Usar precio actual como entrada real
        realEntry = currentPrice
        realRiskDist = abs(realEntry - sl)
        
        totalSize, riskUsdActual, marginUsed = risk.calculatePositionSize(
            refCapital, refRiskPct, realRiskDist, symbolInfo, entryPrice=realEntry
        )
        
        if totalSize is None or totalSize <= 0:
            logger.info(f"[{symbol}] Patron4h: Tamaño de posición inválido o margen insuficiente - descartando")
            return None
            
        rrRatio = round(abs(tpFinalVal - realEntry) / realRiskDist, 2) if realRiskDist > 0 else 0
        expectedProfit = riskUsdActual * rrRatio
        
        minUsdProfit = float(stratConfig.get('minUsdProfit') or stratConfig.get('min_usd_profit') or 10.0)
        if expectedProfit < minUsdProfit:
            logger.info(f"[{symbol}] Patron4h: Beneficio Est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - descartando")
            return None

        minConfidence = float(stratConfig.get('minConfidence') or stratConfig.get('min_confidence') or 70.0)
        if baseConfidence < minConfidence:
            logger.info(f"[{symbol}] Patron4h: confidence={baseConfidence} < minConfidence={minConfidence} - descartando")
            return None

        # Retornar una sola señal robusta unificada
        return [Signal(
            strategy=self.strategy_name,
            symbol=symbol,
            direction=direction,
            entry_price=realEntry,
            stop_loss=sl,
            take_profit=tpFinalVal,
            sl_distance=realRiskDist,
            confidence=baseConfidence,
            setup="Ruptura Estructural 4H/1H",
            status="EN ZONA ✅",
            candleTime=candleTimeStr,
            intervalo="15min",
            riesgo_pips=round(realRiskDist * multiplier, 1),
            rr_ratio=rrRatio,
            break_even=calculateBEPrice(realEntry, sl, tpFinalVal, direction),
            size=totalSize,
            metadata={
                "riskUsd": round(riskUsdActual, 2),
                "expectedProfit": round(expectedProfit, 2),
                "marginUsed": round(marginUsed, 2),
                "velaOrigen": v_origen_time
            }
        )]
