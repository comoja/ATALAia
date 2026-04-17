"""
==============================================================================
  ESTRATEGIA DE TRADING: PATRÓN 4H - ICT / Smart Money Concepts
==============================================================================
  Implementa la lógica completa con evaluación TOP-DOWN simultánea:
    - Contexto en Diario
    - Catalizador en 4H o 1H
    - Entrada confirmada en 15M
  
  NOTAS CRÍTICAS:
    - SL dinámico utilizando cálculo de ATR para el padding
    - Confirmación de volumen para displacement
==============================================================================
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
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
from Sentinel.data.dataLoader import getParametros
from middleware.utils.communications import sendTelegramAlert
from middleware.utils.alertBuilder import buildAlertMessage, buildPatron4HAlertMessage
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from zoneinfo import ZoneInfo
from Sentinel.analysis import technical as tech_module
from Sentinel.analysis.technical import is_in_ote_zone, calculate_ote_zone, resample_to_interval
from Sentinel.core.BaseImbalanceBot import getAssetConfig


logger = logging.getLogger("sentinel")


class Patron4HBot:
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}
        
        strategyConfig = dbManager.getStrategyConfig("Patron4h")
        self.fvg_min_pct         = strategyConfig.get('fvg_min_pct',        0.00005) if strategyConfig else 0.00005
        self.displacement_pct     = strategyConfig.get('displacement_pct',   0.0005)  if strategyConfig else 0.0005
        self.rr_ratio_min         = strategyConfig.get('rr_ratio_min',       1.5)     if strategyConfig else 1.5
        self.max_minutos_fvg      = strategyConfig.get('max_minutos_fvg',    20)      if strategyConfig else 20
        # OTE activado (ICT Fibonacci 62-79%)
        self.usar_filtro_fibonacci = True
        self.ote_fib_min          = strategyConfig.get('ote_fib_min', 0.62) if strategyConfig else 0.62
        self.ote_fib_max          = strategyConfig.get('ote_fib_max', 0.79) if strategyConfig else 0.79
        self.modo_flexible        = True # Permitir detección de tendencia menos estricta
        self.signalsGeneradas = {} # Diccionario por símbolo
        self.timestamps_signals = {} # Diccionario por símbolo
        
        logger.info("Bot iniciado con sistema Top-Down (1D -> 4H -> 1H -> 15M)")

    def getMexicoTime(self) -> datetime:
        return datetime.now(self.MEXICO_TZ)

    def resample_ohlcv(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        return resample_to_interval(df, timeframe)

    def detectar_fvg(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[dict]:
        if idx < 2 or idx >= len(df) - 1:
            return None
        
        if direction == 'LONG':
            low_n = df['low'].iloc[idx]
            high_n2 = df['high'].iloc[idx - 2]
            if low_n > high_n2:
                gap = low_n - high_n2
                if gap / df['close'].iloc[idx] >= self.fvg_min_pct:
                    return {
                        'type': 'Bullish_FVG',
                        'start': high_n2,
                        'end': low_n,
                        'mid': (high_n2 + low_n) / 2,
                        'size': gap,
                        'idx': idx,
                        'idx_start': idx - 2,
                        'vela_idx': idx
                    }
        else:
            high_n = df['high'].iloc[idx]
            low_n2 = df['low'].iloc[idx - 2]
            if high_n < low_n2:
                gap = low_n2 - high_n
                if gap / df['close'].iloc[idx] >= self.fvg_min_pct:
                    return {
                        'type': 'Bearish_FVG',
                        'start': low_n2,
                        'end': high_n,
                        'mid': (low_n2 + high_n) / 2,
                        'size': gap,
                        'idx': idx,
                        'idx_start': idx - 2,
                        'vela_idx': idx
                    }
        return None

    def detectar_displacement(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[dict]:
        if idx < 1:
            return None
        
        vela = df.iloc[idx]
        open_price = vela['open']
        close_price = vela['close']
        high_price = vela['high']
        low_price = vela['low']
        
        cuerpo = abs(close_price - open_price)
        rango = high_price - low_price
        
        if rango == 0:
            return None
        
        mov_pct = cuerpo / close_price
        if mov_pct < self.displacement_pct:
            return None
            
        # Filtro de volumen
        if 'volume' in df.columns:
            start_vol = max(0, idx - 20)
            avg_vol = df['volume'].iloc[start_vol:idx].mean() if idx > start_vol else 0
            curr_vol = vela.get('volume', 0)
            if avg_vol > 0 and curr_vol < (avg_vol * 1.2):
                return None
        
        if direction == 'SHORT':
            if close_price < open_price and (cuerpo / rango) > 0.6:
                # La mecha inferior (rechazo) debe ser muy pequeña en un corto institucional
                if ((close_price - low_price) / rango) <= 0.25:
                    return {
                        'idx': idx,
                        'type': 'Bearish_Displacement',
                        'cuerpo_pct': mov_pct * 100,
                        'vela_open': open_price,
                        'vela_close': close_price,
                        'vela_high': high_price,
                        'vela_low': low_price
                    }
        else:
            if close_price > open_price and (cuerpo / rango) > 0.6:
                # La mecha superior (rechazo) debe ser muy pequeña en un largo institucional
                if ((high_price - close_price) / rango) <= 0.25:
                    return {
                        'idx': idx,
                        'type': 'Bullish_Displacement',
                        'cuerpo_pct': mov_pct * 100,
                        'vela_open': open_price,
                        'vela_close': close_price,
                        'vela_high': high_price,
                        'vela_low': low_price
                    }
        return None

    def detectar_mss(self, df: pd.DataFrame, direction: str) -> bool:
        if len(df) < 5:
            return False
        
        closes = df['close'].values
        highs = df['high'].values
        lows = df['low'].values
        
        if direction == 'SHORT':
            ultimo_high_local = max(highs[-5:-1])
            if closes[-1] < ultimo_high_local:
                return True
        else:
            ultimo_low_local = min(lows[-5:-1])
            if closes[-1] > ultimo_low_local:
                return True
        
        return False

    def obtener_contexto_diario(self, df_1d: pd.DataFrame) -> dict:
        if len(df_1d) < 3:
            return {
                'tendencia': 'LATERAL',
                'max_dia_anterior': None,
                'min_dia_anterior': None,
                'fvgs_diarios': []
            }
        
        max_dia_anterior = float(df_1d['high'].iloc[-2])
        min_dia_anterior = float(df_1d['low'].iloc[-2])
        
        highs = df_1d['high'].iloc[-10:].values
        lows = df_1d['low'].iloc[-10:].values
        closes = df_1d['close'].iloc[-10:].values
        
        cambio_total = (closes[-1] - closes[0]) / closes[0]
        umbral_tendencia = 0.005
        
        altos_mas_altos = highs[-1] > highs[0]
        bajos_mas_altos = lows[-1] > lows[0]
        bajos_mas_bajos = lows[-1] < lows[0]
        
        if cambio_total > umbral_tendencia and bajos_mas_altos:
            tendencia = 'ALCISTA'
        elif cambio_total < -umbral_tendencia and bajos_mas_bajos:
            tendencia = 'BAJISTA'
        elif cambio_total > umbral_tendencia:
            tendencia = 'ALCISTA'
        elif cambio_total < -umbral_tendencia:
            tendencia = 'BAJISTA'
        elif self.modo_flexible:
            tendencia = 'ALCISTA' if closes[-1] > closes[0] else 'BAJISTA'
        else:
            tendencia = 'LATERAL'
        
        fvgs_diarios = []
        for i in range(2, len(df_1d)):
            fvg_alcista = self.detectar_fvg(df_1d, i, 'LONG')
            if fvg_alcista:
                fvgs_diarios.append(fvg_alcista)
            
            fvg_bajista = self.detectar_fvg(df_1d, i, 'SHORT')
            if fvg_bajista:
                fvgs_diarios.append(fvg_bajista)
        
        return {
            'tendencia': tendencia,
            'max_dia_anterior': max_dia_anterior,
            'min_dia_anterior': min_dia_anterior,
            'fvgs_diarios': fvgs_diarios
        }

    def detectar_liquidity_raid(self, precio_actual: float, max_dia_anterior: float,
                                min_dia_anterior: float, tendencia: str) -> Optional[dict]:
        if tendencia == 'BAJISTA':
            if precio_actual < min_dia_anterior:
                return {
                    'tipo': 'RAID_MINIMO',
                    'nivel': min_dia_anterior,
                    'descripcion': 'Precio liquidó mínimo del día anterior'
                }
        elif tendencia == 'ALCISTA':
            if precio_actual > max_dia_anterior:
                return {
                    'tipo': 'RAID_MAXIMO',
                    'nivel': max_dia_anterior,
                    'descripcion': 'Precio liquidó máximo del día anterior'
                }
        return None

    def precio_en_fvg(self, precio: float, fvgs: list, tendencia: str) -> Optional[dict]:
        for fvg in fvgs:
            if tendencia == 'BAJISTA' and fvg['type'] != 'Bearish_FVG':
                continue
            if tendencia == 'ALCISTA' and fvg['type'] != 'Bullish_FVG':
                continue
            lo = min(fvg['start'], fvg['end'])
            hi = max(fvg['start'], fvg['end'])
            if lo <= precio <= hi:
                return fvg
        return None

    def analizar_catalizador(self, df_tf: pd.DataFrame, contexto: dict,liquidity_raid: Optional[dict], nombre_tf: str) -> dict:
        resultado = {
            'timeframe': nombre_tf,
            'hay_reaccion_poi': False,
            'fvg_diario_reacciono': None,
            'hay_displacement': False,
            'displacement_info': None,
            'hay_fvg': False,
            'fvgs': [],
            'hay_mss': False,
            'confirmado': False,
            'tipo_entrada': None,
            'vela_origen_idx': None
        }
        
        if len(df_tf) < 20:
            return resultado
        
        tendencia = contexto['tendencia']
        fvgs_diarios = contexto.get('fvgs_diarios', [])
        direction = 'SHORT' if tendencia == 'BAJISTA' else 'LONG'
        
        adx = ta.ADX(df_tf['high'], df_tf['low'], df_tf['close'], timeperiod=14).iloc[-1]
        mercado_erratico = True if (not pd.isna(adx) and adx < 20) else False
        
        # Veto por mercado lateral
        if mercado_erratico:
            logger.debug(f"[{nombre_tf}] Mercado lateral (ADX={adx:.1f} < 20), sin señales")
            return resultado
        
        aplicar_fibonacci = self.usar_filtro_fibonacci or mercado_erratico
        
        ahora = self.getMexicoTime()
        if ahora.tzinfo is not None: ahora = ahora.replace(tzinfo=None)
        
        for i in range(len(df_tf) - 1, max(len(df_tf) - 10, 0), -1):
            disp = self.detectar_displacement(df_tf, i, direction)
            if disp:
                # Filtrado por antigüedad: El desplazamiento debe ser reciente (máx 60m)
                vela_time = df_tf.index[i]
                if hasattr(vela_time, 'to_pydatetime'): vela_time = vela_time.to_pydatetime()
                if vela_time.tzinfo is not None: vela_time = vela_time.replace(tzinfo=None)
                
                minutos_antiguedad = (ahora - vela_time).total_seconds() / 60
                
                if minutos_antiguedad > 45:
                    logger.debug(f"[{nombre_tf}] Desplazamiento descartado por antigüedad: {minutos_antiguedad:.1f} min (Máx: 45min)")
                    continue
                
                resultado['hay_displacement'] = True
                resultado['displacement_info'] = disp
                resultado['vela_origen_timestamp'] = vela_time
                resultado['vela_origen_idx'] = i
                break
        
        if not resultado['hay_displacement']:
            # Si no hay displacement, buscar el FVG más reciente para guardar su idx
            for i in range(len(df_tf) - 2, max(0, len(df_tf) - 30), -1):
                fvg_check = self.detectar_fvg(df_tf, i, direction)
                if fvg_check:
                    resultado['vela_origen_idx'] = i
                    break
            if resultado['vela_origen_idx'] is None:
                resultado['vela_origen_idx'] = len(df_tf) - 5  # fallback
            return resultado
        
        for i in range(max(1, len(df_tf) - 30), len(df_tf) - 1):
            fvg = self.detectar_fvg(df_tf, i, direction)
            if fvg:
                if aplicar_fibonacci:
                    # ── OTE Filter (ICT): Fibonacci 62-79% del último swing ──────────
                    swing_high = df_tf['high'].iloc[max(0, i-20):i].max()
                    swing_low  = df_tf['low'].iloc[max(0, i-20):i].min()

                    if direction == 'LONG':
                        # Buscamos compras: FVG en retroceso 62-79% del impulso bajista
                        in_ote, ote_zone = is_in_ote_zone(
                            fvg['mid'], swing_high, swing_low, 'LARGO',
                            fib_min=self.ote_fib_min, fib_max=self.ote_fib_max
                        )
                    else:
                        # Buscamos ventas: FVG en retroceso 62-79% del impulso alcista
                        in_ote, ote_zone = is_in_ote_zone(
                            fvg['mid'], swing_low, swing_high, 'CORTO',
                            fib_min=self.ote_fib_min, fib_max=self.ote_fib_max
                        )

                    if in_ote:
                        fvg['ote_zone'] = ote_zone
                        fvg['in_ote'] = True
                        logger.info(
                            f"[Patron4H][{nombre_tf}] FVG mid={fvg['mid']:.4f} "
                            f"✅ en OTE [{ote_zone['ote_low']:.4f}–{ote_zone['ote_high']:.4f}]"
                        )
                    else:
                        fvg['in_ote'] = False
                        logger.info(
                            f"[Patron4H][{nombre_tf}] FVG mid={fvg['mid']:.4f} "
                            f"⚠️ fuera OTE [{ote_zone['ote_low']:.4f}–{ote_zone['ote_high']:.4f}] "
                            f"(se conserva en modo flexible)"
                        )
                    # ────────────────────────────────────────────────────────────────
                resultado['fvgs'].append(fvg)

        resultado['hay_fvg'] = len(resultado['fvgs']) > 0
        resultado['ote_fvg_count'] = sum(1 for f in resultado['fvgs'] if f.get('in_ote', False))
        resultado['hay_mss'] = self.detectar_mss(df_tf, direction)
        
        precio_low = float(df_tf['low'].iloc[-1])
        precio_high = float(df_tf['high'].iloc[-1])
        fvg_reaccion = self.precio_en_fvg(precio_low, fvgs_diarios, tendencia)
        if not fvg_reaccion:
            fvg_reaccion = self.precio_en_fvg(precio_high, fvgs_diarios, tendencia)
        
        if fvg_reaccion and liquidity_raid:
            resultado['hay_reaccion_poi'] = True
            resultado['fvg_diario_reacciono'] = fvg_reaccion
        
        tiene_fvg_alineado = False
        for fvg in resultado['fvgs']:
            if (tendencia == 'BAJISTA' and fvg['type'] == 'Bearish_FVG') or \
               (tendencia == 'ALCISTA' and fvg['type'] == 'Bullish_FVG'):
                tiene_fvg_alineado = True
                break
        
        if self.modo_flexible:
            resultado['confirmado'] = (resultado['hay_displacement'] or (resultado['hay_fvg'] and resultado['hay_mss']))
            if resultado['hay_displacement'] and resultado['hay_fvg']:
                resultado['tipo_entrada'] = f'CASO_A_{nombre_tf}'
            elif resultado['hay_fvg'] and resultado['hay_mss']:
                resultado['tipo_entrada'] = f'CASO_B_{nombre_tf}'
            elif resultado['hay_displacement']:
                resultado['tipo_entrada'] = f'SOLO_DISP_{nombre_tf}'
            elif resultado['hay_fvg']:
                resultado['tipo_entrada'] = f'SOLO_FVG_{nombre_tf}'
        else:
            resultado['confirmado'] = (resultado['hay_displacement'] and resultado['hay_fvg'] and tiene_fvg_alineado)
            if resultado['confirmado']:
                disp_info = resultado['displacement_info']
                if disp_info and disp_info['cuerpo_pct'] > self.displacement_pct * 100 * 1.5:
                    resultado['tipo_entrada'] = f'CASO_A_{nombre_tf}'
                else:
                    resultado['tipo_entrada'] = f'CASO_B_{nombre_tf}'
        
        return resultado

    def _get_atr_padding(self, df: pd.DataFrame, multiplier: float = 0.5) -> float:
        if len(df) < 15:
            return 0.001
        atr = ta.ATR(df['high'], df['low'], df['close'], timeperiod=14).iloc[-1]
        if pd.isna(atr):
            return 0.001
        return atr * multiplier

    def generar_señal_15m(self, catalizador: dict, df_15m: pd.DataFrame, df_tf_sup: pd.DataFrame, contexto: dict) -> Optional[dict]:
        tendencia = contexto['tendencia']
        direction = 'SHORT' if tendencia == 'BAJISTA' else 'LONG'
        fvgs = catalizador.get('fvgs', [])
        fvg_principal = next((f for f in fvgs if (tendencia == 'BAJISTA' and f['type'] == 'Bearish_FVG') or (tendencia == 'ALCISTA' and f['type'] == 'Bullish_FVG')), None)
        
        if not fvg_principal and self.modo_flexible and fvgs:
            fvg_principal = fvgs[0]
        
        disp_info = catalizador.get('displacement_info', {})
    def _get_pip_multiplier(self, symbol: str) -> float:
        """Determina el multiplicador de pips según el activo."""
        symbol_up = symbol.upper()
        if "XAU" in symbol_up or "GOLD" in symbol_up:
            return 1.0
        if any(pair in symbol_up for pair in ["JPY", "HUF"]):
            return 100.0 # Pips para JPY
        if any(crypto in symbol_up for crypto in ["BTC", "ETH", "SOL", "BNB"]):
            return 1.0   # Puntos (Dólares)
        return 10000.0 # Standard Forex Pips

    def _validate_and_adjust_signal(self, entry: float, sl: float, tp: float, direction: str, symbol: str, setup_name: str, timeframe: str, confidence: int, df_tf: pd.DataFrame = None, vela_origen_idx: int = None, symbolInfo: Dict = None) -> Optional[dict]:
        """Calcula riesgo, RR y ajusta TP si es necesario. Retorna None si el RR es inviable o las distancias son muy pequeñas."""
        riesgo = abs(entry - sl)
        if riesgo == 0:
            return None
        
        multiplier = self._get_pip_multiplier(symbol)
        
        min_distance_pips = 6.0
        min_distance_absolute = min_distance_pips / multiplier
        
        if riesgo < min_distance_absolute:
            logger.info(f"[{symbol}] Señal descartada: distancia SL muy pequeña ({riesgo * multiplier:.1f} pips < {min_distance_pips} pips)")
            return None
        
        distancia_tp = abs(tp - entry)
        if distancia_tp < min_distance_absolute:
            logger.info(f"[{symbol}] Señal descartada: distancia TP muy pequeña ({distancia_tp * multiplier:.1f} pips < {min_distance_pips} pips)")
            return None
        
        if df_tf is not None and len(df_tf) >= 14:
            atr = ta.ATR(df_tf['high'], df_tf['low'], df_tf['close'], 14).iloc[-1]
            if not pd.isna(atr) and atr > 0:
                atr_min_distance = atr * 0.3
                if riesgo < atr_min_distance:
                    logger.info(f"[{symbol}] Señal descartada: distancia SL ({riesgo:.5f}) < 0.3*ATR ({atr_min_distance:.5f})")
                    return None
                if distancia_tp < atr_min_distance:
                    logger.info(f"[{symbol}] Señal descartada: distancia TP ({distancia_tp:.5f}) < 0.3*ATR ({atr_min_distance:.5f})")
                    return None
        
        distancia_tp = abs(tp - entry)
        rr_actual = distancia_tp / riesgo
        
        if rr_actual < self.rr_ratio_min:
            if direction == 'LONG' or direction == 'LARGO':
                tp = entry + (riesgo * self.rr_ratio_min)
            else:
                tp = entry - (riesgo * self.rr_ratio_min)
                
            distancia_tp = abs(tp - entry)
            rr_actual = self.rr_ratio_min
            
        if rr_actual < 1.2:
            logger.info(f"[{symbol}] Señal descartada: RR insuficiente ({rr_actual:.2f})")
            return None
        
        # ── FILTRO: Verificar si el precio ya recorrió >60% hacia el TP ──
        # Si el precio ya se acercó demasiado al TP desde la vela original, la señal está "gastada"
        is_valid, recorrido_pct, _ = tech_module.check_tp_exhaustion(df_tf, vela_origen_idx, entry, tp, direction, threshold=0.60)
        if not is_valid:
            logger.info(f"[{symbol}] Señal descartada: Precio ya recorrió {recorrido_pct*100:.1f}% hacia TP (umbral: 60%)")
            return None
        
        # Momentum Filter: Usar momentum pre-calculado desde main.py
        momentum_estado = symbolInfo.get('momentum', '☁️ SIN DATOS') if symbolInfo else '☁️ SIN DATOS'
        momentum_bonus = 0
        
        direction_upper = direction.upper() if direction else ""
        if direction_upper in ("LONG", "LARGO") and momentum_estado in ["🚀 ALCISTA", "💎 GIRO"]:
            momentum_bonus = 10
        elif direction_upper == "SHORT" and momentum_estado in ["📉 BAJISTA"]:
            momentum_bonus = 10
        elif momentum_estado in ["💸 LIQUIDACIÓN", "🌋 PARÁBOLA"]:
            momentum_bonus = -5
            
        logger.info(f"[{symbol}] Momentum: {momentum_estado} → {'+' if momentum_bonus > 0 else ''}{momentum_bonus}% confianza")
        
        # --- SEMÁFORO DE ENTRADA (Price Action) ---
        # El progreso se mide desde la entrada hasta el TP
        # Al ser el momento de la detección, el progreso es inicial (0%)
        status_msg = "EN ZONA ✅"

        return {
            'tipo_entrada': setup_name, 
            'direccion': 'LARGO' if (direction == 'LONG' or direction == 'LARGO') else 'CORTO',
            'entrada': round(entry, 5), 
            'stop_loss': round(sl, 5), 
            'take_profit': round(tp, 5),
            'status': status_msg,
            'riesgo_pips': round(riesgo * multiplier, 1), 
            'rr_ratio': round(rr_actual, 2),
            'timeframe_entrada': '15M', 
            'timeframe_confirmacion': timeframe, 
            'confianza': confidence + momentum_bonus,
            'momentum': momentum_estado
        }

    def _generar_entrada_directa(self, fvg: dict, df_tf: pd.DataFrame, df_15m: pd.DataFrame, direction: str, nivel_origen: float, timeframe: str, symbol: str, vela_origen_idx: int = None, symbolInfo: Dict = None) -> Optional[dict]:
        if len(df_tf) < 5:
            return None
        idx_fvg = fvg['idx']
        if idx_fvg >= len(df_tf) - 1:
            return None
        
        # Niveles estructurales para TP lógicos (sensibilidad aumentada)
        from Sentinel.analysis import technical
        levels = technical.get_structural_levels(df_tf, lookback=50)
        
        vela_confirmacion = df_tf.iloc[idx_fvg + 1]
        entrada = float(vela_confirmacion['close'])
        padding = self._get_atr_padding(df_15m, multiplier=0.5)
        
        if direction == 'SHORT':
            sl = (nivel_origen + padding) if (nivel_origen and nivel_origen > entrada) else float(df_tf['high'].iloc[idx_fvg:idx_fvg+3].max()) + padding
            tp_tecnico = levels['low_zone']
        else:
            sl = (nivel_origen - padding) if (nivel_origen and nivel_origen < entrada) else float(df_tf['low'].iloc[idx_fvg:idx_fvg+3].min()) - padding
            tp_tecnico = levels['high_zone']
        
        return self._validate_and_adjust_signal(entrada, sl, tp_tecnico, direction, symbol, f'CASO_A_{timeframe}', timeframe, 70, df_tf, vela_origen_idx, symbolInfo)

    def _generar_entrada_refinada(self, fvg: dict, df_15m: pd.DataFrame, df_tf_sup: pd.DataFrame, direction: str, nivel_origen: float, tendencia: str, symbol: str, vela_origen_idx: int = None, symbolInfo: Dict = None) -> Optional[dict]:
        if len(df_15m) < 10: return None
        zona_min = min(fvg['start'], fvg['end'])
        zona_max = max(fvg['start'], fvg['end'])
        
        hay_retesteo = any(zona_min <= df_15m['low'].iloc[i] <= zona_max or zona_min <= df_15m['high'].iloc[i] <= zona_max for i in range(-1, -min(30, len(df_15m)), -1))
        if not hay_retesteo: return None
        
        hay_mss = self.detectar_mss(df_15m, direction)
        fvg_15m = next((self.detectar_fvg(df_15m, i, direction) for i in range(max(1, len(df_15m) - 20), len(df_15m) - 1) if self.detectar_fvg(df_15m, i, direction)), None)
        if not fvg_15m or not hay_mss: return None
        
        # Niveles estructurales para TP lógicos
        from Sentinel.analysis import technical
        levels = technical.get_structural_levels(df_tf_sup, lookback=20)
        
        entrada = float(fvg_15m['mid'])
        padding = self._get_atr_padding(df_15m, multiplier=0.5)
        
        if direction == 'SHORT':
            idx_fvg = fvg.get('idx', len(df_tf_sup) - 5)
            sl = (nivel_origen + padding) if (nivel_origen and nivel_origen > entrada) else float(df_tf_sup['high'].iloc[max(0, idx_fvg-2):idx_fvg+3].max()) + padding
            tp_tecnico = levels['low_zone']
        else:
            idx_fvg = fvg.get('idx', len(df_tf_sup) - 5)
            sl = (nivel_origen - padding) if (nivel_origen and nivel_origen < entrada) else float(df_tf_sup['low'].iloc[max(0, idx_fvg-2):idx_fvg+3].min()) - padding
            tp_tecnico = levels['high_zone']
            
        return self._validate_and_adjust_signal(entrada, sl, tp_tecnico, direction, symbol, 'CASO_B_15M', '4H/D', 85, df_tf_sup, vela_origen_idx, symbolInfo)

    def _generar_entrada_solo_displacement(self, df_tf: pd.DataFrame, df_15m: pd.DataFrame, direction: str, disp_info: dict, timeframe: str, symbol: str, vela_origen_idx: int = None, symbolInfo: Dict = None) -> Optional[dict]:
        if len(df_tf) < 5: return None
        idx = min(max(0, disp_info.get('idx', -1)), len(df_tf) - 2)
        entrada = float(df_tf['close'].iloc[idx])
        padding = self._get_atr_padding(df_15m, multiplier=0.8)
        
        if direction == 'SHORT':
            sl = entrada + padding
            tp_tecnico = entrada - (padding * self.rr_ratio_min)
        else:
            sl = entrada - padding
            tp_tecnico = entrada + (padding * self.rr_ratio_min)
            
        return self._validate_and_adjust_signal(entrada, sl, tp_tecnico, direction, symbol, f'SOLO_DISP_{timeframe}', timeframe, 50, df_tf, vela_origen_idx, symbolInfo)

    def _generar_entrada_solo_fvg(self, fvg: dict, df_tf: pd.DataFrame, df_15m: pd.DataFrame, direction: str, timeframe: str, symbol: str, vela_origen_idx: int = None, symbolInfo: Dict = None) -> Optional[dict]:
        if len(df_tf) < 5: return None
        idx = min(max(0, fvg.get('idx', -1)), len(df_tf) - 2)
        entrada = float(df_tf['close'].iloc[idx])
        padding = self._get_atr_padding(df_15m, multiplier=0.8)
        
        if direction == 'SHORT':
            sl = entrada + padding
            tp_tecnico = entrada - (padding * self.rr_ratio_min)
        else:
            sl = entrada - padding
            tp_tecnico = entrada + (padding * self.rr_ratio_min)
            
        return self._validate_and_adjust_signal(entrada, sl, tp_tecnico, direction, symbol, f'SOLO_FVG_{timeframe}', timeframe, 40, df_tf, vela_origen_idx, symbolInfo)

    def generar_señal_15m(self, catalizador: dict, df_15m: pd.DataFrame, df_tf_sup: pd.DataFrame, contexto: dict, symbol: str, symbolInfo: Dict = None) -> Optional[dict]:
        tendencia = contexto['tendencia']
        direction = 'SHORT' if tendencia == 'BAJISTA' else 'LONG'
        fvgs = catalizador.get('fvgs', [])
        fvg_principal = next((f for f in fvgs if (tendencia == 'BAJISTA' and f['type'] == 'Bearish_FVG') or (tendencia == 'ALCISTA' and f['type'] == 'Bullish_FVG')), None)
        
        if not fvg_principal and self.modo_flexible and fvgs:
            fvg_principal = fvgs[0]
        
        disp_info = catalizador.get('displacement_info', {})
        nivel_origen = disp_info.get('vela_low' if direction == 'LONG' else 'vela_high')
        vela_origen_idx = catalizador.get('vela_origen_idx')
        
        if catalizador['hay_displacement'] and catalizador['hay_fvg']:
            return self._generar_entrada_directa(fvg_principal, df_tf_sup, df_15m, direction, nivel_origen, catalizador['timeframe'], symbol, vela_origen_idx, symbolInfo)
        elif catalizador['hay_fvg'] and catalizador['hay_mss']:
            return self._generar_entrada_refinada(fvg_principal, df_15m, df_tf_sup, direction, nivel_origen, tendencia, symbol, vela_origen_idx, symbolInfo)
        elif catalizador['hay_displacement'] and self.modo_flexible:
            return self._generar_entrada_solo_displacement(df_tf_sup, df_15m, direction, disp_info, catalizador['timeframe'], symbol, vela_origen_idx, symbolInfo)
        elif catalizador['hay_fvg'] and self.modo_flexible:
            return self._generar_entrada_solo_fvg(fvg_principal, df_tf_sup, df_15m, direction, catalizador['timeframe'], symbol, vela_origen_idx, symbolInfo)
        
        logger.info(f"Rechazada generar_señal_15m: Sin combinación válida (disp={catalizador['hay_displacement']}, fvg={catalizador['hay_fvg']}, mss={catalizador['hay_mss']})")
        return None

    def analizar_top_down(self, datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> dict:
        df_15m = datos.get('15m')
        df_1h = datos.get('1h')
        df_4h = datos.get('4h')
        df_1d = datos.get('1d')
        
        contexto = self.obtener_contexto_diario(df_1d)
        if contexto['tendencia'] == 'LATERAL':
            logger.info(f"[{symbolInfo['symbol']}] Rechazada: Tendencia LATERAL en diario")
            return {'status': 'TENDENCIA_LATERAL'}
            
        precio_actual = float(df_15m['close'].iloc[-1])
        liquidity_raid = self.detectar_liquidity_raid(precio_actual, contexto['max_dia_anterior'], contexto['min_dia_anterior'], contexto['tendencia'])
        
        catalizador_4h = self.analizar_catalizador(df_4h, contexto, liquidity_raid, '4H')
        catalizador_1h = self.analizar_catalizador(df_1h, contexto, liquidity_raid, '1h')
        
        catalizador_final = None
        df_tf_sup = None
        
        if catalizador_4h['confirmado']:
            catalizador_final = catalizador_4h
            df_tf_sup = df_4h
        elif catalizador_1h['confirmado']:
            catalizador_final = catalizador_1h
            df_tf_sup = df_1h
        else:
            catalizador_15m = self.analizar_catalizador(df_15m, contexto, liquidity_raid, '15M')
            catalizador_final = catalizador_15m
            df_tf_sup = df_15m
            
        if catalizador_final and catalizador_final['hay_displacement']:
            señal = self.generar_señal_15m(catalizador_final, df_15m, df_tf_sup, contexto, symbolInfo['symbol'], symbolInfo)
            if señal:
                return {'status': 'SENAL_GENERADA', 'señal': señal}
            else:
                logger.info(f"[{symbolInfo['symbol']}] Rechazada: Displacement detectado pero no se pudo generar una señal con RR viable")
                
        else:
            logger.info(f"[{symbolInfo['symbol']}] Rechazada: Sin displacement confirmado en ninguna temporalidad (4H/1H/15M)")
        
        return {'status': 'SIN_ENTRADA_VALIDA'}

    async def _executeTrades(self, signal: Dict, symbolInfo: Dict, df_15m: pd.DataFrame = None):
        if not signal:
            return

        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                return

        for account in self.accounts:
            # Excluir cuenta maestra de señales (SENTINEL)
            if account['idCuenta'] == 1: continue
            if not dbManager.isEstrategiaHabilitadaParaCuenta(account['idCuenta'], 'Patron4h'):
                continue
            
            posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=abs(signal['entrada'] - signal['stop_loss']),
                symbolInfo=symbolInfo,
                entryPrice=signal.get('entrada')
            )
            
            if posSize is None or posSize == 0:
                continue
            
            signal['profit'] = riskUsd
            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbolInfo['symbol'],
                "direction": signal['direccion'],
                "entryPrice": signal['entrada'],
                "openTime": self.getMexicoTime().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": signal['stop_loss'],
                "takeProfit": signal['take_profit'],
                "size": posSize,
                "intervalo": "15min",
                "status": "OPEN",
                "strategy": "Patron4h",
                "margin_used": marginUsed,
            }
            
            # Ejecución centralizada vía Gateway (DB + Telegram + Broker)
            from middleware.execution.broker_gateway import gateway
            
            # Normalización para el generador de alertas
            now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
            last_closed = get_last_closed_candle(now_cdmx, interval=15)
            signal_norm = {
                **signal,
                "direction": signal.get("direccion"),
                "entryPrice": signal.get("entrada"),
                "confidence": signal.get("confianza", 70),
                "setup": signal.get("tipo_entrada", "N/A"),
                "candle_time": last_closed.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            success, msgId = await gateway.execute_trade(trade, signal_norm, account, "Patron4h", df=df_15m)
            if success and msgId:
                self.lastMessageIds[symbolInfo['symbol']] = msgId
        
        self.signalsGeneradas[symbolInfo['symbol']] = True

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None):
        symbol = symbolInfo['symbol']
        logger.info(f"▶ ENTRANDO análisis para {symbol}")
        df_15m = preloadedData.get('15m') if preloadedData else None
        if df_15m is None or len(df_15m) < 100:
            logger.info(f"◀ SALIENDO análisis para {symbol} (datos insuficientes)")
            return
        
        df_1h = self.resample_ohlcv(df_15m, '1h')
        df_4h = self.resample_ohlcv(df_15m, '4h')
        df_1d = self.resample_ohlcv(df_15m, '1d')
        
        datos = {'15m': df_15m, '1h': df_1h, '4h': df_4h, '1d': df_1d}
        
        if self.signalsGeneradas.get(symbol, False) and self.timestamps_signals.get(symbol):
            ahora = self.getMexicoTime().replace(tzinfo=None)
            minutos_desde = (ahora - self.timestamps_signals[symbol]).total_seconds() / 60
            if minutos_desde > self.max_minutos_fvg:
                self.signalsGeneradas[symbol] = False
            else:
                logger.info(f"[{symbol}] Cooldown: Señal generada hace {minutos_desde:.1f}m (Límite: {self.max_minutos_fvg}m)")
                return
        
        resultado = self.analizar_top_down(datos, symbolInfo)
        
        if resultado['status'] == 'SENAL_GENERADA' and resultado.get('señal'):
            señal = resultado['señal']
            self.timestamps_signals[symbol] = self.getMexicoTime().replace(tzinfo=None)
            
            signal_telegram = {
                **señal,
                "strategy": "Patron4h",
                "direction": señal['direccion']
            }
            await self._executeTrades(signal_telegram, symbolInfo, df_15m)

        logger.info(f"◀ SALIENDO análisis para {symbol}")

def executePatron4H(datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> Optional[Dict]:
    bot = Patron4HBot()
    df_15m = datos.get('15m')
    if df_15m is None: return None
    datos_completos = {
        '15m': df_15m,
        '1h': datos.get('1h') if datos.get('1h') is not None else bot.resample_ohlcv(df_15m, '1h'),
        '4h': datos.get('4h') if datos.get('4h') is not None else bot.resample_ohlcv(df_15m, '4h'),
        '1d': datos.get('1d') if datos.get('1d') is not None else bot.resample_ohlcv(df_15m, '1d')
    }
    return bot.analizar_top_down(datos_completos, symbolInfo)

if __name__ == "__main__":
    print("Patron4H Strategy Module - Top Down Synchronous Analysis")
