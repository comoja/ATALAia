"""
==============================================================================
  ESTRATEGIA DE TRADING: PATRÓN 4H - ICT / Smart Money Concepts
==============================================================================
  Implementa la lógica completa del pseudocódigo con ciclo de 3 temporalidades:
    CICLO 1 (4H) → Contexto y catalizador en 4H
    CICLO 2 (1H)  → Refinamiento en 1H  
    CICLO 3 (15M) → Entrada confirmada en 15M
  
  El sistema rota por los 3 ciclos, guardando estado entre ellos.
  La entrada real siempre se ejecuta en 15M.

  NOTAS CRÍTICAS:
    - El FVG es obligatorio: si no hay ineficiencia confirmada, no hay entrada
    - Stop Loss va donde se originó el movimiento de ruptura, no al último pivote
    - No usar Break Even prematuro
    - Salida por estructura (objetivo de liquidez mayor), no por ratios fijos

  DEPENDENCIAS: pandas, numpy, talib, pytz
==============================================================================
"""

import logging
from datetime import datetime
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
from middleware.utils.alertBuilder import buildAlertMessage
from middleware.config.constants import TIMEZONE

logger = logging.getLogger(__name__)


def getAssetConfig(symbol: str) -> dict:
    """Obtiene configuración de SL/TP basada en el tipo de activo."""
    symbolData = dbManager.getSymbol(symbol)
    assetType = symbolData.get('tipo') if symbolData else 'MONEDA'
    
    typeConfig = dbManager.getSymbolTypeConfig(assetType)
    
    if typeConfig:
        return {
            "sl": float(typeConfig.get('sl_atr', 1.5)),
            "tp": float(typeConfig.get('tp_atr', 3.0))
        }
    
    return {"sl": 1.5, "tp": 3.0}


class Patron4HBot:
    """
    Estrategia Patrón 4H basada en ICT/Smart Money Concepts.
    
    Sistema de CICLOS que rota por 3 temporalidades:
    - CICLO 1 (4H): Análisis de contexto y catalizador en 4H
    - CICLO 2 (1H):  Refinamiento en 1H
    - CICLO 3 (15M): Entrada confirmada en 15M
    
    El estado se guarda entre ciclos para mantener coherencia.
    La entrada real siempre se ejecuta en 15M.
    """
    
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    CICLO_4H = 0
    CICLO_1H = 1
    CICLO_15M = 2
    
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}
        
        strategyConfig = dbManager.getStrategyConfig("Patron4h")
        self.fvg_min_pct = strategyConfig.get('fvg_min_pct', 0.00005) if strategyConfig else 0.00005
        self.displacement_pct = strategyConfig.get('displacement_pct', 0.0005) if strategyConfig else 0.0005
        self.rr_ratio_min = strategyConfig.get('rr_ratio_min', 1.5) if strategyConfig else 1.5
        self.max_minutos_fvg = strategyConfig.get('max_minutos_fvg', 240) if strategyConfig else 240
        
        self.modo_flexible = True
        
        self.ciclo_actual = self.CICLO_4H
        self.signalGenerada = False
        self.timestamp_signal = None
        self.signal_enviada = False
        
        self.estado_patron = {
            'contexto': None,
            'liquidity_raid': None,
            'fase2_4h': None,
            'fase2_1h': None,
            'señal': None,
            'ciclo_confirmado': None
        }
        
        logger.info("[Patron4H] Bot iniciado con sistema de ciclos 4H → 1H → 15M")

    def getMexicoTime(self) -> datetime:
        return datetime.now(self.MEXICO_TZ)

    def resample_ohlcv(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
        """Resamplea velas a una temporalidad mayor."""
        if df.empty:
            return df
        
        df = df.copy()
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        
        rule_map = {'1h': '1h', '4h': '4h', '1d': '1d', '1H': '1h', '4H': '4h', '1D': '1d'}
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
        
        if df.index[-1] < df_resampled.index[-1]:
            df_resampled = df_resampled.iloc[:-1]
        
        return df_resampled.dropna()

    def detectar_fvg(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[dict]:
        """
        Detecta un Fair Value Gap en un índice específico.
        FVG Alcista: Low[idx] > High[idx-2]
        FVG Bajista: High[idx] < Low[idx-2]
        """
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

    def detectar_fvgs_en_rango(self, df: pd.DataFrame, start_idx: int, direction: str, 
                               nivel_min: float, nivel_max: float, max_velas: int = 50) -> list:
        """Busca FVGs dentro de un rango de precios específico."""
        fvgs = []
        for i in range(start_idx, min(start_idx + max_velas, len(df) - 3)):
            vela = df.iloc[i]
            if vela['high'] > nivel_max or vela['low'] < nivel_min:
                continue
            
            fvg = self.detectar_fvg(df, i, direction)
            if fvg:
                fvgs.append(fvg)
        
        return fvgs

    def detectar_displacement(self, df: pd.DataFrame, idx: int, direction: str) -> Optional[dict]:
        """
        Detecta displacement (movimiento fuerte que rompe estructura).
        El cuerpo de la vela debe cerrar por encima/debajo del último impulso.
        """
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
        
        if direction == 'SHORT':
            if close_price < open_price and cuerpo / rango > 0.5:
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
            if close_price > open_price and cuerpo / rango > 0.5:
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
        """
        Detecta Market Structure Shift (MSS).
        El precio rompe el último máximo/mínimo local relevante.
        """
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
        """
        FASE 1: Obtiene el contexto diario.
        - Máximo y mínimo del día anterior
        - Dirección principal (orderflow: bajos más bajos = bajista)
        """
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
        altos_mas_bajos = highs[-1] < highs[0]
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
        """
        Detecta si el precio ha liquidado (raided) el máximo o mínimo del día anterior.
        """
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
        """Verifica si el precio está dentro de un FVG alineado con la tendencia."""
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

    def analizar_catalizador(self, df_tf: pd.DataFrame, contexto: dict, 
                           liquidity_raid: Optional[dict], nombre_tf: str) -> dict:
        """
        Análisis de catalizador para cualquier temporalidad.
        Detecta: Displacement + FVG + MSS + Reacción en POI
        """
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
            'tipo_entrada': None
        }
        
        if len(df_tf) < 5:
            return resultado
        
        tendencia = contexto['tendencia']
        fvgs_diarios = contexto.get('fvgs_diarios', [])
        direction = 'SHORT' if tendencia == 'BAJISTA' else 'LONG'
        
        for i in range(len(df_tf) - 1, max(len(df_tf) - 6, 0), -1):
            disp = self.detectar_displacement(df_tf, i, direction)
            if disp:
                resultado['hay_displacement'] = True
                resultado['displacement_info'] = disp
                break
        
        if not resultado['hay_displacement']:
            return resultado
        
        for i in range(max(1, len(df_tf) - 30), len(df_tf) - 1):
            fvg = self.detectar_fvg(df_tf, i, direction)
            if fvg:
                resultado['fvgs'].append(fvg)
        
        resultado['hay_fvg'] = len(resultado['fvgs']) > 0
        
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
            if tendencia == 'BAJISTA' and fvg['type'] == 'Bearish_FVG':
                tiene_fvg_alineado = True
                break
            if tendencia == 'ALCISTA' and fvg['type'] == 'Bullish_FVG':
                tiene_fvg_alineado = True
                break
        
        if self.modo_flexible:
            resultado['confirmado'] = (
                resultado['hay_displacement'] or
                (resultado['hay_fvg'] and resultado['hay_mss'])
            )
            if resultado['hay_displacement'] and resultado['hay_fvg']:
                resultado['tipo_entrada'] = f'CASO_A_{nombre_tf}'
            elif resultado['hay_fvg'] and resultado['hay_mss']:
                resultado['tipo_entrada'] = f'CASO_B_{nombre_tf}'
            elif resultado['hay_displacement']:
                resultado['tipo_entrada'] = f'SOLO_DISP_{nombre_tf}'
            elif resultado['hay_fvg']:
                resultado['tipo_entrada'] = f'SOLO_FVG_{nombre_tf}'
        else:
            resultado['confirmado'] = (
                resultado['hay_displacement'] and
                resultado['hay_fvg'] and
                tiene_fvg_alineado
            )
            
            if resultado['hay_displacement'] and resultado['hay_fvg'] and tiene_fvg_alineado:
                disp_info = resultado['displacement_info']
                if disp_info and disp_info['cuerpo_pct'] > self.displacement_pct * 100 * 1.5:
                    resultado['tipo_entrada'] = f'CASO_A_{nombre_tf}'
                else:
                    resultado['tipo_entrada'] = f'CASO_B_15M'
        
        return resultado

    def generar_señal_15m(self, catalizador: dict, df_15m: pd.DataFrame,
                          df_tf_sup: pd.DataFrame, contexto: dict) -> Optional[dict]:
        """
        Genera la señal de entrada en 15M basada en el catalizador confirmado.
        
        CASO A: Entrada directa si el displacement es extremo
        CASO B: Esperar retesteo + MSS + FVG 15M
        FLEXIBLE: Permite señales con menos requisitos
        """
        tendencia = contexto['tendencia']
        direction = 'SHORT' if tendencia == 'BAJISTA' else 'LONG'
        tipo_entrada = catalizador.get('tipo_entrada')
        
        fvgs = catalizador.get('fvgs', [])
        
        fvg_principal = None
        for fvg in fvgs:
            if tendencia == 'BAJISTA' and fvg['type'] == 'Bearish_FVG':
                fvg_principal = fvg
                break
            if tendencia == 'ALCISTA' and fvg['type'] == 'Bullish_FVG':
                fvg_principal = fvg
                break
        
        if not fvg_principal and self.modo_flexible:
            if fvgs:
                fvg_principal = fvgs[0]
        
        disp_info = catalizador.get('displacement_info', {})
        nivel_origen = disp_info.get('vela_low' if direction == 'LONG' else 'vela_high')
        
        if catalizador['hay_displacement'] and catalizador['hay_fvg']:
            return self._generar_entrada_directa(fvg_principal, df_tf_sup, direction, nivel_origen, catalizador['timeframe'])
        elif catalizador['hay_fvg'] and catalizador['hay_mss']:
            return self._generar_entrada_refinada(fvg_principal, df_15m, df_tf_sup, direction, nivel_origen, tendencia)
        elif catalizador['hay_displacement'] and self.modo_flexible:
            return self._generar_entrada_solo_displacement(df_tf_sup, direction, disp_info, catalizador['timeframe'])
        elif catalizador['hay_fvg'] and self.modo_flexible:
            return self._generar_entrada_solo_fvg(fvg_principal, df_tf_sup, direction, catalizador['timeframe'])
        
        return None

    def _generar_entrada_directa(self, fvg: dict, df_tf: pd.DataFrame, direction: str,
                                  nivel_origen: float, timeframe: str) -> Optional[dict]:
        """CASO A: Entrada directa al confirmar FVG en timeframe superior.
        
        SL: Donde nace el movimiento original (origen de la ruptura)
        TP: Objetivos de liquidez (low/high relevante de estructura)
        """
        if len(df_tf) < 5:
            return None
        
        idx_fvg = fvg['idx']
        if idx_fvg >= len(df_tf) - 1:
            return None
        
        vela_confirmacion = df_tf.iloc[idx_fvg + 1] if idx_fvg + 1 < len(df_tf) else None
        if vela_confirmacion is None:
            return None
        
        entrada = float(vela_confirmacion['close'])
        fecha_entrada = df_tf.index[idx_fvg + 1]
        
        if direction == 'SHORT':
            if nivel_origen and nivel_origen > entrada:
                sl = nivel_origen * 1.001
            else:
                sl = float(df_tf['high'].iloc[idx_fvg:idx_fvg+3].max()) * 1.001
            
            bajos_relevantes = df_tf['low'].iloc[max(0, idx_fvg-20):idx_fvg].values
            if len(bajos_relevantes) > 0:
                percentil_10 = np.percentile(bajos_relevantes, 10)
                tp = float(percentil_10)
            else:
                tp = entrada * 0.99
        else:
            if nivel_origen and nivel_origen < entrada:
                sl = nivel_origen * 0.999
            else:
                sl = float(df_tf['low'].iloc[idx_fvg:idx_fvg+3].min()) * 0.999
            
            altos_relevantes = df_tf['high'].iloc[max(0, idx_fvg-20):idx_fvg].values
            if len(altos_relevantes) > 0:
                percentil_90 = np.percentile(altos_relevantes, 90)
                tp = float(percentil_90)
            else:
                tp = entrada * 1.01
        
        riesgo = abs(entrada - sl)
        if riesgo == 0:
            return None
        
        rr_real = abs(tp - entrada) / riesgo if riesgo > 0 else 0
        
        return {
            'tipo_entrada': f'CASO_A_{timeframe}',
            'direccion': 'LARGO' if direction == 'LONG' else 'CORTO',
            'entrada': round(entrada, 5),
            'stop_loss': round(sl, 5),
            'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * 10000, 1),
            'rr_ratio': round(rr_real, 2),
            'fvg': fvg,
            'nivel_origen': nivel_origen,
            'timeframe_entrada': '15M',
            'timeframe_confirmacion': timeframe,
            'fecha_entrada': str(fecha_entrada),
            'confianza': 70
        }

    def _generar_entrada_refinada(self, fvg: dict, df_15m: pd.DataFrame,
                                  df_tf_sup: pd.DataFrame, direction: str,
                                  nivel_origen: float, tendencia: str) -> Optional[dict]:
        """
        CASO B: Entrada refinada en 15M.
        - Esperar retesteo del FVG del timeframe superior
        - Buscar MSS + FVG 15M
        
        SL: Donde nace el movimiento original (origen de la ruptura)
        TP: Objetivos de liquidez de temporalidad mayor (4H/Diario)
        """
        if len(df_15m) < 10:
            return None
        
        fvg_start = fvg['start']
        fvg_end = fvg['end']
        zona_min = min(fvg_start, fvg_end)
        zona_max = max(fvg_start, fvg_end)
        
        hay_retesteo = False
        for i in range(-1, -min(30, len(df_15m)), -1):
            low_vela = float(df_15m['low'].iloc[i])
            high_vela = float(df_15m['high'].iloc[i])
            
            if zona_min <= low_vela <= zona_max or zona_min <= high_vela <= zona_max:
                hay_retesteo = True
                break
        
        if not hay_retesteo:
            logger.info(f"[Patron4H] Sin retesteo del FVG {fvg['type']} - esperando")
            return None
        
        hay_mss = self.detectar_mss(df_15m, direction)
        
        fvg_15m = None
        for i in range(max(1, len(df_15m) - 20), len(df_15m) - 1):
            fvg_15m = self.detectar_fvg(df_15m, i, direction)
            if fvg_15m:
                break
        
        if fvg_15m is None:
            logger.info("[Patron4H] Sin FVG 15M - No hay dirección ni intencionalidad")
            return None
        
        if not hay_mss:
            logger.info("[Patron4H] Sin MSS 15M - estructura no confirmada")
            return None
        
        entrada = float(fvg_15m['mid'])
        
        if direction == 'SHORT':
            if nivel_origen and nivel_origen > entrada:
                sl = nivel_origen * 1.001
            else:
                idx_fvg = fvg.get('idx', len(df_tf_sup) - 5)
                sl = float(df_tf_sup['high'].iloc[max(0, idx_fvg-2):idx_fvg+3].max()) * 1.001
            
            objetivos_bajos = df_tf_sup['low'].iloc[max(0, len(df_tf_sup)-20):len(df_tf_sup)].nsmallest(3).values
            tp = float(min(objetivos_bajos)) if len(objetivos_bajos) > 0 else entrada * 0.99
        else:
            if nivel_origen and nivel_origen < entrada:
                sl = nivel_origen * 0.999
            else:
                idx_fvg = fvg.get('idx', len(df_tf_sup) - 5)
                sl = float(df_tf_sup['low'].iloc[max(0, idx_fvg-2):idx_fvg+3].min()) * 0.999
            
            objetivos_altos = df_tf_sup['high'].iloc[max(0, len(df_tf_sup)-20):len(df_tf_sup)].nlargest(3).values
            tp = float(max(objetivos_altos)) if len(objetivos_altos) > 0 else entrada * 1.01
        
        riesgo = abs(entrada - sl)
        if riesgo == 0:
            return None
        
        rr_real = abs(tp - entrada) / riesgo if riesgo > 0 else 0
        
        if rr_real < 0.5:
            logger.info(f"[Patron4H] R:R = {rr_real:.2f} muy bajo - ajustando")
            rr_real = 1.0
        
        return {
            'tipo_entrada': 'CASO_B_15M',
            'direccion': 'LARGO' if direction == 'LONG' else 'CORTO',
            'entrada': round(entrada, 5),
            'stop_loss': round(sl, 5),
            'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * 10000, 1),
            'rr_ratio': round(rr_real, 2),
            'fvg_sup': fvg,
            'fvg_15m': fvg_15m,
            'nivel_origen': nivel_origen,
            'hay_retesteo': hay_retesteo,
            'hay_mss': hay_mss,
            'timeframe_entrada': '15M',
            'timeframe_confirmacion': '4H/D',
            'confianza': 85
        }

    def _generar_entrada_solo_displacement(self, df_tf: pd.DataFrame, direction: str,
                                           disp_info: dict, timeframe: str) -> Optional[dict]:
        """Entrada solo con displacement (modo flexible)."""
        if len(df_tf) < 5:
            return None
        
        idx = disp_info.get('idx', -1)
        if idx < 0 or idx >= len(df_tf) - 1:
            idx = len(df_tf) - 2
        
        vela_actual = df_tf.iloc[idx]
        entrada = float(vela_actual['close'])
        
        sl_porcentaje = 0.003
        
        if direction == 'SHORT':
            sl = entrada * (1 + sl_porcentaje)
            tp = entrada * (1 - sl_porcentaje * self.rr_ratio_min)
        else:
            sl = entrada * (1 - sl_porcentaje)
            tp = entrada * (1 + sl_porcentaje * self.rr_ratio_min)
        
        riesgo = abs(entrada - sl)
        if riesgo == 0:
            return None
        
        return {
            'tipo_entrada': f'SOLO_DISP_{timeframe}',
            'direccion': 'LARGO' if direction == 'LONG' else 'CORTO',
            'entrada': round(entrada, 5),
            'stop_loss': round(sl, 5),
            'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * 10000, 1),
            'rr_ratio': round(abs(tp - entrada) / riesgo, 2),
            'displacement_info': disp_info,
            'nivel_origen': disp_info.get('vela_high' if direction == 'SHORT' else 'vela_low'),
            'timeframe_entrada': '15M',
            'timeframe_confirmacion': timeframe,
            'confianza': 50
        }

    def _generar_entrada_solo_fvg(self, fvg: dict, df_tf: pd.DataFrame, direction: str,
                                    timeframe: str) -> Optional[dict]:
        """Entrada solo con FVG (modo flexible)."""
        if len(df_tf) < 5:
            return None
        
        idx = fvg.get('idx', -1)
        if idx < 0 or idx >= len(df_tf) - 1:
            idx = len(df_tf) - 2
        
        vela_actual = df_tf.iloc[idx]
        entrada = float(vela_actual['close'])
        
        sl_porcentaje = 0.003
        
        if direction == 'SHORT':
            sl = entrada * (1 + sl_porcentaje)
            tp = entrada * (1 - sl_porcentaje * self.rr_ratio_min)
        else:
            sl = entrada * (1 - sl_porcentaje)
            tp = entrada * (1 + sl_porcentaje * self.rr_ratio_min)
        
        riesgo = abs(entrada - sl)
        if riesgo == 0:
            return None
        
        return {
            'tipo_entrada': f'SOLO_FVG_{timeframe}',
            'direccion': 'LARGO' if direction == 'LONG' else 'CORTO',
            'entrada': round(entrada, 5),
            'stop_loss': round(sl, 5),
            'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * 10000, 1),
            'rr_ratio': round(abs(tp - entrada) / riesgo, 2),
            'fvg': fvg,
            'timeframe_entrada': '15M',
            'timeframe_confirmacion': timeframe,
            'confianza': 40
        }

    def ejecutar_ciclo(self, datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> dict:
        """
        Ejecuta un ciclo de análisis según el estado actual del contador.
        Rota entre: 4H → 1H → 15M → 4H → ...
        
        Retorna el estado del ciclo ejecutado.
        """
        symbol = symbolInfo['symbol']
        
        df_15m = datos.get('15m')
        df_1h = datos.get('1h')
        df_4h = datos.get('4h')
        df_1d = datos.get('1d')
        
        contexto = self.estado_patron['contexto']
        liquidity_raid = self.estado_patron['liquidity_raid']
        
        if contexto is None:
            contexto = self.obtener_contexto_diario(df_1d)
            self.estado_patron['contexto'] = contexto
            logger.info(f"[Patron4H] Contexto diario: {contexto['tendencia']}, Max: {contexto['max_dia_anterior']}, Min: {contexto['min_dia_anterior']}")
        
        if contexto['tendencia'] == 'LATERAL':
            self._avanzar_ciclo()
            return {'status': 'TENDENCIA_LATERAL', 'ciclo': self.ciclo_actual}
        
        if liquidity_raid is None:
            precio_actual = float(df_15m['close'].iloc[-1])
            liquidity_raid = self.detectar_liquidity_raid(
                precio_actual,
                contexto['max_dia_anterior'],
                contexto['min_dia_anterior'],
                contexto['tendencia']
            )
            self.estado_patron['liquidity_raid'] = liquidity_raid
            if liquidity_raid:
                logger.info(f"[Patron4H] Liquidity Raid: {liquidity_raid['tipo']} en {liquidity_raid['nivel']}")
        
        resultado = {'status': 'EN_PROCESO', 'ciclo': self.ciclo_actual, 'timeframe': None}
        
        if self.ciclo_actual == self.CICLO_4H:
            logger.info(f"[Patron4H] >>> CICLO 1/3: Análisis en 4H")
            
            catalizador_4h = self.analizar_catalizador(df_4h, contexto, liquidity_raid, '4H')
            self.estado_patron['fase2_4h'] = catalizador_4h
            
            logger.info(f"[Patron4H] 4H - Displacement: {catalizador_4h['hay_displacement']}, "
                       f"FVG: {catalizador_4h['hay_fvg']}, MSS: {catalizador_4h['hay_mss']}, "
                       f"Reacción POI: {catalizador_4h['hay_reaccion_poi']}")
            
            if catalizador_4h['confirmado']:
                self.estado_patron['ciclo_confirmado'] = '4H'
                logger.info(f"[Patron4H] ✓ Catalizador 4H confirmado!")
                resultado['status'] = 'CATALIZADOR_CONFIRMADO'
                resultado['timeframe'] = '4H'
            else:
                logger.info(f"[Patron4H] 4H - Sin catalizador válido, avanzando...")
            
            self._avanzar_ciclo()
            
        elif self.ciclo_actual == self.CICLO_1H:
            logger.info(f"[Patron4H] >>> CICLO 2/3: Análisis en 1H")
            
            catalizador_1h = self.analizar_catalizador(df_1h, contexto, liquidity_raid, '1H')
            self.estado_patron['fase2_1h'] = catalizador_1h
            
            logger.info(f"[Patron4H] 1H - Displacement: {catalizador_1h['hay_displacement']}, "
                       f"FVG: {catalizador_1h['hay_fvg']}, MSS: {catalizador_1h['hay_mss']}")
            
            if catalizador_1h['confirmado']:
                self.estado_patron['ciclo_confirmado'] = '1H'
                logger.info(f"[Patron4H] ✓ Catalizador 1H confirmado!")
                resultado['status'] = 'CATALIZADOR_CONFIRMADO'
                resultado['timeframe'] = '1H'
            else:
                logger.info(f"[Patron4H] 1H - Sin catalizador válido")
            
            self._avanzar_ciclo()
            
        else:
            logger.info(f"[Patron4H] >>> CICLO 3/3: Búsqueda de entrada en 15M")
            
            catalizador_final = None
            df_tf_sup = None
            
            if self.estado_patron['ciclo_confirmado'] == '4H':
                catalizador_final = self.estado_patron['fase2_4h']
                df_tf_sup = df_4h
                logger.info(f"[Patron4H] Usando catalizador 4H para entrada")
            elif self.estado_patron['ciclo_confirmado'] == '1H':
                catalizador_final = self.estado_patron['fase2_1h']
                df_tf_sup = df_1h
                logger.info(f"[Patron4H] Usando catalizador 1H para entrada")
            else:
                catalizador_15m = self.analizar_catalizador(df_15m, contexto, liquidity_raid, '15M')
                catalizador_final = catalizador_15m
                df_tf_sup = df_15m
                logger.info(f"[Patron4H] Sin catalizador HTF, usando 15M directo")
            
            if catalizador_final and catalizador_final['hay_displacement']:
                señal = self.generar_señal_15m(catalizador_final, df_15m, df_tf_sup, contexto)
                
                if señal:
                    self.estado_patron['señal'] = señal
                    self.estado_patron['ciclo_confirmado'] = '15M_ENTRADA'
                    logger.info(f"[Patron4H] ✓✓✓ SEÑAL GENERADA: {señal['direccion']} | "
                               f"Entry: {señal['entrada']} | SL: {señal['stop_loss']} | "
                               f"TP: {señal['take_profit']} | R:R: {señal['rr_ratio']}")
                    resultado['status'] = 'SENAL_GENERADA'
                    resultado['señal'] = señal
                else:
                    logger.info(f"[Patron4H] Sin entrada válida en 15M - reseteando")
                    self._resetear_estado()
            else:
                logger.info(f"[Patron4H] Sin displacement confirmado en 15M")
                self._resetear_estado()
            
            self._avanzar_ciclo()
        
        return resultado

    def _avanzar_ciclo(self):
        """Avanza al siguiente ciclo."""
        self.ciclo_actual = (self.ciclo_actual + 1) % 3

    def _resetear_estado(self):
        """Resetea el estado del patrón para comenzar un nuevo análisis."""
        self.estado_patron = {
            'contexto': None,
            'liquidity_raid': None,
            'fase2_4h': None,
            'fase2_1h': None,
            'señal': None,
            'ciclo_confirmado': None
        }
        self.ciclo_actual = self.CICLO_4H

    def obtener_señal_pendiente(self) -> Optional[dict]:
        """Retorna la señal pendiente si existe."""
        return self.estado_patron.get('señal')

    async def _executeTrades(self, signal: Dict, symbolInfo: Dict):
        """Ejecuta trades para todas las cuentas habilitadas."""
        if not signal:
            return

        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                logger.warning("[Patron4H] No hay cuentas disponibles")
                return

        for account in self.accounts:
            if not dbManager.isEstrategiaHabilitadaParaCuenta(account['idCuenta'], 'Patron4h'):
                logger.info(f"[Patron4H] Estrategia deshabilitada para cuenta {account['idCuenta']}")
                continue
            
            posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=abs(signal['entrada'] - signal['stop_loss']),
                symbolInfo=symbolInfo,
                entryPrice=signal.get('entrada')
            )
            
            direction = signal['direction']
            entryPrice = signal['entrada']
            slDist = abs(entryPrice - signal['stop_loss'])
            
            slPrice = signal['stop_loss']
            tpPrice = signal['take_profit']
            
            if posSize is None or posSize == 0:
                marginUsed = 0
                logger.warning(f"[{account['idCuenta']}] Trade no ejecutado: size=0")
                continue
            
            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbolInfo['symbol'],
                "direction": direction,
                "entryPrice": entryPrice,
                "openTime": self.getMexicoTime().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": slPrice,
                "takeProfit": tpPrice,
                "size": posSize,
                "intervalo": "15min",
                "status": "OPEN",
                "strategy": "Patron4h",
                "margin_used": marginUsed,
            }
            
            if account['idCuenta'] != 1:
                dbManager.buscaTrade(trade)
                
                message = self._formatAlertMessage(signal, trade)
                msgId = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                if msgId:
                    self.lastMessageIds[symbolInfo['symbol']] = msgId
                
                logger.info(f"✅ Alerta Patron4H enviada para {symbolInfo['symbol']} a cuenta {account['idCuenta']} | Size: {posSize}")
        
        self.signal_enviada = True
        self._resetear_estado()

    def _formatAlertMessage(self, signal: Dict, trade: Dict) -> str:
        """Formatea el mensaje de alerta para Telegram."""
        direction = signal['direction']
        directionStr = "COMPRA" if direction == "LARGO" else "VENTA"
        colorHeader = "🟩" if direction == "LARGO" else "🟥"
        
        close = signal['entryPrice']
        tp = trade['takeProfit']
        sl = signal['stop_loss']
        confianza = signal.get('confianza', 0)
        rr = signal.get('rr_ratio', 0)
        riesgo = signal.get('riesgo_pips', 0)
        tipo_entrada = signal.get('tipo_entrada', 'UNKNOWN')
        tf_entrada = signal.get('timeframe_entrada', '15M')
        tf_confirm = signal.get('timeframe_confirmacion', signal.get('timeframe_entrada', '15M'))
        
        text = (
            f"{colorHeader*3} <b>SEÑAL DE {directionStr}</b> {colorHeader*3}\n"
            f"<center><i>Estrategia: PATRÓN 4H</i></center>\n"
            f"<center><b>{trade['symbol']}</b> ({tf_entrada})</center>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Confianza: <b>{confianza:.0f}%</b></center>\n"
            f"<center>Tipo: <b>{tipo_entrada}</b></center>\n"
            f"<center>Confirmado en: <b>{tf_confirm}</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔹 ENTRADA:   <b>{close:,.5f}</b>\n"
            f"🔴 STOP LOSS: <b>{sl:,.5f}</b>\n"
            f"🟢 TAKE PROFIT: <b>{tp:,.5f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center><b>GESTIÓN:</b></center>\n"
            f"• Riesgo: <b>{riesgo:.1f} pips</b>\n"
            f"• R:R: <b>{rr:.2f}</b>\n"
            f"• Cantidad: <b>{trade['size']:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<i>⚠️ Sin BE prematuro</i>\n"
            f"<i>🎯 Salida por estructura</i>\n"
        )
        return text

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None):
        """Ciclo principal de análisis - ejecuta un ciclo de la secuencia 4H → 1H → 15M."""
        logger.info(f"[Patron4H] ===== INICIANDO CICLO PATRÓN 4H ({self.ciclo_actual + 1}/3) =====")
        
        symbol = symbolInfo['symbol']
        
        df_15m = preloadedData.get('15m') if preloadedData else None
        if df_15m is None or len(df_15m) < 100:
            logger.warning(f"[Patron4H] Datos 15M insuficientes para {symbol}")
            return
        
        df_1h = self.resample_ohlcv(df_15m, '1H')
        df_4h = self.resample_ohlcv(df_15m, '4H')
        df_1d = self.resample_ohlcv(df_15m, '1D')
        
        datos = {
            '15m': df_15m,
            '1h': df_1h,
            '4h': df_4h,
            '1d': df_1d
        }
        
        logger.info(f"[Patron4H] Velas: 15M={len(df_15m)}, 1H={len(df_1h)}, 4H={len(df_4h)}, 1D={len(df_1d)}")
        
        if self.signalGenerada and self.timestamp_signal:
            ahora = self.getMexicoTime().replace(tzinfo=None)
            minutos_desde = (ahora - self.timestamp_signal).total_seconds() / 60
            if minutos_desde > self.max_minutos_fvg:
                self.signalGenerada = False
                self.signal_enviada = False
                self._resetear_estado()
            else:
                logger.info(f"[Patron4H] Señal ya generada hace {minutos_desde:.1f} min")
                return
        
        resultado = self.ejecutar_ciclo(datos, symbolInfo)
        
        if resultado['status'] == 'SENAL_GENERADA' and resultado.get('señal'):
            señal = resultado['señal']
            
            self.signalGenerada = True
            self.timestamp_signal = self.getMexicoTime().replace(tzinfo=None)
            
            signal_telegram = {
                **señal,
                "strategy": "Patron4h",
                "confidence": señal['confianza'],
                "entryPrice": señal['entrada'],
                "slDistance": abs(señal['entrada'] - señal['stop_loss']),
                "stopLoss": señal['stop_loss'],
                "takeProfit": señal['take_profit'],
                "symbolInfo": symbolInfo
            }
            
            await self._executeTrades(signal_telegram, symbolInfo)


def executePatron4H(datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> Optional[Dict]:
    """
    Función de ejecución directa (para backtesting o llamadas síncronas).
    Ejecuta un ciclo completo y retorna el resultado.
    """
    bot = Patron4HBot()
    
    df_15m = datos.get('15m')
    df_1h = datos.get('1h')
    df_4h = datos.get('4h')
    df_1d = datos.get('1d')
    
    if df_15m is None or df_4h is None or df_1d is None:
        return None
    
    datos_completos = {
        '15m': df_15m,
        '1h': df_1h if df_1h is not None else bot.resample_ohlcv(df_15m, '1H'),
        '4h': df_4h if df_4h is not None else bot.resample_ohlcv(df_15m, '4H'),
        '1d': df_1d if df_1d is not None else bot.resample_ohlcv(df_15m, '1D')
    }
    
    resultado = {'ciclos': []}
    
    for i in range(3):
        ciclo_resultado = bot.ejecutar_ciclo(datos_completos, symbolInfo)
        resultado['ciclos'].append(ciclo_resultado)
        
        if ciclo_resultado['status'] == 'SENAL_GENERADA':
            resultado['señal'] = ciclo_resultado.get('señal')
            break
        
        if ciclo_resultado['status'] in ['TENDENCIA_LATERAL', 'EN_PROCESO']:
            continue
    
    resultado['estado_final'] = bot.estado_patron
    
    return resultado


if __name__ == "__main__":
    print("Patron4H Strategy Module - Sistema de ciclos 4H → 1H → 15M")
    print("Usa: executePatron4H(datos, symbolInfo)")
