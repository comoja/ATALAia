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
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}
        
        strategyConfig = dbManager.getStrategyConfig("Patron4h")
        self.fvg_min_pct = strategyConfig.get('fvg_min_pct', 0.00005) if strategyConfig else 0.00005
        self.displacement_pct = strategyConfig.get('displacement_pct', 0.0005) if strategyConfig else 0.0005
        self.rr_ratio_min = strategyConfig.get('rr_ratio_min', 1.5) if strategyConfig else 1.5
        self.max_minutos_fvg = strategyConfig.get('max_minutos_fvg', 240) if strategyConfig else 240
        
        self.modo_flexible = True
        self.signalGenerada = False
        self.timestamp_signal = None
        
        logger.info("[Patron4H] Bot iniciado con sistema Top-Down (1D -> 4H -> 1H -> 15M)")

    def getMexicoTime(self) -> datetime:
        return datetime.now(self.MEXICO_TZ)

    def resample_ohlcv(self, df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
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

    def analizar_catalizador(self, df_tf: pd.DataFrame, contexto: dict, 
                           liquidity_raid: Optional[dict], nombre_tf: str) -> dict:
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
        nivel_origen = disp_info.get('vela_low' if direction == 'LONG' else 'vela_high')
        
        if catalizador['hay_displacement'] and catalizador['hay_fvg']:
            return self._generar_entrada_directa(fvg_principal, df_tf_sup, df_15m, direction, nivel_origen, catalizador['timeframe'])
        elif catalizador['hay_fvg'] and catalizador['hay_mss']:
            return self._generar_entrada_refinada(fvg_principal, df_15m, df_tf_sup, direction, nivel_origen, tendencia)
        elif catalizador['hay_displacement'] and self.modo_flexible:
            return self._generar_entrada_solo_displacement(df_tf_sup, df_15m, direction, disp_info, catalizador['timeframe'])
        elif catalizador['hay_fvg'] and self.modo_flexible:
            return self._generar_entrada_solo_fvg(fvg_principal, df_tf_sup, df_15m, direction, catalizador['timeframe'])
        return None

    def _generar_entrada_directa(self, fvg: dict, df_tf: pd.DataFrame, df_15m: pd.DataFrame, direction: str, nivel_origen: float, timeframe: str) -> Optional[dict]:
        if len(df_tf) < 5:
            return None
        idx_fvg = fvg['idx']
        if idx_fvg >= len(df_tf) - 1:
            return None
        vela_confirmacion = df_tf.iloc[idx_fvg + 1]
        entrada = float(vela_confirmacion['close'])
        padding = self._get_atr_padding(df_15m, multiplier=0.5)
        
        if direction == 'SHORT':
            sl = (nivel_origen + padding) if (nivel_origen and nivel_origen > entrada) else float(df_tf['high'].iloc[idx_fvg:idx_fvg+3].max()) + padding
            bajos_relevantes = df_tf['low'].iloc[max(0, idx_fvg-20):idx_fvg].values
            tp = float(np.percentile(bajos_relevantes, 10)) if len(bajos_relevantes) > 0 else entrada * 0.99
        else:
            sl = (nivel_origen - padding) if (nivel_origen and nivel_origen < entrada) else float(df_tf['low'].iloc[idx_fvg:idx_fvg+3].min()) - padding
            altos_relevantes = df_tf['high'].iloc[max(0, idx_fvg-20):idx_fvg].values
            tp = float(np.percentile(altos_relevantes, 90)) if len(altos_relevantes) > 0 else entrada * 1.01
        
        riesgo = abs(entrada - sl)
        if riesgo == 0: return None
        return {
            'tipo_entrada': f'CASO_A_{timeframe}', 'direccion': 'LARGO' if direction == 'LONG' else 'CORTO',
            'entrada': round(entrada, 5), 'stop_loss': round(sl, 5), 'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * 10000, 1), 'rr_ratio': round(abs(tp - entrada) / riesgo, 2),
            'timeframe_entrada': '15M', 'timeframe_confirmacion': timeframe, 'confianza': 70
        }

    def _generar_entrada_refinada(self, fvg: dict, df_15m: pd.DataFrame, df_tf_sup: pd.DataFrame, direction: str, nivel_origen: float, tendencia: str) -> Optional[dict]:
        if len(df_15m) < 10: return None
        zona_min = min(fvg['start'], fvg['end'])
        zona_max = max(fvg['start'], fvg['end'])
        
        hay_retesteo = any(zona_min <= df_15m['low'].iloc[i] <= zona_max or zona_min <= df_15m['high'].iloc[i] <= zona_max for i in range(-1, -min(30, len(df_15m)), -1))
        if not hay_retesteo: return None
        
        hay_mss = self.detectar_mss(df_15m, direction)
        fvg_15m = next((self.detectar_fvg(df_15m, i, direction) for i in range(max(1, len(df_15m) - 20), len(df_15m) - 1) if self.detectar_fvg(df_15m, i, direction)), None)
        if not fvg_15m or not hay_mss: return None
        
        entrada = float(fvg_15m['mid'])
        padding = self._get_atr_padding(df_15m, multiplier=0.5)
        
        if direction == 'SHORT':
            idx_fvg = fvg.get('idx', len(df_tf_sup) - 5)
            sl = (nivel_origen + padding) if (nivel_origen and nivel_origen > entrada) else float(df_tf_sup['high'].iloc[max(0, idx_fvg-2):idx_fvg+3].max()) + padding
            objetivos_bajos = df_tf_sup['low'].iloc[max(0, len(df_tf_sup)-20):].nsmallest(3).values
            tp = float(min(objetivos_bajos)) if len(objetivos_bajos) > 0 else entrada * 0.99
        else:
            idx_fvg = fvg.get('idx', len(df_tf_sup) - 5)
            sl = (nivel_origen - padding) if (nivel_origen and nivel_origen < entrada) else float(df_tf_sup['low'].iloc[max(0, idx_fvg-2):idx_fvg+3].min()) - padding
            objetivos_altos = df_tf_sup['high'].iloc[max(0, len(df_tf_sup)-20):].nlargest(3).values
            tp = float(max(objetivos_altos)) if len(objetivos_altos) > 0 else entrada * 1.01
            
        riesgo = abs(entrada - sl)
        if riesgo == 0: return None
        rr_real = abs(tp - entrada) / riesgo if riesgo > 0 else 0
        return {
            'tipo_entrada': 'CASO_B_15M', 'direccion': 'LARGO' if direction == 'LONG' else 'CORTO',
            'entrada': round(entrada, 5), 'stop_loss': round(sl, 5), 'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * 10000, 1), 'rr_ratio': round(rr_real if rr_real >= 0.5 else 1.0, 2),
            'timeframe_entrada': '15M', 'timeframe_confirmacion': '4H/D', 'confianza': 85
        }

    def _generar_entrada_solo_displacement(self, df_tf: pd.DataFrame, df_15m: pd.DataFrame, direction: str, disp_info: dict, timeframe: str) -> Optional[dict]:
        if len(df_tf) < 5: return None
        idx = min(max(0, disp_info.get('idx', -1)), len(df_tf) - 2)
        entrada = float(df_tf['close'].iloc[idx])
        padding = self._get_atr_padding(df_15m, multiplier=0.8)
        
        if direction == 'SHORT':
            sl = entrada + padding
            tp = entrada - (padding * self.rr_ratio_min)
        else:
            sl = entrada - padding
            tp = entrada + (padding * self.rr_ratio_min)
            
        riesgo = abs(entrada - sl)
        if riesgo == 0: return None
        return {
            'tipo_entrada': f'SOLO_DISP_{timeframe}', 'direccion': 'LARGO' if direction == 'LONG' else 'CORTO',
            'entrada': round(entrada, 5), 'stop_loss': round(sl, 5), 'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * 10000, 1), 'rr_ratio': round(abs(tp - entrada) / riesgo, 2),
            'timeframe_entrada': '15M', 'timeframe_confirmacion': timeframe, 'confianza': 50
        }

    def _generar_entrada_solo_fvg(self, fvg: dict, df_tf: pd.DataFrame, df_15m: pd.DataFrame, direction: str, timeframe: str) -> Optional[dict]:
        if len(df_tf) < 5: return None
        idx = min(max(0, fvg.get('idx', -1)), len(df_tf) - 2)
        entrada = float(df_tf['close'].iloc[idx])
        padding = self._get_atr_padding(df_15m, multiplier=0.8)
        
        if direction == 'SHORT':
            sl = entrada + padding
            tp = entrada - (padding * self.rr_ratio_min)
        else:
            sl = entrada - padding
            tp = entrada + (padding * self.rr_ratio_min)
            
        riesgo = abs(entrada - sl)
        if riesgo == 0: return None
        return {
            'tipo_entrada': f'SOLO_FVG_{timeframe}', 'direccion': 'LARGO' if direction == 'LONG' else 'CORTO',
            'entrada': round(entrada, 5), 'stop_loss': round(sl, 5), 'take_profit': round(tp, 5),
            'riesgo_pips': round(riesgo * 10000, 1), 'rr_ratio': round(abs(tp - entrada) / riesgo, 2),
            'timeframe_entrada': '15M', 'timeframe_confirmacion': timeframe, 'confianza': 40
        }

    def analizar_top_down(self, datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> dict:
        df_15m = datos.get('15m')
        df_1h = datos.get('1h')
        df_4h = datos.get('4h')
        df_1d = datos.get('1d')
        
        contexto = self.obtener_contexto_diario(df_1d)
        if contexto['tendencia'] == 'LATERAL':
            return {'status': 'TENDENCIA_LATERAL'}
            
        precio_actual = float(df_15m['close'].iloc[-1])
        liquidity_raid = self.detectar_liquidity_raid(precio_actual, contexto['max_dia_anterior'], contexto['min_dia_anterior'], contexto['tendencia'])
        
        catalizador_4h = self.analizar_catalizador(df_4h, contexto, liquidity_raid, '4H')
        catalizador_1h = self.analizar_catalizador(df_1h, contexto, liquidity_raid, '1H')
        
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
            señal = self.generar_señal_15m(catalizador_final, df_15m, df_tf_sup, contexto)
            if señal:
                return {'status': 'SENAL_GENERADA', 'señal': señal}
                
        return {'status': 'SIN_ENTRADA_VALIDA'}

    async def _executeTrades(self, signal: Dict, symbolInfo: Dict):
        if not signal:
            return

        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                return

        for account in self.accounts:
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
            
            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbolInfo['symbol'],
                "direction": signal['direccion'],  # Original 'direction' from signal format mappings
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
            
            if account['idCuenta'] != 1:
                dbManager.buscaTrade(trade)
                message = self._formatAlertMessage(signal, trade)
                msgId = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                if msgId:
                    self.lastMessageIds[symbolInfo['symbol']] = msgId
        
        self.signalGenerada = True

    def _formatAlertMessage(self, signal: Dict, trade: Dict) -> str:
        directionStr = "COMPRA" if signal['direccion'] == "LARGO" else "VENTA"
        colorHeader = "🟩" if signal['direccion'] == "LARGO" else "🟥"
        return (
            f"{colorHeader*3} <b>SEÑAL DE {directionStr}</b> {colorHeader*3}\n"
            f"<center><i>Estrategia: PATRÓN 4H</i></center>\n"
            f"<center><b>{trade['symbol']}</b> ({signal.get('timeframe_entrada', '15M')})</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔹 ENTRADA: <b>{signal['entrada']:,.5f}</b>\n"
            f"🔴 STOP LOSS: <b>{signal['stop_loss']:,.5f}</b>\n"
            f"🟢 TAKE PROFIT: <b>{signal['take_profit']:,.5f}</b>\n"
        )

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None):
        symbol = symbolInfo['symbol']
        df_15m = preloadedData.get('15m') if preloadedData else None
        if df_15m is None or len(df_15m) < 100:
            return
        
        df_1h = self.resample_ohlcv(df_15m, '1H')
        df_4h = self.resample_ohlcv(df_15m, '4H')
        df_1d = self.resample_ohlcv(df_15m, '1D')
        
        datos = {'15m': df_15m, '1h': df_1h, '4h': df_4h, '1d': df_1d}
        
        if self.signalGenerada and self.timestamp_signal:
            ahora = self.getMexicoTime().replace(tzinfo=None)
            minutos_desde = (ahora - self.timestamp_signal).total_seconds() / 60
            if minutos_desde > self.max_minutos_fvg:
                self.signalGenerada = False
            else:
                return
        
        resultado = self.analizar_top_down(datos, symbolInfo)
        
        if resultado['status'] == 'SENAL_GENERADA' and resultado.get('señal'):
            señal = resultado['señal']
            self.timestamp_signal = self.getMexicoTime().replace(tzinfo=None)
            
            signal_telegram = {
                **señal,
                "strategy": "Patron4h",
                "direction": señal['direccion']
            }
            await self._executeTrades(signal_telegram, symbolInfo)

def executePatron4H(datos: Dict[str, pd.DataFrame], symbolInfo: Dict) -> Optional[Dict]:
    bot = Patron4HBot()
    df_15m = datos.get('15m')
    if df_15m is None: return None
    datos_completos = {
        '15m': df_15m,
        '1h': datos.get('1h') if datos.get('1h') is not None else bot.resample_ohlcv(df_15m, '1H'),
        '4h': datos.get('4h') if datos.get('4h') is not None else bot.resample_ohlcv(df_15m, '4H'),
        '1d': datos.get('1d') if datos.get('1d') is not None else bot.resample_ohlcv(df_15m, '1D')
    }
    return bot.analizar_top_down(datos_completos, symbolInfo)

if __name__ == "__main__":
    print("Patron4H Strategy Module - Top Down Synchronous Analysis")
