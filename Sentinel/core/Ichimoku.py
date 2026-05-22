import logging
from datetime import datetime, time
from zoneinfo import ZoneInfo
from typing import Dict, Optional
import pandas as pd
import numpy as np
import talib as ta
import pytz

from middleware.database import dbManager
from Sentinel.analysis import technical
from Sentinel.analysis import risk
from middleware.utils.alertBuilder import adjustTPForMinRR, getPipMultiplier, calculateBEPrice
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class IchimokuBot:
    """
    Estrategia Ichimoku solicitada para Sentinel.
    - Indicadores: Ichimoku (9, 26, 52), Bollinger Bands (20, 2).
    - Filtro de Sesión: Horario configurable (default 08:00 a 17:00 UTC).
    - Entradas: Precio respecto al Kumo, Tenkan-sen vs Kijun-sen, precio cerrando
      sobre/debajo de BB media, y BB expandiéndose.
    - Salidas: Stop Loss en Kijun-sen, Take Profit RR 1:2.
    - Emergencia: Cierre de posición si el precio cruza la BB media en contra (gestionable por el tracker).
    """
    
    def __init__(self):
        self.strategy_name = "Ichimoku"
        self._signals_sent = {}
        
        # Obtener parametros de la base de datos si existen, si no se usan defaultso
        strategyConfig = dbManager.getStrategyConfig("Ichimoku") or {}
        self.start_hour_utc = strategyConfig.get("start_hour_utc", 7)
        self.end_hour_utc = strategyConfig.get("end_hour_utc", 22)
        self.min_confidence = strategyConfig.get("min_confidence", 75)
        logger.info("IchimokuBot iniciado — Horario UTC %02d:00 a %02d:00", self.start_hour_utc, self.end_hour_utc)

    def _calc_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        
        # Bandas de Bollinger (20, 2)
        df['bb_upper'], df['bb_middle'], df['bb_lower'] = ta.BBANDS(
            df['close'].values, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0
        )
        
        # Ichimoku (9, 26, 52)
        # Tenkan-sen (9)
        high_9 = df['high'].rolling(window=9).max()
        low_9 = df['low'].rolling(window=9).min()
        df['tenkan_sen'] = (high_9 + low_9) / 2
        
        # Kijun-sen (26)
        high_26 = df['high'].rolling(window=26).max()
        low_26 = df['low'].rolling(window=26).min()
        df['kijun_sen'] = (high_26 + low_26) / 2
        
        # Senkou Span A (Shifted 26 periods ahead)
        # En pandas, para comparar con el precio actual, traemos el Kumo que se generó hace 26 periodos.
        df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(26)
        
        # Senkou Span B (52)
        high_52 = df['high'].rolling(window=52).max()
        low_52 = df['low'].rolling(window=52).min()
        df['senkou_span_b'] = ((high_52 + low_52) / 2).shift(26)
        
        return df

    def _check_time_filter(self, candle_time: datetime) -> bool:
        # Asumimos que la vela viene con el timezone local del sistema configurado en TIMEZONE
        # O si es naive, le asignamos el de la configuración local
        if candle_time.tzinfo is None:
            candle_time = pytz.timezone(TIMEZONE).localize(candle_time)
            
        utc_time = candle_time.astimezone(pytz.utc)
        return self.start_hour_utc <= utc_time.hour < self.end_hour_utc

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo["symbol"]
        logger.info(f"[{symbol}] Analizando...")
        # Búsqueda de timeframe adecuado
        
        intervalos= ["5min", "15min","30min", "1h", "4h"]
            
        interval_used = "30min"
        logger.info(f"[{symbol}] Buscando en timeframe {interval_used} ...")
        master = preloadedData.get(symbol) if preloadedData else None
        if isinstance(master, dict):
            df = master.get(interval_used)
            if df is None:
                df = master.get('1h')
                interval_used = "1h"
            if df is None:
                df = master.get('15min')
                interval_used = "15min"          
        else:
            df = master
        intervaloAnterior = intervalos[intervalos.index(interval_used)-1]    
        if df is None or len(df) < 80: # Requerimos al menos 52 + 26 = 78 velas para el Kumo
            logger.info(f"[{symbol}] No hay suficientes datos para el timeframe {interval_used}")
            return None
            
        # Calcular todos los indicadores
        df = self._calc_indicators(df)
        
        row = df.iloc[-1]
        prev_row = df.iloc[-2]
        
        now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
        interval_val = 5 if '5min' in interval_used else (15 if '15min' in interval_used else 60)
        last_v = get_last_closed_candle(now_cdmx, interval=interval_val, df=df)
        last_closed_ts = last_v.name if hasattr(last_v, 'name') else last_v
        
        candle_time_str = last_closed_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(last_closed_ts, 'strftime') else str(last_closed_ts)
        
        # 1. Filtro de Tiempo
        if not self._check_time_filter(last_closed_ts):
            logger.info(f"[{symbol}] Tiempo fuera de horario")
            return None
            
        # EXTRAE AQUÍ LOS VALORES DE LA VELA ACTUAL
        velaGuia = preloadedData.get(symbol, {}).get(intervaloAnterior)
        if velaGuia is None or len(velaGuia) == 0:
            logger.info(f"[{symbol}] No hay datos para el timeframe {intervaloAnterior}")
            return None

        velaGuia = velaGuia.iloc[-1]
        closeGuia = float(velaGuia['close'])
        openGuia = float(velaGuia['open'])
        highGuia = float(velaGuia['high'])
        lowGuia = float(velaGuia['low'])
        # IDENTIFICA EL COLOR DE LA VELA GUIA
        if closeGuia > openGuia:
            color_velaGuia = "Verde"
        elif closeGuia < openGuia:
            color_velaGuia = "Roja"
        else:
            color_vela = "Doji / Neutra" # Abrió y cerró al mismo precio
            
        close = float(row['close'])
        open_price = float(row['open']) # <--- Añade esto para obtener la apertura
        
        # IDENTIFICA EL COLOR DE LA VELA ACTUAL
        if close > open_price:
            color_vela = "Verde"
        elif close < open_price:
            color_vela = "Roja"
        else:
            color_vela = "Doji / Neutra" # Abrió y cerró al mismo precio
            
        logger.info(f"[{symbol}] Vela actual es de color: {color_vela}")

        tenkan = float(row['tenkan_sen']) # linea azul
        kijun = float(row['kijun_sen']) # linea cafe
        span_a = float(row['senkou_span_a']) # linea verde de nube
        span_b = float(row['senkou_span_b']) # linea roja de nube
        
        bb_upper = float(row['bb_upper']) # linea superior de Bollinger
        bb_middle = float(row['bb_middle']) # linea media de Bollinger
        bb_lower = float(row['bb_lower']) # linea inferior de Bollinger
        
        prev_bb_upper = float(prev_row['bb_upper'])
        prev_bb_lower = float(prev_row['bb_lower'])
        
        # Evitar cálculos con NaN
        if any(pd.isna([span_a, span_b, bb_upper, tenkan, kijun])):                
            logger.info(f"[{symbol}] No hay suficientes datos para el timeframe {interval_used}")
            return None
            
        kumo_max = max(span_a, span_b)
        kumo_min = min(span_a, span_b)
        
        bb_width_current = bb_upper - bb_lower
        bb_width_prev = prev_bb_upper - prev_bb_lower
        
        direction = None
        macd, macd_signal, macdHist = ta.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        macd = macd.iloc[-1]
        macd_signal = macd_signal.iloc[-1]
        macdHist_val = macdHist.iloc[-1] if not pd.isna(macdHist.iloc[-1]) else 0
        macdhist_anterior = macdHist.iloc[-2] if not pd.isna(macdHist.iloc[-2]) else 0

        # Lógica de colores e impulso
        if macdHist_val > 0:
            if macdHist_val > macdhist_anterior:
                color = "Verde Oscuro"
                impulso = "Alcista Ganando Fuerza (Fuerte)"
            else:
                color = "Verde Claro"
                impulso = "Alcista Perdiendo Fuerza (Debilidad)"

        elif macdHist_val < 0:
            if macdHist_val < macdhist_anterior:
                color = "Rojo Oscuro"
                impulso = "Bajista Ganando Fuerza (Fuerte)"
            else:
                color = "Rojo Claro"
                impulso = "Bajista Perdiendo Fuerza (Debilidad)"
        else:
            color = "Gris"
            impulso = "Cruce / Neutro"
        
        logger.info(f"[{symbol}] vela[{interval_used}]={color_vela}, velaGuia[{intervaloAnterior}]={color_velaGuia},MACD color={color}, impulso={impulso}")
        #  logger.info(f"[{symbol}] close={close}, tenkan={tenkan}, kijun={kijun}, span_a={span_a}, span_b={span_b}, bb_upper={bb_upper}, bb_middle={bb_middle}, bb_lower={bb_lower}, bb_width_current={bb_width_current}, bb_width_prev={bb_width_prev}")
        # 2. Reglas de Entrada en Largo (Compra)
        if (close > kumo_max and 
            tenkan > kijun and 
            close > bb_middle and 
            span_a > span_b and # nube verde
            span_a == kumo_max and # linea verde de la nube se encuentra encima  de span_b (linea roja de la nube)
            bb_width_current > bb_width_prev and
            macdHist_val > 0 and  # MACD histograma sea verde
            color_velaGuia == "Verde"):
            direction = "LARGO"
            
        # 3. Reglas de Entrada en Corto (Venta)
        elif (close < kumo_min and 
            tenkan < kijun and 
            close < bb_middle and 
            span_a < span_b and  # nube roja
            span_b == kumo_min and #  linea roja de la nube se encuentra debajo de span_a (linea verde de la nube)
            bb_width_current > bb_width_prev and
            macdHist_val < 0 and # MACD histograma sea rojo
            color_velaGuia == "Roja"):
            direction = "CORTO"
            
        if not direction:
            logger.info(f"[{symbol}] No hay señal para {interval_used} - sin direccion")
            return None
            
        # 4. Salidas y Gestión de Riesgo
        # Stop Loss: línea Kijun-sen del Ichimoku
        sl = kijun
        
        # Validar distancia de SL
        if direction == "LARGO" and sl >= close:
            logger.info(f"[{symbol}] No hay señal para {interval_used} - sl >= close")
            return None
        if direction == "CORTO" and sl <= close:
            logger.info(f"[{symbol}] No hay señal para {interval_used} - sl <= close")
            return None
            
        risk_dist = abs(close - sl)
        if risk_dist <= 0:
            logger.info(f"[{symbol}] No hay señal para {interval_used} - risk_dist <= 0")
            return None
        
        # Take Profit: Relación Riesgo:Beneficio de 1:1.5
        if direction == "LARGO":
            # tp = close + (risk_dist * 1.5)
            tp = bb_upper + (risk_dist * (0.75 if color == "Verde Oscuro" else 0.25))
        else:
            # tp = close - (risk_dist * 1.5)
            tp = bb_lower - (risk_dist * (0.75 if color == "Rojo Oscuro" else 0.25))
            
        # Tamaño de la Posición
        refCapital = float(symbolInfo.get('refCapital', 10000.0))
        refRiskPct = float(symbolInfo.get('refRiskPct', 1.0))
        
        size, riskUsdActual, marginUsed = risk.calculatePositionSize(
            refCapital, refRiskPct, risk_dist, symbolInfo, entryPrice=close
        )
        
        if size is None or size <= 0:
            logger.info(f"[{symbol}] Ichimoku: Tamaño de posición inválido o margen insuficiente")
            return None
            
        multiplier = getPipMultiplier(symbol)
        expectedProfit = riskUsdActual * 2
        
        # Deduplication
        
        sig_key = f"{symbol}_{candle_time_str}_{direction}"
        if sig_key in self._signals_sent:
            logger.info(f"[{symbol}] No hay señal para {interval_used} - sig_key in self._signals_sent")
            return None
        
        # Validación de salud de la señal para no entrar en niveles sobreextendidos
        if not technical.check_signal_health(close, tp, sl, direction, close, threshold=0.65, candle_time=candle_time_str)[0]:
            logger.info(f"[{symbol}] No hay señal para {interval_used} - check_signal_health")
            return None
        
        # Break Even al 1:1
        be_trigger = calculateBEPrice(close, sl, tp, direction)
        
        self._signals_sent[sig_key] = True
        
        # Nota: La regla "Cierre de Emergencia: Si el precio cruza la banda media de Bollinger en sentido 
        # contrario a la operación" se envía en metadata para ser auditada por el Tracker.
        
        return Signal(
            strategy="Ichimoku",
            symbol=symbol,
            direction=direction,
            entry_price=close,
            stop_loss=sl,
            take_profit=tp,
            sl_distance=risk_dist,
            confidence=self.min_confidence,
            setup="Ichimoku + BB + MACD",
            status="EN ZONA ✅",
            candleTime=candle_time_str,
            intervalo=interval_used,
            riesgo_pips=round(risk_dist * multiplier, 1),
            rr_ratio=2.0,
            break_even=be_trigger,
            size=size,
            metadata={
                "riskUsd": round(riskUsdActual, 2),
                "expectedProfit": round(expectedProfit, 2),
                "marginUsed": round(marginUsed, 2),
                "bb_middle": bb_middle,
                "emergency_exit": "CROSS_BB_MIDDLE_AGAINST",
                "tenkan": round(tenkan, 5),
                "kijun": round(kijun, 5)
            }
        )
