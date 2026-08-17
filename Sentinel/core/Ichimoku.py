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
    Estrategia Ichimoku SMC Multi-Timeframe para Sentinel.
    - Opera en cascada de prioridad: 30min primero, 15min si no hay señal en 30min.
    - Indicadores: Ichimoku (9, 26, 52), Bollinger Bands (20, 2), MACD.
    - Filtro HTF: Doble Kumo en 1H como sesgo direccional (aplica a ambos TF).
    - Filtros SMC: Sweep de liquidez, VSA de absorcion institucional.
    - Salidas: SL en Kijun-sen, TP1 (1.5RR), TP2 (2.5RR), TP3 (4.0RR).
    - Emergencia: Cierre si precio cruza BB middle en contra.
    """

    # Timeframes intentados en orden de prioridad (mayor -> menor)
    TIMEFRAME_CASCADE = ["30min", "15min"]

    def __init__(self):
        self.strategy_name = "Ichimoku"
        self._signals_sent = {}

        # Obtener parametros de la base de datos si existen, si no se usan defaults
        strategyConfig = dbManager.getStrategyConfig("Ichimoku") or {}
        self.start_hour_utc = strategyConfig.get("start_hour_utc", 7)
        self.end_hour_utc = strategyConfig.get("end_hour_utc", 22)
        self.min_confidence = strategyConfig.get("min_confidence", 75)

        # Parametros institucionales SMC
        self.useVolumeFilter = strategyConfig.get("useVolumeFilter", True)
        self.volumeMultiplier = strategyConfig.get("volumeMultiplier", 1.2)
        self.useLiquidityFilter = strategyConfig.get("useLiquidityFilter", True)
        self.sweepLookback = strategyConfig.get("sweepLookback", 20)

        logger.info(
            "IchimokuBot iniciado — Cascada TF: %s | UTC %02d:00-%02d:00 | SMC Vol=%s Liq=%s",
            self.TIMEFRAME_CASCADE, self.start_hour_utc, self.end_hour_utc,
            self.useVolumeFilter, self.useLiquidityFilter
        )

    def _calc_indicators(self, df: pd.DataFrame, tenkanPeriod: int = 9, kijunPeriod: int = 26, senkouPeriod: int = 52, displacement: int = 26) -> pd.DataFrame:
        df = df.copy()
        
        # Bandas de Bollinger (20, 2)
        df['bb_upper'], df['bb_middle'], df['bb_lower'] = ta.BBANDS(
            df['close'].values, timeperiod=20, nbdevup=2, nbdevdn=2, matype=0
        )
        
        # Ichimoku dinamico
        # Tenkan-sen
        highT = df['high'].rolling(window=tenkanPeriod).max()
        lowT = df['low'].rolling(window=tenkanPeriod).min()
        df['tenkan_sen'] = (highT + lowT) / 2
        
        # Kijun-sen
        highK = df['high'].rolling(window=kijunPeriod).max()
        lowK = df['low'].rolling(window=kijunPeriod).min()
        df['kijun_sen'] = (highK + lowK) / 2
        
        # Senkou Span A (Shifted displacement periods ahead)
        df['senkou_span_a'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2).shift(displacement)
        
        # Senkou Span B
        highS = df['high'].rolling(window=senkouPeriod).max()
        lowS = df['low'].rolling(window=senkouPeriod).min()
        df['senkou_span_b'] = ((highS + lowS) / 2).shift(displacement)
        
        # Indicadores de SMC (Volumen y Extremos de Liquidez)
        df['volumeMa'] = df['volume'].rolling(window=14).mean()
        df['localHigh'] = df['high'].shift(1).rolling(window=self.sweepLookback).max()
        df['localLow'] = df['low'].shift(1).rolling(window=self.sweepLookback).min()
        
        return df

    def _check_time_filter(self, candle_time: datetime) -> bool:
        """Verifica si la vela cerrada cae dentro de la ventana horaria UTC configurada."""
        # Si la vela no tiene timezone, se le asigna el timezone local configurado
        if candle_time.tzinfo is None:
            candle_time = pytz.timezone(TIMEZONE).localize(candle_time)

        # Convertir a UTC para la comparacion de la ventana horaria
        utc_time = candle_time.astimezone(pytz.utc)

        return self.start_hour_utc <= utc_time.hour < self.end_hour_utc

    def _get_htf_df(self, symbol: str, preloadedData: Optional[Dict], df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Obtiene el DataFrame HTF (1H o 4H) para el filtro Doble Kumo."""
        if preloadedData and isinstance(preloadedData.get(symbol), dict):
            dfH1 = preloadedData[symbol].get('1h')
            if dfH1 is not None:
                return dfH1
            dfH1 = preloadedData[symbol].get('4h')
            if dfH1 is not None:
                return dfH1
        # Fallback: resamplear el TF actual a 1H
        return self.resample_ohlcv(df, '1h') if hasattr(self, 'resample_ohlcv') else None

    def _calc_htf_trend(self, dfH1: Optional[pd.DataFrame], tenkanPeriod: int = 9, kijunPeriod: int = 26, senkouPeriod: int = 52, displacement: int = 26) -> str:
        """Calcula el sesgo de tendencia en HTF usando Doble Kumo Ichimoku."""
        if dfH1 is None or len(dfH1) < senkouPeriod:
            return "NEUTRAL"

        highTH1 = dfH1['high'].rolling(window=tenkanPeriod).max()
        lowTH1 = dfH1['low'].rolling(window=tenkanPeriod).min()
        tenkanH1 = (highTH1 + lowTH1) / 2
        highKH1 = dfH1['high'].rolling(window=kijunPeriod).max()
        lowKH1 = dfH1['low'].rolling(window=kijunPeriod).min()
        kijunH1 = (highKH1 + lowKH1) / 2
        spanAH1 = ((tenkanH1 + kijunH1) / 2).shift(displacement)
        highSH1 = dfH1['high'].rolling(window=senkouPeriod).max()
        lowSH1 = dfH1['low'].rolling(window=senkouPeriod).min()
        spanBH1 = ((highSH1 + lowSH1) / 2).shift(displacement)

        lastSpanA = spanAH1.iloc[-1]
        lastSpanB = spanBH1.iloc[-1]
        lastCloseH1 = dfH1['close'].iloc[-1]

        if pd.isna(lastSpanA) or pd.isna(lastSpanB):
            return "NEUTRAL"
        if lastCloseH1 > max(lastSpanA, lastSpanB):
            return "ALCISTA"
        if lastCloseH1 < min(lastSpanA, lastSpanB):
            return "BAJISTA"
        return "NEUTRAL"

    def _analyze_timeframe(
        self,
        symbol: str,
        df: pd.DataFrame,
        interval_used: str,
        intervaloAnterior: str,
        preloadedData: Optional[Dict],
        symbolInfo: Dict,
        htfTrend: str,
        tenkanPeriod: int = 9,
        kijunPeriod: int = 26,
        senkouPeriod: int = 52,
        displacement: int = 26,
        minRrVal: float = 1.5,
        stratConfig: Dict = None,
    ) -> Optional[Signal]:
        """
        Ejecuta el analisis Ichimoku SMC completo sobre un DataFrame y timeframe dados.
        Retorna una Signal si se cumplen todas las condiciones, o None en caso contrario.
        """
        df = self._calc_indicators(df, tenkanPeriod, kijunPeriod, senkouPeriod, displacement)

        row = df.iloc[-1]
        prev_row = df.iloc[-2]

        now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
        interval_val = 5 if '5min' in interval_used else (15 if '15min' in interval_used else 60)
        last_v = get_last_closed_candle(now_cdmx, interval=interval_val, df=df)
        last_closed_ts = last_v.name if hasattr(last_v, 'name') else last_v

        candle_time_str = last_closed_ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(last_closed_ts, 'strftime') else str(last_closed_ts)

        # 1. Filtro de Tiempo
        if not self._check_time_filter(last_closed_ts):
            logger.info(f"[{symbol}][{interval_used}] Tiempo fuera de horario")
            return None

        # Vela guia del TF inferior
        velaGuia = preloadedData.get(symbol, {}).get(intervaloAnterior) if preloadedData else None
        if velaGuia is None or len(velaGuia) == 0:
            logger.info(f"[{symbol}][{interval_used}] No hay datos para vela guia {intervaloAnterior}")
            return None

        velaGuia = velaGuia.iloc[-1]
        closeGuia = float(velaGuia['close'])
        openGuia = float(velaGuia['open'])
        color_velaGuia = "Verde" if closeGuia > openGuia else ("Roja" if closeGuia < openGuia else "Doji / Neutra")

        close = float(row['close'])
        open_price = float(row['open'])
        color_vela = "Verde" if close > open_price else ("Roja" if close < open_price else "Doji / Neutra")
        logger.info(f"[{symbol}][{interval_used}] Vela={color_vela} | VelaGuia[{intervaloAnterior}]={color_velaGuia}")

        tenkan = float(row['tenkan_sen'])
        kijun = float(row['kijun_sen'])
        span_a = float(row['senkou_span_a'])
        span_b = float(row['senkou_span_b'])
        bb_upper = float(row['bb_upper'])
        bb_middle = float(row['bb_middle'])
        bb_lower = float(row['bb_lower'])
        prev_bb_upper = float(prev_row['bb_upper'])
        prev_bb_lower = float(prev_row['bb_lower'])
        volume = float(row['volume'])
        volumeMa = float(row['volumeMa'])
        localHigh = float(row['localHigh'])
        localLow = float(row['localLow'])

        # Evitar calculos con NaN
        if any(pd.isna([span_a, span_b, bb_upper, tenkan, kijun, volumeMa, localHigh, localLow])):
            logger.info(f"[{symbol}][{interval_used}] NaN en indicadores — saltando")
            return None

        kumo_max = max(span_a, span_b)
        kumo_min = min(span_a, span_b)
        bb_width_current = bb_upper - bb_lower
        bb_width_prev = prev_bb_upper - prev_bb_lower

        # Impulse MACD
        if stratConfig is None: stratConfig = {}
        useImpulseMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 1)))
        macdSlow = int(stratConfig.get('macdSlow', 34))
        macdSignal = int(stratConfig.get('macdSignal', 9))
        
        if useImpulseMacdFilter:
            impulse_macd, _ = technical.calculateImpulseMacd(df, lengthMa=macdSlow, lengthSignal=macdSignal)
            macdHist_val = float(impulse_macd.iloc[-1])
            macdhist_anterior = float(impulse_macd.iloc[-2])
        else:
            macdHist_val = df["impulseMacd"].iloc[-1] if "impulseMacd" in df.columns else 0.0
            macdhist_anterior = df["impulseMacd"].iloc[-2] if "impulseMacd" in df.columns else 0.0

        if macdHist_val > 0:
            impulso = "Alcista Ganando Fuerza" if macdHist_val > macdhist_anterior else "Alcista Perdiendo Fuerza"
        elif macdHist_val < 0:
            impulso = "Bajista Ganando Fuerza" if macdHist_val < macdhist_anterior else "Bajista Perdiendo Fuerza"
        else:
            impulso = "Cruce / Neutro"
        logger.info(f"[{symbol}][{interval_used}] Impulse MACD impulso={impulso} | HTF={htfTrend}")

        # VSA: Absorcion institucional
        atr14 = ta.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
        candleSpread = abs(row['high'] - row['low'])
        isVolumeAbsorbed = (
            not pd.isna(atr14)
            and volume > 1.5 * volumeMa
            and candleSpread < 0.4 * atr14
        )
        if isVolumeAbsorbed:
            logger.info(f"[{symbol}][{interval_used}] VSA: Absorcion institucional detectada.")

        # Sweeps de liquidez (ultimas 5 velas)
        bullishSweep = False
        bearishSweep = False
        if self.useLiquidityFilter:
            for offset in range(-5, 0):
                candleRow = df.iloc[offset]
                if candleRow['low'] < candleRow['localLow'] and candleRow['close'] > candleRow['localLow']:
                    bullishSweep = True
                if candleRow['high'] > candleRow['localHigh'] and candleRow['close'] < candleRow['localHigh']:
                    bearishSweep = True

        # 2. Reglas de Entrada en Largo
        direction = None
        if (close > kumo_max
                and tenkan > kijun
                and close > bb_middle
                and span_a > span_b
                and span_a == kumo_max
                and bb_width_current > bb_width_prev
                and macdHist_val > 0
                and color_velaGuia == "Verde"
                and htfTrend in ("ALCISTA", "NEUTRAL")):

            hasVol = volumeMa > 0
            volumeFilterOk = not self.useVolumeFilter or not hasVol or (volume > self.volumeMultiplier * volumeMa and not isVolumeAbsorbed)
            liquidityFilterOk = not self.useLiquidityFilter or bullishSweep

            if volumeFilterOk and liquidityFilterOk:
                direction = "LARGO"
            else:
                logger.info(f"[{symbol}][{interval_used}] SMC bloqueo Largo: Vol={volumeFilterOk} Liq={liquidityFilterOk}")

        # 3. Reglas de Entrada en Corto
        elif (close < kumo_min
                and tenkan < kijun
                and close < bb_middle
                and span_a < span_b
                and span_b == kumo_min
                and bb_width_current > bb_width_prev
                and macdHist_val < 0
                and color_velaGuia == "Roja"
                and htfTrend in ("BAJISTA", "NEUTRAL")):

            hasVol = volumeMa > 0
            volumeFilterOk = not self.useVolumeFilter or not hasVol or (volume > self.volumeMultiplier * volumeMa and not isVolumeAbsorbed)
            liquidityFilterOk = not self.useLiquidityFilter or bearishSweep

            if volumeFilterOk and liquidityFilterOk:
                direction = "CORTO"
            else:
                logger.info(f"[{symbol}][{interval_used}] SMC bloqueo Corto: Vol={volumeFilterOk} Liq={liquidityFilterOk}")

        if not direction:
            logger.info(f"[{symbol}][{interval_used}] Sin señal")
            return None

        # 4. Salidas y Gestion de Riesgo
        sl = kijun
        if direction == "LARGO" and sl >= close:
            logger.info(f"[{symbol}][{interval_used}] SL >= close — descartado")
            return None
        if direction == "CORTO" and sl <= close:
            logger.info(f"[{symbol}][{interval_used}] SL <= close — descartado")
            return None

        risk_dist = abs(close - sl)
        if risk_dist <= 0:
            return None

        # Take Profits Parciales Dinamicos
        tp1 = close + (risk_dist * minRrVal) if direction == "LARGO" else close - (risk_dist * minRrVal)
        tp2Target = close + (risk_dist * (minRrVal + 1.0)) if direction == "LARGO" else close - (risk_dist * (minRrVal + 1.0))
        tp2 = max(tp2Target, localHigh) if direction == "LARGO" else min(tp2Target, localLow)
        tp3 = close + (risk_dist * (minRrVal + 2.5)) if direction == "LARGO" else close - (risk_dist * (minRrVal + 2.5))

        # Tamaño de la Posicion
        refCapital = float(symbolInfo.get('refCapital', 10000.0))
        refRiskPct = float(symbolInfo.get('refRiskPct', 1.0))
        size, riskUsdActual, marginUsed = risk.calculatePositionSize(
            refCapital, refRiskPct, risk_dist, symbolInfo, entryPrice=close
        )
        if size is None or size <= 0:
            logger.info(f"[{symbol}][{interval_used}] Tamano de posicion invalido")
            return None

        multiplier = getPipMultiplier(symbol)
        expectedProfit = riskUsdActual * minRrVal

        # Deduplicacion por vela+direccion
        sig_key = f"{symbol}_{candle_time_str}_{direction}_{interval_used}"
        if sig_key in self._signals_sent:
            logger.info(f"[{symbol}][{interval_used}] Señal duplicada — ignorada")
            return None

        # Validacion de salud de la señal
        if not technical.check_signal_health(close, tp1, sl, direction, close, threshold=0.65, candle_time=candle_time_str)[0]:
            logger.info(f"[{symbol}][{interval_used}] check_signal_health fallido")
            return None

        be_trigger = calculateBEPrice(close, sl, tp1, direction)
        self._signals_sent[sig_key] = True

        logger.info(f"[{symbol}][{interval_used}] ✅ SEÑAL {direction} | Entry={close} | SL={sl:.5f} | TP1={tp1:.5f} | HTF={htfTrend}")

        return Signal(
            strategy="Ichimoku",
            symbol=symbol,
            direction=direction,
            entry_price=close,
            stop_loss=sl,
            take_profit=tp1,
            take_profit2=tp2,
            take_profit3=tp3,
            sl_distance=risk_dist,
            confidence=self.min_confidence,
            setup=f"Ichimoku SMC [{interval_used}] + BB + MACD + HTF:{htfTrend}",
            status="EN ZONA ✅",
            candleTime=candle_time_str,
            intervalo=interval_used,
            riesgo_pips=round(risk_dist * multiplier, 1),
            rr_ratio=minRrVal,
            break_even=be_trigger,
            size=size,
            metadata={
                "riskUsd": round(riskUsdActual, 2),
                "expectedProfit": round(expectedProfit, 2),
                "marginUsed": round(marginUsed, 2),
                "bb_middle": bb_middle,
                "emergency_exit": "CROSS_BB_MIDDLE_AGAINST",
                "tenkan": round(tenkan, 5),
                "kijun": round(kijun, 5),
                "kijun_trailing": True,
                "htf_trend": htfTrend,
                "timeframe": interval_used,
            }
        )

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Optional[Dict] = None, apiKey: str = None) -> Optional[Signal]:
        """
        Punto de entrada del bot. Recorre TIMEFRAME_CASCADE (30min -> 15min) y devuelve
        la primera señal valida encontrada. Si ninguno genera señal, retorna None.

        HTF RELATIVO: Cada TF usa su propio nivel superior como sesgo Doble Kumo:
            15min  ->  HTF = 30min
            30min  ->  HTF = 1h
            1h     ->  HTF = 4h
        Esto garantiza que el Kumo filtro siempre corresponda al mismo instrumento de analisis.
        """
        symbol = symbolInfo["symbol"]
        logger.info(f"[{symbol}] IchimokuBot — iniciando cascada {self.TIMEFRAME_CASCADE}")

        # Cargar parametros dinamicamente desde base de datos
        stratConfig = dbManager.getSymbolStrategyConfig("Ichimoku", symbol) or {}
        tenkanPeriod = int(stratConfig.get('tenkan_period', 9))
        kijunPeriod = int(stratConfig.get('kijun_period', 26))
        senkouPeriod = int(stratConfig.get('senkou_period', 52))
        displacement = int(stratConfig.get('displacement', 26))
        minRrVal = float(stratConfig.get('min_rr', 1.5))
        if minRrVal <= 0:
            minRrVal = 1.5

        masterData = preloadedData.get(symbol) if preloadedData else None

        # Mapa de TF -> su HTF inmediato superior para el Doble Kumo
        intervalos_ordenados = ["5min", "15min", "30min", "1h", "4h"]
        htfMap = {
            "5min":  "15min",
            "15min": "30min",
            "30min": "1h",
            "1h":    "4h",
            "4h":    "1d",    # poco comun pero completo
        }

        # Cascada de timeframes
        for interval_used in self.TIMEFRAME_CASCADE:
            logger.info(f"[{symbol}] Intentando TF: {interval_used}")

            df = None
            if isinstance(masterData, dict):
                df = masterData.get(interval_used)
            elif masterData is not None:
                df = masterData

            if df is None or len(df) < 80:
                logger.info(f"[{symbol}][{interval_used}] Datos insuficientes (<80 velas) — saltando")
                continue

            # ── HTF RELATIVO: nivel inmediatamente superior al TF actual ───────────
            htfKey = htfMap.get(interval_used, "1h")
            dfHtf = None
            if isinstance(masterData, dict):
                dfHtf = masterData.get(htfKey)
            # Fallback: resamplear el TF actual al nivel superior
            if dfHtf is None and hasattr(self, 'resample_ohlcv'):
                dfHtf = self.resample_ohlcv(df, htfKey)

            htfTrend = self._calc_htf_trend(dfHtf, tenkanPeriod, kijunPeriod, senkouPeriod, displacement)
            logger.info(f"[{symbol}][{interval_used}] Doble Kumo HTF ({htfKey}): {htfTrend}")

            # TF inferior como vela guia (un nivel abajo del TF actual)
            tf_idx = intervalos_ordenados.index(interval_used)
            intervaloAnterior = intervalos_ordenados[tf_idx - 1] if tf_idx > 0 else "5min"

            signal = self._analyze_timeframe(
                symbol=symbol,
                df=df,
                interval_used=interval_used,
                intervaloAnterior=intervaloAnterior,
                preloadedData=preloadedData,
                symbolInfo=symbolInfo,
                htfTrend=htfTrend,
                tenkanPeriod=tenkanPeriod,
                kijunPeriod=kijunPeriod,
                senkouPeriod=senkouPeriod,
                displacement=displacement,
                minRrVal=minRrVal,
            )

            if signal is not None:
                logger.info(f"[{symbol}] Señal encontrada en {interval_used} (HTF={htfKey}) — deteniendo cascada")
                return signal

            logger.info(f"[{symbol}][{interval_used}] Sin señal — bajando al siguiente TF")

        logger.info(f"[{symbol}] Cascada completa sin señal")
        return None

