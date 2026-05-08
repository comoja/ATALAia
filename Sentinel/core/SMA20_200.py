"""
SMA20-200 Trading Strategy Bot
Strategy based on SMA 20 periods as trigger and SMA 200 as trend filter.
 Modularized and refactored.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple
import pandas as pd
import numpy as np
import talib as ta
import asyncio
import os
import sys
import pytz

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.api import twelvedata
from middleware.config import constants as config
from Sentinel.analysis import technical, risk
from Sentinel.analysis.technical import check_tp_exhaustion, check_signal_health
from Sentinel.ml import model as mlModel
from middleware.utils.communications import sendTelegramAlert, alertaInmediata, deleteTelegramMessage
from middleware.utils import momentum
from middleware.utils.alertBuilder import buildSMAAlertMessage, adjustTPForMinRR, getPipMultiplier, calculateBEPrice
from middleware.database import dbManager
from Sentinel.data.dataLoader import getParametros
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from zoneinfo import ZoneInfo


logger = logging.getLogger("sentinel")


from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")


class SMABot:
    def __init__(self):
        self.lastMessageIds = {}
        self.lastSignals = {}
        self.sentMessages = []
        
        self.model_clf = mlModel.loadModel(config.MODEL_FILE_PATH)
        if self.model_clf is None:
            logger.warning("No se pudo cargar el modelo clasificador ML")
        
        self.model_reg = mlModel.loadRegModel(config.MODEL_REG_FILE_PATH)
        if self.model_reg is None:
            logger.warning("No se pudo cargar el modelo regresor ML")
        
        logger.info("ML inicializado")

    async def cleanupOldMessages(self, token: str, chatId: str):
        ahora = datetime.now()
        maxAntiguedad = timedelta(hours=2)
        
        mensajesAEliminar = [
            msg for msg in self.sentMessages
            if msg["token"] == token 
            and msg["chatId"] == chatId
            and (ahora - msg["sentTime"]) > maxAntiguedad
        ]
        
        for msg in mensajesAEliminar:
            success = await deleteTelegramMessage(msg["token"], msg["chatId"], msg["msgId"])
            if success:
                self.sentMessages.remove(msg)

    def debug_log(self, symbol, msg):
        logger.info(f"[{symbol}] {msg}")

    def build_features(self, df):
        row = df.iloc[-1]
        close = row["close"]
        atr = row["atr"]
        
        cci = ta.CCI(df['high'], df['low'], df['close'], timeperiod=14).iloc[-1]
        
        rsi = ta.RSI(df['close'], timeperiod=14).iloc[-1]
        
        macd, macd_signal, macdHist = ta.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
        macd = macd.iloc[-1]
        macdHist_val = macdHist.iloc[-1] if not pd.isna(macdHist.iloc[-1]) else 0
        
        ema20 = ta.EMA(df['close'], timeperiod=20).iloc[-1]
        ema50 = ta.EMA(df['close'], timeperiod=50).iloc[-1] if len(df) >= 50 else close
        emaDist = (close - ema20) / close
        emaTrend = 1 if ema20 > ema50 else -1
        
        sar = ta.SAR(df['high'], df['low'], acceleration=0.05, maximum=0.2).iloc[-1]
        sarDist = (close - sar) / close if not pd.isna(sar) else 0
        sarTrend = 1 if close > sar else -1
        
        ema50_series = df["close"].tail(10) if len(df) >= 10 else df["close"]
        x = np.arange(len(ema50_series))
        slopeEma50 = np.polyfit(x, ema50_series.values, 1)[0] if len(ema50_series) >= 2 else 0
        
        vol_media = df["volume"].rolling(20).mean().iloc[-1] if len(df) >= 20 else df["volume"].mean()
        volRatio = df["volume"].iloc[-1] / vol_media if vol_media > 0 else 1.0
        volRegime = 1 if volRatio > 1.5 else -1 if volRatio < 0.7 else 0
        
        features = {
            "close": close,
            "atr": atr,
            "atr_norm": atr / close,
            "sma20": row["sma20"],
            "sma200": row["sma200"],
            "dist_sma20": (close - row["sma20"]) / close,
            "dist_sma200": (close - row["sma200"]) / close,
            "log_return": np.log(df["close"].iloc[-1] / df["close"].iloc[-2]),
            "range": (row["high"] - row["low"]) / close,
            "cci": cci if not pd.isna(cci) else 0,
            "emaDist": emaDist,
            "emaTrend": emaTrend,
            "lag1": df["close"].iloc[-1] / df["close"].iloc[-2] if len(df) >= 2 else 1.0,
            "lag2": df["close"].iloc[-1] / df["close"].iloc[-3] if len(df) >= 3 else 1.0,
            "lag3": df["close"].iloc[-1] / df["close"].iloc[-4] if len(df) >= 4 else 1.0,
            "macdHist": macdHist_val,
            "macdNorm": macdHist_val / close if not pd.isna(macdHist_val) else 0,
            "pendienteRsi": rsi - ta.RSI(df['close'], timeperiod=14).iloc[-3] if len(df) >= 3 and not pd.isna(rsi) else 0,
            "rsi": rsi if not pd.isna(rsi) else 50,
            "sarDist": sarDist,
            "sarTrend": sarTrend,
            "slopeEma50": slopeEma50 / close,
            "volRatio": volRatio,
            "volRegime": volRegime,
        }
        sma_series = df["sma20"].tail(10)
        x = np.arange(len(sma_series))
        slope = np.polyfit(x, sma_series.values, 1)[0]
        features["sma_slope"] = slope
        return pd.DataFrame([features])

    def getPendiente(self, serie, periodos=3):
        y = serie.iloc[-periodos:].values
        x = np.arange(periodos)
        if len(y) < periodos:
            return 0
        m, b = np.polyfit(x, y, 1)
        return m
    
    def esSenalDuplicada(self, symbol, direction, candleTime):
        if symbol not in self.lastSignals:
            return False
        last = self.lastSignals[symbol]
        return last["direction"] == direction and last["candleTime"] == candleTime

    def detectar_rebote_sma_doble(self, df, sma20, intervalo, symbol=None, tendencia=None):
        cdmx_tz = pytz.timezone(TIMEZONE)
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(cdmx_tz)
        elif df.index.tzinfo != cdmx_tz:
            df.index = df.index.tz_convert(cdmx_tz)

        velas_analisis = 25
        touches = []
        for i in range(len(df) - velas_analisis - 1, len(df)):
            price = df["close"].iloc[i]
            sma = df["sma20"].iloc[i]
            atr = df["atr"].iloc[i]
            low, high, open_price, close_price = df["low"].iloc[i], df["high"].iloc[i], df["open"].iloc[i], df["close"].iloc[i]

            tolerancia_pct = 0.8
            max_wick_pct = 1.2
            tolerancia_pips = (atr / price) * tolerancia_pct * price
            max_wick_pips = atr * max_wick_pct
            dist_low = (sma - low) if low < sma else float('inf')
            dist_high = (high - sma) if high > sma else float('inf')

            toque_direccion, toque_dist = None, None
            if dist_low < tolerancia_pips and dist_low < max_wick_pips:
                toque_direccion, toque_dist = "ALCISTA", dist_low
            elif dist_high > 0 and dist_high < tolerancia_pips and dist_high < max_wick_pips:
                toque_direccion, toque_dist = "BAJISTA", dist_high

            if toque_direccion:
                touches.append({
                    "idx": i, "direccion": toque_direccion, "time_cdmx": df.index[i],
                    "open": open_price, "high": high, "low": low, "close": close_price,
                    "sma": sma, "atr": atr, "dist": toque_dist
                })

        if len(touches) < 2:
            return None, None

        for idx in range(len(touches) - 1, 0, -1):
            t1, t2 = touches[idx - 1], touches[idx]
            if t1["direccion"] != t2["direccion"] or (t1["close"] > t1["open"]) != (t2["close"] > t2["open"]): continue
            if tendencia and t1["direccion"] != tendencia: continue
            
            separacion = t2["idx"] - t1["idx"]
            if separacion < 0 or separacion > 25: continue

            close_actual = df["close"].iloc[-1]
            
            # Lógica corregida: seguir la tendencia
            # BAJISTA toca desde arriba → CORTO (venta)
            # ALCISTA toca desde abajo → LARGO (compra)
            if t1["direccion"] == "BAJISTA" and close_actual <= sma20 * 1.005:
                return "CORTO", t2["time_cdmx"]  # Venta siguiendo tendencia
            elif t1["direccion"] == "ALCISTA" and close_actual >= sma20 * 0.995:
                return "LARGO", t2["time_cdmx"]  # Compra siguiendo tendencia

        return None, None
    
    def identificarTendencia(self, df, precioActual, sma20):
        # Calculamos la pendiente normalizada como porcentaje (%)
        pendienteSma20 = (self.getPendiente(df["sma20"].tail(10), 10) / sma20) * 100
        
        # Umbral mucho más realista (0.005% de inclinación por vela)
        strat_config = dbManager.getStrategyConfig("SMA20_200") or {}
        threshold = float(strat_config.get('slope_threshold', 0.005))
        
        if precioActual > sma20 and pendienteSma20 > threshold:
            return "ALCISTA"
        elif precioActual < sma20 and pendienteSma20 < -threshold:
            return "BAJISTA"
        return "NEUTRAL"

    async def validarTendencia1h(self, symbol: str, tendencia15m: str, apiKey: str) -> bool:
        if not apiKey:
            return True
        try:
            df1h = await twelvedata.getTimeSeries({
                "symbol": symbol, "interval": "1h", "outputsize": 200,
                "apikey": apiKey, "timezone": TIMEZONE
            })
            if df1h is None or len(df1h) < 20:
                return True
                
            df1h["sma20"] = ta.SMA(df1h["close"].values, timeperiod=20)
            df1h = df1h.dropna(subset=["sma20"])
            
            if len(df1h) < 10: return True
            
            close1h = df1h["close"].iloc[-1]
            sma20_1h = df1h["sma20"].iloc[-1]
            tendencia1h = self.identificarTendencia(df1h, close1h, sma20_1h)
            
            if tendencia1h == "NEUTRAL" or tendencia1h != tendencia15m:
                return False
            return True
        except Exception as e:
            return True

    def detectar_consolidacion_oro_puro(self, df: pd.DataFrame, sma20: float, direction: str, symbol: str) -> dict | None:
        velas_consolidacion = []
        for i in range(-10, 0):
            precio_medio = (df["close"].iloc[i] + df["open"].iloc[i]) / 2
            if abs(precio_medio - sma20) / sma20 < 0.003:
                velas_consolidacion.append({"high": df["high"].iloc[i], "low": df["low"].iloc[i]})
        
        if len(velas_consolidacion) < 5: return None
        
        if "bb_width" in df.columns:
            bb_width_actual = df["bb_width"].iloc[-1]
            bb_width_promedio = df["bb_width"].tail(50).mean()
            if bb_width_actual > bb_width_promedio:
                return None
        
        base_high = max(v["high"] for v in velas_consolidacion)
        base_low = min(v["low"] for v in velas_consolidacion)
        rango_base = base_high - base_low
        close_actual = df["close"].iloc[-1]
        
        if direction == "CORTO" and close_actual < (base_low - rango_base * 0.3):
            return {"type": "CORTO", "sl": base_high + rango_base * 0.2}
        elif direction == "LARGO" and close_actual > (base_high + rango_base * 0.3):
            return {"type": "LARGO", "sl": base_low - rango_base * 0.2}
        return None

    def detectar_volumen_anormal(self, df: pd.DataFrame, symbol: str) -> dict | None:
        if "volume" not in df.columns: return None
        vol_prom = df["volume"].tail(20).mean()
        vol_act = df["volume"].iloc[-1]
        if vol_prom > 0 and (vol_act / vol_prom) > 2.5:
            return {"ratio": vol_act / vol_prom, "volumen_actual": vol_act}
        return None

    def detectar_extension_extrema(self, df: pd.DataFrame, sma20: float) -> dict | None:
        close = df["close"].iloc[-1]
        distancia = abs(close - sma20) / sma20 * 100
        atr_promedio = df["atr"].tail(20).mean()
        distancia_atr = distancia / (atr_promedio / close * 100) if (atr_promedio / close * 100) > 0 else 0
        
        if distancia_atr > 4:
            return {"distancia_pct": distancia, "distancia_atr": distancia_atr}
        return None

    async def _getAndPrepareData(self, symbolInfo: Dict, apiKey: str, nVelas: int, interval: str, rawDf: pd.DataFrame = None) -> pd.DataFrame | None:
        symbol = symbolInfo['symbol']
        def prepareDf(dfInput):
            dfInput = dfInput.dropna(subset=['close', 'high', 'low']).copy()
            for col in ['close', 'high', 'low']: dfInput[col] = pd.to_numeric(dfInput[col], errors='coerce')
            dfInput = dfInput[(dfInput['close'] > 0) & (dfInput['high'] > 0) & (dfInput['low'] > 0)]
            if len(dfInput) < 200: return None
            # Punto 3: Evitar cálculos redundantes si ya existen en el DF
            if all(col in dfInput.columns for col in ['sma20', 'sma200', 'atr', 'bb_upper', 'rsi', 'cci', 'macd']):
                return dfInput.dropna(subset=['sma20', 'sma200', 'atr', 'bb_upper', 'rsi', 'cci', 'macd'])

            dfInput["sma20"] = ta.SMA(dfInput["close"].values, timeperiod=20)
            dfInput["sma200"] = ta.SMA(dfInput["close"].values, timeperiod=200)
            dfInput["atr"] = ta.ATR(dfInput["high"].values, dfInput["low"].values, dfInput["close"].values, 14)
            dfInput["rsi"] = ta.RSI(dfInput["close"].values, timeperiod=14)
            dfInput["cci"] = ta.CCI(dfInput["high"].values, dfInput["low"].values, dfInput["close"].values, timeperiod=14)
            macd_vals = ta.MACD(dfInput["close"].values, fastperiod=12, slowperiod=26, signalperiod=9)
            dfInput["macd"] = macd_vals[0]
            dfInput["bb_upper"], dfInput["bb_middle"], dfInput["bb_lower"] = ta.BBANDS(dfInput["close"].values, timeperiod=20, nbdevup=2, nbdevdn=2)
            dfInput["bb_width"] = (dfInput["bb_upper"] - dfInput["bb_lower"]) / dfInput["bb_middle"]
            return dfInput.dropna(subset=['sma20', 'sma200', 'atr', 'bb_upper', 'rsi', 'cci', 'macd'])


        if rawDf is not None and len(rawDf) >= 200:
            df = prepareDf(rawDf)
            if df is not None and len(df) >= 50:
                if interval == "15min" and (df.index[-1] - df.index[-2]).total_seconds() / 60 < 10:
                    df = prepareDf(df[df.index.minute % 15 == 0].copy())
                return df
                
        params = {"symbol": symbol, "interval": interval, "outputsize": nVelas, "apikey": apiKey, "timezone": TIMEZONE}
        df = await twelvedata.getTimeSeries(params)
        return prepareDf(df) if df is not None else None
    
    def _validar_filtros_basicos(self, df: pd.DataFrame, close: float, sma20: float, sma200: float, atr: float, direction: str, symbol: str) -> bool:
        if abs(close - sma20) / close * 100 < (atr / close * 100) * 0.5:
            return False
            
        rango = (df["high"].tail(20).max() - df["low"].tail(20).min()) / close
        if rango < (atr / close) * 3:
            return False
            
        velas_contrarias = sum(1 for i in range(-4, 0) if (direction == "LARGO" and df["close"].iloc[i] < df["open"].iloc[i]) or (direction == "CORTO" and df["close"].iloc[i] > df["open"].iloc[i]))
        if velas_contrarias >= 2:
            return False
            
        if (direction == "LARGO" and close < sma200) or (direction == "CORTO" and close > sma200):
            return False
            
        return True

    def _validar_ml(self, df: pd.DataFrame, close: float, sma20: float, atr: float) -> Tuple[bool, float, float]:
        try:
            features = self.build_features(df)
            prob = self.model_clf.predict_proba(features)[0][1] if self.model_clf else 0.55
            expected_return = max(0.2, min(self.model_reg.predict(features)[0] if self.model_reg else 0.5, 2.0))
        except Exception:
            prob = 0.55
            expected_return = 0.5
        
        distanciaSma20Pct = abs(close - sma20) / close * 100
        atrRelativo = atr / close * 100
        threshold = 0.40 if distanciaSma20Pct < atrRelativo * 0.5 else 0.45 if distanciaSma20Pct < atrRelativo else 0.50 if distanciaSma20Pct < atrRelativo * 1.5 else 0.55
        
        return prob >= threshold, prob, expected_return

    async def _get_signal(self, df: pd.DataFrame, symbol: str, intervalo: str, apiKey: str = None, symbolInfo: Dict = None) -> Optional[Signal]:
        cdmx_tz = pytz.timezone(TIMEZONE)
        if df.index.tzinfo is None: df.index = df.index.tz_localize(cdmx_tz)
        else: df.index = df.index.tz_convert(cdmx_tz)
        
        close, sma20, sma200, atr = df["close"].iloc[-1], df["sma20"].iloc[-1], df["sma200"].iloc[-1], df["atr"].iloc[-1]
        tendencia = self.identificarTendencia(df, close, sma20)
        
        adx = ta.ADX(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
        adx_series = pd.Series(adx).dropna()
        adx_val = float(adx_series.iloc[-1]) if len(adx_series) > 0 else 25.0
        if adx_val < 20:
            return None
        
        if tendencia == "NEUTRAL":
            return None

        if apiKey and not await self.validarTendencia1h(symbol, tendencia, apiKey):
            return None

        direction, double_touch_time = self.detectar_rebote_sma_doble(df, sma20, intervalo, symbol, tendencia)
        consolidacion = None
        if not direction:
            consolidacion = self.detectar_consolidacion_oro_puro(df, sma20, tendencia, symbol)
            if not consolidacion:
                return None
            direction, double_touch_time = consolidacion["type"], df.index[-1]
        
        momentum_estado = symbolInfo.get('momentum', '☁️ SIN DATOS') if symbolInfo else '☁️ SIN DATOS'
        momentum_bonus, _ = momentum.getMomentumBonus(momentum_estado, direction)
        
        bb_bonus = 0
        if "bb_lower" in df.columns and "bb_upper" in df.columns:
            bb_lower = df["bb_lower"].iloc[-1]
            bb_upper = df["bb_upper"].iloc[-1]
            if direction == "LARGO" and close <= bb_lower:
                bb_bonus = 10
            elif direction == "CORTO" and close >= bb_upper:
                bb_bonus = 10

        if not self._validar_filtros_basicos(df, close, sma20, sma200, atr, direction, symbol):
            return None

        if double_touch_time and (datetime.now(cdmx_tz) - double_touch_time).total_seconds() / 60 > 60:
            return None

        ml_ok, prob, expected_return = self._validar_ml(df, close, sma20, atr)
        if not ml_ok:
            return None

        vol_anormal = self.detectar_volumen_anormal(df, symbol)
        ext_extrema = self.detectar_extension_extrema(df, sma20)

        levels = technical.get_structural_levels(df, lookback=40)
        atr_padding = atr * 0.2

        if consolidacion:
            stop_loss = consolidacion["sl"]
        else:
            if direction == "LARGO":
                stop_loss = min(close - (atr * 1.2), levels['swing_low'] - atr_padding)
            else:
                stop_loss = max(close + (atr * 1.2), levels['swing_high'] + atr_padding)
        
        sl_dist = abs(close - stop_loss)
        tp_initial = levels['high_zone'] if direction == "LARGO" else levels['low_zone']
        
        tp_factor = 1 + expected_return
        tp_ml = close + sl_dist * tp_factor if direction == "LARGO" else close - sl_dist * tp_factor
        
        if direction == "LARGO":
            take_profit = min(tp_initial, tp_ml) if tp_initial > tp_ml else tp_initial
        else:
            take_profit = max(tp_initial, tp_ml) if tp_initial < tp_ml else tp_initial


        strat_config = dbManager.getStrategyConfig("SMA20_200") or {}
        min_rr_val = float(strat_config.get('min_rr', 1.5))

        multiplier = getPipMultiplier(symbol)
        risk_usd = float(strat_config.get('risk_usd', 100.0))
        size = (risk_usd / (sl_dist * multiplier)) if (sl_dist > 0 and multiplier > 0) else 0

        # --- FILTRO: Ganancia Mínima Est. ---
        min_usd_profit = float(strat_config.get('min_usd_profit', 10.0))
        rr_ratio = round(abs(take_profit - close) / sl_dist, 2)
        expected_profit = (sl_dist * multiplier * size) * rr_ratio
        if expected_profit < min_usd_profit:
            logger.info(f"[{symbol}] SMA20_200: Beneficio Est. ${expected_profit:.2f} < ${min_usd_profit:.2f} - descartando")
            return None

        min_confidence = float(strat_config.get('min_confidence', 70))
        take_profit = adjustTPForMinRR(close, stop_loss, take_profit, direction, minRR=min_rr_val)
        
        # Calcular Break Even inteligente
        be_trigger = calculateBEPrice(close, stop_loss, take_profit, direction)
        
        # Calcular confianza
        confianza = int(prob * 100) + bb_bonus + momentum_bonus
        
        # Filtrar por confianza mínima
        if confianza < min_confidence:
            logger.info(f"[{symbol}] Señal descartada: confidence={confianza} < min_confidence={min_confidence}")
            return None

        
        rr_actual = abs(take_profit - close) / sl_dist
        multiplier = getPipMultiplier(symbol)
        
        vela_origen_idx = len(df) - 5
        is_valid, _, mensaje = check_tp_exhaustion(df, vela_origen_idx, close, take_profit, stop_loss, direction, threshold=0.60, timeframe="15M")
        if not is_valid:
            return None
        
        current_price = float(df['close'].iloc[-1])
        now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
        interval_min = 5 # default
        if 'min' in intervalo: interval_min = int(intervalo.replace('min', ''))
        
        last_v = get_last_closed_candle(now_cdmx, interval=interval_min, df=df)
        last_closed_ts = last_v.name if hasattr(last_v, 'name') else last_v
        last_closed_str = last_closed_ts.strftime("%Y-%m-%d %H:%M:%S")

        is_valid, _, mensaje = check_signal_health(close, take_profit, stop_loss, direction, current_price, threshold=0.65, candle_time=last_closed_str)
        if not is_valid:
            return None
        
        return Signal(
            strategy="SMA20_200",
            symbol=symbol,
            direction=direction,
            entry_price=close,
            stop_loss=stop_loss,
            take_profit=take_profit,
            sl_distance=sl_dist,
            confidence=confianza,
            setup="Consolidacion" if consolidacion else "Doble Toque",
            status="EN ZONA ✅",
            candleTime=last_closed.strftime("%Y-%m-%d %H:%M:%S"),
            intervalo=intervalo,
            riesgo_pips=round(sl_dist * multiplier, 1),
            rr_ratio=round(rr_actual, 2),
            break_even=be_trigger,
            metadata={
                "tendencia": tendencia,
                "volumenAnormal": vol_anormal,
                "extensionExtrema": ext_extrema,
                "bollingerBonus": bb_bonus,
                "momentum": momentum_estado
            }
        )

    def _filtrar_velas_completas(self, df: pd.DataFrame, ahora_cdmx, interval: str) -> pd.DataFrame:
        interval_map = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '1h': 60, '4h': 240, '1day': 1440}
        minutes = interval_map.get(interval, 60)
        df_copy = df.copy()
        df_copy.index = df_copy.index.tz_convert(ahora_cdmx.tzinfo) if df_copy.index.tzinfo else df_copy.index.tz_localize(ahora_cdmx.tzinfo)
        return df_copy[df_copy.index <= (ahora_cdmx - pd.Timedelta(minutes=minutes))].copy()

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f"▶ ENTRANDO análisis para {symbol}")
        master = preloadedData.get(symbol) if preloadedData else None
        
        # Punto 3: Master Dictionary integration
        ahora_cdmx = datetime.now(pytz.timezone(TIMEZONE))
        interval = symbolInfo.get('intervalo', '15min')
        
        if isinstance(master, dict):
            df = master.get(interval)
        else:
            df = master

        if df is None: return None
        
        df = self._filtrar_velas_completas(df, ahora_cdmx, interval)
        if len(df) < 50: return None
        
        _, _, _, nVelas, _ = getParametros()
        data = await self._getAndPrepareData(symbolInfo, apiKey, nVelas, interval, df)
        if data is None: return None

        signal = await self._get_signal(data, symbol, interval, apiKey, symbolInfo)
        if signal and not self.esSenalDuplicada(symbol, signal.direction, signal.candleTime):
            self.lastSignals[symbol] = {"direction": signal.direction, "candleTime": signal.candleTime}
            return signal

        return None

