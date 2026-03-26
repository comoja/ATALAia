"""
SMA20-200 Trading Strategy Bot
Strategy based on SMA 20 periods as trigger and SMA 200 as trend filter.
"""
import logging
from datetime import datetime, timedelta
from typing import Dict, Any
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
from Sentinel.ml import model as mlModel
from middleware.utils.communications import sendTelegramAlert, alertaInmediata, deleteTelegramMessage
from middleware.utils.alertBuilder import buildSMAAlertMessage
from middleware.database import dbManager
from middleware.scheduler.autoScheduler import getTiempoEspera, isRestTime
from Sentinel.data.dataLoader import getParametros
from middleware.config.constants import TIMEZONE


logger = logging.getLogger(__name__)


class SMABot:
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}
        self.lastSignals = {}
        self.sentMessages = []
        
        self.model_clf = mlModel.loadModel(config.MODEL_FILE_PATH)
        if self.model_clf is None:
            logger.warning("[SMA BOT] No se pudo cargar el modelo clasificador ML")
        
        self.model_reg = mlModel.loadRegModel(config.MODEL_REG_FILE_PATH)
        if self.model_reg is None:
            logger.warning("[SMA BOT] No se pudo cargar el modelo regresor ML")
        
        logger.info("[SMA BOT] ML inicializado")

    async def cleanupOldMessages(self, token: str, chatId: str):
        ahora = datetime.now()
        maxAntiguedad = timedelta(hours=2)
        
        mensajesAEliminar = [
            msg for msg in self.sentMessages
            if msg["token"] == token 
            and msg["chatId"] == chatId
            and (ahora - msg["sentTime"]) > maxAntiguedad
        ]
        
        if mensajesAEliminar:
            logger.info(f"[SMA BOT] Limpiando {len(mensajesAEliminar)} mensajes antiguos de Telegram")
        
        for msg in mensajesAEliminar:
            success = await deleteTelegramMessage(msg["token"], msg["chatId"], msg["msgId"])
            if success:
                self.sentMessages.remove(msg)

    def debug_log(self, symbol, msg):
        logger.info(f"[SMA DEBUG] [{symbol}] {msg}")

    def build_features(self, df):
        row = df.iloc[-1]

        features = {
            "close": row["close"],
            "atr": row["atr"],
            "atr_norm": row["atr"] / row["close"],
            "sma20": row["sma20"],
            "sma200": row["sma200"],
            "dist_sma20": (row["close"] - row["sma20"]) / row["close"],
            "dist_sma200": (row["close"] - row["sma200"]) / row["close"],
            "log_return": np.log(df["close"].iloc[-1] / df["close"].iloc[-2]),
            "range": (row["high"] - row["low"]) / row["close"],
        }

        # pendiente SMA (mejor que tu getPendiente corto)
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
        """
        Evita enviar la misma señal en la misma vela
        """
        
        if symbol not in self.lastSignals:
            return False
        
        last = self.lastSignals[symbol]
        
        if last["direction"] == direction and last["candle_time"] == candleTime:
            return True
        
        return False

    def detectar_rebote_sma_doble(self, df, sma20, intervalo, symbol=None, tendencia=None):
        cdmx_tz = pytz.timezone(TIMEZONE)
        ahora_cdmx = datetime.now(cdmx_tz)

        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(cdmx_tz)
        elif df.index.tzinfo != cdmx_tz:
            df.index = df.index.tz_convert(cdmx_tz)

        tolerancia_pct = 0.8
        max_wick_pct = 1.2
        symbol_type = 'MONEDA'
        
        if symbol:
            try:
                symbol_info = dbManager.getSymbol(symbol)
                if symbol_info and symbol_info.get('tipo'):
                    tipo = symbol_info['tipo']
                    type_config = dbManager.getSymbolTypeConfig(tipo)
                    if type_config:
                        tolerancia_pct = float(type_config.get('tolerancia_atr', 0.8))
                        max_wick_pct = float(type_config.get('max_wick_atr', 1.2))
                        symbol_type = tipo
            except:
                symbol_type = 'MONEDA'
        velas_analisis = 25
        
        logger.info(f"[SMA20] [{symbol}] Tipo: {symbol_type} | Tolerancia: {tolerancia_pct}% ATR | Max mecha: {max_wick_pct}x ATR")

        logger.info(f"[SMA20] [{symbol}] Analizando ultimas {velas_analisis} velas:")
        for i in range(max(0, len(df) - 10), len(df)):
            vela = df.iloc[i]
            tiempo = df.index[i]
            color = "VERDE" if vela["close"] > vela["open"] else "ROJO"
            logger.info(f"  [{tiempo.strftime('%H:%M:%S')}] O:{vela['open']:.5f} H:{vela['high']:.5f} L:{vela['low']:.5f} C:{vela['close']:.5f} | {color} | SMA20:{vela['sma20']:.5f} | ATR:{vela['atr']:.5f}")

        touches = []

        for i in range(len(df) - velas_analisis - 1, len(df)):
            price = df["close"].iloc[i]
            sma = df["sma20"].iloc[i]
            atr = df["atr"].iloc[i]
            low = df["low"].iloc[i]
            high = df["high"].iloc[i]
            open_price = df["open"].iloc[i]
            close_price = df["close"].iloc[i]

            candle_time_cdmx = df.index[i]
            tolerancia = (atr / price) * tolerancia_pct
            tolerancia_pips = tolerancia * price
            max_wick_pips = atr * max_wick_pct

            dist_low = (sma - low) if low < sma else float('inf')
            dist_high = (high - sma) if high > sma else float('inf')

            toque_direccion = None
            toque_dist = None

            is_green = close_price > open_price
            is_red = close_price < open_price

            if dist_low < tolerancia_pips and dist_low < max_wick_pips:
                toque_direccion = "ALCISTA"
                toque_dist = dist_low
            elif dist_high > 0 and dist_high < tolerancia_pips and dist_high < max_wick_pips:
                toque_direccion = "BAJISTA"
                toque_dist = dist_high

            if toque_direccion:
                touches.append({
                    "idx": i,
                    "direccion": toque_direccion,
                    "time_cdmx": candle_time_cdmx,
                    "open": open_price,
                    "high": high,
                    "low": low,
                    "close": close_price,
                    "sma": sma,
                    "atr": atr,
                    "dist": toque_dist
                })
                color = "VERDE" if is_green else "ROJO"
                logger.info(
                    f"[TOQUE] [{symbol}] {candle_time_cdmx.strftime('%H:%M:%S')} | O:{open_price:.5f} H:{high:.5f} L:{low:.5f} C:{close_price:.5f} | {color} | {toque_direccion} | SMA20:{sma:.5f} ATR:{atr:.5f} | dist={toque_dist:.4f} tol={tolerancia_pips:.4f}"
                )
            else:
                dist_a_sma = min(dist_low if dist_low != float('inf') else 999999, dist_high if dist_high != float('inf') else 999999)
                if dist_a_sma < max_wick_pips * 2:
                    color = "VERDE" if is_green else "ROJO"
                    logger.info(
                        f"[CERCA SMA] [{symbol}] {candle_time_cdmx.strftime('%H:%M:%S')} | O:{open_price:.5f} H:{high:.5f} L:{low:.5f} C:{close_price:.5f} | {color} | SMA20:{sma:.5f} | dist={dist_a_sma:.4f} tol={tolerancia_pips:.4f} maxWick={max_wick_pips:.4f}"
                    )

        logger.info(f"[TOQUES] [{symbol}] Total toques detectados: {len(touches)}")
        for t in touches:
            color = "VERDE" if t['close'] > t['open'] else "ROJO"
            logger.info(f"  -> {t['time_cdmx'].strftime('%H:%M:%S')} | O:{t['open']:.5f} H:{t['high']:.5f} L:{t['low']:.5f} C:{t['close']:.5f} | {color} | {t['direccion']} | SMA20:{t['sma']:.5f} ATR:{t['atr']:.5f} | dist={t['dist']:.4f}")

        if len(touches) < 2:
            logger.warning(f"[TOQUES] [{symbol}] No hay suficientes toques ({len(touches)} < 2)")
            return None, None

        for idx in range(len(touches) - 1, 0, -1):
            t1 = touches[idx - 1]
            t2 = touches[idx]

            if t1["direccion"] != t2["direccion"]:
                logger.info(f"[TOQUES] [{symbol}] Direcciones diferentes: {t1['direccion']} vs {t2['direccion']}")
                continue

            t1Green = t1["close"] > t1["open"]
            t2Green = t2["close"] > t2["open"]
            if t1Green != t2Green:
                color1 = "VERDE" if t1Green else "ROJO"
                color2 = "VERDE" if t2Green else "ROJO"
                logger.info(f"[TOQUES] [{symbol}] Colores diferentes: {color1} vs {color2} - requieren mismo color")
                continue

            if tendencia:
                if t1["direccion"] == "ALCISTA" and tendencia != "ALCISTA":
                    logger.info(f"[TOQUES] [{symbol}] Tendencia {tendencia} no coincide con direccion ALCISTA")
                    continue
                if t1["direccion"] == "BAJISTA" and tendencia != "BAJISTA":
                    logger.info(f"[TOQUES] [{symbol}] Tendencia {tendencia} no coincide con direccion BAJISTA")
                    continue

            separacion = t2["idx"] - t1["idx"]

            if separacion < 0:
                logger.info(f"[TOQUES] [{symbol}] Separación inválida: {separacion}")
                continue

            double_touch_time = t2["time_cdmx"]

            vela_actual = df.iloc[-1]
            close_actual = vela_actual["close"]

            if separacion > 25:
                logger.info(f"[TOQUES] [{symbol}] Separacion muy grande: {separacion} > 25")
                continue

            logger.info(f"[TOQUES] [{symbol}] Doble toque: {t1['time_cdmx'].strftime('%H:%M')} -> {t2['time_cdmx'].strftime('%H:%M')} | sep={separacion} | direccion={t1['direccion']}")

            umbral_Confirmacion = 0.5

            if t1["direccion"] == "ALCISTA":
                if close_actual >= sma20 * (1 - umbral_Confirmacion / 100):
                    logger.info(f"[TOQUES] [{symbol}] ✓ SEÑAL LARGO confirmada | close={close_actual:.4f} >= {umbral_Confirmacion}% debajo sma20={sma20:.4f}")
                    return "LARGO", double_touch_time
                else:
                    logger.info(f"[TOQUES] [{symbol}] ✗ LARGO no confirmada | close={close_actual:.4f} muy debajo sma20={sma20:.4f}")
            else:
                if close_actual <= sma20 * (1 + umbral_Confirmacion / 100):
                    logger.info(f"[TOQUES] [{symbol}] ✓ SEÑAL CORTO confirmada | close={close_actual:.4f} <= {umbral_Confirmacion}% arriba sma20={sma20:.4f}")
                    return "CORTO", double_touch_time
                else:
                    logger.info(f"[TOQUES] [{symbol}] ✗ CORTO no confirmada | close={close_actual:.4f} muy arriba sma20={sma20:.4f}")

        logger.warning(f"[TOQUES] [{symbol}] No se encontró doble toque válido para confirmar señal")
        return None, None
    
    def identificarTendencia(self, df, precioActual, sma20):
        pendienteSma20 = self.getPendiente(df["sma20"].tail(10), 10)
        
        atrRelativo = df["atr"].iloc[-1] / precioActual
        slopeThreshold = max(0.00005, atrRelativo * 0.5)
        
        logger.info(f"[TENDENCIA] precio={precioActual:.5f} sma20={sma20:.5f} | pendiente={pendienteSma20:.6f} | threshold={slopeThreshold:.6f} | atrRel={atrRelativo:.6f}")
        
        if precioActual > sma20 and pendienteSma20 > slopeThreshold:
            return "ALCISTA"
        elif precioActual < sma20 and pendienteSma20 < -slopeThreshold:
            return "BAJISTA"
        return "NEUTRAL"

    async def validarTendencia1h(self, symbol: str, tendencia15m: str, df15m: pd.DataFrame) -> bool:
        """
        Valida que la tendencia en 1H coincida con la tendencia en 15M.
        Usa los datos de 15min para resamplear a 1H (sin llamada adicional a API).
        """
        try:
            df1h = df15m.resample('1H').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
            
            if df1h.empty or len(df1h) < 50:
                logger.warning(f"[{symbol}] No se pudieron obtener datos de 1H - omitiendo filtro multi-tiempo")
                return True
            
            df1h = technical.calculateFeatures(df1h)
            if "sma20" not in df1h.columns:
                df1h["sma20"] = ta.SMA(df1h["close"], timeperiod=20)
            if "atr" not in df1h.columns:
                df1h["atr"] = ta.ATR(df1h["high"], df1h["low"], df1h["close"], 14)
            df1h = df1h.dropna()
            
            if len(df1h) < 50:
                logger.warning(f"[{symbol}] Datos 1H insuficientes tras procesar - omitiendo filtro multi-tiempo")
                return True
            
            close1h = df1h["close"].iloc[-1]
            sma20_1h = df1h["sma20"].iloc[-1]
            tendencia1h = self.identificarTendencia(df1h, close1h, sma20_1h)
            
            logger.info(f"[{symbol}] Tendencia 1H: {tendencia1h} | Tendencia 15M: {tendencia15m}")
            
            if tendencia1h == "NEUTRAL":
                logger.info(f"[{symbol}] ❌ Tendencia NEUTRAL en 1H - señal invalidada por multi-tiempo")
                return False
            
            if tendencia1h != tendencia15m:
                logger.info(f"[{symbol}] ⚠️ Tendencias divergentes: 1H={tendencia1h} vs 15M={tendencia15m}")
            
            return True
            
        except Exception as e:
            logger.error(f"[{symbol}] Error validando tendencia 1H: {e}")
            return True

    def detectar_consolidacion_oro_puro(self, df: pd.DataFrame, sma20: float, direction: str, symbol: str) -> dict | None:
        """
        Detecta consolidación "Oro Puro":
        - Precio formando base lateral (mínimo 5 velas)
        - La base coincide con la SMA20
        - Retorna niveles de ruptura si detecta consolidación válida
        """
        ventana = 10
        tolerancia_cerca_sma = 0.003

        velas_consolidacion = []
        for i in range(-ventana, 0):
            close_i = df["close"].iloc[i]
            open_i = df["open"].iloc[i]
            
            precio_medio = (close_i + open_i) / 2
            distancia_sma = abs(precio_medio - sma20) / sma20
            
            if distancia_sma < tolerancia_cerca_sma:
                velas_consolidacion.append({
                    "idx": i,
                    "high": df["high"].iloc[i],
                    "low": df["low"].iloc[i],
                    "close": close_i,
                    "open": open_i
                })
        
        if len(velas_consolidacion) < 5:
            logger.debug(f"[{symbol}] Consolidación: solo {len(velas_consolidacion)} velas cerca SMA20 (necesario 5+)")
            return None
        
        base_high = max(v["high"] for v in velas_consolidacion)
        base_low = min(v["low"] for v in velas_consolidacion)
        
        rango_base = base_high - base_low
        
        vela_actual = df.iloc[-1]
        close_actual = vela_actual["close"]
        high_actual = vela_actual["high"]
        low_actual = vela_actual["low"]
        
        if direction == "CORTO":
            if close_actual < base_low and high_actual < base_high:
                umbral_ruptura = base_low - rango_base * 0.3
                if close_actual < umbral_ruptura:
                    logger.info(f"[{symbol}] ✓ Consolidación CORTO detectada | base_high={base_high:.5f} base_low={base_low:.5f}")
                    return {
                        "type": "CORTO",
                        "base_high": base_high,
                        "base_low": base_low,
                        "ruptura_nivel": base_low,
                        "sl": base_high + rango_base * 0.2
                    }
        else:
            if close_actual > base_high and low_actual > base_low:
                umbral_ruptura = base_high + rango_base * 0.3
                if close_actual > umbral_ruptura:
                    logger.info(f"[{symbol}] ✓ Consolidación LARGO detectada | base_high={base_high:.5f} base_low={base_low:.5f}")
                    return {
                        "type": "LARGO",
                        "base_high": base_high,
                        "base_low": base_low,
                        "ruptura_nivel": base_high,
                        "sl": base_low - rango_base * 0.2
                    }
        
        logger.debug(f"[{symbol}] Consolidación detectada pero sin ruptura clara")
        return None

    def detectar_volumen_anormal(self, df: pd.DataFrame, symbol: str, multiplicador: float = 2.5) -> dict | None:
        """
        Detecta volumen anormal (picos enormes).
        Retorna info si hay pico de volumen > multiplicador * promedio.
        """
        if "volume" not in df.columns:
            return None
        
        volumen_promedio = df["volume"].tail(20).mean()
        volumen_actual = df["volume"].iloc[-1]
        
        if volumen_promedio == 0:
            return None
        
        ratio = volumen_actual / volumen_promedio
        
        if ratio > multiplicador:
            logger.warning(f"[{symbol}] ⚠️ Volumen anormal: {ratio:.1f}x promedio ({volumen_actual:.0f} vs {volumen_promedio:.0f})")
            return {
                "ratio": ratio,
                "volumen_actual": volumen_actual,
                "volumen_promedio": volumen_promedio
            }
        
        return None

    def detectar_extension_extrema(self, df: pd.DataFrame, sma20: float, symbol: str) -> dict | None:
        """
        Detecta si el precio está extremadamente extendido de la SMA20.
        Combina: distancia extrema + aceleración + volumen (opcional).
        """
        close = df["close"].iloc[-1]
        distancia = abs(close - sma20) / sma20 * 100
        
        atr_promedio = df["atr"].tail(20).mean()
        distancia_atr = distancia / (atr_promedio / close * 100) if (atr_promedio / close * 100) > 0 else 0
        
        if distancia_atr > 4:
            body_actual = abs(df["close"].iloc[-1] - df["open"].iloc[-1])
            body_anterior = abs(df["close"].iloc[-2] - df["open"].iloc[-2])
            
            es_aceleracion = body_actual > body_anterior * 1.3
            
            resultado = {
                "distancia_pct": distancia,
                "distancia_atr": distancia_atr,
                "es_aceleracion": es_aceleracion,
                "body_actual": body_actual,
                "body_anterior": body_anterior
            }
            
            logger.warning(
                f"[{symbol}] ⚠️ Extensión extrema detectada: "
                f"dist={distancia:.2f}% ({distancia_atr:.1f}x ATR) | "
                f"aceleración={es_aceleracion}"
            )
            
            return resultado
        
        return None

    def validar_sma200_cercano(self, precio_actual, sma200, umbral_pct=0.02):
        distancia_pct = abs(precio_actual - sma200) / precio_actual * 100
        return distancia_pct < umbral_pct

    def detectar_consolidacion(self, df, sma20, ventana=10):
        precio = df["close"].iloc[-ventana:]
        precio_max = precio.max()
        precio_min = precio.min()
        rango_pct = (precio_max - precio_min) / precio_min * 100
        cerca_sma = abs(precio.iloc[-1] - sma20) / sma20 * 100 < 1.5
        return rango_pct < 2.5 and cerca_sma

    def detectar_pullback(self, df, sma20, tendencia):
        precio_minimos = df["low"].tail(5)
        
        if tendencia == "ALCISTA":
            toca_sma = precio_minimos.min() <= sma20 * 1.01
            rechazo = df["close"].iloc[-1] > df["open"].iloc[-1] or df["close"].iloc[-2] > df["open"].iloc[-2]
            return toca_sma and rechazo
        elif tendencia == "BAJISTA":
            toca_sma = precio_minimos.max() >= sma20 * 0.99
            rechazo = df["close"].iloc[-1] < df["open"].iloc[-1] or df["close"].iloc[-2] < df["open"].iloc[-2]
            return toca_sma and rechazo
        return False

    def detectar_extension_extrema(self, df, sma20, tendencia, umbral_pct=5.0):
        precio_actual = df["close"].iloc[-1]
        distancia_pct = abs(precio_actual - sma20) / sma20 * 100
        
        if distancia_pct < umbral_pct:
            return False
        
        if tendencia == "ALCISTA":
            ultimo_movimiento = (df["close"].iloc[-1] - df["close"].iloc[-5]) / df["close"].iloc[-5] * 100
            volumen_promedio = df["volume"].rolling(20).mean().iloc[-1]
            volumen_actual = df["volume"].iloc[-1]
            volumen_anormal = volumen_actual > volumen_promedio * 2
            return ultimo_movimiento > 4 and volumen_anormal
        elif tendencia == "BAJISTA":
            ultimo_movimiento = (df["close"].iloc[-5] - df["close"].iloc[-1]) / df["close"].iloc[-5] * 100
            volumen_promedio = df["volume"].rolling(20).mean().iloc[-1]
            volumen_actual = df["volume"].iloc[-1]
            volumen_anormal = volumen_actual > volumen_promedio * 2
            return ultimo_movimiento > 4 and volumen_anormal
        return False

    def detectar_breakout_consolidacion(self, df, tendencia):
        precio_actual = df["close"].iloc[-1]
        precio_open = df["open"].iloc[-1]
        volumen = df["volume"].iloc[-1]
        volumen_promedio = df["volume"].rolling(20).mean().iloc[-1]
        
        if tendencia == "ALCISTA":
            return precio_actual > precio_open and volumen > volumen_promedio * 1.3
        elif tendencia == "BAJISTA":
            return precio_actual < precio_open and volumen > volumen_promedio * 1.3
        return False

    def detectar_reversion(self, df, tendencia):
        if tendencia == "ALCISTA":
            return df["close"].iloc[-1] < df["open"].iloc[-1] and df["close"].iloc[-2] < df["open"].iloc[-2]
        elif tendencia == "BAJISTA":
            return df["close"].iloc[-1] > df["open"].iloc[-1] and df["close"].iloc[-2] > df["open"].iloc[-2]
        return False

    async def _getAndPrepareData(self, symbolInfo: Dict, apiKey: str, nVelas: int, interval: str, rawDf: pd.DataFrame = None) -> pd.DataFrame | None:
        
        symbol = symbolInfo['symbol']
        logger.info(f"[{symbol}]  con intervalo: {interval}")
        minAfterDropna = 50

        def prepareDf(dfInput):
            dfInput = dfInput.copy()
            nanCounts = dfInput[['close', 'high', 'low', 'open', 'volume']].isna().sum()
            #logger.info(f"[{symbol}] rawDf velas={len(dfInput)}, NaN en cols clave: {nanCounts.to_dict()}")
            dfInput = dfInput.dropna(subset=['close', 'high', 'low'])
            #logger.info(f"[{symbol}] Tras dropna(OHLC): {len(dfInput)} velas")
            if len(dfInput) < 200:
                #logger.warning(f"[{symbol}] Solo {len(dfInput)} velas válidas tras dropna(subset).")
                return None
            for col in ['close', 'high', 'low']:
                dfInput[col] = pd.to_numeric(dfInput[col], errors='coerce')
            invalidMask = (dfInput['close'] <= 0) | (dfInput['high'] <= 0) | (dfInput['low'] <= 0)
            invalidCount = invalidMask.sum()
            if invalidCount > 0:
                #logger.info(f"[{symbol}] Velas con valores inválidos (≤0): {invalidCount}")
                dfInput = dfInput[~invalidMask]
            if len(dfInput) < 200:
                #logger.warning(f"[{symbol}] Solo {len(dfInput)} velas tras filtrar valores inválidos.")
                return None
            if "sma20" not in dfInput.columns:
                dfInput["sma20"] = ta.SMA(dfInput["close"].values, timeperiod=20)
            if "sma200" not in dfInput.columns:
                dfInput["sma200"] = ta.SMA(dfInput["close"].values, timeperiod=200)
            if "atr" not in dfInput.columns:
                dfInput["atr"] = ta.ATR(dfInput["high"].values, dfInput["low"].values, dfInput["close"].values, 14)
            nanAfterIndics = dfInput[['sma20', 'sma200', 'atr']].isna().sum()
            #logger.info(f"[{symbol}] NaN tras indicadores: {nanAfterIndics.to_dict()}")
            result = dfInput.dropna(subset=['sma20', 'sma200', 'atr'])
            #logger.info(f"[{symbol}] Tras dropna final: {len(result)} velas")
            return result

        if rawDf is not None and len(rawDf) >= 200:
            df = prepareDf(rawDf)
            if df is not None and len(df) >= minAfterDropna:
                if interval == "15min" and len(df) > 0:
                    diff = (df.index[-1] - df.index[-2]).total_seconds() / 60
                    if diff < 10:
                        logger.info(f"[{symbol}] rawDf es de {diff}min, filtrando a 15min...")
                        df = df[df.index.minute % 15 == 0].copy()
                        df = prepareDf(df)
                        logger.info(f"[{symbol}] ✓ Tras filtro 15min: {len(df)} velas | ultimas: {df.index[-1].strftime('%H:%M')}")
                logger.info(f"[{symbol}] ✓ USANDO rawDf pre-cargado: {len(df)} velas | ultimas: {df.index[-1].strftime('%H:%M')}")
                return df
            logger.warning(f"[{symbol}] rawDf insuficiente ({len(df) if df is not None else 0} velas). Descargando...")
        logger.info(f"[{symbol}] ✗ rawDf es None o < 200 velas, descargando datos nuevos...")
        params = {
                "symbol": symbol,
                "interval": interval,
                "outputsize": nVelas,
                "apikey": apiKey,
                "outputSize": 5000,
                "timezone":TIMEZONE
            }
        df = await twelvedata.getTimeSeries(params)
        #df = await twelvedata.getTimeSeries({"symbol": symbol, "interval": interval, "apikey": apiKey, "outputSize": 5000})
        if df is None:
            logger.warning(f"[{symbol}] Error obteniendo datos ")
            return None
        
        logger.info(f"[{symbol}] Descarga : {len(df)} velas")
        df = prepareDf(df)
        logger.info(f"[{symbol}] Tras prepareDf: {len(df) if df is not None else 'None'} velas")
        
        if df is None or len(df) < minAfterDropna:
            logger.warning(f"[{symbol}] Datos insuficientes para SMA20-200 ({len(df) if df is not None else 0} velas).")
            return None
        
        return df
    

    async def _get_signal(self, df: pd.DataFrame, symbol: str, intervalo: str, apiKey: str = None):
        cdmx_tz = pytz.timezone(TIMEZONE)
        ahora_cdmx = datetime.now(cdmx_tz)
        
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(cdmx_tz)
        elif df.index.tzinfo != cdmx_tz:
            df.index = df.index.tz_convert(cdmx_tz)
        
        candle_time = df.index[-1]
        close = df["close"].iloc[-1]
        sma20 = df["sma20"].iloc[-1]
        sma200 = df["sma200"].iloc[-1]
        atr = df["atr"].iloc[-1]

        logger.info(f"[_get_signal] [{symbol}] df received: {len(df)} velas, ultimas 2: {df.index[-2].strftime('%H:%M')}, {df.index[-1].strftime('%H:%M')}")

        tendencia = self.identificarTendencia(df, close, sma20)

        if tendencia == "NEUTRAL":
            pendiente = self.getPendiente(df["sma20"].tail(10), 10)
            logger.info(f"[{symbol}] ❌ Tendencia NEUTRAL (pendiente={pendiente:.6f}) - descartado")
            return None
        
        logger.info(f"[{symbol}] ✓ Tendencia: {tendencia}")

        logger.info(f"[SMA20] [{symbol}] Analizando ultimas 10 velas ({intervalo}):")
        for i in range(max(0, len(df) - 10), len(df)):
            vela = df.iloc[i]
            tiempo = df.index[i]
            color = "VERDE" if vela["close"] > vela["open"] else "ROJO"
            cuerpo = abs(vela["close"] - vela["open"])
            mechaSup = vela["high"] - max(vela["open"], vela["close"])
            mechaInf = min(vela["open"], vela["close"]) - vela["low"]
            distSma = abs(vela["close"] - vela["sma20"]) / vela["close"] * 100
            volumen = vela.get("volume", 0)
            logger.info(f"  [{tiempo.strftime('%H:%M:%S')}] O:{vela['open']:.5f} H:{vela['high']:.5f} L:{vela['low']:.5f} C:{vela['close']:.5f} | {color} | Cuerpo:{cuerpo:.5f} | MechSup:{mechaSup:.5f} | MechInf:{mechaInf:.5f} | SMA20:{vela['sma20']:.5f} | DistSMA:{distSma:.3f}% | ATR:{vela['atr']:.5f} | Vol:{volumen:.0f}")

        # Filtro multi-tiempo 1H deshabilitado
        # if apiKey:
        #     tendencia1hOk = await self.validarTendencia1h(symbol, tendencia, df)
        #     if not tendencia1hOk:
        #         return None
        
        # 🔥 FILTRO DISTANCIA MÍNIMA AL SMA20
        distancia_pct = abs(close - sma20) / close * 100
        atr_pct = atr / close * 100
        umbral_distancia = atr_pct * 0.5
        
        if distancia_pct < umbral_distancia:
            logger.info(f"[{symbol}] ❌ Precio muy cerca del SMA20 ({distancia_pct:.3f}% < {umbral_distancia}%) - lateral descartado\n")
            return None
        
        logger.info(f"[{symbol}] ✓ Distancia SMA20: {distancia_pct:.3f}%")

        # 🔥 FILTRO LATERALIDAD (ATR dinámico)
        rango = (df["high"].tail(20).max() - df["low"].tail(20).min()) / close
        umbral = (atr / close) * 3

        if rango < umbral:
            logger.info(f"[{symbol}] ❌ Mercado lateral (rango={rango:.4f} < umbral={umbral:.4f}) - descartado\n")
            return None
        
        logger.info(f"[{symbol}] ✓ Rango correcto: {rango:.4f}")

        distanciaSma200 = abs(close - sma200)
        umbralMuroAtr = atr * 1.5
        if distanciaSma200 < umbralMuroAtr:
            logger.info(f"[{symbol}] ⚠️ SMA200 muy cerca ({distanciaSma200:.4f} < {umbralMuroAtr:.4f}) - posible muro")
        else:
            logger.info(f"[{symbol}] ✓ SMA200 distancia OK ({distanciaSma200:.4f} > {umbralMuroAtr:.4f})")

        distancia_ext = abs(close - sma20) / close * 100
        atr_promedio = df["atr"].tail(20).mean()
        atr_relativo = atr_promedio / close * 100
        extension_threshold = atr_relativo * 3
        
        if distancia_ext > extension_threshold:
            logger.warning(f"[{symbol}] ⚠️ Precio extendido ({distancia_ext:.3f}% > {extension_threshold:.3f}%) - posible corrección\n")

        # 🔥 REBOTE SMA20 DOBLE
        direction, double_touch_time = self.detectar_rebote_sma_doble(df, sma20, intervalo, symbol, tendencia)

        consolidacion = None
        
        if not direction:
            logger.info(f"[{symbol}] Sin doble toque - buscando consolidación Oro Puro...")
            consolidacion = self.detectar_consolidacion_oro_puro(df, sma20, tendencia, symbol)
            if not consolidacion:
                logger.info(f"[{symbol}] ❌ Sin doble toque ni consolidación válida\n")
                return None
            direction = consolidacion["type"]
            double_touch_time = df.index[-1]
            logger.info(f"[{symbol}] ✓ Consolidación Oro Puro detectada: {direction}")
        else:
            logger.info(f"[{symbol}] ✓ Doble toque detectado: {direction} | tiempo={double_touch_time}")

        # =========================
        # 🔥 ML FEATURE EXTRACTION
        # =========================
        features = self.build_features(df)

        # =========================
        # 🔵 CLASIFICACIÓN (FILTRO)
        # =========================
        try:
            prob = self.model_clf.predict_proba(features)[0][1]
        except:
            prob = 0.5

        distanciaSma20Pct = abs(close - sma20) / close * 100
        atrRelativo = atr / close * 100

        if distanciaSma20Pct < atrRelativo * 0.5:
            threshold = 0.40
        elif distanciaSma20Pct < atrRelativo * 1.0:
            threshold = 0.45
        elif distanciaSma20Pct < atrRelativo * 1.5:
            threshold = 0.50
        else:
            threshold = 0.55

        logger.info(f"[{symbol}] ML threshold={threshold} | distSMA20={distanciaSma20Pct:.3f}% | atrRel={atrRelativo:.3f}%")

        if prob < threshold:
            logger.info(f"[{symbol}] ❌ Filtrado ML | prob={prob:.2f} < {threshold}\n")
            return None
        
        logger.info(f"[{symbol}] ✓ ML OK | prob={prob:.2f}\n")

        volumen_anormal = self.detectar_volumen_anormal(df, symbol)
        extension_extrema = self.detectar_extension_extrema(df, sma20, symbol)
        
        if extension_extrema:
            logger.warning(f"[{symbol}] ⚠️ EXTENSIÓN EXTREMA detectada - verificar si continuar")
        
        if volumen_anormal:
            logger.warning(f"[{symbol}] ⚠️ VOLUMEN ANORMAL {volumen_anormal['ratio']:.1f}x promedio")

        # =========================
        # 🔴 REGRESIÓN (TP DINÁMICO)
        # =========================
        try:
            expected_return = self.model_reg.predict(features)[0]
        except:
            expected_return = 0.5

        expected_return = max(0.2, min(expected_return, 2.0))

        logger.info(f"[{symbol}] ML exp_ret={expected_return:.2f}")

        # 🔥 FILTRO: Ignorar señales con más de 1 hora de antigüedad
        if double_touch_time:
            signal_age = ahora_cdmx - double_touch_time
            signal_age_minutes = signal_age.total_seconds() / 60
            if signal_age_minutes > 60:
                logger.info(f"[{symbol}] ❌ Doble toque muy antiguo ({signal_age_minutes:.0f}min > 60min)\n")
                return None
            
            logger.info(f"[{symbol}] ✓ Antigüedad doble toque: {signal_age_minutes:.0f}min")

        # 🔥 FILTRO DIRECCIÓN vs SMA200
        if direction == "LARGO" and close < sma200:
            logger.info(f"[{symbol}] ❌ LARGO pero close={close:.4f} < sma200={sma200:.4f}\n")
            return None

        if direction == "CORTO" and close > sma200:
            logger.info(f"[{symbol}] ❌ CORTO pero close={close:.4f} > sma200={sma200:.4f}\n")
            return None
        
        logger.info(f"[{symbol}] ✓ Direccion vs SMA200 OK")

        # 🔥 FILTRO: Velas recientes contrarias
        velas_recientes = 3
        velas_contrarias = 0
        for i in range(-velas_recientes, 0):
            body = df["close"].iloc[i] - df["open"].iloc[i]
            color = "VERDE" if body > 0 else "ROJO"
            if direction == "LARGO" and body < 0:
                velas_contrarias += 1
                logger.debug(f"[{symbol}] Vela {i} {color} contraria a LARGO")
            elif direction == "CORTO" and body > 0:
                velas_contrarias += 1
                logger.debug(f"[{symbol}] Vela {i} {color} contraria a CORTO")
        
        if velas_contrarias >= 2:
            logger.info(f"[{symbol}] ❌ {velas_contrarias}/{velas_recientes} velas contrarias - descartado\n")
            return None

        # 🎯 SL / TP
        if consolidacion:
            if direction == "LARGO":
                stop_loss = consolidacion["sl"]
            else:
                stop_loss = consolidacion["sl"]
            sl_dist = abs(close - stop_loss)
            logger.info(f"[{symbol}] SL desde consolidación: {stop_loss:.6f}")
        elif direction == "LARGO":
            stop_loss = df["low"].tail(3).min() * 0.998
            sl_dist = close - stop_loss
        else:
            stop_loss = df["high"].tail(3).max() * 1.002
            sl_dist = stop_loss - close
        
        if direction == "LARGO":
            tp_factor = 1 + expected_return
            take_profit = close + sl_dist * tp_factor
        else:
            tp_factor = 1 + expected_return
            take_profit = close - sl_dist * tp_factor
        # =========================
        # 🔥 AJUSTE POR VOLATILIDAD
        # =========================
        vol_factor = atr / close

        if vol_factor > 0.02:
            take_profit *= 0.8  # alta volatilidad → TP más conservador
        elif vol_factor < 0.005:
            take_profit *= 1.2  # baja volatilidad → deja correr más
        # =========================
        # 🚨 (7) PROTECCIÓN TP
        # =========================
        max_tp_pct = 0.05 if vol_factor > 0.01 else 0.08

        if abs(take_profit - close) / close > max_tp_pct:
            logger.warning(f"[{symbol}] TP ajustado por límite dinámico ({max_tp_pct*100:.1f}%)")
            
            if direction == "LARGO":
                take_profit = close * (1 + max_tp_pct)
            else:
                take_profit = close * (1 - max_tp_pct)

        logger.info(
            f"🎯 SEÑAL {direction} detectada para {symbol} | "
            f"Entrada: {close:.6f} | SL: {stop_loss:.6f} | TP: {take_profit:.6f} | "
            f"Dist SL: {sl_dist:.6f} | R:R 1:2.5"
        )

        return {
            "strategy": "SMA20-200",
            "direction": direction,
            "entryPrice": close,
            "slDistance": sl_dist,
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
            "confidence": int(prob * 100),
            "symbol": symbol,
            "candle_time": candle_time,
            "sma20": sma20,
            "sma200": sma200,
            "atr": atr,
            "setup": "Consolidacion Oro Puro" if consolidacion else "Doble Toque SMA20",
            "tendencia": tendencia,
            "volumenAnormal": volumen_anormal,
            "extensionExtrema": extension_extrema
        }

    async def _execute_trades(self, signal: Dict, symbolInfo):

        symbol = symbolInfo['symbol']

        for account in self.accounts:

            posSize, _ = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'],
                symbolInfo=symbolInfo
            )

            if posSize is None:
                continue

            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']

            slPrice = entryPrice - slDist if direction == "LARGO" else entryPrice + slDist
            tpPrice = signal['takeProfit']

            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbol,
                "direction": direction,
                "entryPrice": entryPrice,
                "openTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": slPrice,
                "takeProfit": tpPrice,
                "size": posSize,
                "intervalo": symbolInfo.get('intervalo', ''),
                "status": "OPEN",
            }

            if account['idCuenta'] != 1:

                dbManager.buscaTrade(trade)

                await self.cleanupOldMessages(account['TokenMsg'], account['idGrupoMsg'])

                message = self._format_alert_message(signal, trade)

                msgId = await sendTelegramAlert(
                    account['TokenMsg'],
                    account['idGrupoMsg'],
                    message
                )

                if msgId:
                    self.lastMessageIds[symbol] = msgId
                    self.sentMessages.append({
                        "token": account['TokenMsg'],
                        "chatId": account['idGrupoMsg'],
                        "msgId": msgId,
                        "sentTime": datetime.now()
                    })

                    self.lastSignals[symbol] = {
                        "direction": signal['direction'],
                        "candle_time": signal['candle_time']
                    }

                    logger.info(f"✅ Señal enviada {symbol}")


    def _format_alert_message(self, signal: Dict, trade: Dict) -> str:
        return buildSMAAlertMessage(signal, trade)

    def _filtrar_velas_completas(self, df: pd.DataFrame, ahora_cdmx, interval: str) -> pd.DataFrame:
        interval_map = {
            '1min': 1,
            '5min': 5,
            '15min': 15,
            '30min': 30,
            '1h': 60,
            '2h': 120,
            '4h': 240,
            '1day': 1440,
            '1week': 10080,
        }
        
        minutes = interval_map.get(interval, 60)
        
        df_copy = df.copy()
        
        if df_copy.index.tzinfo is not None:
            df_copy.index = df_copy.index.tz_convert(ahora_cdmx.tzinfo)
        else:
            df_copy.index = df_copy.index.tz_localize(ahora_cdmx.tzinfo)
        
        cutoff_time = ahora_cdmx - pd.Timedelta(minutes=minutes)
        
        df_filtered = df_copy[df_copy.index <= cutoff_time].copy()
        
        return df_filtered

        

    async def runAnalysisCycle_for_symbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None):
        symbol = symbolInfo['symbol']
        
        if preloadedData:
            df = preloadedData.get(symbol)

        if df is None:
            logger.info(f"[SMA BOT] ❌ df es None")
            return

        cdmx_tz = pytz.timezone(TIMEZONE)
        ahora_cdmx = datetime.now(cdmx_tz)
        
        interval = symbolInfo.get('intervalo', '15min')
        df = self._filtrar_velas_completas(df, ahora_cdmx, interval)
        
        if len(df) < 50:
            logger.warning(f"[SMA BOT] [{symbol}] Menos de 50 velas completas. Saltando...")
            return
        
        logger.info(f"[SMA BOT] ✔ df OK ({len(df)} velas completas)")

        self.debug_log(symbol, f"✔ DF recibido: {len(df)} velas")
       
        _, _, _, nVelas, _ = getParametros()
       
        data = await self._getAndPrepareData(symbolInfo, apiKey, nVelas, interval, df)

        if data is None:
            return

        # 🔥 FIX: pasar intervalo y apiKey para multi-tiempo
        signal = await self._get_signal(data, symbol, interval, apiKey)

        if signal:
            direction = signal['direction']
            candleTime = signal['candle_time']

            # 🔥 CONTROL DUPLICADOS
            if self.esSenalDuplicada(symbol, direction, candleTime):
                logger.info(f"[{symbol}] Señal duplicada evitada.")
                return

            # 🔥 CARGAR CUENTAS
            if not self.accounts:
                self.accounts = dbManager.getAccount()
                logger.info(f"[SMA BOT] [{symbol}] Cuentas cargadas: {len(self.accounts)}")
            
            if not self.accounts:
                logger.warning(f"[SMA BOT] [{symbol}] No hay cuentas activas. No se enviarán alertas.")
                return

            await self._execute_trades(signal, symbolInfo)
