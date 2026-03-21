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

from middlend.api import twelvedata
from middlend.analysis import technical, risk
from middlend.ml import model as mlModel
from middlend.core.communications import sendTelegramAlert, alertaInmediata
from middlend import configConstants as config
from middlend.database import dbManager
from middlend.scheduler.autoScheduler import getTiempoEspera, isRestTime
from middlend.data.dataLoader import getParametros

logger = logging.getLogger(__name__)


class SMABot:
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}
        self.lastSignals = {}  # {symbol: {direction, candle_time}}

    def debug_log(self, symbol, msg):
        logger.info(f"[SMA DEBUG] [{symbol}] {msg}")

    def getPendiente(self, serie, periodos=3):
        y = serie.iloc[-periodos:].values
        x = np.arange(periodos)
        if len(y) < periodos:
            return 0
        m, b = np.polyfit(x, y, 1)
        return m
    
    def es_senal_duplicada(self, symbol, direction, candle_time):
        """
        Evita enviar la misma señal en la misma vela
        """
        
        if symbol not in self.lastSignals:
            return False
        
        last = self.lastSignals[symbol]
        
        if last["direction"] == direction and last["candle_time"] == candle_time:
            return True
        
        return False

    def detectar_rebote_sma_doble(self, df, sma20, intervalo, symbol=None):
        """
        Doble toque de MECHAS en la misma dirección, separados por al menos 1 vela terminada.
        Toma las últimas 4 velas y filtra las que tocan el SMA20.
        Los datos de 12Data ya vienen en CDT gracias al parametro timezone.
        Retorna (direction, double_touch_time) o (None, None) si no hay doble toque válido.
        """

        cdmx_tz = pytz.timezone("America/Mexico_City")
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
            elif i >= len(df) - 10:
                toca_low = low <= sma
                toca_high = high >= sma
                logger.debug(f"[DEBUG] vela {candle_time_cdmx.strftime('%H:%M')} | {'VERDE' if is_green else 'ROJO'} | toca_low={toca_low} dist_low={dist_low:.2f} | toca_high={toca_high} dist_high={dist_high:.2f} | tol={tolerancia_pips:.2f} max={max_wick_pips:.2f}")

            if toque_direccion:
                touches.append({
                    "idx": i,
                    "direccion": toque_direccion,
                    "time_cdmx": candle_time_cdmx,
                    "low": low,
                    "high": high,
                    "sma": sma,
                    "dist": toque_dist
                })
                color = "VERDE" if is_green else "ROJO"
                logger.info(
                    f"[TOQUE MECHA] {ahora_cdmx.strftime('%H:%M:%S')} | vela({candle_time_cdmx.strftime('%H:%M')}) | {color} | {toque_direccion} | dist={toque_dist:.2f} tol={tolerancia_pips:.2f} max={max_wick_pips:.2f}"
                )

        if len(touches) < 2:
            logger.info(f"[INFO] {ahora_cdmx.strftime('%H:%M:%S')} | Velas con toque: {len(touches)} (< 2)")
            return None, None

        for idx in range(len(touches) - 1, 0, -1):
            t1 = touches[idx - 1]
            t2 = touches[idx]

            if t1["direccion"] != t2["direccion"]:
                continue

            separacion = t2["idx"] - t1["idx"]

            if separacion < 1:
                continue

            double_touch_time = t2["time_cdmx"]

            """logger.info(
                f"[DOBLE TOQUE] {ahora_cdmx.strftime('%H:%M:%S')} | "
                f"t1({t1['time_cdmx'].strftime('%H:%M')})->t2({t2['time_cdmx'].strftime('%H:%M')}) | "
                f"{t1['direccion']} | sep={separacion}"
            )"""

            vela_actual = df.iloc[-1]
            close_actual = vela_actual["close"]

            if t1["direccion"] == "ALCISTA":
                if close_actual > sma20:
                    return "LARGO", double_touch_time
                """else:
                    logger.info(
                        f"[DOBLE TOQUE] Condición no cumplida LARGO: "
                        f"close={close_actual:.2f} {'<' if close_actual < sma20 else '>'} sma20={sma20:.2f}"
                    )"""
            else:
                if close_actual < sma20:
                    return "CORTO", double_touch_time
                """ else:
                    logger.info(
                        f"[DOBLE TOQUE] Condición no cumplida CORTO: "
                        f"close={close_actual:.2f} {'>' if close_actual > sma20 else '<'} sma20={sma20:.2f}"
                    )"""

        return None, None
    
    def identificar_tendencia(self, df, precio_actual, sma20):
        pendiente_sma20 = self.getPendiente(df["sma20"].tail(10), 10)
        
        slope_threshold = 0.0002
        
        if precio_actual > sma20 and pendiente_sma20 > slope_threshold:
            return "ALCISTA"
        elif precio_actual < sma20 and pendiente_sma20 < -slope_threshold:
            return "BAJISTA"
        return "NEUTRAL"

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

    async def _get_and_prepare_data(self, symbolInfo: Dict, apiKey: str, nVelas: int, interval: str, raw_df: pd.DataFrame = None) -> pd.DataFrame | None:

        symbol = symbolInfo['symbol']

        if raw_df is not None and len(raw_df) >= 200:
            df = raw_df.copy()
        else:
            df = await twelvedata.getTimeSeries(symbol, interval, apiKey, nVelas)
            if df is None or len(df) < 200:
                logger.warning(f"[{symbol}] Datos insuficientes para análisis SMA20-200 ({len(df) if df is not None else 0} velas).")
                return None
        
        if "sma20" not in df.columns:
            df["sma20"] = ta.SMA(df["close"], timeperiod=20)
        if "sma200" not in df.columns:
            df["sma200"] = ta.SMA(df["close"], timeperiod=200)
        if "atr" not in df.columns:
            df["atr"] = ta.ATR(df["high"], df["low"], df["close"], 14)
        
        df = df.dropna()
        
        if len(df) < 50:
            logger.warning(f"[{symbol}] Datos insuficientes tras calcular SMAs.")
            return None
        
        return df
    

    async def _get_signal(self, df: pd.DataFrame, symbol: str, intervalo: str):
        cdmx_tz = pytz.timezone("America/Mexico_City")
        ahora_cdmx = datetime.now(cdmx_tz)
        
        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(cdmx_tz)
        elif df.index.tzinfo != cdmx_tz:
            df.index = df.index.tz_convert(cdmx_tz)
        
        interval_map = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '1h': 60, '2h': 120, '4h': 240}
        interval_minutes = interval_map.get(intervalo, 60)
        
        ultima_vela_cdmx = df.index[-1]
        vela_end_time = ultima_vela_cdmx + pd.Timedelta(minutes=interval_minutes)
        delay_total = ahora_cdmx - vela_end_time
        delay_minutos = delay_total.total_seconds() / 60
        
        if delay_minutos < 2:
            delay_minutos = 0
        
        if delay_minutos > 60:
            logger.warning(f"⚠️ [{symbol}] DATOS CON DELAY: {delay_minutos/60:.1f}h | vela({ultima_vela_cdmx.strftime('%H:%M')}) actual({ahora_cdmx.strftime('%H:%M:%S')})")
        elif delay_minutos > 2:
            logger.warning(f"⚠️ [{symbol}] DATOS CON DELAY: {delay_minutos:.1f}min | vela({ultima_vela_cdmx.strftime('%H:%M')}) actual({ahora_cdmx.strftime('%H:%M:%S')})")
        
        candle_time = df.index[-1]
        close = df["close"].iloc[-1]
        sma20 = df["sma20"].iloc[-1]
        sma200 = df["sma200"].iloc[-1]
        atr = df["atr"].iloc[-1]

        tendencia = self.identificar_tendencia(df, close, sma20)

        if tendencia == "NEUTRAL":
            return None

        # 🔥 FILTRO DISTANCIA MÍNIMA AL SMA20
        distancia_pct = abs(close - sma20) / close * 100
        umbral_distancia = 0.1
        
        if distancia_pct < umbral_distancia:
            logger.info(f"[{symbol}] Precio muy cerca del SMA20 ({distancia_pct:.3f}% < {umbral_distancia}%) - lateral descartado")
            return None

        # 🔥 FILTRO LATERALIDAD (ATR dinámico)
        rango = (df["high"].tail(20).max() - df["low"].tail(20).min()) / close
        umbral = (atr / close) * 3

        if rango < umbral:
            logger.info(f"[{symbol}] Mercado lateral - descartado")
            return None

        # 🔥 FILTRO SMA200 (deshabilitado para estrategia SMA20 only)
        # if abs(close - sma200) / close < 0.02:
        #     return None

        # 🔥 REBOTE SMA20 DOBLE
        direction, double_touch_time = self.detectar_rebote_sma_doble(df, sma20, intervalo, symbol)

        if not direction:
            return None

        # 🔥 FILTRO: Ignorar señales con más de 1 hora de antigüedad
        if double_touch_time:
            signal_age = ahora_cdmx - double_touch_time
            signal_age_minutes = signal_age.total_seconds() / 60
            if signal_age_minutes > 60:
                # logger.warning(f"⚠️ [{symbol}] SEÑAL DESCARTADA: Doble toque tiene {signal_age_minutes:.0f}min de antigüedad (>1h)")
                return None

        # 🔥 FILTRO DIRECCIÓN vs SMA200
        if direction == "LARGO" and close < sma200:
            return None

        if direction == "CORTO" and close > sma200:
            return None

        # 🔥 FILTRO: Velas recientes contrarias
        velas_recientes = 3
        velas_contrarias = 0
        for i in range(-velas_recientes, 0):
            body = df["close"].iloc[i] - df["open"].iloc[i]
            if direction == "LARGO" and body < 0:
                velas_contrarias += 1
            elif direction == "CORTO" and body > 0:
                velas_contrarias += 1
        
        if velas_contrarias >= 2:
            logger.info(f"[{symbol}] Señal rechazada: {velas_contrarias}/{velas_recientes} velas recientes contrarias")
            return None

        # 🎯 SL / TP
        if direction == "LARGO":
            stop_loss = df["low"].tail(3).min() * 0.998
            sl_dist = close - stop_loss
            take_profit = close + sl_dist * 2.5
        else:
            stop_loss = df["high"].tail(3).max() * 1.002
            sl_dist = stop_loss - close
            take_profit = close - sl_dist * 2.5

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
            "confidence": 85,
            "symbol": symbol,
            "candle_time": candle_time,
            "sma20": sma20,
            "sma200": sma200,
            "atr": atr,
            "setup": "Doble Toque SMA20",
            "tendencia": tendencia
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

                message = self._format_alert_message(signal, trade)

                msgId = await sendTelegramAlert(
                    account['TokenMsg'],
                    account['idGrupoMsg'],
                    message
                )

                # 🔥 SOLO guardar si se envió
                if msgId:
                    self.lastMessageIds[symbol] = msgId

                    self.lastSignals[symbol] = {
                        "direction": signal['direction'],
                        "candle_time": signal['candle_time']
                    }

                    logger.info(f"✅ Señal enviada {symbol}")


    def _format_alert_message(self, signal: Dict, trade: Dict) -> str:
        direction = signal['direction']
        directionStr = "COMPRA" if direction == "LARGO" else "VENTA"
        colorHeader = "🟩" if direction == "LARGO" else "🟥"
        
        close = signal['entryPrice']
        tp = trade['takeProfit']
        sl = trade['stopLoss']
        confianza = signal['confidence']
        setup = signal['setup']
        tendencia = signal['tendencia']
        
        text = (
            f"{colorHeader}{colorHeader}{colorHeader} "
            f"<b>SEÑAL DE {directionStr}</b> "
            f"{colorHeader}{colorHeader}{colorHeader}\n"
            f"<center><i>Estrategia: SMA20-200</i></center>\n"
            f"<center><b>{trade['symbol']}</b> ({trade['intervalo']})</center>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Setup: <b>{setup}</b></center>\n"
            f"<center>Tendencia: <b>{tendencia}</b></center>\n"
            f"<center>Confianza: <b>{confianza}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔹 ENTRADA:   <b>{close:,.6f}</b>\n"
            f"🔴 SL: <b>{sl:,.6f}</b>\n"
            f"🟢 TP: <b>{tp:,.6f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"• SMA20: {signal['sma20']:.6f}\n"
            f"• SMA200: {signal['sma200']:.6f}\n"
            f"• ATR: {signal['atr']:.6f}\n"
            f"• Cantidad: <b>{trade['size']:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
        return text

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

    async def runAnalysisCycle_for_symbol(self, symbolInfo: Dict, preloaded_data: Dict = None, apiKey: str = None):
        # logger.info("🔥 ENTRANDO A SMA BOT")
        symbol = symbolInfo['symbol']
        # logger.info(f"[SMA BOT] INICIO {symbol}")

        self.debug_log(symbol, "==== INICIO ANALISIS SMA ====")

        df = preloaded_data.get(symbol)

        if df is None:
            logger.info(f"[SMA BOT] ❌ df es None")
            return

        cdmx_tz = pytz.timezone("America/Mexico_City")
        ahora_cdmx = datetime.now(cdmx_tz)
        
        interval = symbolInfo.get('intervalo', '15min')
        df = self._filtrar_velas_completas(df, ahora_cdmx, interval)
        
        if len(df) < 50:
            logger.warning(f"[SMA BOT] [{symbol}] Menos de 50 velas completas. Saltando...")
            return
        
        logger.info(f"[SMA BOT] ✔ df OK ({len(df)} velas completas)")

        self.debug_log(symbol, f"✔ DF recibido: {len(df)} velas")
       
        _, _, _, nVelas, _ = getParametros()
       
        data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval, df)

        if data is None:
            return

        # 🔥 FIX: pasar intervalo
        signal = await self._get_signal(data, symbol, interval)

        if signal:
            direction = signal['direction']
            candle_time = signal['candle_time']

            # 🔥 CONTROL DUPLICADOS
            if self.es_senal_duplicada(symbol, direction, candle_time):
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
"""
    async def runAnalysisCycle_for_symbol(self, symbolInfo: Dict, preloaded_data: Dict = None, apiKey: str = None):
        if not self.accounts:
            return
        
        symbol = symbolInfo['symbol']
        interval = symbolInfo.get('intervalo', '1h')
        if apiKey is None:
            apiKey, _, _, nVelas, _ = getParametros()
        else:
            _, _, _, nVelas, _ = getParametros()
        
        logger.debug(f"Analizando {symbol} con SMA20-200 en intervalo {interval}...")

        if preloaded_data and symbol in preloaded_data:
            data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval, preloaded_data[symbol])
        else:
            data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval)
        
        if data is None:
            return
        
        signal = await self._get_signal(data, symbol)
        if signal:
            symbol = symbolInfo['symbol']
            direction = signal['direction']
            candle_time = data.index[-1]
            
            if self.es_senal_duplicada(symbol, direction, candle_time):
                logger.info(f"[{symbol}] Señal duplicada evitada.")
                return
            logger.info(f"[{symbol}] Señal SMA20-200: {signal['direction']} ({signal['confidence']}% confianza)")
            await self._execute_trades(signal, symbolInfo)
        else:
            logger.debug(f"[{symbol}] Sin señal SMA20-200 en intervalo {interval}.")
"""