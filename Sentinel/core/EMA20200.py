import logging
import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any
import pandas as pd
import numpy as np
import talib as ta
import pytz
import os
import sys

# =========================
# PATH
# =========================
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

# =========================
# IMPORTS
# =========================
from middleware.api import twelvedata
from middleware.database import dbManager
from Sentinel.analysis import technical, risk
from Sentinel.analysis.technical import resample_to_interval, check_tp_exhaustion
from Sentinel.data.dataLoader import getParametros
from Sentinel.ml import model as mlModel
from middleware.config import constants as config
from middleware.utils.communications import sendTelegramAlert
from middleware.utils.alertBuilder import buildImbalanceLDNAlertMessage, buildImbalanceNYAlertMessage, adjustTPForMinRR, getPipMultiplier, calculateRR

from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle

logger = logging.getLogger("sentinel")

# =========================
# BOT
# =========================
class EMA20200Bot:

    def __init__(self):
        self.accounts = []
        self.openTrades = {}
        self.lastSignals = {}
        self.lastMessageIds = {}

        # config base
        self.interval = "5min"
        self.fast = 20
        self.slow = 200

        self.atrPeriod = 14
        self.minSlope = 5
        self.slopePeriods = 10
        self.minSeparationPct = 0.001

        # Mantenimiento estado pullback
        self.waitingPullback = {}
        self.pullbackTolerance = 0.0015
        
        # ML Genuino (importado de arquitectura general)
        self.model_clf = mlModel.loadModel(config.MODEL_FILE_PATH)
        if self.model_clf is None:
            logger.warning("No se pudo cargar el modelo ML")

        logger.info("Bot EMA + ML Genuino iniciado")

    def getMexicoTime(self) -> datetime:
        return datetime.now(pytz.timezone(TIMEZONE))

    # =========================
    # RESAMPLE HTF
    # =========================
    def resampleTo1H(self, df: pd.DataFrame) -> pd.DataFrame:
        return technical.resample_to_interval(df, '1h')

    # =========================
    # INDICADORES
    # =========================
    def ema(self, df, period):
        return ta.EMA(df['close'].values, timeperiod=period)

    def atr(self, df):
        return ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=self.atrPeriod)

    def slope(self, series):
        y = series.dropna().tail(self.slopePeriods).values
        if len(y) < self.slopePeriods:
            return 0
        x = np.arange(len(y))
        m, _ = np.polyfit(x, y, 1)
        return (m / np.mean(y)) * 100

    # =========================
    # CRUCE & PULLBACK
    # =========================
    def detectCross(self, emaFast, emaSlow):
        diff = emaFast - emaSlow
        crossUp = (diff > 0) & (diff.shift(1) <= 0)
        crossDown = (diff < 0) & (diff.shift(1) >= 0)

        if crossUp.iloc[-1]: return "LARGO"
        if crossDown.iloc[-1]: return "CORTO"
        return None

    def getHTFTrend(self, df1h):
        if len(df1h) < self.slow: return None
        ema200 = self.ema(df1h, self.slow)
        price = df1h['close'].iloc[-1]
        if price > ema200[-1]: return "LARGO"
        if price < ema200[-1]: return "CORTO"
        return None

    def isPullbackToEMA(self, price, ema_val):
        return abs(price - ema_val) / ema_val < self.pullbackTolerance

    # =========================
    # ML & SCORE
    # =========================
    def build_features(self, df: pd.DataFrame) -> pd.DataFrame:
        row = df.iloc[-1]
        features = {
            "close": row["close"],
            "atr": row["atr"],
            "atr_norm": row["atr"] / row["close"],
            "sma20": row["ema20"],
            "sma200": row["ema200"],
            "dist_sma20": (row["close"] - row["ema20"]) / row["close"],
            "dist_sma200": (row["close"] - row["ema200"]) / row["close"],
            "log_return": np.log(row["close"] / df["close"].iloc[-2]) if len(df) > 1 else 0,
            "range": (row["high"] - row["low"]) / row["close"],
            "sma_slope": self.slope(df["ema20"].tail(10))
        }
        return pd.DataFrame([features])

    def evaluateML(self, df: pd.DataFrame) -> float:
        if self.model_clf is None:
            return 0.55
        try:
            features = self.build_features(df)
            prob = self.model_clf.predict_proba(features)[0][1]
            return prob
        except Exception as e:
            logger.error(f"Error evaluating ML: {e}")
            return 0.55

    # =========================
    # EJECUCIÓN (SYNC)
    # =========================
    async def _executeTrades(self, signal: dict, symbolInfo: dict):
        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                logger.warning("No hay cuentas disponibles")
                return

        symbol = symbolInfo['symbol']
        
        for account in self.accounts:
            # Excluir cuenta maestra de señales (SENTINEL)
            if account['idCuenta'] == 1: continue
            if not dbManager.isEstrategiaHabilitadaParaCuenta(account['idCuenta'], "EMA20_200"):
                continue
                
            posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                capital=float(account['Capital']), 
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'], 
                symbolInfo=symbolInfo, 
                entryPrice=signal['entryPrice']
            )

            if posSize is None or posSize == 0:
                continue

            signal['profit'] = riskUsd
            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']
            # Garantizar RR mínimo de 1.5
            slPrice = entryPrice - slDist if direction == "LARGO" else entryPrice + slDist
            
            # TP estructural prioritario, con fallback a 2.0 RR
            tp_initial = signal.get('tpStructural')
            if not tp_initial:
                tp_initial = entryPrice + (slDist * 2) if direction == "LARGO" else entryPrice - (slDist * 2)
                
            tpPrice = adjustTPForMinRR(entryPrice, slPrice, tp_initial, direction, minRR=1.5)
            
            rr_actual = calculateRR(entryPrice, slPrice, tpPrice)
            multiplier = getPipMultiplier(symbol)
            
            min_distance_pips = 6.0
            min_distance_absolute = min_distance_pips / multiplier
            
            if slDist < min_distance_absolute:
                logger.info(f"[EMA20200] {symbol} rechazada: distancia SL muy pequeña ({slDist * multiplier:.1f} pips < {min_distance_pips} pips)")
                return
            
            tpDist = abs(tpPrice - entryPrice)
            if tpDist < min_distance_absolute:
                logger.info(f"[EMA20200] {symbol} rechazada: distancia TP muy pequeña ({tpDist * multiplier:.1f} pips < {min_distance_pips} pips)")
                return
            
            # Enriquecer señal con métricas para el mensaje
            signal['riesgo_pips'] = round(slDist * multiplier, 1)
            signal['rr_ratio'] = round(rr_actual, 2)

            trade = {
                "idCuenta": account['idCuenta'], "symbol": symbol, "direction": direction,
                "entryPrice": entryPrice, "openTime": self.getMexicoTime().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": slPrice, "takeProfit": tpPrice, "size": posSize,
                "intervalo": symbolInfo.get('intervalo', self.interval), "status": "OPEN",
                "strategy": "EMA20200", "margin_used": marginUsed,
            }

            # Ejecución centralizada vía Gateway (DB + Telegram + Broker)
            from middleware.execution.broker_gateway import gateway
            success, msgId = await gateway.execute_trade(trade, signal, account, "EMA20200", df=df)
            
            if success and msgId:
                self.lastMessageIds[symbol] = msgId
                    
        self.lastSignals[symbol] = signal['candle_time']

    # =========================
    # ANALYZE
    # =========================
    async def analyze(self, symbolInfo: Dict, preloadedData: Dict = None):
        symbol = symbolInfo['symbol']
        logger.info(f"▶ ENTRANDO análisis para {symbol}")

        try:
            if preloadedData and symbol in preloadedData:
                df = preloadedData[symbol]
                # Fallback resample si vienen en 1min
                if (df.index[-1] - df.index[-2]).total_seconds() / 60 < 5:
                    df = df[df.index.minute % 5 == 0].copy()
            else:
                apiKey, _, _, _, _ = getParametros()
                df = await twelvedata.getTimeSeries({
                    "symbol": symbol, "interval": self.interval, "apikey": apiKey, "outputSize": 500, "timezone": TIMEZONE
                })

            if df is None or len(df) < self.slow:
                return

            df = df.copy()
            df.index = pd.to_datetime(df.index)
            
            ema20 = pd.Series(self.ema(df, self.fast), index=df.index)
            ema200 = pd.Series(self.ema(df, self.slow), index=df.index)
            atr_series = pd.Series(self.atr(df), index=df.index)
            
            df["ema20"] = ema20
            df["ema200"] = ema200
            df["atr"] = atr_series
            
            df1h = self.resampleTo1H(df)
            htfTrend = self.getHTFTrend(df1h) if len(df1h) > self.slow else None

            directionCross = self.detectCross(ema20, ema200)
            price = df['close'].iloc[-1]
            ema20_last = ema20.iloc[-1]

            # Detect and save cross
            if directionCross:
                self.waitingPullback[symbol] = {"direction": directionCross, "active": True}
                logger.info(f"[{symbol}] Cruce EMA detectado ({directionCross}), esperando pullback...")
                return

            if symbol not in self.waitingPullback:
                logger.info(f"[{symbol}] Rechazada: Sin cruce EMA previo pendiente")
                return

            state = self.waitingPullback[symbol]
            direction = state["direction"]

            if htfTrend and direction != htfTrend:
                logger.info(f"[{symbol}] Rechazada: Tendencia HTF ({htfTrend}) contraria a dirección ({direction})")
                self.waitingPullback.pop(symbol, None)
                return

            if not self.isPullbackToEMA(price, ema20_last):
                logger.info(f"[{symbol}] Rechazada: Precio no ha retrocedido a EMA20 (precio={price:.5f}, ema20={ema20_last:.5f})")
                return

            # Feature Extract / Score
            slope_val = self.slope(ema20)
            separation = abs(ema20_last - ema200.iloc[-1]) / ema200.iloc[-1]
            atr_val = atr_series.iloc[-1]
            
            # ADX Filter: Verificar mercado con tendencia
            adx = ta.ADX(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
            adx_series = pd.Series(adx).dropna()
            adx_val = float(adx_series.iloc[-1]) if len(adx_series) > 0 else 25.0
            if adx_val < 20:
                logger.info(f"[{symbol}] Rechazada: Mercado lateral (ADX={adx_val:.1f} < 20)")
                return
            
            # Momentum Filter: Usar momentum pre-calculado desde main.py
            momentum_estado = symbolInfo.get('momentum', '☁️ SIN DATOS') if symbolInfo else '☁️ SIN DATOS'
            momentum_bonus = 0
            momentum_alineado = False
            
            if direction == "LARGO" and momentum_estado in ["🚀 ALCISTA", "💎 GIRO"]:
                momentum_bonus = 10
                momentum_alineado = True
            elif direction == "CORTO" and momentum_estado in ["📉 BAJISTA"]:
                momentum_bonus = 10
                momentum_alineado = True
            elif momentum_estado in ["💸 LIQUIDACIÓN", "🌋 PARÁBOLA"]:
                momentum_bonus = -5  # Señal debil
            
            logger.info(f"[{symbol}] Momentum: {momentum_estado} → {'+' if momentum_bonus > 0 else ''}{momentum_bonus}% confianza")
            
            # Simple filters
            if abs(slope_val) < 0.5 or separation < self.minSeparationPct:
                logger.info(f"[{symbol}] Filtros EMA básicos insuficientes")
                return
                
            # ML Filter
            prob = self.evaluateML(df)
            
            distanciaSma20Pct = abs(price - ema20_last) / price * 100
            atrRelativo = atr_val / price * 100
            threshold = 0.40 if distanciaSma20Pct < atrRelativo * 0.5 else 0.50

            if prob < threshold:
                logger.info(f"[{symbol}] ❌ Filtrado ML | prob={prob:.2f} < {threshold}")
                return
                
            logger.info(f"[{symbol}] ✓ ML OK | prob={prob:.2f}")

            # Construir Signal con niveles estructurales (sensibilidad aumentada)
            levels = technical.get_structural_levels(df, lookback=40)
            atr_padding = atr_val * 0.2
            
            if direction == "LARGO":
                sl_price = levels['swing_low'] - atr_padding
                # Asegurar que el SL no sea ridículamente pequeño o grande
                sl_dist = max(atr_val * 0.8, min(price - sl_price, atr_val * 2.5))
                tp_structural = levels['high_zone']
            else:
                sl_price = levels['swing_high'] + atr_padding
                sl_dist = max(atr_val * 0.8, min(sl_price - price, atr_val * 2.5))
                tp_structural = levels['low_zone']

            # --- SEMÁFORO DE ENTRADA (Price Action) ---
            total_dist = abs(tp_structural - price)
            # Al ser el momento de la detección, el progreso es inicial (0%)
            progress_pct = 0
            
            status_msg = "EN ZONA ✅"
            if progress_pct > 100: status_msg = "META ALCANZADA 🚨"
            elif progress_pct > 50: status_msg = "ALEJÁNDOSE ⚠️"

            now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
            last_closed = get_last_closed_candle(now_cdmx, interval=5)
            signal = {
                "direction": direction,
                "entryPrice": price,
                "slDistance": sl_dist,
                "tpStructural": tp_structural,
                "candle_time": last_closed.strftime("%Y-%m-%d %H:%M:%S"),
                "status": status_msg,
                "slope": slope_val,
                "separation": separation,
                "confidence": int(prob * 100) + momentum_bonus,
                "momentum": momentum_estado,
                "setup": "EMA Pullback"
            }
            
            # ── FILTRO: Verificar si el precio ya recorrió >60% hacia el TP ──
            vela_origen_idx = len(df) - 5  # Usar vela actual como origen para pullback
            is_valid, recorrido_pct, _ = check_tp_exhaustion(df, vela_origen_idx, price, tp_structural, direction, threshold=0.60)
            if not is_valid:
                logger.info(f"[{symbol}] Señal descartada: Precio ya recorrió {recorrido_pct*100:.1f}% hacia TP (umbral: 60%)")
                return
            
            if symbol in self.lastSignals and self.lastSignals[symbol] == signal['candle_time']:
                logger.info(f"[{symbol}] Rechazada: Señal duplicada para misma vela")
                return

            await self._executeTrades(signal, symbolInfo)
            
            # Limpiar pullback al gatillarse
            self.waitingPullback.pop(symbol, None)
            
        except Exception as e:
            logger.error(f"Error analizando {symbol}: {e}")
        finally:
            logger.info(f"◀ SALIENDO análisis para {symbol}")