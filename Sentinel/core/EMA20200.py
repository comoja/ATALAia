import logging
import asyncio
from datetime import datetime
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
from Sentinel.data.dataLoader import getParametros
from Sentinel.ml import model as mlModel
from middleware.config import constants as config
from middleware.utils.communications import sendTelegramAlert
from middleware.utils.alertBuilder import buildAlertMessage
from middleware.config.constants import TIMEZONE

logger = logging.getLogger(__name__)

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
            logger.warning("[EMA BOT] No se pudo cargar el modelo ML")

        logger.info("[EMA INTEGRATED BOT + ML Genuino] iniciado")

    def getMexicoTime(self) -> datetime:
        return datetime.now(pytz.timezone(TIMEZONE))

    # =========================
    # RESAMPLE HTF
    # =========================
    def resampleTo1H(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)

        df1h = df.resample('1h', label='right', closed='right').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
        })
        if df.index[-1] < df1h.index[-1]:
            df1h = df1h.iloc[:-1]
        return df1h.dropna()

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
            logger.error(f"[EMA BOT] Error evaluating ML: {e}")
            return 0.55

    # =========================
    # EJECUCIÓN (SYNC)
    # =========================
    async def _executeTrades(self, signal: dict, symbolInfo: dict):
        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                logger.warning("[EMA BOT] No hay cuentas disponibles")
                return

        symbol = symbolInfo['symbol']
        
        for account in self.accounts:
            if not dbManager.isEstrategiaHabilitadaParaCuenta(account['idCuenta'], "EMA20200"):
                continue
                
            posSize, _, marginUsed = risk.calculatePositionSize(
                capital=float(account['Capital']), 
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'], 
                symbolInfo=symbolInfo, 
                entryPrice=signal['entryPrice']
            )

            if posSize is None or posSize == 0:
                continue

            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']
            slPrice = entryPrice - slDist if direction == "LARGO" else entryPrice + slDist
            
            # Simple 1:2 R:R as default for missing regression ML
            tpPrice = entryPrice + (slDist * 2) if direction == "LARGO" else entryPrice - (slDist * 2)

            trade = {
                "idCuenta": account['idCuenta'], "symbol": symbol, "direction": direction,
                "entryPrice": entryPrice, "openTime": self.getMexicoTime().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": slPrice, "takeProfit": tpPrice, "size": posSize,
                "intervalo": symbolInfo.get('intervalo', self.interval), "status": "OPEN",
                "strategy": "EMA20200", "margin_used": marginUsed,
            }

            if account['idCuenta'] != 1:
                dbManager.buscaTrade(trade)
                message = buildAlertMessage(
                    strategy_name="EMA20200", signal_dir=direction, symbol=symbol, 
                    entry=entryPrice, sl=slPrice, tp=tpPrice, timeframe=self.interval
                )
                msgId = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                if msgId:
                    self.lastMessageIds[symbol] = msgId
                    
        self.lastSignals[symbol] = signal['candle_time']

    # =========================
    # ANALYZE
    # =========================
    async def analyze(self, symbolInfo: Dict, preloadedData: Dict = None):
        symbol = symbolInfo['symbol']

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
                return

            if symbol not in self.waitingPullback:
                return

            state = self.waitingPullback[symbol]
            direction = state["direction"]

            if htfTrend and direction != htfTrend:
                self.waitingPullback.pop(symbol, None)
                return

            if not self.isPullbackToEMA(price, ema20_last):
                return

            # Feature Extract / Score
            slope_val = self.slope(ema20)
            separation = abs(ema20_last - ema200.iloc[-1]) / ema200.iloc[-1]
            atr_val = atr_series.iloc[-1]
            
            # Simple filters
            if abs(slope_val) < 1 or separation < self.minSeparationPct:
                logger.debug(f"[{symbol}] Filtros EMA básicos insuficientes")
                return
                
            # ML Filter
            prob = self.evaluateML(df)
            
            distanciaSma20Pct = abs(price - ema20_last) / price * 100
            atrRelativo = atr_val / price * 100
            threshold = 0.40 if distanciaSma20Pct < atrRelativo * 0.5 else 0.50

            if prob < threshold:
                logger.info(f"[{symbol}] ❌ EMA20200 Filtrado ML | prob={prob:.2f} < {threshold}")
                return
                
            logger.info(f"[{symbol}] ✓ EMA ML OK | prob={prob:.2f}")

            # Construir Signal
            sl_dist = atr_val * 1.5
            signal = {
                "direction": direction,
                "entryPrice": price,
                "slDistance": sl_dist,
                "candle_time": df.index[-1].strftime("%Y-%m-%d %H:%M:%S")
            }
            
            if symbol in self.lastSignals and self.lastSignals[symbol] == signal['candle_time']:
                return

            await self._executeTrades(signal, symbolInfo)
            
            # Limpiar pullback al gatillarse
            self.waitingPullback.pop(symbol, None)
            
        except Exception as e:
            logger.error(f"[EMA BOT] Error analizando {symbol}: {e}")