import logging
import asyncio
from datetime import datetime
from typing import Dict
import pandas as pd
import numpy as np
import talib as ta
import pytz
import os
import sys
import random

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
from middleware.utils.communications import sendTelegramAlert
from middleware.utils.alertBuilder import buildSMAAlertMessage
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

        # config base
        self.interval = "5min"
        self.fast = 20
        self.slow = 200

        self.atrPeriod = 14
        self.minSlope = 5
        self.slopePeriods = 10
        self.minSeparationPct = 0.001

        # NUEVO (pullback)
        self.waitingPullback = {}
        self.pullbackTolerance = 0.0015

        logger.info("[EMA INTEGRATED BOT + QUANT] iniciado")

    # =========================
    # RESAMPLE HTF
    # =========================
    def resampleTo1H(self, df):
        df = df.copy()
        df.index = pd.to_datetime(df.index)

        df1h = df.resample('1H', label='right', closed='right').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last'
        })

        if df.index[-1] < df1h.index[-1]:
            df1h = df1h.iloc[:-1]

        return df1h.dropna()

    # =========================
    # INDICADORES
    # =========================
    def ema(self, df, period):
        return ta.EMA(df['close'], timeperiod=period)

    def atr(self, df):
        return ta.ATR(df['high'], df['low'], df['close'], timeperiod=self.atrPeriod)

    def slope(self, series):
        y = series.dropna().tail(self.slopePeriods).values
        if len(y) < self.slopePeriods:
            return 0
        x = np.arange(len(y))
        m, _ = np.polyfit(x, y, 1)
        return (m / np.mean(y)) * 100

    # =========================
    # CRUCE
    # =========================
    def detectCross(self, emaFast, emaSlow):
        diff = emaFast - emaSlow
        crossUp = (diff > 0) & (diff.shift(1) <= 0)
        crossDown = (diff < 0) & (diff.shift(1) >= 0)

        if crossUp.iloc[-1]:
            return "LARGO"
        if crossDown.iloc[-1]:
            return "CORTO"
        return None

    # =========================
    # MTF TREND
    # =========================
    def getHTFTrend(self, df1h):
        ema200 = self.ema(df1h, self.slow)
        price = df1h['close'].iloc[-1]

        if price > ema200.iloc[-1]:
            return "LARGO"
        if price < ema200.iloc[-1]:
            return "CORTO"
        return None

    # =========================
    # PULLBACK
    # =========================
    def isPullbackToEMA(self, price, ema):
        return abs(price - ema) / ema < self.pullbackTolerance

    # =========================
    # SCORING (ML SIMPLE)
    # =========================
    def scoreSignal(self, slope, atr, separation):
        score = 0

        if abs(slope) > 5:
            score += 1
        if atr > 0:
            score += 1
        if separation > self.minSeparationPct:
            score += 1

        return score

    # =========================
    # BACKTEST ENGINE
    # =========================
    def runBacktest(self, df):

        balance = 1000
        trades = []

        ema20 = self.ema(df, self.fast)
        ema200 = self.ema(df, self.slow)
        atr = self.atr(df)

        position = None

        for i in range(200, len(df)):

            price = df['close'].iloc[i]

            direction = None
            if ema20.iloc[i] > ema200.iloc[i] and ema20.iloc[i-1] <= ema200.iloc[i-1]:
                direction = "LARGO"
            elif ema20.iloc[i] < ema200.iloc[i] and ema20.iloc[i-1] >= ema200.iloc[i-1]:
                direction = "CORTO"

            if position is None and direction:

                sl = price - atr.iloc[i]*1.5 if direction=="LARGO" else price + atr.iloc[i]*1.5
                tp = price + atr.iloc[i]*2 if direction=="LARGO" else price - atr.iloc[i]*2

                position = {
                    "dir": direction,
                    "entry": price,
                    "sl": sl,
                    "tp": tp
                }

            if position:
                high = df['high'].iloc[i]
                low = df['low'].iloc[i]

                closed = False

                if position["dir"] == "LARGO":
                    if low <= position["sl"]:
                        pnl = position["sl"] - position["entry"]
                        closed = True
                    elif high >= position["tp"]:
                        pnl = position["tp"] - position["entry"]
                        closed = True
                else:
                    if high >= position["sl"]:
                        pnl = position["entry"] - position["sl"]
                        closed = True
                    elif low <= position["tp"]:
                        pnl = position["entry"] - position["tp"]
                        closed = True

                if closed:
                    balance += pnl
                    trades.append(pnl)
                    position = None

        winrate = sum(1 for t in trades if t > 0) / len(trades) if trades else 0

        return {
            "balance": balance,
            "trades": len(trades),
            "winrate": winrate
        }

    # =========================
    # OPTIMIZADOR
    # =========================
    def optimizeParameters(self, df):

        best = None
        bestScore = -999

        for fast in [10,20,30]:
            for slope in [3,5,8]:
                self.fast = fast
                self.minSlope = slope

                result = self.runBacktest(df)
                score = result["balance"]

                if score > bestScore:
                    bestScore = score
                    best = (fast, slope)

        logger.info(f"[OPTIMIZER] Mejor config: EMA{best[0]} slope={best[1]} balance={bestScore}")

        return best

    # =========================
    # ANALYZE
    # =========================
    async def analyze(self, symbolInfo: Dict, preloadedData: Dict = None):

        symbol = symbolInfo['symbol']

        try:
            if preloadedData and symbol in preloadedData:
                df = preloadedData[symbol]
            else:
                apiKey, _, _, _, _ = getParametros()
                df = await twelvedata.getTimeSeries({
                    "symbol": symbol,
                    "interval": self.interval,
                    "apikey": apiKey,
                    "outputSize": 500
                })

            if df is None or len(df) < 300:
                return

            df = technical.calculateFeatures(df)
            df.index = pd.to_datetime(df.index)

            df1h = self.resampleTo1H(df)
            htfTrend = self.getHTFTrend(df1h)

            ema20 = self.ema(df, self.fast)
            ema200 = self.ema(df, self.slow)

            direction = self.detectCross(ema20, ema200)

            price = df['close'].iloc[-1]
            ema20_last = ema20.iloc[-1]

            if direction:
                self.waitingPullback[symbol] = {"direction": direction, "active": True}
                return

            if symbol not in self.waitingPullback:
                return

            state = self.waitingPullback[symbol]
            direction = state["direction"]

            if direction != htfTrend:
                self.waitingPullback.pop(symbol, None)
                return

            if not self.isPullbackToEMA(price, ema20_last):
                return

            slope = self.slope(ema20)
            separation = abs(ema20_last - ema200.iloc[-1]) / ema200.iloc[-1]
            atr = self.atr(df).iloc[-1]

            score = self.scoreSignal(slope, atr, separation)

            if score < 2:
                logger.info(f"[{symbol}] SCORE BAJO {score}")
                return

            logger.info(f"[{symbol}] SCORE OK {score}")

            self.waitingPullback.pop(symbol, None)

        except Exception as e:
            logger.error(f"{symbol} error: {e}", exc_info=True)

    # =========================
    # LOOP
    # =========================
    async def run(self, symbols):

        while True:
            tasks = [self.analyze(s) for s in symbols]
            await asyncio.gather(*tasks)
            await asyncio.sleep(60)