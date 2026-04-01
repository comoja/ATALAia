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

        # config
        self.interval = "5min"
        self.fast = 20
        self.slow = 200

        self.atrPeriod = 14
        self.minSlope = 10
        self.slopePeriods = 10
        self.minSeparationPct = 0.001

        logger.info("[EMA INTEGRATED BOT] iniciado")

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
    # FILTRO ANTI-RANGO
    # =========================
    def validSeparation(self, emaFast, emaSlow):
        diff = abs(emaFast.iloc[-1] - emaSlow.iloc[-1])
        return (diff / emaSlow.iloc[-1]) > self.minSeparationPct

    # =========================
    # DUPLICADOS
    # =========================
    def isDuplicate(self, symbol, direction, candleTime):
        last = self.lastSignals.get(symbol)
        if not last:
            return False
        return last["direction"] == direction and last["time"] == candleTime

    # =========================
    # TRAILING STOP
    # =========================
    def updateTrailing(self, trade, price, atr):
        if trade['direction'] == "LARGO":
            newSL = price - atr * 1.2
            if newSL > trade['sl']:
                trade['sl'] = newSL
        else:
            newSL = price + atr * 1.2
            if newSL < trade['sl']:
                trade['sl'] = newSL

    # =========================
    # EJECUCION (HOOK)
    # =========================
    async def executeOrder(self, signal, account):
        logger.info(f"[ORDER] {signal['symbol']} {signal['direction']} ejecutada")

    # =========================
    # ANALISIS PRINCIPAL
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

            # ===== HTF =====
            df1h = self.resampleTo1H(df)
            if len(df1h) < 200:
                return

            ema200HTF = self.ema(df1h, self.slow)
            trend = "LARGO" if df1h['close'].iloc[-1] > ema200HTF.iloc[-1] else "CORTO"

            # ===== LTF =====
            emaFast = self.ema(df, self.fast)
            emaSlow = self.ema(df, self.slow)

            direction = self.detectCross(emaFast, emaSlow)
            if not direction or direction != trend:
                return

            if not self.validSeparation(emaFast, emaSlow):
                return

            if abs(self.slope(emaFast)) < self.minSlope:
                return

            atrSeries = self.atr(df)
            atr = atrSeries.iloc[-1]
            atrAvg = atrSeries.tail(20).mean()

            if atr < atrAvg:
                return

            candleTime = df.index[-1]

            if self.isDuplicate(symbol, direction, candleTime):
                return

            price = df['close'].iloc[-1]

            # ===== TRADE ACTIVO =====
            if symbol in self.openTrades:
                self.updateTrailing(self.openTrades[symbol], price, atr)
                return

            # ===== NUEVO TRADE =====
            if direction == "LARGO":
                sl = price - atr * 1.5
                tp = price + atr * 2.0
            else:
                sl = price + atr * 1.5
                tp = price - atr * 2.0

            slDistance = abs(price - sl)
            if slDistance == 0:
                return

            if not self.accounts:
                self.accounts = dbManager.getAccount()

            account = self.accounts[0]

            size, _, _ = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=slDistance,
                symbolInfo=symbolInfo,
                entryPrice=price
            )

            if not size:
                return

            signal = {
                "symbol": symbol,
                "direction": direction,
                "entryPrice": price,
                "stopLoss": sl,
                "takeProfit": tp,
                "size": size
            }

            await self.executeOrder(signal, account)

            self.openTrades[symbol] = {
                "direction": direction,
                "entry": price,
                "sl": sl,
                "tp": tp
            }

            await sendTelegramAlert(
                account['TokenMsg'],
                account['idGrupoMsg'],
                buildSMAAlertMessage(signal, {})
            )

            self.lastSignals[symbol] = {
                "direction": direction,
                "time": candleTime
            }

            logger.info(f"[EMA BOT] {symbol} {direction}")

        except Exception as e:
            logger.error(f"{symbol} error: {e}", exc_info=True)

    # =========================
    # LOOP
    # =========================
    async def run(self, symbols):

        while True:
            tasks = [self.analyze(s) for s in symbols]
            await asyncio.gather(*tasks)

            # sincronización simple (cada 60s)
            await asyncio.sleep(60)