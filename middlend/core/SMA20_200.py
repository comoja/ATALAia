"""
SMA20-200 Trading Strategy Bot
Strategy based on SMA 20 periods as trigger and SMA 200 as trend filter.
"""
import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd
import numpy as np
import talib as ta
import asyncio
import os
import sys

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

    def getPendiente(self, serie, periodos=3):
        y = serie.iloc[-periodos:].values
        x = np.arange(periodos)
        if len(y) < periodos:
            return 0
        m, b = np.polyfit(x, y, 1)
        return m

    def identificar_tendencia(self, df, precio_actual, sma20):
        pendiente_sma20 = self.getPendiente(df["close"].tail(20), 3)
        if precio_actual > sma20 and pendiente_sma20 > 0:
            return "ALCISTA"
        elif precio_actual < sma20 and pendiente_sma20 < 0:
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

    async def _get_signal(self, df: pd.DataFrame, symbol: str) -> Dict[str, Any] | None:
        close = df["close"].iloc[-1]
        sma20 = df["sma20"].iloc[-1]
        sma200 = df["sma200"].iloc[-1]
        atr_val = df["atr"].iloc[-1]
        
        tendencia = self.identificar_tendencia(df, close, sma20)
        
        if tendencia == "NEUTRAL":
            logger.info(f"[{symbol}] Tendencia neutral - No hay operación.")
            return None
        
        sma200_cerca = self.validar_sma200_cercano(close, sma200)
        if sma200_cerca:
            logger.info(f"[{symbol}] SMA200 muy cerca - Sin recorrido suficiente.")
            return None
        
        setup_type = None
        direction = None
        stop_loss = None
        take_profit = None
        confianza = 0
        
        if self.detectar_extension_extrema(df, sma20, tendencia):
            setup_type = "REVERSION"
            if tendencia == "ALCISTA":
                direction = "CORTO"
                stop_loss = df["high"].tail(3).max() * 1.002
                take_profit = sma20
            else:
                direction = "LARGO"
                stop_loss = df["low"].tail(3).min() * 0.998
                take_profit = sma20
            confianza = 60
        elif self.detectar_consolidacion(df, sma20):
            setup_type = "CONSOLIDACION"
            if self.detectar_breakout_consolidacion(df, tendencia):
                if tendencia == "ALCISTA":
                    direction = "LARGO"
                    stop_loss = df["low"].tail(10).min() * 0.998
                    distancia_sl = close - stop_loss
                    take_profit = close + distancia_sl * 2
                else:
                    direction = "CORTO"
                    stop_loss = df["high"].tail(10).max() * 1.002
                    distancia_sl = stop_loss - close
                    take_profit = close - distancia_sl * 2
                confianza = 75
        elif self.detectar_pullback(df, sma20, tendencia):
            setup_type = "PULLBACK"
            if tendencia == "ALCISTA":
                direction = "LARGO"
                stop_loss = sma20 * 0.995
                distancia_sl = close - stop_loss
                take_profit = close + distancia_sl * 2.5
            else:
                direction = "CORTO"
                stop_loss = sma20 * 1.005
                distancia_sl = stop_loss - close
                take_profit = close - distancia_sl * 2.5
            confianza = 80
        
        if direction is None:
            logger.info(f"[{symbol}] No se detectó ningún setup válido.")
            return None
        
        return {
            "strategy": "SMA20-200",
            "direction": direction,
            "confidence": confianza,
            "entryPrice": close,
            "slDistance": abs(close - stop_loss),
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
            "setup": setup_type,
            "tendencia": tendencia,
            "sma20": sma20,
            "sma200": sma200,
            "atr": atr_val,
            "symbolInfo": symbol
        }

    async def _execute_trades(self, signal: Dict, symbolInfo):
        if not signal:
            return

        for account in self.accounts:
            posSize, riskUsd = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'],
                symbolInfo=symbolInfo
            )
            if posSize is None:
                logger.warning(f"[{account['idCuenta']}] No se pudo calcular el tamaño de posición para {symbolInfo['symbol']}.")
                continue
            
            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']
            
            slPrice = entryPrice - slDist if direction == "LARGO" else entryPrice + slDist
            
            ratioBase = 2.2 if signal['confidence'] > 75 else 2.0
            tpPrice = entryPrice + (slDist * ratioBase) if direction == "LARGO" else entryPrice - (slDist * ratioBase)
            
            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbolInfo['symbol'],
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
                
                msgId = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                if msgId:
                    self.lastMessageIds[symbolInfo['symbol']] = msgId
                    
                logger.info(f"✅ Alerta SMA20-200 enviada para {symbolInfo['symbol']} a la cuenta {account['idCuenta']}")

    def _format_alert_message(self, signal: Dict, trade: Dict) -> str:
        direction = signal['direction']
        directionStr = "COMPRA" if direction == "LARGO" else "VENTA"
        colorHeader = "🟢" if direction == "LARGO" else "🔴"
        
        close = signal['entryPrice']
        tp = trade['takeProfit']
        sl = trade['stopLoss']
        confianza = signal['confidence']
        setup = signal['setup']
        tendencia = signal['tendencia']
        
        text = (
            f"{colorHeader}{colorHeader}{colorHeader} "
            f"<b>SEÑAL SMA20-200 DE {directionStr}</b> "
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
            f"🛡️ SL: <b>{sl:,.6f}</b>\n"
            f"🎯 TP: <b>{tp:,.6f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"• SMA20: {signal['sma20']:.6f}\n"
            f"• SMA200: {signal['sma200']:.6f}\n"
            f"• ATR: {signal['atr']:.6f}\n"
            f"• Cantidad: <b>{trade['size']:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
        return text

    async def runAnalysisCycle(self, preloaded_data: Dict = None):
        """
        Run SMA20-200 analysis cycle.
        If preloaded_data is provided, uses it instead of fetching new data.
        preloaded_data format: {symbol: dataframe}
        """
        self.accounts = dbManager.getAccount()
        if not self.accounts:
            logger.error("No se encontraron cuentas en la base de datos.")
            return

        logger.info("Iniciando ciclo SMA20-200...")
        
        symbolsToScan = dbManager.getSymbols()
        
        for symbolInfo in symbolsToScan:
            symbol = symbolInfo['symbol']
            apiKey, interval, _, nVelas, waitMin = getParametros()
            symbolInfo['intervalo'] = interval
            
            logger.debug(f"Analizando {symbol} con SMA20-200 en intervalo {interval}...")

            # Use preloaded data if available
            if preloaded_data and symbol in preloaded_data:
                data = preloaded_data[symbol]
            else:
                data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval)
            
            if data is None:
                continue
            
            signal = await self._get_signal(data, symbol)
            if signal:
                logger.info(f"[{symbol}] Señal SMA20-200: {signal['direction']} ({signal['confidence']}% confianza)")
                await self._execute_trades(signal, symbolInfo)
            else:
                logger.debug(f"[{symbol}] Sin señal SMA20-200 en intervalo {interval}.")
            await asyncio.sleep(5)
        logger.info("✅ Ciclo SMA20-200 completado.")

    async def runAnalysisCycle_for_symbol(self, symbolInfo: Dict, preloaded_data: Dict = None, apiKey: str = None):
        """Procesa un solo símbolo (usado para análisis secuencial)."""
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
            logger.info(f"[{symbol}] Señal SMA20-200: {signal['direction']} ({signal['confidence']}% confianza)")
            await self._execute_trades(signal, symbolInfo)
        else:
            logger.debug(f"[{symbol}] Sin señal SMA20-200 en intervalo {interval}.")
