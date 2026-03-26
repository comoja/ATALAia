"""
Core Trading Bot Class
"""
import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd
import numpy as np
import asyncio

import sys, os
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.api import twelvedata
from middleware.config import constants as config
from Sentinel.analysis import technical, risk
from Sentinel.ml import model as mlModel
from middleware.utils.communications import sendTelegramAlert, alertaInmediata, deleteTelegramMessage
from middleware.database import dbManager
from middleware.scheduler.autoScheduler import getTiempoEspera, isRestTime
from Sentinel.data.dataLoader import getParametros

logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self, mlModelInstance):
        self.model = mlModelInstance
        self.accounts = []
        self.estadosPorSimbolo = {}
        self.lastMessageIds = {}  # {symbol: message_id}

    def calcularAngulos(self, df, ventana=14):
        columnasCalculo = ['close', 'rsi', 'cci', 'macd']
        for col in columnasCalculo:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = np.nan
        
        for col in columnasCalculo:
            minV, maxV = df[col].rolling(ventana).min(), df[col].rolling(ventana).max()
            rango = maxV - minV
            dfNorm = 100 * (df[col] - minV) / rango.replace(0, np.nan)
            df[f'ang_{col}'] = np.degrees(np.arctan(dfNorm.diff(1)))
        return df

    def obtenerEstado(self, angR, angP):
        if pd.isna(angR) or pd.isna(angP): return "☁️ SIN DATOS", "Esperando..."
        if angP < -70 and angR > -20: return "💎 GIRO", "🎯 OPORTUNIDAD: Rebote detectado."
        if angR <= -75: return "💸 LIQUIDACIÓN", "🚨 CRÍTICA: Desplome vertical."
        if angR >= 75:  return "🌋 PARÁBOLA", "⚠️ ALERTA: Subida extrema."
        if angR > 30:   return "🚀 ALCISTA", "✅ Tendencia positiva."
        if angR < -30:  return "📉 BAJISTA", "🔻 Presión de venta."
        return "☁️ NEUTRAL", "💤 Sin movimiento claro."

    def obtenerIcono(self, angulo): 
        if pd.isna(angulo): return "⚪"
        return "🧊" if angulo <= -75 else ("🔥" if angulo >= 75 else ("📈" if angulo > 0 else "📉"))

    async def momentum(self, symbol, df, intervalo=None):   
        from Sentinel.database import dbManager
        
        df = self.calcularAngulos(df)
        last = df.iloc[-1]
        
        closePrice = last.get('close', 0)
        estadoActual, notaMensaje = self.obtenerEstado(last.get('ang_rsi'), last.get('ang_close'))
        estadoPrevio = self.estadosPorSimbolo.get(symbol)
        
        if estadoActual != estadoPrevio:
            intervalText = f"({intervalo})" if intervalo else ""
            mensajeFinal = (
                f"<b><center>MOMENTUM {symbol} {intervalText}</center></b>\n"
                f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"<b>PRECIO:</b> ${closePrice:,.2f}\n"
                f"<b>ESTADO:</b> {estadoActual}\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"<b>RSI:</b>  {self.obtenerIcono(last.get('ang_rsi'))} {last.get('ang_rsi', 0):>6.1f}° ({last.get('rsi', 0):.1f})\n"
                f"<b>CCI:</b>  {self.obtenerIcono(last.get('ang_cci'))} {last.get('ang_cci', 0):>6.1f}° ({last.get('cci', 0):.1f})\n"
                f"<b>MACD:</b> {self.obtenerIcono(last.get('ang_macd'))} {last.get('ang_macd', 0):>6.1f}° ({last.get('macd', 0):.2f})\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"<b>NOTA:</b> <i>{notaMensaje}</i>"
            )

            esCritico = estadoActual in ["💸 LIQUIDACIÓN", "💎 GIRO", "🌋 PARÁBOLA"]
            
            esLateral = False
            cambioPorcentual = 0
            if len(df) >= 2:
                precioActual = float(last.get('close', 0))
                precioAnterior = float(df.iloc[-2].get('close', 0))
                if precioAnterior > 0:
                    cambioPorcentual = abs((precioActual - precioAnterior) / precioAnterior * 100)
                    esLateral = cambioPorcentual < 0.5
            
            if esLateral:
                logger.info(f"[{symbol}] Filtrado MOMENTUM: Movimiento lateral ({cambioPorcentual:.2f}%)")
            elif estadoActual not in ["☁️ SIN DATOS", "☁️ NEUTRAL"]:
                # Delete previous message if 1h interval
                if intervalo == '1h' and symbol in self.lastMessageIds:
                    cuentas = dbManager.getAccount(1)
                    if cuentas:
                        cuenta = cuentas[0]
                        prevMsgId = self.lastMessageIds[symbol]
                        await deleteTelegramMessage(cuenta['TokenMsg'], cuenta['idGrupoMsg'], prevMsgId)
                
                # Send new message
                cuentas = dbManager.getAccount(1)
                if cuentas:
                    cuenta = cuentas[0]
                    msgId = await sendTelegramAlert(cuenta['TokenMsg'], cuenta['idGrupoMsg'], mensajeFinal, esCritico)
                    if msgId:
                        self.lastMessageIds[symbol] = msgId
                        
            self.estadosPorSimbolo[symbol] = estadoActual

        return self.estadosPorSimbolo

    async def _get_and_prepare_data(self, symbolInfo: Dict, apiKey: str, nVelas: int, interval: str, raw_df: pd.DataFrame = None) -> pd.DataFrame | None:
        """Fetches, prepares, and enriches data with technical features."""
        symbol = symbolInfo['symbol']
        
        # 1. Download data or use provided raw data
        if raw_df is not None and len(raw_df) >= 100:
            df = raw_df.copy()
        else:
            logger.info(f"[{symbol}] Obteniendo datos de 12Data para estrategia Sniper...")
            df = await twelvedata.getTimeSeries({"symbol": symbol, "interval": interval, "apikey": apiKey, "outputSize": nVelas})
            if df is None or len(df) < 100:
                logger.warning(f"[{symbol}] Datos insuficientes para análisis ({len(df) if df is not None else 0} velas).")
                return None
        
        # 2. Calculate features
        dfFeatured = technical.calculateFeatures(df)
        
        # 3. Define ML target (needed for data cleaning consistency)
        dfFinal = mlModel.defineMlTarget(dfFeatured)
        
        return dfFinal

    async def _get_signal(self, df: pd.DataFrame, symbol: str) -> Dict[str, Any] | None:
        """Analyzes the data to generate a trading signal dictionary."""
        
        X, _ = mlModel.cleanDataForModel(df)
        if len(X) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes tras limpieza ({len(X)} filas).")
            return None

        # --- Get Current Values ---
        latest = X.iloc[-1]
        latestFullData = df.iloc[-1]
        
        close = latestFullData["close"]
        currentAtr = latest["atr"]
        avgAtr = df["atr"].iloc[-20:].mean()
        volPercent = (currentAtr / close) * 100
        
        # --- FILTERS (VETO) ---
        if currentAtr < avgAtr * 0.5:
            logger.info(f"[{symbol}] Volatilidad baja (ATR: {currentAtr:.4f} < 50% avg: {avgAtr:.4f}). Señal descartada.")
            return None
        
        # --- PREDICTION ---
        proba = mlModel.predictProba(self.model, X)
        if proba is None:
            return None

        # --- STRATEGY LOGIC (SNIPER ADVANCED) ---
        direction = None
        confianza = 0
        
        # Get current and previous values
        histVal = latestFullData["macdHist"]
        prevHistVal = df["macdHist"].iloc[-2]
        prev2HistVal = df["macdHist"].iloc[-3]
        
        macdLine = latestFullData["macd"]
        macdSignal = latestFullData["macdSig"]
        
        rsi = latest["rsi"]
        prevRsi = df["rsi"].iloc[-2]
        
        close = latestFullData["close"]
        prevClose = df["close"].iloc[-2]
        
        ema20 = latestFullData["ema20"]
        ema50 = latestFullData["ema50"]
        
        # --- 1. MACD Signal Line Crossover (More reliable than histogram)
        macdCrossLong = (macdLine > macdSignal) and (prevClose <= df["macd"].iloc[-2] < df["macdSig"].iloc[-2])
        macdCrossShort = (macdLine < macdSignal) and (prevClose >= df["macd"].iloc[-2] > df["macdSig"].iloc[-2])
        
        # --- 2. MACD Histogram Momentum (improving or weakening)
        histImprovingLong = histVal > prevHistVal  # Histogram getting bigger (more bullish)
        histImprovingShort = histVal < prevHistVal  # Histogram getting smaller (more bearish)
        
        # --- 3. MACD Zero Line Cross (strong signal)
        macdZeroCrossLong = (prevHistVal <= 0 and histVal > 0)
        macdZeroCrossShort = (prevHistVal >= 0 and histVal < 0)
        
        # --- 4. EMA Trend Confirmation (EMA20 above EMA50 = bullish)
        emaTrendLong = ema20 > ema50
        emaTrendShort = ema20 < ema50
        
        # --- 5. RSI Momentum & Divergence
        msg_rsi = ""
        if rsi >= 68:
                msg_rsi = "🟩🟩🟩 <b>SOBRECOMPRA</b> 🟩🟩🟩\n"
        elif rsi <= 32:
            msg_rsi = "🟥🟥🟥 <b>SOBREVENTA</b> 🟥🟥🟥\n"        
        if msg_rsi != "":
            msg_rsi += (
                f"━━━━━━━━━━━━━━━━\n"
                f"<center>{symbol}</center>\n"
                f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
                f"━━━━━━━━━━━━━━━━\n"
                # f"• RSI: {rsi:.2f} | "
                #  f"Pend.: {pendiente_rsi_val:.2f} {'🟢' if pendiente_rsi_val > 0 else '🔴'}\n"
                #  f"• CCI: {cci_val:.2f} | "
                #  f"Pend.: {pendiente_cci_val:.2f} {'🟢' if pendiente_cci_val > 0 else '🔴'}\n"
                #  f"• MACD: {'ALCISTA 🟢' if hist_val > 0 else 'BAJISTA 🔴'}\n"
                #  f"• Volatilidad: {vol_porcentaje:.3f}% ({fuerza_vol})\n"
                #  f"• Vela: {msg_vela}\n"
                # f"━━━━━━━━━━━━━━━━\n"
            )
            await alertaInmediata(1, msg_rsi)
        rsiImprovingLong = rsi > prevRsi
        rsiImprovingShort = rsi < prevRsi
        
        # RSI Divergence: Price makes higher low but RSI makes lower low (bullish hidden divergence)
        # Or: Price makes lower high but RSI makes higher high (bearish hidden divergence)
        priceHigherLow = close > df["low"].iloc[-2]
        rsiLowerLow = rsi < prevRsi
        
        priceLowerHigh = close < df["high"].iloc[-2]
        rsiHigherHigh = rsi > prevRsi
        
        # --- 6. MACD Divergence (Regular)
        # Find local extrema in last 5 bars
        prices = df["close"].iloc[-5:].values
        hists = df["macdHist"].iloc[-5:].values
        
        priceHigherHigh = prices[-1] > np.max(prices[:-1])
        histLowerHigh = hists[-1] < np.max(hists[:-1])
        
        priceLowerLow = prices[-1] < np.min(prices[:-1])
        histHigherLow = hists[-1] > np.min(hists[:-1])
        
        # --- Technical Confirmation (momentum)
        techConfLong = (latestFullData["pendienteCci"] > 0.5 and latestFullData["pendienteRsi"] > 0.1)
        techConfShort = (latestFullData["pendienteCci"] < -0.5 and latestFullData["pendienteRsi"] < -0.1)
        
        # --- MOMENTUM FILTER ---
        dfWithAngles = self.calcularAngulos(df.copy())
        lastAngle = dfWithAngles.iloc[-1]
        momentumEstado, _ = self.obtenerEstado(lastAngle.get('ang_rsi'), lastAngle.get('ang_close'))
        
        momentumVeto = momentumEstado in ["💸 LIQUIDACIÓN", "🌋 PARÁBOLA"]
        momentumBullish = momentumEstado in ["🚀 ALCISTA", "💎 GIRO"]
        momentumBearish = momentumEstado in ["📉 BAJISTA"]
        
        if momentumVeto:
            logger.info(f"[{symbol}] Filtrado MOMENTUM: Estado crítico ({momentumEstado}). Señal vetada.")
            return None
        
        # --- MAIN SIGNAL CONDITIONS ---
        # LARGOS: ML proba + (MACD improving OR zero cross OR cross) + (EMA trend OR RSI improving)
        isLongCandidate = (
            proba >= config.PROBA_THRESHOLD_LONG and
            (histImprovingLong or macdZeroCrossLong or macdCrossLong) and
            (emaTrendLong or rsiImprovingLong or techConfLong) and
            rsi < config.RSI_OVERBOUGHT_THRESHOLD and
            not (priceHigherHigh and histLowerHigh)  # No bearish divergence
        )
        
        # CORTOS: ML proba + (MACD weakening OR zero cross OR cross) + (EMA trend OR RSI improving)
        isShortCandidate = (
            proba <= config.PROBA_THRESHOLD_SHORT and
            (histImprovingShort or macdZeroCrossShort or macdCrossShort) and
            (emaTrendShort or rsiImprovingShort or techConfShort) and
            rsi > config.RSI_SOLD_THRESHOLD and
            not (priceLowerLow and histHigherLow)  # No bullish divergence
        )
        
        if isLongCandidate:
            direction = "LARGO"
            # Calculate base confidence
            confianza = proba * 100
            
            # Bonifications
            if macdZeroCrossLong:
                confianza += 15
            elif macdCrossLong:
                confianza += 10
            else:
                confianza += 5
                
            if emaTrendLong:
                confianza += 8
            if rsiImprovingLong:
                confianza += 5
            if priceLowerLow and histHigherLow:  # Hidden bullish divergence
                confianza += 12
                
        elif isShortCandidate:
            direction = "CORTO"
            # Calculate base confidence
            confianza = (1 - proba) * 100
            
            # Bonifications
            if macdZeroCrossShort:
                confianza += 15
            elif macdCrossShort:
                confianza += 10
            else:
                confianza += 5
                
            if emaTrendShort:
                confianza += 8
            if rsiImprovingShort:
                confianza += 5
            if priceHigherHigh and histLowerHigh:  # Hidden bearish divergence
                confianza += 12
                
        else:
            return None # No signal
        
        # --- Apply Momentum Bonus/Penalty ---
        momentumBonus = 0
        momentumPenalty = 0
        
        if direction == "LARGO" and momentumBullish:
            momentumBonus = 10
        elif direction == "LARGO" and momentumBearish:
            momentumPenalty = 15
        elif direction == "CORTO" and momentumBearish:
            momentumBonus = 10
        elif direction == "CORTO" and momentumBullish:
            momentumPenalty = 15
        
        if momentumBonus > 0:
            logger.info(f"[{symbol}] MOMENTUM favorable ({momentumEstado}): +{momentumBonus}% confianza")
            confianza += momentumBonus
        elif momentumPenalty > 0:
            logger.info(f"[{symbol}] MOMENTUM desfavorable ({momentumEstado}): -{momentumPenalty}% confianza")
            confianza -= momentumPenalty
            
        # --- Apply Bonuses/Penalties ---
        # Candle patterns
        cdlEngulfing = latestFullData.get("cdlEngulfing", 0)
        cdlHammer = latestFullData.get("cdlHammer", 0)
        cdlShootingStar = latestFullData.get("cdlShootingStar", 0)
        cdlDoji = latestFullData.get("cdlDoji", 0)

        if (direction == "LARGO" and (cdlEngulfing > 0 or cdlHammer > 0)) or (direction == "CORTO" and (cdlEngulfing < 0 or cdlShootingStar < 0)):
            confianza *= 1.15
        elif cdlDoji != 0:
            confianza *= 0.70  # Doji = indecision
        else:
            confianza *= 0.50 # Penalty if no confirming candle

        # --- Minimum Confidence Filter ---
        if confianza < config.MIN_CONFIDENCE_THRESHOLD:
            logger.info(f"[{symbol}] Filtrado: Confianza muy baja ({confianza:.1f}% < {config.MIN_CONFIDENCE_THRESHOLD}%).")
            return None

        # --- FINAL FILTERS ---
        if confianza < config.CONTRARIAN_CONFIDENCE_THRESHOLD:
            isAgainstTrend = (direction == "LARGO" and close < ema50) or (direction == "CORTO" and close > ema50)
            if isAgainstTrend:
                logger.info(f"[{symbol}] Filtrado: Intento de contratendencia con confianza baja ({confianza:.1f}%).")
                return None
        
        # --- SAR Filter (VETO) ---
        sarTrend = latestFullData.get("sarTrend")
        if sarTrend is not None and not pd.isna(sarTrend):
            sarFilterLong = direction == "LARGO" and sarTrend < 0
            sarFilterShort = direction == "CORTO" and sarTrend > 0
            if sarFilterLong or sarFilterShort:
                logger.info(f"[{symbol}] Filtrado SAR: Señal {direction} contra tendencia SAR ({sarTrend}).")
                return None

        return {
            "direction": direction,
            "confidence": confianza,
            "entryPrice": close,
            "slDistance": latest["atr"] * (config.ATR_MULTIPLIER_HIGH_CONFIDENCE if proba >= 0.65 or proba <= 0.35 else config.ATR_MULTIPLIER_DEFAULT),
            "latestMetrics": latestFullData.to_dict(),
            "symbolInfo": symbol
        }
    
    async def _execute_trades(self, signal: Dict, symbolInfo):
        """Processes a valid signal, calculates risk, and sends alerts for all accounts."""
        if not signal:
            return

        for account in self.accounts:
            # --- Risk and Position Sizing ---
            posSize, riskUsd = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'],
                symbolInfo=symbolInfo
            )
            if posSize is None:
                logger.warning(f"[{account['idCuenta']}] No se pudo calcular el tamaño de posición para {symbolInfo['symbol']}.")
                continue
            
            # --- Define SL/TP ---
            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']

            slPrice = entryPrice - slDist if direction == "LARGO" else entryPrice + slDist
            
            # Dynamic RR
            ratioBase = config.HIGH_CONFIDENCE_RISK_REWARD_RATIO if signal['confidence'] > 85 else config.BASE_RISK_REWARD_RATIO
            
            tpPrice = entryPrice + (slDist * ratioBase) if direction == "LARGO" else entryPrice - (slDist * ratioBase)
            
            # --- Create Trade Object ---
            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbolInfo['symbol'],
                "direction": direction,
                "entryPrice": entryPrice,
                "openTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": slPrice,
                "takeProfit": tpPrice,
                "size": posSize,
                "intervalo": symbolInfo['intervalo'], # Assumes this info is passed
                "status": "OPEN",
                # ... other fields for DB
            }
            
            # --- Persist and Alert ---
            if account['idCuenta'] != 1: # Original logic to exclude account 1
                dbManager.buscaTrade(trade) # Assumes this function saves the trade
                
                # Format and send alert
                message = self._format_alert_message(signal, trade)
                
                # Delete previous message if 1h interval
                intervalo = symbolInfo.get('intervalo', '')
                symbol = symbolInfo['symbol']
                
                if intervalo == '1h' and symbol in self.lastMessageIds:
                    prevMsgId = self.lastMessageIds[symbol]
                    await deleteTelegramMessage(account['TokenMsg'], account['idGrupoMsg'], prevMsgId)
                
                # Send new message and save message_id
                msgId = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                if msgId:
                    self.lastMessageIds[symbol] = msgId
                    
                logger.info(f"✅ Alerta SNIPER enviada para {symbolInfo['symbol']} a la cuenta {account['idCuenta']}")
    
    def _format_alert_message(self, signal: Dict, trade: Dict) -> str:
        """Formats the beautiful Telegram message from the original script."""
        direction = signal['direction']
        directionStr = "COMPRA" if direction == "LARGO" else "VENTA"
        colorHeader = "🟩" if direction == "LARGO" else "🟥"
        
        close = signal['entryPrice']
        tp = trade['takeProfit']
        sl = trade['stopLoss']
        confianza = signal['confidence']
        
        latest = signal['latestMetrics']
        
        rsi_val = latest.get('rsi', 0)
        pendiente_rsi_val = latest.get('pendienteRsi', 0)
        cci_val = latest.get('cci', 0)
        pendiente_cci_val = latest.get('pendienteCci', 0)
        hist_val = latest.get('macdHist', 0)
        
        currentAtr = latest.get('atr', 0)
        avgAtr = latest.get('atr', currentAtr)
        vol_porcentaje = (currentAtr / close) * 100 if close > 0 else 0
        
        if vol_porcentaje > 3:
            fuerza_vol = "ALTA"
        elif vol_porcentaje > 1.5:
            fuerza_vol = "MEDIA"
        else:
            fuerza_vol = "BAJA"
        
        punto_be = close
        
        ulabel = "TAKE PROFIT" if directionStr == "LARGO" else "STOP LOSS"
        uemoji = "🟢" if directionStr == "LARGO" else "🔴"
        uvalor = tp if directionStr == "LARGO" else sl
        
        label = "TAKE PROFIT" if directionStr == "CORTO" else "STOP LOSS"
        emoji = "🟢" if directionStr == "CORTO" else "🔴"
        valor = tp if directionStr == "CORTO" else sl
        
        upmensaje = f"{uemoji} <b>{ulabel}: {uvalor:,.5f}</b>"
        dnmensaje = f"{emoji} <b>{label}: {valor:,.5f}</b>"
        
        text = (
            f"{colorHeader*3} <b>SEÑAL DE {directionStr}</b> {colorHeader*3}\n"
            f"<center><i>Estrategia: ML SNIPER</i></center>\n"
            f"<center><b>{trade['symbol']}</b> ({trade['intervalo']})</center>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Confianza: <b>{confianza:.1f}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"{upmensaje}\n"
            f"🛡️ Break even: {punto_be:,.6f}\n"
            f"🔹 ENTRADA:   <b>{close:,.5f}</b>\n"
            f"{dnmensaje}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center><b>DATOS TÉCNICOS:</b></center>\n"
            f"• RSI: {rsi_val:.2f} | Pend.: {pendiente_rsi_val:.2f} {'🟢' if pendiente_rsi_val > 0 else '🔴'}\n"
            f"• CCI: {cci_val:.2f} | Pend.: {pendiente_cci_val:.2f} {'🟢' if pendiente_cci_val > 0 else '🔴'}\n"
            f"• MACD: {'ALCISTA 🟢' if hist_val > 0 else 'BAJISTA 🔴'}\n"
            f"• Volatilidad: <b>{vol_porcentaje:.3f}%</b> ({fuerza_vol})\n"
            f"• Cantidad: <b>{trade['size']:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
        return text
    
    

    async def runAnalysisCycle(self, preloaded_data: Dict = None):
        """The main operational loop of the bot."""
        self.accounts = dbManager.getAccount()
        if not self.accounts:
            logger.error("No se encontraron cuentas en la base de datos. El bot no puede operar.")
            return

        logger.info("Iniciando ciclo de análisis...")
        
        symbolsToScan = dbManager.getSymbols()
        
        for symbolInfo in symbolsToScan:
            symbol = symbolInfo['symbol']
            # These parameters are now fetched per symbol, as in the original logic
            apiKey, interval, _, nVelas, waitMin = getParametros()
            symbolInfo['intervalo'] = interval # Augment symbolInfo
            
            logger.debug(f"Analizando {symbol} en intervalo {interval}...")

            # Use preloaded data if available
            raw_df = preloaded_data.get(symbol) if preloaded_data else None
            data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval, raw_df)
            if data is None:
                continue
            
            await self.momentum(symbol, data, interval)
            
            signal = await self._get_signal(data, symbol)
            if signal:
                logger.info(f"[{symbol}] Señal generada: {signal['direction']} ({signal['confidence']:.1f}% confianza)")
                await self._execute_trades(signal, symbolInfo)
            else:
                logger.debug(f"[{symbol}] Sin señal en intervalo {interval}.")
            await asyncio.sleep(5)
        logger.info("✅ Ciclo de análisis completado.")

    async def runAnalysisCycle_for_symbol(self, symbolInfo: Dict, preloaded_data: Dict = None, apiKey: str = None):
        """Procesa un solo símbolo (usado para análisis secuencial)."""
        if not self.accounts:
            return
        
        symbol = symbolInfo['symbol']
        interval = symbolInfo.get('intervalo', '15min')
        if apiKey is None:
            apiKey, _, _, nVelas, _ = getParametros()
        else:
            _, _, _, nVelas, _ = getParametros()
        
        logger.debug(f"Analizando {symbol} con Sniper en intervalo {interval}...")

        raw_df = preloaded_data.get(symbol) if preloaded_data else None
        data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval, raw_df)
        if data is None:
            return
        
        await self.momentum(symbol, data, interval)
        
        signal = await self._get_signal(data, symbol)
        if signal:
            logger.info(f"[{symbol}] Señal Sniper: {signal['direction']} ({signal['confidence']:.1f}% confianza)")
            await self._execute_trades(signal, symbolInfo)
        else:
            logger.debug(f"[{symbol}] Sin señal Sniper en intervalo {interval}.")


