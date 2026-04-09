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
from middleware.utils.alertBuilder import buildSniperAlertMessage, adjustTPForMinRR, getPipMultiplier, calculateRR
from middleware.database import dbManager
from middleware.scheduler.autoScheduler import getTiempoEspera, isRestTime
from Sentinel.data.dataLoader import getParametros
from middleware.config.constants import TIMEZONE
from middleware.utils.momentum import calcularAngulos, obtenerEstado

logger = logging.getLogger(__name__)

class SniperBot:
    def __init__(self, mlModelInstance):
        self.model = mlModelInstance
        self.accounts = []
        self.estadosPorSimbolo = {}
        self.lastMessageIds = {}  # {symbol: message_id}



    async def _get_and_prepare_data(self, symbolInfo: Dict, apiKey: str, nVelas: int, interval: str, raw_df: pd.DataFrame = None) -> pd.DataFrame | None:
        """Fetches, prepares, and enriches data with technical features."""
        symbol = symbolInfo['symbol']
        
        # 1. Download data or use provided raw data
        if raw_df is not None and len(raw_df) >= 100:
            df = raw_df.copy()
        else:
            logger.info(f"[{symbol}] Obteniendo datos de 12Data...")
            params = {
                "symbol": symbol,
                "interval": "5min",
                "apikey": apiKey,
                "outputSize": 5000,
                "timezone":TIMEZONE
                }
            df = await twelvedata.getTimeSeries(params)
            #df = await twelvedata.getTimeSeries({"symbol": symbol, "interval": interval, "apikey": apiKey, "outputSize": nVelas})
            if df is None or len(df) < 100:
                logger.warning(f"[{symbol}] Datos insuficientes para análisis ({len(df) if df is not None else 0} velas).")
                return None
        
        # 2. Calculate features
        dfFeatured = technical.calculateFeatures(df)
        if dfFeatured is None:
            logger.error(f"[{symbol}] Error crítico: calculateFeatures devolvió None")
            return None
        
        # 3. Define ML target (needed for data cleaning consistency)
        dfFinal = mlModel.defineMlTarget(dfFeatured)
        if dfFinal is None:
            logger.error(f"[{symbol}] Error crítico: defineMlTarget devolvió None")
            return None
            
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
            logger.info(f"[{symbol}] Rechazada: ML no pudo calcular probabilidad")
            return None

        # --- INDICADORES TÉCNICOS ---
        histVal = latestFullData["macdHist"]
        prevHistVal = df["macdHist"].iloc[-2]
        
        macdLine = latestFullData["macd"]
        macdSignal = latestFullData["macdSig"]
        
        rsi = latest["rsi"]
        prevRsi = df["rsi"].iloc[-2]
        
        close = latestFullData["close"]
        
        ema20 = latestFullData["ema20"]
        ema50 = latestFullData["ema50"]
        
        # --- SEÑALES INDIVIDUALES ---
        # MACD
        macdCrossLong = (macdLine > macdSignal) and (df["macd"].iloc[-2] <= df["macdSig"].iloc[-2])
        macdCrossShort = (macdLine < macdSignal) and (df["macd"].iloc[-2] >= df["macdSig"].iloc[-2])
        histImprovingLong = histVal > prevHistVal
        histImprovingShort = histVal < prevHistVal
        macdZeroCrossLong = (prevHistVal <= 0 and histVal > 0)
        macdZeroCrossShort = (prevHistVal >= 0 and histVal < 0)
        
        # EMA Trend
        emaTrendLong = ema20 > ema50
        emaTrendShort = ema20 < ema50
        
        # RSI
        rsiImprovingLong = rsi > prevRsi
        rsiImprovingShort = rsi < prevRsi
        
        # Alerta de sobrecompra/sobreventa (informativa)
        """
        if rsi >= 68:
            await alertaInmediata(1, f"🟩🟩🟩 <b>SOBRECOMPRA</b> 🟩🟩🟩\n━━━━━━━━━━━━━━━━\n<center>{symbol}</center>\n<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n━━━━━━━━━━━━━━━━\n")
            await asyncio.sleep(2)
        elif rsi <= 32:
            await alertaInmediata(1, f"🟥🟥🟥 <b>SOBREVENTA</b> 🟥🟥🟥\n━━━━━━━━━━━━━━━━\n<center>{symbol}</center>\n<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n━━━━━━━━━━━━━━━━\n")
            await asyncio.sleep(2)
        """
        # Divergencia MACD (últimas 5 velas)
        prices = df["close"].iloc[-5:].values
        hists = df["macdHist"].iloc[-5:].values
        bearishDivergence = (prices[-1] > np.max(prices[:-1])) and (hists[-1] < np.max(hists[:-1]))
        bullishDivergence = (prices[-1] < np.min(prices[:-1])) and (hists[-1] > np.min(hists[:-1]))
        
        # CCI + RSI pendientes
        techConfLong = (latestFullData["pendienteCci"] > 0.5 and latestFullData["pendienteRsi"] > 0.1)
        techConfShort = (latestFullData["pendienteCci"] < -0.5 and latestFullData["pendienteRsi"] < -0.1)
        
        # --- MOMENTUM FILTER (único veto técnico además de ATR) ---
        dfWithAngles = calcularAngulos(df.copy())
        lastAngle = dfWithAngles.iloc[-1]
        momentumEstado, _ = obtenerEstado(lastAngle.get('ang_rsi'), lastAngle.get('ang_close'))
        
        momentumBullish = momentumEstado in ["🚀 ALCISTA", "💎 GIRO"]
        momentumBearish = momentumEstado in ["📉 BAJISTA"]
        momentumVeto = momentumEstado in ["💸 LIQUIDACIÓN"]
        if momentumVeto:
            logger.info(f"[{symbol}] Filtrado MOMENTUM: Estado crítico ({momentumEstado}). Señal vetada.")
            return None
        
        # ============================================================
        # SISTEMA DINÁMICO DE CONFIRMACIONES (en vez de AND rígido)
        # Se cuentan las confirmaciones técnicas. Se necesitan >= 2 de 5.
        # Esto evita que un solo indicador desalineado bloquee todo.
        # ============================================================
        
        # --- Determinar dirección por ML ---
        if proba >= config.PROBA_THRESHOLD_LONG:
            direction = "LARGO"
            confianza = proba * 100
        elif proba <= config.PROBA_THRESHOLD_SHORT:
            direction = "CORTO"
            confianza = (1 - proba) * 100
        else:
            logger.info(f"[{symbol}] Rechazada: ML indeciso (proba={proba:.2f}, zona neutral)")
            return None
        
        # --- Contar confirmaciones técnicas ---
        confirmaciones = 0
        detalles = []
        
        if direction == "LARGO":
            if histImprovingLong or macdZeroCrossLong or macdCrossLong:
                confirmaciones += 1
                detalles.append("MACD")
            if emaTrendLong:
                confirmaciones += 1
                detalles.append("EMA")
            if rsiImprovingLong:
                confirmaciones += 1
                detalles.append("RSI")
            if techConfLong:
                confirmaciones += 1
                detalles.append("CCI+RSI_pend")
            if rsi < config.RSI_OVERBOUGHT_THRESHOLD:
                confirmaciones += 1
                detalles.append("RSI_no_sobrecompra")
            # Veto por divergencia bajista
            if bearishDivergence:
                confirmaciones -= 1
                detalles.append("⚠️DIV_BAJISTA")
        else:  # CORTO
            if histImprovingShort or macdZeroCrossShort or macdCrossShort:
                confirmaciones += 1
                detalles.append("MACD")
            if emaTrendShort:
                confirmaciones += 1
                detalles.append("EMA")
            if rsiImprovingShort:
                confirmaciones += 1
                detalles.append("RSI")
            if techConfShort:
                confirmaciones += 1
                detalles.append("CCI+RSI_pend")
            if rsi > config.RSI_SOLD_THRESHOLD:
                confirmaciones += 1
                detalles.append("RSI_no_sobreventa")
            # Veto por divergencia alcista
            if bullishDivergence:
                confirmaciones -= 1
                detalles.append("⚠️DIV_ALCISTA")
        
        # --- ADX Dinámico: ajustar exigencia según estado del mercado ---
        try:
            high = df['high'].values.astype(float)
            low = df['low'].values.astype(float)
            cls = df['close'].values.astype(float)
            tr = np.maximum(high[1:] - low[1:], np.maximum(np.abs(high[1:] - cls[:-1]), np.abs(low[1:] - cls[:-1])))
            n = 14
            if len(tr) >= n * 2:
                dm_plus = np.where((high[1:] - high[:-1]) > (low[:-1] - low[1:]), np.maximum(high[1:] - high[:-1], 0), 0)
                dm_minus = np.where((low[:-1] - low[1:]) > (high[1:] - high[:-1]), np.maximum(low[:-1] - low[1:], 0), 0)
                atr_smooth = pd.Series(tr).rolling(n).mean().values
                di_plus = 100 * pd.Series(dm_plus).rolling(n).mean().values / np.where(atr_smooth > 0, atr_smooth, 1)
                di_minus = 100 * pd.Series(dm_minus).rolling(n).mean().values / np.where(atr_smooth > 0, atr_smooth, 1)
                dx = 100 * np.abs(di_plus - di_minus) / np.where((di_plus + di_minus) > 0, di_plus + di_minus, 1)
                adx_val = float(pd.Series(dx).rolling(n).mean().iloc[-1])
            else:
                adx_val = 25  # Default neutral
        except:
            adx_val = 25
        
        mercado_erratico = adx_val < 20
        min_confirmaciones = 3 if mercado_erratico else 2
        logger.info(f"[{symbol}] ADX={adx_val:.1f} ({'ERRÁTICO' if mercado_erratico else 'TENDENCIA'}) → mín_conf={min_confirmaciones}")
        logger.info(f"[{symbol}] Confirmaciones: {confirmaciones}/5 ({', '.join(detalles)})")
        
        if confirmaciones < min_confirmaciones:
            logger.info(f"[{symbol}] Rechazada: Solo {confirmaciones} confirmaciones (mín={min_confirmaciones})")
            return None
        
        # --- Bonificaciones por señales fuertes ---
        if direction == "LARGO":
            if macdZeroCrossLong: confianza += 15
            elif macdCrossLong: confianza += 10
            if emaTrendLong: confianza += 8
            if bullishDivergence: confianza += 12  # Divergencia oculta alcista
        else:
            if macdZeroCrossShort: confianza += 15
            elif macdCrossShort: confianza += 10
            if emaTrendShort: confianza += 8
            if bearishDivergence: confianza += 12
        
        # Bonus por cantidad de confirmaciones (3+ = señal muy sólida)
        if confirmaciones >= 4:
            confianza += 10
        elif confirmaciones >= 3:
            confianza += 5
        
        # --- Momentum Bonus/Penalty ---
        if direction == "LARGO" and momentumBullish:
            logger.info(f"[{symbol}] MOMENTUM favorable ({momentumEstado}): +10% confianza")
            confianza += 10
        elif direction == "LARGO" and momentumBearish:
            logger.info(f"[{symbol}] MOMENTUM desfavorable ({momentumEstado}): -15% confianza")
            confianza -= 15
        elif direction == "CORTO" and momentumBearish:
            logger.info(f"[{symbol}] MOMENTUM favorable ({momentumEstado}): +10% confianza")
            confianza += 10
        elif direction == "CORTO" and momentumBullish:
            logger.info(f"[{symbol}] MOMENTUM desfavorable ({momentumEstado}): -15% confianza")
            confianza -= 15
            
        # --- Candle Patterns (solo bonus, sin penalización injusta) ---
        cdlEngulfing = latestFullData.get("cdlEngulfing", 0)
        cdlHammer = latestFullData.get("cdlHammer", 0)
        cdlShootingStar = latestFullData.get("cdlShootingStar", 0)
        cdlDoji = latestFullData.get("cdlDoji", 0)

        if (direction == "LARGO" and (cdlEngulfing > 0 or cdlHammer > 0)) or (direction == "CORTO" and (cdlEngulfing < 0 or cdlShootingStar < 0)):
            confianza *= 1.10
            logger.info(f"[{symbol}] Patrón de vela favorable: +10% confianza")
        elif cdlDoji != 0:
            confianza *= 0.95  # Doji = indecisión leve, no destruir la señal

        # --- Minimum Confidence Filter ---
        if confianza < config.MIN_CONFIDENCE_THRESHOLD:
            logger.info(f"[{symbol}] Filtrado: Confianza muy baja ({confianza:.1f}% < {config.MIN_CONFIDENCE_THRESHOLD}%).")
            return None

        # --- NEW: Veto Parábola condicional (Solo si la confianza no es extrema) ---
        if momentumEstado == "🌋 PARÁBOLA" and confianza < 80:
            logger.info(f"[{symbol}] Filtrado MOMENTUM: Estado PARÁBOLA con confianza insuficiente ({confianza:.1f} < 80).")
            return None

        # --- Contratendencia ---
        if confianza < config.CONTRARIAN_CONFIDENCE_THRESHOLD:
            isAgainstTrend = (direction == "LARGO" and close < ema50) or (direction == "CORTO" and close > ema50)
            if isAgainstTrend:
                logger.info(f"[{symbol}] Filtrado: Contratendencia con confianza baja ({confianza:.1f}%).")
                return None

        # Niveles estructurales para SL y TP lógicos (sensibilidad aumentada)
        from Sentinel.analysis import technical
        levels = technical.get_structural_levels(latestFullData, lookback=40)
        atr_val = latest["atr"]
        atr_padding = atr_val * 0.2
        
        if direction == "LARGO":
            sl_price = levels['swing_low'] - atr_padding
            sl_dist = max(atr_val * 1.2, min(close - sl_price, atr_val * 3.0))
            tp_structural = levels['high_zone']
        else:
            sl_price = levels['swing_high'] + atr_padding
            sl_dist = max(atr_val * 1.2, min(sl_price - close, atr_val * 3.0))
            tp_structural = levels['low_zone']

        return {
            "direction": direction,
            "confidence": confianza,
            "entryPrice": close,
            "slDistance": sl_dist,
            "tpStructural": tp_structural,
            "latestMetrics": latestFullData.to_dict(),
            "symbolInfo": symbol,
            "confirmaciones": confirmaciones,
            "detalles_conf": detalles
        }
    
    async def _execute_trades(self, signal: Dict, symbolInfo: Dict):
        """Processes a valid signal, calculates risk, and sends alerts for all accounts."""
        if not signal:
            return

        for account in self.accounts:
            # --- Risk and Position Sizing ---
            posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'],
                symbolInfo=symbolInfo,
                entryPrice=signal.get('entryPrice')
            )
            
            # --- Define SL/TP ---
            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']

            slPrice = entryPrice - slDist if direction == "LARGO" else entryPrice + slDist
            
            # Dynamic RR
            ratioBase = config.HIGH_CONFIDENCE_RISK_REWARD_RATIO if signal['confidence'] > 85 else config.BASE_RISK_REWARD_RATIO
            
            # TP estructural prioritario, con fallback basado en ratioBase
            tp_initial = signal.get('tpStructural')
            if not tp_initial:
                tp_initial = entryPrice + (slDist * ratioBase) if direction == "LARGO" else entryPrice - (slDist * ratioBase)
                
            tpPrice = adjustTPForMinRR(entryPrice, slPrice, tp_initial, direction, minRR=1.5)
            
            rr_actual = calculateRR(entryPrice, slPrice, tpPrice)
            multiplier = getPipMultiplier(symbolInfo['symbol'])
            
            min_distance_pips = 10.0
            min_distance_absolute = min_distance_pips / multiplier
            
            if slDist < min_distance_absolute:
                logger.info(f"[Sniper] {symbolInfo['symbol']} rechazada: distancia SL muy pequeña ({slDist * multiplier:.1f} pips < {min_distance_pips} pips)")
                return
            
            tpDist = abs(tpPrice - entryPrice)
            if tpDist < min_distance_absolute:
                logger.info(f"[Sniper] {symbolInfo['symbol']} rechazada: distancia TP muy pequeña ({tpDist * multiplier:.1f} pips < {min_distance_pips} pips)")
                return
            
            # Enriquecer señal con métricas para el constructor de alertas
            signal['riesgo_pips'] = round(slDist * multiplier, 1)
            signal['rr_ratio'] = round(rr_actual, 2)
            
            if posSize is None:
                posSize = 0
                marginUsed = 0
                logger.warning(f"[{account['idCuenta']}] Trade no ejecutada: {symbolInfo['symbol']} - size=0 (margen/riesgo excede capital)")
            
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
                "intervalo": symbolInfo.get('intervalo', ''),
                "status": "OPEN",
                "strategy": "Sniper",
                "margin_used": marginUsed,
            }
            
            # --- Execution and Alert via Gateway ---
            from middleware.execution.broker_gateway import gateway
            success, msgId = await gateway.execute_trade(trade, signal, account, "Sniper", df=latestFullData)
            
            # Delete previous message if interval is 1h and we have a new msgId
            intervalo = symbolInfo.get('intervalo', '')
            symbol = symbolInfo['symbol']
            if success and msgId and intervalo == '1h' and symbol in self.lastMessageIds:
                prevMsgId = self.lastMessageIds[symbol]
                await deleteTelegramMessage(account['TokenMsg'], account['idGrupoMsg'], prevMsgId)
                self.lastMessageIds[symbol] = msgId
            elif success and msgId:
                self.lastMessageIds[symbol] = msgId
                    
                logger.info(f"✅ Alerta enviada para {symbolInfo['symbol']} a la cuenta {account['idCuenta']}")
    
    

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
            
            logger.info(f"Analizando {symbol} en intervalo {interval}...")

            # Use preloaded data if available
            raw_df = preloaded_data.get(symbol) if preloaded_data else None
            data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval, raw_df)
            if data is None:
                continue
            

            
            signal = await self._get_signal(data, symbol)
            if signal:
                logger.info(f"[{symbol}] Señal generada: {signal['direction']} ({signal['confidence']:.1f}% confianza)")
                await self._execute_trades(signal, symbolInfo)
            else:
                logger.info(f"[{symbol}] Sin señal en intervalo {interval}.")
            await asyncio.sleep(5)
        logger.info("✅ Ciclo de análisis completado.")

    async def runAnalysisCycle_for_symbol(self, symbolInfo: Dict, preloaded_data: Dict = None, apiKey: str = None):
        """Procesa un solo símbolo (usado para análisis secuencial)."""
        
        symbol = symbolInfo['symbol']
        interval = symbolInfo.get('intervalo', '15min')
        logger.info(f"▶ ENTRANDO análisis para {symbol} ({interval})")
        
        if apiKey is None:
            apiKey, _, _, nVelas, _ = getParametros()
        else:
            _, _, _, nVelas, _ = getParametros()

        raw_df = preloaded_data.get(symbol) if preloaded_data else None
        data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval, raw_df)
        if data is None:
            logger.info(f"◀ SALIENDO análisis para {symbol} (sin datos)")
            return

        signal = await self._get_signal(data, symbol)
        if signal:
            signal['candle_time'] = data.index[-1].strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[{symbol}] Señal: {signal['direction']} ({signal['confidence']:.1f}% confianza)")
            await self._execute_trades(signal, symbolInfo)
        else:
            logger.info(f"[{symbol}] Sin señal en intervalo {interval}.")
        
        logger.info(f"◀ SALIENDO análisis para {symbol}")


