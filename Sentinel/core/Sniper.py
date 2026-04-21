"""
Core Trading Bot Class
"""
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
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
from Sentinel.analysis.technical import check_tp_exhaustion
from Sentinel.ml import model as mlModel
from middleware.utils.communications import sendTelegramAlert, alertaInmediata, deleteTelegramMessage
from middleware.utils.alertBuilder import buildSniperAlertMessage, adjustTPForMinRR, getPipMultiplier, calculateRR
from middleware.database import dbManager
from middleware.scheduler.autoScheduler import getTiempoEspera, isRestTime
from Sentinel.data.dataLoader import getParametros
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from middleware.utils.momentum import calcularAngulos, obtenerEstado
from Sentinel.analysis.orderblocks import detect_order_blocks, ob_confluence_score

from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class SniperBot:
    def __init__(self, mlModelInstance):
        self.model = mlModelInstance
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

    async def _get_signal(self, df: pd.DataFrame, symbol: str, symbolInfo: Dict = None) -> Optional[Signal]:
        """Analyzes the data to generate a Signal object."""
        
        X, _ = mlModel.cleanDataForModel(df)
        if len(X) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes tras limpieza ({len(X)} filas).")
            return None

        # --- Get Current Values ---
        latest = X.iloc[-1]
        self.latestFullData = df  # Guardar DataFrame completo para acceso histórico
        
        close = self.latestFullData["close"].iloc[-1]
        currentAtr = latest["atr"]
        avgAtr = df["atr"].iloc[-20:].mean()
        
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
        histVal = self.latestFullData["macdHist"].iloc[-1]
        prevHistVal = df["macdHist"].iloc[-2]
        
        macdLine = self.latestFullData["macd"].iloc[-1]
        macdSignal = self.latestFullData["macdSig"].iloc[-1]
        
        rsi = latest["rsi"]
        prevRsi = df["rsi"].iloc[-2]
        
        ema20 = self.latestFullData["ema20"].iloc[-1]
        ema50 = self.latestFullData["ema50"].iloc[-1]
        
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
        
        # Divergencia MACD (últimas 5 velas)
        prices = df["close"].iloc[-5:].values
        hists = df["macdHist"].iloc[-5:].values
        bearishDivergence = (prices[-1] > np.max(prices[:-1])) and (hists[-1] < np.max(hists[:-1]))
        bullishDivergence = (prices[-1] < np.min(prices[:-1])) and (hists[-1] > np.min(hists[:-1]))
        
        # CCI + RSI pendientes
        techConfLong = (self.latestFullData["pendienteCci"].iloc[-1] > 0.5 and self.latestFullData["pendienteRsi"].iloc[-1] > 0.1)
        techConfShort = (self.latestFullData["pendienteCci"].iloc[-1] < -0.5 and self.latestFullData["pendienteRsi"].iloc[-1] < -0.1)
        
        # --- MOMENTUM FILTER (usar pre-calculado desde main.py) ---
        momentumEstado = symbolInfo.get('momentum', '☁️ SIN DATOS') if symbolInfo else '☁️ SIN DATOS'
        if momentumEstado == '☁️ SIN DATOS':
            dfWithAngles = calcularAngulos(df.copy())
            lastAngle = dfWithAngles.iloc[-1]
            momentumEstado, _ = obtenerEstado(lastAngle.get('ang_rsi'), lastAngle.get('ang_close'))
        
        momentumBullish = momentumEstado in ["🚀 ALCISTA", "💎 GIRO"]
        momentumBearish = momentumEstado in ["📉 BAJISTA"]
        momentumNeutral = momentumEstado in ["☁️ NEUTRAL"]
        momentumVeto = momentumEstado in ["💸 LIQUIDACIÓN"]
        
        if momentumVeto:
            logger.info(f"[{symbol}] Filtrado MOMENTUM: Estado crítico ({momentumEstado}). Señal vetada.")
            return None
        
        # --- Determinar dirección por ML ---
        from middleware.database import dbManager
        strat_config = dbManager.getStrategyConfig("Sniper") or {}
        
        # Usar umbral dinámico desde BD si existe, de lo contrario usar constante
        min_conf_db = float(strat_config.get('min_confidence', 55)) / 100.0
        thresh_long = min_conf_db
        thresh_short = 1.0 - min_conf_db
        
        if proba >= thresh_long:
            direction = "LARGO"
            confianza = proba * 100
        elif proba <= thresh_short:
            direction = "CORTO"
            confianza = (1 - proba) * 100
        else:
            logger.info(f"[{symbol}] Rechazada: ML indeciso (proba={proba:.2f}, zona neutral o debajo de umbral {min_conf_db})")
            return None

        
        # --- NEW: Verificar tendencia cuando momentum es Neutral ---
        if momentumNeutral and len(self.latestFullData) >= 8:
            close = self.latestFullData["close"].iloc[-1]
            price_4h_ago = self.latestFullData["close"].iloc[-8]
            if direction == "LARGO" and close <= price_4h_ago:
                logger.info(f"[{symbol}] Filtrado MOMENTUM: Neutral + precio lateral/bajista (no comprar aún)")
                return None
            elif direction == "CORTO" and close >= price_4h_ago:
                logger.info(f"[{symbol}] Filtrado MOMENTUM: Neutral + precio lateral/alcista (no vender aún)")
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
            if bullishDivergence:
                confirmaciones -= 1
                detalles.append("⚠️DIV_ALCISTA")
        
        # --- ADX Dinámico ---
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
                adx_val = 25
        except:
            adx_val = 25
        
        mercado_erratico = adx_val < 20
        min_confirmaciones = 3 if mercado_erratico else 2
        
        if mercado_erratico:
            logger.info(f"[{symbol}] Rechazada: Mercado lateral (ADX={adx_val:.1f} < 20)")
            return None
        
        if confirmaciones < min_confirmaciones:
            logger.info(f"[{symbol}] Rechazada: Solo {confirmaciones} confirmaciones (mín={min_confirmaciones})")
            return None
        
        # --- Confidence Bonus ---
        if direction == "LARGO":
            if macdZeroCrossLong: confianza += 15
            elif macdCrossLong: confianza += 10
            if emaTrendLong: confianza += 8
            if bullishDivergence: confianza += 12
        else:
            if macdZeroCrossShort: confianza += 15
            elif macdCrossShort: confianza += 10
            if emaTrendShort: confianza += 8
            if bearishDivergence: confianza += 12
        
        if confirmaciones >= 4: confianza += 10
        elif confirmaciones >= 3: confianza += 5
        
        if direction == "LARGO" and momentumBullish: confianza += 10
        elif direction == "LARGO" and momentumBearish: confianza -= 15
        elif direction == "CORTO" and momentumBearish: confianza += 10
        elif direction == "CORTO" and momentumBullish: confianza -= 15
            
        cdlEngulfing = self.latestFullData["cdlEngulfing"].iloc[-1] if "cdlEngulfing" in self.latestFullData.columns else 0
        cdlHammer = self.latestFullData["cdlHammer"].iloc[-1] if "cdlHammer" in self.latestFullData.columns else 0
        cdlShootingStar = self.latestFullData["cdlShootingStar"].iloc[-1] if "cdlShootingStar" in self.latestFullData.columns else 0

        if (direction == "LARGO" and (cdlEngulfing > 0 or cdlHammer > 0)) or (direction == "CORTO" and (cdlEngulfing < 0 or cdlShootingStar < 0)):
            confianza *= 1.10

        # Order Block
        ob_dir = 'LARGO' if direction == 'LARGO' else 'CORTO'
        obs_sniper = detect_order_blocks(df, ob_dir, lookback=60)
        ob_conf_data = ob_confluence_score(close, obs_sniper, ob_dir, atr=currentAtr)
        ob_score = ob_conf_data['score']

        if ob_conf_data['in_ob_zone']:
            confianza += 10
        elif ob_score >= 10:
            confianza += 5

        if confianza < config.MIN_CONFIDENCE_THRESHOLD:
            logger.info(f"[{symbol}] Filtrado: Confianza muy baja ({confianza:.1f}%).")
            return None

        # Structural Levels
        levels = technical.get_structural_levels(self.latestFullData, lookback=40)
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

        # Exhaustion Filter
        vela_origen_idx = len(df) - 5
        is_valid, _, mensaje = check_tp_exhaustion(df, vela_origen_idx, close, tp_structural, sl_price, direction, threshold=0.60, timeframe="15M")
        if not is_valid:
            logger.info(f"[{symbol}] Señal descartada: Exhaustion - {mensaje}")
            return None

        # Prepare final SL/TP (logic moved from _execute_trades to here)
        slPrice = close - sl_dist if direction == "LARGO" else close + sl_dist
        ratioBase = config.HIGH_CONFIDENCE_RISK_REWARD_RATIO if confianza > 85 else config.BASE_RISK_REWARD_RATIO
        
        tp_initial = tp_structural if tp_structural else (close + (sl_dist * ratioBase) if direction == "LARGO" else close - (sl_dist * ratioBase))
        tpPrice = adjustTPForMinRR(close, slPrice, tp_initial, direction, minRR=1.5)
        
        multiplier = getPipMultiplier(symbol)
        
        return Signal(
            strategy="Sniper",
            symbol=symbol,
            direction=direction,
            entry_price=close,
            stop_loss=slPrice,
            take_profit=tpPrice,
            sl_distance=sl_dist,
            confidence=confianza,
            setup=f"ML SNIPER {symbolInfo.get('intervalo', '15min').upper()}",
            status="EN ZONA ✅",
            candle_time="", # Will be set in runAnalysisCycle
            intervalo=symbolInfo.get('intervalo', '15min'),
            riesgo_pips=round(sl_dist * multiplier, 1),
            rr_ratio=round(calculateRR(close, slPrice, tpPrice), 2),
            metadata={
                "ob_score": ob_score,
                "in_ob_zone": ob_conf_data['in_ob_zone'],
                "confirmaciones": confirmaciones,
                "detalles_conf": detalles
            }
        )

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloaded_data: Dict = None, apiKey: str = None) -> Optional[Signal]:
        """Procesa un solo símbolo y devuelve una señal si existe."""
        
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
            return None

        signal = await self._get_signal(data, symbol, symbolInfo)
        if signal:
            now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
            last_closed = get_last_closed_candle(now_cdmx, interval=15)
            signal.candle_time = last_closed.strftime("%Y-%m-%d %H:%M:%S")
            logger.info(f"[{symbol}] Señal generada: {signal.direction} ({signal.confidence:.1f}% confianza)")
            return signal
        
        return None



