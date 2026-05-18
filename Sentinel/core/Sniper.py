"""
Core Trading Bot Class
"""
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional, Tuple, List
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
from Sentinel.analysis.technical import check_tp_exhaustion, check_signal_health
from Sentinel.ml import model as mlModel
from middleware.utils.communications import sendTelegramAlert, alertaInmediata, deleteTelegramMessage
from middleware.utils.alertBuilder import buildSniperAlertMessage, adjustTPForMinRR, getPipMultiplier, calculateRR, calculateBEPrice
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
        self._signals_sent = {}   # {"SYMBOL_candleTime": True} — dedup intra-ciclo
        self.latestFullData = None

    def _is_jpy_symbol(self, symbol: str, symbolInfo: Dict = None) -> bool:
        symbolInfo = symbolInfo or {}
        symbol_name = (symbol or symbolInfo.get("symbol") or "").upper()
        base_currency = str(symbolInfo.get("base_currency", "") or "").upper()
        quote_currency = str(symbolInfo.get("quote_currency", "") or "").upper()
        pip_value = symbolInfo.get("pip")

        if base_currency == "JPY" or quote_currency == "JPY" or "JPY" in symbol_name:
            return True

        try:
            return float(pip_value) >= 0.01
        except (TypeError, ValueError):
            return False

    def _is_exotic_symbol(self, symbolInfo: Dict = None) -> bool:
        symbolInfo = symbolInfo or {}
        return str(symbolInfo.get("tipo", "") or "").upper() == "EXOTIC"

    def _get_symbol_config_float(self, symbolInfo: Dict, key: str, default: float) -> float:
        try:
            value = (symbolInfo or {}).get(key)
            return default if value is None else float(value)
        except (TypeError, ValueError):
            return default

    def _get_symbol_config_int(self, symbolInfo: Dict, key: str, default: int) -> int:
        try:
            value = (symbolInfo or {}).get(key)
            return default if value is None else int(value)
        except (TypeError, ValueError):
            return default

    def _get_ml_thresholds(self, strat_config: Dict, symbol: str, symbolInfo: Dict = None) -> Tuple[float, float]:
        base_long = float(strat_config.get("proba_threshold_long", config.PROBA_THRESHOLD_LONG))
        base_short = float(strat_config.get("proba_threshold_short", config.PROBA_THRESHOLD_SHORT))

        if self._is_jpy_symbol(symbol, symbolInfo) or self._is_exotic_symbol(symbolInfo):
            default_adjust = float(strat_config.get("jpy_threshold_adjust_pct", 0.0))
            adjust_pct = self._get_symbol_config_float(symbolInfo, "sniper_threshold_adjust_pct", default_adjust) / 100.0
            base_long = 0.5 + ((base_long - 0.5) * (1 + adjust_pct))
            base_short = 0.5 - ((0.5 - base_short) * (1 + adjust_pct))

        return min(0.99, max(0.5, base_long)), max(0.01, min(0.5, base_short))

    def _get_min_confidence(self, strat_config: Dict, symbol: str, symbolInfo: Dict = None) -> float:
        min_confidence = float(strat_config.get("min_confidence", 70))
        if self._is_jpy_symbol(symbol, symbolInfo) or self._is_exotic_symbol(symbolInfo):
            default_adjust = float(strat_config.get("jpy_min_confidence_adjust_pct", 0.0))
            adjust_pct = self._get_symbol_config_float(symbolInfo, "sniper_min_confidence_adjust_pct", default_adjust) / 100.0
            min_confidence *= (1 + adjust_pct)
        return min_confidence

    def _get_extra_confirmations(self, strat_config: Dict, symbol: str, symbolInfo: Dict = None) -> int:
        if self._is_jpy_symbol(symbol, symbolInfo) or self._is_exotic_symbol(symbolInfo):
            default_extra = int(strat_config.get("jpy_extra_confirmations", 0))
            return self._get_symbol_config_int(symbolInfo, "sniper_extra_confirmations", default_extra)
        return 0

    def _get_max_rr(self, strat_config: Dict, symbol: str, symbolInfo: Dict, ratioBase: float) -> float:
        default_max_rr = float(strat_config.get("max_rr", min(ratioBase, 1.5)))
        return self._get_symbol_config_float(symbolInfo, "sniper_max_rr", default_max_rr)

    async def _get_and_prepare_data(self, symbolInfo: Dict, apiKey: str, nVelas: int, interval: str, raw_df: pd.DataFrame = None) -> pd.DataFrame | None:
        """Fetches, prepares, and enriches data with technical features."""
        symbol = symbolInfo['symbol']
        
        # 1. Download data or use provided raw data
        if isinstance(raw_df, dict):
            df = None
            for key in (interval, interval.lower(), "15min", "5min"):
                if key in raw_df and raw_df[key] is not None:
                    df = raw_df[key]
                    break
            if df is not None:
                df = df.copy()
        elif raw_df is not None and len(raw_df) >= 100:
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
            if df is None or len(df) < 100:
                logger.warning(f"[{symbol}] Datos insuficientes para análisis ({len(df) if df is not None else 0} velas).")
                return None

        if df is None or len(df) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes para análisis ({len(df) if df is not None else 0} velas).")
            return None

        df = technical.filter_to_closed_candles(df)
        
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

    def _calculate_dynamic_adx(self, df: pd.DataFrame) -> float:
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
                return float(pd.Series(dx).rolling(n).mean().iloc[-1])
            return 25.0
        except Exception as e:
            logger.warning(f"Error calculando ADX dinámico: {e}")
            return 25.0

    def _evaluate_technical_confirmations(self, df: pd.DataFrame, direction: str, symbol: str) -> Tuple[int, List[str], Dict[str, float]]:
        histVal = self.latestFullData["macdHist"].iloc[-1]
        prevHistVal = df["macdHist"].iloc[-2]
        macdLine = self.latestFullData["macd"].iloc[-1]
        macdSignal = self.latestFullData["macdSig"].iloc[-1]
        rsi = self.latestFullData["rsi"].iloc[-1]
        prevRsi = df["rsi"].iloc[-2]
        ema20 = self.latestFullData["ema20"].iloc[-1]
        ema50 = self.latestFullData["ema50"].iloc[-1]
        
        macdCrossLong = (macdLine > macdSignal) and (df["macd"].iloc[-2] <= df["macdSig"].iloc[-2])
        macdCrossShort = (macdLine < macdSignal) and (df["macd"].iloc[-2] >= df["macdSig"].iloc[-2])
        histImprovingLong = histVal > prevHistVal
        histImprovingShort = histVal < prevHistVal
        macdZeroCrossLong = (prevHistVal <= 0 and histVal > 0)
        macdZeroCrossShort = (prevHistVal >= 0 and histVal < 0)
        
        emaTrendLong = ema20 > ema50
        emaTrendShort = ema20 < ema50
        
        rsiImprovingLong = rsi > prevRsi
        rsiImprovingShort = rsi < prevRsi
        
        prices = df["close"].iloc[-5:].values
        hists = df["macdHist"].iloc[-5:].values
        bearishDivergence = (prices[-1] > np.max(prices[:-1])) and (hists[-1] < np.max(hists[:-1]))
        bullishDivergence = (prices[-1] < np.min(prices[:-1])) and (hists[-1] > np.min(hists[:-1]))
        
        techConfLong = (self.latestFullData["pendienteCci"].iloc[-1] > 0.5 and self.latestFullData["pendienteRsi"].iloc[-1] > 0.1)
        techConfShort = (self.latestFullData["pendienteCci"].iloc[-1] < -0.5 and self.latestFullData["pendienteRsi"].iloc[-1] < -0.1)
        
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
        else:
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
                
        metrics = {
            "rsi": float(rsi),
            "macdHist": float(histVal),
            "macd": float(macdLine),
            "macdSig": float(macdSignal),
            "pendienteRsi": float(self.latestFullData["pendienteRsi"].iloc[-1]),
            "pendienteCci": float(self.latestFullData["pendienteCci"].iloc[-1]),
            "macdZeroCrossLong": macdZeroCrossLong,
            "macdCrossLong": macdCrossLong,
            "emaTrendLong": emaTrendLong,
            "bullishDivergence": bullishDivergence,
            "macdZeroCrossShort": macdZeroCrossShort,
            "macdCrossShort": macdCrossShort,
            "emaTrendShort": emaTrendShort,
            "bearishDivergence": bearishDivergence,
            "macd_alcista": macdLine > macdSignal
        }
        
        return confirmaciones, detalles, metrics

    async def _get_signal(self, df: pd.DataFrame, symbol: str, symbolInfo: Dict = None) -> Optional[Signal]:
        """Analyzes the data to generate a Signal object using granular validations."""
        df = technical.filter_to_closed_candles(df)
        
        X, _ = mlModel.cleanDataForModel(df)
        if len(X) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes tras limpieza ({len(X)} filas).")
            return None

        # --- Get Current Values ---
        latest = X.iloc[-1]
        self.latestFullData = df
        
        close = self.latestFullData["close"].iloc[-1]
        currentAtr = latest["atr"]
        avgAtr = df["atr"].iloc[-20:].mean()
        
        # --- 1. Volatility Veto ---
        if currentAtr < avgAtr * 0.5:
            logger.info(f"[{symbol}] Volatilidad baja (ATR: {currentAtr:.4f} < 50% avg: {avgAtr:.4f}). Señal descartada.")
            return None
        
        # --- 2. ML Prediction ---
        proba = mlModel.predictProba(self.model, X)
        if proba is None:
            logger.info(f"[{symbol}] Rechazada: ML no pudo calcular probabilidad")
            return None

        strat_config = dbManager.getStrategyConfig("Sniper") or {}
        thresh_long, thresh_short = self._get_ml_thresholds(strat_config, symbol, symbolInfo)
        
        if proba >= thresh_long:
            direction = "LARGO"
            confianza = proba * 100
        elif proba <= thresh_short:
            direction = "CORTO"
            confianza = (1 - proba) * 100
        else:
            logger.info(f"[{symbol}] Rechazada: proba_largo={proba:.2f} dentro de zona neutra ({thresh_short:.2f}-{thresh_long:.2f})")
            return None
            
        ml_confidence = confianza
        logger.info(f"[{symbol}] ML proba_largo={proba:.2f} -> {direction} ({ml_confidence:.1f}% base, thresholds {thresh_short:.2f}/{thresh_long:.2f})")

        # --- 3. Momentum Check ---
        momentumEstado = symbolInfo.get('momentum', '☁️ SIN DATOS') if symbolInfo else '☁️ SIN DATOS'
        if momentumEstado == '☁️ SIN DATOS':
            dfWithAngles = calcularAngulos(df.copy())
            lastAngle = dfWithAngles.iloc[-1]
            momentumEstado, _ = obtenerEstado(lastAngle.get('ang_rsi'), lastAngle.get('ang_close'))
            
        momentumVeto = momentumEstado in ["💸 LIQUIDACIÓN"]
        if momentumVeto:
            logger.info(f"[{symbol}] Filtrado MOMENTUM crítico ({momentumEstado}). Señal vetada.")
            return None
            
        momentumNeutral = momentumEstado in ["☁️ NEUTRAL"]
        if momentumNeutral and len(self.latestFullData) >= 8:
            price_4h_ago = self.latestFullData["close"].iloc[-8]
            if direction == "LARGO" and close <= price_4h_ago:
                logger.info(f"[{symbol}] Filtrado MOMENTUM: Neutral + lateral/bajista")
                return None
            elif direction == "CORTO" and close >= price_4h_ago:
                logger.info(f"[{symbol}] Filtrado MOMENTUM: Neutral + lateral/alcista")
                return None

        # --- 4. Technical Confirmations ---
        confirmaciones, detalles, metrics = self._evaluate_technical_confirmations(df, direction, symbol)
        
        if direction == "LARGO" and not metrics["macd_alcista"]:
            logger.info(f"[{symbol}] CONTRADICCIÓN: ML dice LARGO pero MACD bajista")
            return None
        elif direction == "CORTO" and metrics["macd_alcista"]:
            logger.info(f"[{symbol}] CONTRADICCIÓN: ML dice CORTO pero MACD alcista")
            return None

        logger.info(f"[{symbol}] Confirmaciones: {confirmaciones} - {detalles}")

        # --- 5. ADX Filter ---
        adx_val = self._calculate_dynamic_adx(df)
        mercado_erratico = adx_val < 20
        min_confirmaciones = 3 if mercado_erratico else 2
        min_confirmaciones += self._get_extra_confirmations(strat_config, symbol, symbolInfo)
        
        if mercado_erratico:
            logger.info(f"[{symbol}] Rechazada: Mercado lateral (ADX={adx_val:.1f} < 20)")
            return None
            
        if confirmaciones < min_confirmaciones:
            logger.info(f"[{symbol}] Rechazada: Solo {confirmaciones} confirmaciones (mín={min_confirmaciones})")
            return None

        # --- 6. Confidence Bonus ---
        if direction == "LARGO":
            if metrics["macdZeroCrossLong"]: confianza += 15
            elif metrics["macdCrossLong"]: confianza += 10
            if metrics["emaTrendLong"]: confianza += 8
            if metrics["bullishDivergence"]: confianza += 12
        else:
            if metrics["macdZeroCrossShort"]: confianza += 15
            elif metrics["macdCrossShort"]: confianza += 10
            if metrics["emaTrendShort"]: confianza += 8
            if metrics["bearishDivergence"]: confianza += 12
            
        if confirmaciones >= 4: confianza += 10
        elif confirmaciones >= 3: confianza += 5
        
        momentumBullish = momentumEstado in ["🚀 ALCISTA", "💎 GIRO"]
        momentumBearish = momentumEstado in ["📉 BAJISTA"]
        if direction == "LARGO" and momentumBullish: confianza += 10
        elif direction == "LARGO" and momentumBearish: confianza -= 15
        elif direction == "CORTO" and momentumBearish: confianza += 10
        elif direction == "CORTO" and momentumBullish: confianza -= 15
            
        cdlEngulfing = self.latestFullData.get("cdlEngulfing", pd.Series([0])).iloc[-1]
        cdlHammer = self.latestFullData.get("cdlHammer", pd.Series([0])).iloc[-1]
        cdlShootingStar = self.latestFullData.get("cdlShootingStar", pd.Series([0])).iloc[-1]

        if (direction == "LARGO" and (cdlEngulfing > 0 or cdlHammer > 0)) or (direction == "CORTO" and (cdlEngulfing < 0 or cdlShootingStar < 0)):
            confianza *= 1.10

        # --- 7. Order Blocks ---
        ob_dir = 'LARGO' if direction == 'LARGO' else 'CORTO'
        obs_sniper = detect_order_blocks(df, ob_dir, lookback=60)
        ob_conf_data = ob_confluence_score(close, obs_sniper, ob_dir, atr=currentAtr)
        ob_score = ob_conf_data['score']

        if ob_conf_data['in_ob_zone']: confianza += 10
        elif ob_score >= 10: confianza += 5

        # Filtrar por confianza mínima
        min_confidence = self._get_min_confidence(strat_config, symbol, symbolInfo)
        if confianza < min_confidence:
            logger.info(f"[{symbol}] Filtrado: Confianza {confianza:.1f}% < min_confidence={min_confidence}")
            return None

        # --- 8. Structural Levels ---
        levels = technical.get_structural_levels(self.latestFullData, lookback=40)
        atr_padding = currentAtr * 0.2
        
        if direction == "LARGO":
            sl_price = levels['swing_low'] - atr_padding
            sl_dist = max(currentAtr * 1.2, min(close - sl_price, currentAtr * 3.0))
            tp_structural = levels['high_zone']
        else:
            sl_price = levels['swing_high'] + atr_padding
            sl_dist = max(currentAtr * 1.2, min(sl_price - close, currentAtr * 3.0))
            tp_structural = levels['low_zone']

        # --- 9. Exhaustion ---
        vela_origen_idx = len(df) - 5
        is_valid, _, mensaje = check_tp_exhaustion(df, vela_origen_idx, close, tp_structural, sl_price, direction, threshold=0.60, timeframe="15M")
        if not is_valid:
            logger.info(f"[{symbol}] Señal descartada: Exhaustion - {mensaje}")
            return None

        # --- 10. SL / TP Final ---
        slPrice = close - sl_dist if direction == "LARGO" else close + sl_dist
        ratioBase = config.HIGH_CONFIDENCE_RISK_REWARD_RATIO if confianza > 85 else config.BASE_RISK_REWARD_RATIO
        min_rr_val = float(strat_config.get('min_rr', 1.5))
        max_rr_val = self._get_max_rr(strat_config, symbol, symbolInfo, ratioBase)
        max_rr_val = max(min_rr_val, max_rr_val)

        tp_by_rr = close + (sl_dist * max_rr_val) if direction == "LARGO" else close - (sl_dist * max_rr_val)
        structural_is_valid = (
            (direction == "LARGO" and tp_structural and tp_structural > close) or
            (direction == "CORTO" and tp_structural and tp_structural < close)
        )

        if structural_is_valid:
            tpPrice = min(tp_structural, tp_by_rr) if direction == "LARGO" else max(tp_structural, tp_by_rr)
        else:
            tpPrice = tp_by_rr

        rr_final = calculateRR(close, slPrice, tpPrice)
        if rr_final < min_rr_val:
            logger.info(f"[{symbol}] Señal descartada: TP estructural deja RR={rr_final:.2f} < min_rr={min_rr_val:.2f}")
            return None

        multiplier = getPipMultiplier(symbol)
        risk_usd = float(strat_config.get('risk_usd', 100.0))
        size = (risk_usd / (sl_dist * multiplier)) if (sl_dist > 0 and multiplier > 0) else 0

        # --- FILTRO: Ganancia Mínima Estimada ---
        min_usd_profit = float(strat_config.get('min_usd_profit', 10.0))
        expected_profit = risk_usd * rr_final

        if expected_profit < min_usd_profit:
            logger.info(f"[{symbol}] Sniper: Beneficio Est. ${expected_profit:.2f} < ${min_usd_profit:.2f} - descartando")
            return None
        
        is_valid, _, mensaje = check_signal_health(close, tpPrice, slPrice, direction, close, threshold=0.65)
        if not is_valid:
            logger.info(f"[{symbol}] Señal descartada: Health - {mensaje}")
            return None
        
        confianza = min(100, max(0, confianza))
        metrics.update({"atr": float(currentAtr)})
        
        break_even = calculateBEPrice(close, slPrice, tpPrice, direction)
        
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
            candleTime="", 
            intervalo=symbolInfo.get('intervalo', '15min'),
            riesgo_pips=round(sl_dist * getPipMultiplier(symbol), 1),
            rr_ratio=round(calculateRR(close, slPrice, tpPrice), 2),
            break_even=break_even,
            metadata={
                "ob_score": ob_score,
                "in_ob_zone": ob_conf_data['in_ob_zone'],
                "confirmaciones": confirmaciones,
                "detalles_conf": detalles,
                "ml_proba_largo": round(float(proba), 4),
                "ml_confidence_base": round(float(ml_confidence), 2),
                "ml_threshold_long": round(float(thresh_long), 4),
                "ml_threshold_short": round(float(thresh_short), 4),
                "symbol_tipo": (symbolInfo or {}).get("tipo"),
                "symbol_pip": (symbolInfo or {}).get("pip"),
                "symbol_quote_currency": (symbolInfo or {}).get("quote_currency"),
                "tp_structural": tp_structural,
                "latestMetrics": metrics
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
            interval_val = 15 # default
            if 'min' in interval: interval_val = int(interval.replace('min', ''))
            elif '15M' in interval.upper(): interval_val = 15
            
            last_v = get_last_closed_candle(now_cdmx, interval=interval_val, df=data)
            last_closed_ts = last_v.name if hasattr(last_v, 'name') else last_v
            signal.candleTime = last_closed_ts.strftime("%Y-%m-%d %H:%M:%S")

            
            sig_key = f"{symbol}_{signal.candleTime}"
            if sig_key in self._signals_sent:
                logger.info(f"[{symbol}] Señal ya emitida en este ciclo para vela {signal.candleTime} — omitiendo duplicado.")
                return None
            self._signals_sent[sig_key] = True
            
            logger.info(f"[{symbol}] Señal generada: {signal.direction} ({signal.confidence:.1f}% confianza)")
            return signal
        
        return None
