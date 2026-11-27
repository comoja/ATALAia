"""
Microservicio Autónomo de Monitoreo y Ejecución de SNIPER (microSniper.py)
Estrategia: SNIPER (Smart Order Block, Momentum & Machine Learning Precision)
Opera sobre emisoras individuales en temporalidad intradiaria (15min por defecto),
utilizando:
  - Detección de Order Blocks (zonas de reacción institucional) y niveles estructurales.
  - Modelo de Machine Learning (RandomForest) entrenado en Sentinel para predicción de probabilidad.
  - R:R y Swing adaptativos basados en la jerarquía:
      symbolstrategyconfig -> symboltypeconfig -> strategyconfig -> userSNIPER.
  - Salidas: Take Profit Estructural (R:R >= 1.5 acotado a 2.5 ATR), Stop Loss Estructural
    (Swing High/Low +- 0.1 ATR) y BreakEven Automático adaptativo.
  - Opcional: Trailing Stop Loss (TSL) una vez alcanzado el BreakEven.
Registra operaciones en la tabla trades con strategy = 'SNIPER'.
Estándar de Nomenclatura: camelCase para funciones y variables.
"""

import os
import sys
import time
import math
import json
import logging
import asyncio
import pymysql
import urllib.request
import urllib.error
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple, Set

import numpy as np
import pandas as pd
import requests
from sqlalchemy import text

# Asegurar que el directorio raíz de ATALAia esté en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal
from cachetools import TTLCache
_sniperCandlesCache = TTLCache(maxsize=128, ttl=8)
from backend.core.pnl_calculator import calculate_unrealized_pnl, calculate_margin
from backend.services.microRatio import calculateSymbolNATR
from backend.services.trade_executor import (
    check_account_margin_health,
    check_projected_margin,
    execute_single_asset_entry,
    execute_single_asset_close,
    send_broker_order,
    get_account_broker
)
from Sentinel.ml import model as mlModel
from Sentinel.core.Sniper import SniperBot
from Sentinel.analysis import technical
from Sentinel.core.models import Signal
from middleware.config import constants as config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [microSniper] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("microSniper")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://127.0.0.1:8002/webhook/tradingview")
STRATEGY_NAME = "SNIPER"
SNIPER_MODEL_PATH = config.MODEL_FILE_PATH

_lastSniperLogTime: Dict[Tuple[int, str], float] = {}
_failedOrderCooldown: Dict[Tuple[int, str], float] = {}
_globalSniperBot: Optional[SniperBot] = None


def shouldLogStatus(idCuenta: int, symbol: str, intervalSec: float = 30.0) -> bool:
    key = (idCuenta, symbol)
    now = time.time()
    last = _lastSniperLogTime.get(key, 0.0)
    if now - last >= intervalSec:
        _lastSniperLogTime[key] = now
        return True
    return False


def isOrderInCooldown(idCuenta: int, symbol: str) -> bool:
    key = (idCuenta, symbol)
    now = time.time()
    until = _failedOrderCooldown.get(key, 0.0)
    return now < until


def setOrderCooldown(idCuenta: int, symbol: str, seconds: float = 120.0) -> None:
    key = (idCuenta, symbol)
    _failedOrderCooldown[key] = time.time() + seconds


def isCryptoSymbol(symbol: Optional[str] = None) -> bool:
    sym = (symbol or "").upper().replace("/", "").replace("_", "")
    return any(c in sym for c in ["BTC", "ETH", "ETHEREUM", "SOL", "LTC", "XRP", "DOGE"])


def isMarketOpenForSymbol(symbol: Optional[str] = None) -> bool:
    if isCryptoSymbol(symbol):
        return True
    nowUtc = datetime.now(timezone.utc)
    weekday = nowUtc.weekday()
    hour = nowUtc.hour
    minute = nowUtc.minute
    # Cierre de mercado Forex fin de semana: viernes 21:00 UTC a domingo 21:00 UTC
    if weekday == 4 and (hour > 21 or (hour == 21 and minute >= 0)):
        return False
    if weekday == 5:
        return False
    if weekday == 6 and (hour < 21):
        return False

    sym = (symbol or "").upper().replace("/", "").replace("_", "")
    # Receso diario de mantenimiento Petróleo / Energías (XTI, XBR, OIL, BRENT): 23:00 a 00:00 UTC (17:00 - 18:00 CST)
    if any(k in sym for k in ["XBR", "XTI", "BRENT", "CRUDE", "OIL"]):
        if weekday in [0, 1, 2, 3, 4] and hour == 23:
            return False

    # Receso diario de mantenimiento Metales (XAU, XAG, GOLD, SILVER): 22:00 a 23:00 UTC (16:00 - 17:00 CST)
    if any(k in sym for k in ["XAU", "XAG", "GOLD", "SILVER"]):
        if weekday in [0, 1, 2, 3, 4] and hour == 22:
            return False

    return True


def isWeekendClosingWindow(symbol: Optional[str] = None, dbSession=None) -> bool:
    if isCryptoSymbol(symbol):
        return False
    nowUtc = datetime.now(timezone.utc)
    weekday = nowUtc.weekday()
    hour = nowUtc.hour
    minute = nowUtc.minute
    # Viernes después de las 20:30 UTC
    if weekday == 4 and (hour > 20 or (hour == 20 and minute >= 30)):
        return True
    return False


def isDividendShortRestricted(dbSession, symbol: str) -> Tuple[bool, str]:
    if not symbol or isCryptoSymbol(symbol):
        return False, ""
    try:
        clean = symbol.upper().replace("/", "").replace("_", "").strip()
        sql = text("""
            SELECT id, symbol, fecha_ex_dividendo, fecha_pago, dividendo_por_accion
            FROM dividendos_restricciones
            WHERE UPPER(REPLACE(REPLACE(symbol, '/', ''), '_', '')) = :sym
              AND CURDATE() BETWEEN DATE_SUB(fecha_ex_dividendo, INTERVAL 1 DAY)
                                AND COALESCE(fecha_pago, DATE_ADD(fecha_ex_dividendo, INTERVAL 7 DAY))
            LIMIT 1
        """)
        row = dbSession.execute(sql, {"sym": clean}).fetchone()
        if row:
            fEx = row[2].strftime("%Y-%m-%d") if hasattr(row[2], "strftime") else str(row[2])
            fPago = row[3].strftime("%Y-%m-%d") if hasattr(row[3], "strftime") and row[3] else "vigente"
            monto = float(row[4]) if row[4] else 0.0
            return True, f"Ex-Div: {fEx} | Pago: {fPago} | Monto: ${monto:.2f}"
    except Exception as ex:
        logger.warning(f"Error verificando restricción de dividendos para {symbol}: {ex}")
    return False, ""


def isWithinOperatingHours(
    hInicioStr: str = "08:00",
    hFinStr: str = "16:00",
    currentDt: Optional[datetime] = None,
    hInicio2Str: Optional[str] = None,
    hFin2Str: Optional[str] = None
) -> bool:
    try:
        nowLocal = currentDt if currentDt is not None else datetime.now()
        currentMinutes = nowLocal.hour * 60 + nowLocal.minute

        def sanitizeTime(t: Any) -> Optional[str]:
            if t is None:
                return None
            s = str(t).strip().lower()
            if s in ["", "null", "none", "nan", "undefined"]:
                return None
            if ":" not in s:
                return None
            return str(t).strip()

        def parseMinutes(hhmm: str) -> int:
            parts = str(hhmm).strip().split(":")
            return int(parts[0]) * 60 + int(parts[1])

        def checkWindow(startStr: Optional[str], endStr: Optional[str]) -> bool:
            cleanStart = sanitizeTime(startStr)
            cleanEnd = sanitizeTime(endStr)
            if not cleanStart or not cleanEnd:
                return False
            startM = parseMinutes(cleanStart)
            endM = parseMinutes(cleanEnd)
            if startM <= endM:
                return startM <= currentMinutes <= endM
            else:
                # Cruza la medianoche (ej. 23:00 a 10:00)
                return currentMinutes >= startM or currentMinutes <= endM

        # Si viene en formato con comas en una sola cadena
        cleanH1 = sanitizeTime(hInicioStr)
        cleanH2 = sanitizeTime(hFinStr)
        if cleanH1 and cleanH2 and "," in cleanH1 and "," in cleanH2:
            sParts = cleanH1.split(",")
            eParts = cleanH2.split(",")
            if len(sParts) >= 2 and len(eParts) >= 2:
                return checkWindow(sParts[0], eParts[0]) or checkWindow(sParts[1], eParts[1])

        if checkWindow(hInicioStr, hFinStr):
            return True

        cleanS2 = sanitizeTime(hInicio2Str)
        cleanE2 = sanitizeTime(hFin2Str)
        if cleanS2 and cleanE2:
            return checkWindow(cleanS2, cleanE2)

        return False
    except Exception as ex:
        logger.warning(f"Error parseando horario ({hInicioStr}-{hFinStr} / {hInicio2Str}-{hFin2Str}): {ex}")
        return False


def getSniperBotInstance() -> SniperBot:
    global _globalSniperBot
    if _globalSniperBot is None:
        model = mlModel.loadModel(SNIPER_MODEL_PATH)
        if model is None:
            logger.error(f"❌ No se pudo cargar el modelo ML desde {SNIPER_MODEL_PATH}")
        _globalSniperBot = SniperBot(mlModelInstance=model)
    return _globalSniperBot


def getAdaptiveConfigForSymbol(dbSession, symbol: str) -> Dict[str, Any]:
    """
    Obtiene la configuración adaptativa de SNIPER en cascada:
    1. Base desde strategyconfig (strategy = 'Sniper')
    2. Adaptación por tipo de activo desde symboltypeconfig
    3. Overrides específicos por símbolo desde symbolstrategyconfig
    """
    configData = {
        "minRr": 1.5,
        "maxRr": 2.5,
        "minConfidence": 70.0,
        "slAtrMult": 1.5,
        "tpAtrMult": 2.5,
        "lookback": 20,
        "probaThresholdLong": 0.60,
        "probaThresholdShort": 0.40,
        "jpyThresholdAdjustPct": 20.0,
        "jpyMinConfidenceAdjustPct": 10.0,
        "jpyExtraConfirmations": 1,
        "tipo": "FOREX"
    }

    try:
        # 1. strategyconfig
        sqlStrat = text("SELECT min_rr, max_rr, min_confidence, proba_threshold_long, proba_threshold_short, jpy_threshold_adjust_pct, jpy_min_confidence_adjust_pct, jpy_extra_confirmations FROM strategyconfig WHERE strategy = 'Sniper' LIMIT 1")
        rowStrat = dbSession.execute(sqlStrat).mappings().fetchone()
        if rowStrat:
            if rowStrat.get("min_rr") is not None: configData["minRr"] = float(rowStrat["min_rr"])
            if rowStrat.get("max_rr") is not None: configData["maxRr"] = float(rowStrat["max_rr"])
            if rowStrat.get("min_confidence") is not None: configData["minConfidence"] = float(rowStrat["min_confidence"])
            if rowStrat.get("proba_threshold_long") is not None: configData["probaThresholdLong"] = float(rowStrat["proba_threshold_long"])
            if rowStrat.get("proba_threshold_short") is not None: configData["probaThresholdShort"] = float(rowStrat["proba_threshold_short"])
            if rowStrat.get("jpy_threshold_adjust_pct") is not None: configData["jpyThresholdAdjustPct"] = float(rowStrat["jpy_threshold_adjust_pct"])
            if rowStrat.get("jpy_min_confidence_adjust_pct") is not None: configData["jpyMinConfidenceAdjustPct"] = float(rowStrat["jpy_min_confidence_adjust_pct"])
            if rowStrat.get("jpy_extra_confirmations") is not None: configData["jpyExtraConfirmations"] = int(rowStrat["jpy_extra_confirmations"])

        # 2. Determinar tipo de símbolo y symboltypeconfig
        cleanSym = symbol.replace("/", "").upper()
        sqlSym = text("SELECT tipo, pip FROM symbols WHERE UPPER(REPLACE(symbol, '/', '')) = :s LIMIT 1")
        rowSym = dbSession.execute(sqlSym, {"s": cleanSym}).mappings().fetchone()
        symTipo = (rowSym.get("tipo") if rowSym else "FOREX") or "FOREX"
        configData["tipo"] = symTipo.upper()

        sqlType = text("SELECT sl_atr, tp_atr FROM symboltypeconfig WHERE tipo = :tp LIMIT 1")
        rowType = dbSession.execute(sqlType, {"tp": symTipo}).mappings().fetchone()
        if rowType:
            if rowType.get("sl_atr") is not None: configData["slAtrMult"] = float(rowType["sl_atr"])
            if rowType.get("tp_atr") is not None: configData["tpAtrMult"] = float(rowType["tp_atr"])

        # 3. symbolstrategyconfig
        sqlSymStrat = text("SELECT parametersJson, jsonIMACD FROM symbolstrategyconfig WHERE strategy = 'Sniper' AND symbol = :sym LIMIT 1")
        rowSymStrat = dbSession.execute(sqlSymStrat, {"sym": symbol}).mappings().fetchone()
        if rowSymStrat and rowSymStrat.get("parametersJson"):
            rawP = rowSymStrat["parametersJson"]
            pDict = json.loads(rawP) if isinstance(rawP, str) else rawP
            if pDict.get("minRr") is not None: configData["minRr"] = float(pDict["minRr"])
            if pDict.get("minConfidence") is not None: configData["minConfidence"] = float(pDict["minConfidence"])
            if pDict.get("probaThresholdLong") is not None: configData["probaThresholdLong"] = float(pDict["probaThresholdLong"])
            if pDict.get("lookback") is not None: configData["lookback"] = int(pDict["lookback"])
    except Exception as ex:
        logger.warning(f"Error consultando jerarquía adaptativa para {symbol}: {ex}")

    return configData


def fetchSniperCandlesHistory(symbol: str, timeframe: str = "15min", days: int = 180, limitBars: int = 300) -> pd.DataFrame:
    """
    Obtiene velas OHLCV unificadas y en tiempo real para SNIPER (15min por defecto),
    complementando con velas recientes de 1min hasta el instante actual.
    Retorna columnas estándar: close, high, low, open, volume.
    Incluye caché en memoria TTL de 8s para máxima velocidad.
    """
    cacheKey = (str(symbol).upper().strip(), str(timeframe).lower().strip(), int(days), int(limitBars))
    if cacheKey in _sniperCandlesCache:
        return _sniperCandlesCache[cacheKey].copy()

    db = SessionLocal()
    try:
        conn = db.connection().connection
        tfLower = timeframe.lower().strip()
        tfMap = {"15min": "15min", "15m": "15min", "5min": "5min", "5m": "5min", "1h": "1h", "1min": "1min", "1m": "1min", "1d": "1D"}
        targetTf = tfMap.get(tfLower, "15min")
        dtStart = datetime.now() - timedelta(days=max(30, days))

        sqlExact = """
            SELECT timestamp, close, high, low, open, COALESCE(volume, 0.0) AS volume
            FROM candles FORCE INDEX (idx_candles_sym_tf_ts)
            WHERE symbol = %s AND timeframe = %s AND timestamp >= %s
            ORDER BY timestamp ASC
        """
        dfExact = pd.read_sql(sqlExact, conn, params=(symbol, targetTf, dtStart.strftime("%Y-%m-%d %H:%M:%S")))
        if not dfExact.empty and len(dfExact) >= 20:
            dfExact["timestamp"] = pd.to_datetime(dfExact["timestamp"])
            dfExact = dfExact.drop_duplicates(subset=["timestamp"], keep="last").set_index("timestamp")
            for c in ["close", "high", "low", "open", "volume"]:
                dfExact[c] = pd.to_numeric(dfExact[c], errors="coerce")

            # Complementar con 1min recientes
            if targetTf != "1min" and not dfExact.empty:
                lastDt = dfExact.index[-1]
                sqlRecent1m = """
                    SELECT timestamp, close, high, low, open, COALESCE(volume, 0.0) AS volume
                    FROM candles FORCE INDEX (idx_candles_sym_tf_ts)
                    WHERE symbol = %s AND timeframe = '1min' AND timestamp >= %s
                    ORDER BY timestamp ASC
                """
                dfRecent = pd.read_sql(sqlRecent1m, conn, params=(symbol, lastDt.strftime("%Y-%m-%d %H:%M:%S")))
                if not dfRecent.empty and len(dfRecent) > 1:
                    dfRecent["timestamp"] = pd.to_datetime(dfRecent["timestamp"])
                    dfRecent = dfRecent.drop_duplicates(subset=["timestamp"], keep="first").set_index("timestamp")
                    for c in ["close", "high", "low", "open", "volume"]:
                        dfRecent[c] = pd.to_numeric(dfRecent[c], errors="coerce")
                    ruleMap = {"15min": "15min", "15m": "15min", "5min": "5min", "5m": "5min", "1h": "1h", "1d": "1D"}
                    rule = ruleMap.get(tfLower, "15min")
                    dfRecentRes = dfRecent.resample(rule).agg({
                        "close": "last", "high": "max", "low": "min", "open": "first", "volume": "sum"
                    }).dropna(subset=["close"])
                    if not dfRecentRes.empty:
                        dfExact = pd.concat([dfExact, dfRecentRes])
                        dfExact = dfExact[~dfExact.index.duplicated(keep="last")].sort_index()

            dfRes = dfExact.dropna(subset=["close"]).tail(limitBars)
            try:
                rowOffset = db.execute(text("SELECT priceOffset FROM symbols WHERE symbol = :s OR REPLACE(symbol, '/', '') = REPLACE(:s, '/', '') LIMIT 1"), {"s": symbol}).fetchone()
                if rowOffset and rowOffset[0] is not None and float(rowOffset[0]) != 0.0:
                    offset = float(rowOffset[0])
                    for col in ["close", "high", "low", "open"]:
                        if col in dfRes.columns:
                            dfRes[col] = dfRes[col] + offset
            except Exception as eOffset:
                logger.warning(f"Error aplicando priceOffset para {symbol} en fetchSniperCandlesHistory: {eOffset}")
            _sniperCandlesCache[cacheKey] = dfRes
            return dfRes.copy()

        # Fallback resampleando de 1min/5min
        sqlFallback = """
            SELECT timestamp, close, high, low, open, COALESCE(volume, 0.0) AS volume
            FROM candles
            WHERE symbol = %s AND timeframe IN ('1min', '5min') AND timestamp >= %s
            ORDER BY timestamp ASC
        """
        dfFb = pd.read_sql(sqlFallback, conn, params=(symbol, dtStart.strftime("%Y-%m-%d %H:%M:%S")))
        if dfFb.empty:
            return pd.DataFrame()
        dfFb["timestamp"] = pd.to_datetime(dfFb["timestamp"])
        dfFb = dfFb.drop_duplicates(subset=["timestamp"], keep="first").set_index("timestamp")
        for c in ["close", "high", "low", "open", "volume"]:
            dfFb[c] = pd.to_numeric(dfFb[c], errors="coerce")
        ruleMap = {"15min": "15min", "15m": "15min", "5min": "5min", "5m": "5min", "1h": "1h", "1d": "1D"}
        rule = ruleMap.get(tfLower, "15min")
        dfRes = dfFb.resample(rule).agg({
            "close": "last", "high": "max", "low": "min", "open": "first", "volume": "sum"
        }).dropna(subset=["close"]).tail(limitBars)
        try:
            rowOffset = db.execute(text("SELECT priceOffset FROM symbols WHERE symbol = :s OR REPLACE(symbol, '/', '') = REPLACE(:s, '/', '') LIMIT 1"), {"s": symbol}).fetchone()
            if rowOffset and rowOffset[0] is not None and float(rowOffset[0]) != 0.0:
                offset = float(rowOffset[0])
                for col in ["close", "high", "low", "open"]:
                    if col in dfRes.columns:
                        dfRes[col] = dfRes[col] + offset
        except Exception:
            pass
        return dfRes
    except Exception as e:
        logger.error(f"Error en fetchSniperCandlesHistory para {symbol}: {e}")
        return pd.DataFrame()
    finally:
        db.close()


_cached_ml_model = None

def getCachedMlModel():
    global _cached_ml_model
    if _cached_ml_model is None:
        try:
            _cached_ml_model = mlModel.loadModel()
        except Exception as e:
            logger.warning(f"No se pudo precargar modelo ML: {e}")
            _cached_ml_model = None
    return _cached_ml_model


def runSniperBacktest(
    df: pd.DataFrame,
    symbol: str,
    minConfidence: float = 65.0,
    minRr: float = 1.5,
    maxRr: float = 2.5,
    initialCapital: float = 1000.0,
    allocationPct: float = 1.0,
    usarBreakEven: bool = True,
    usarTSL: bool = True,
    pipSize: float = 0.0001,
    timeframe: str = "15min",
    adaptiveCfg: Optional[Dict[str, Any]] = None,
    precomputedProbas: Optional[np.ndarray] = None,
    precomputedDfOp: Optional[pd.DataFrame] = None,
    filtroHorario: bool = False,
    horaInicio: str = "08:00",
    horaFin: str = "16:00",
    horaInicio2: Optional[str] = None,
    horaFin2: Optional[str] = None
) -> Dict[str, Any]:
    """
    Ejecuta el BackTest táctico para la estrategia SNIPER (15min) sobre un par:
      - Utiliza features técnicas y predicción ML si el modelo está disponible.
      - Parámetros adaptativos jerárquicos (minConfidence, minRr, slAtrMult).
      - Gestión de riesgo: Stop Loss (1.5x ATR), Take Profit (R:R adaptativo),
        Break-Even (alcanzado el 50% del TP) y Trailing Stop Loss (TSL).
      - Genera métricas institucionales, curva de equidad y bitácora detallada de trades.
    """
    if df is None or df.empty or len(df) < 30:
        return {
            "status": "error",
            "message": "Datos insuficientes para el backtest",
            "startDate": "-",
            "endDate": "-",
            "totalBars": 0,
            "totalDays": 0,
            "history": [],
            "equityCurve": [{"datetime": "-", "equity": initialCapital}],
            "trades": [],
            "metrics": {
                "totalTrades": 0, "winningTrades": 0, "losingTrades": 0,
                "winRate": 0.0, "netProfit": 0.0, "totalReturnPct": 0.0,
                "profitFactor": 0.0, "sharpeRatio": 0.0, "maxDrawdown": 0.0,
                "finalCapital": initialCapital
            }
        }

    df = df.sort_index()
    startDt = df.index[0]
    endDt = df.index[-1]
    startDateStr = startDt.strftime("%Y-%m-%d %H:%M") if hasattr(startDt, "strftime") else str(startDt)[:16]
    endDateStr = endDt.strftime("%Y-%m-%d %H:%M") if hasattr(endDt, "strftime") else str(endDt)[:16]
    totalDays = max(1, (endDt - startDt).days) if hasattr(endDt, "days") or isinstance(endDt, (datetime, pd.Timestamp)) else 1

    # Formatear historia de velas para Chart.js
    historyList = []
    for idx, row in df.iterrows():
        ts_str = idx.strftime("%Y-%m-%d %H:%M") if hasattr(idx, "strftime") else str(idx)[:16]
        historyList.append({
            "datetime": ts_str,
            "open": round(float(row["open"]), 5),
            "high": round(float(row["high"]), 5),
            "low": round(float(row["low"]), 5),
            "close": round(float(row["close"]), 5),
            "volume": float(row.get("volume", 0.0) or 0.0)
        })

    # Cargar features y modelo ML
    thresh_long = 0.55
    thresh_short = 0.45
    min_conf = float(minConfidence or 65.0)
    effective_rr = float(minRr or 1.5)
    sl_mult = 1.5

    if adaptiveCfg:
        if adaptiveCfg.get("probaThresholdLong"): thresh_long = float(adaptiveCfg["probaThresholdLong"])
        if adaptiveCfg.get("probaThresholdShort"): thresh_short = float(adaptiveCfg["probaThresholdShort"])
        if adaptiveCfg.get("minConfidence"): min_conf = float(adaptiveCfg["minConfidence"])
        if adaptiveCfg.get("minRr"): effective_rr = float(adaptiveCfg["minRr"])
        if adaptiveCfg.get("slAtrMult"): sl_mult = float(adaptiveCfg["slAtrMult"])

    if precomputedProbas is not None and precomputedDfOp is not None:
        probas = precomputedProbas
        df_op = precomputedDfOp
    else:
        probas = None
        df_op = df
        try:
            df_feats = technical.calculateFeatures(df.copy())
            if df_feats is not None and not df_feats.empty:
                df_ml = mlModel.defineMlTarget(df_feats)
                X, _ = mlModel.cleanDataForModel(df_ml)
                model = getCachedMlModel()
                if model is not None and X is not None and not X.empty:
                    feats = mlModel.MODEL_FEATURES
                    X_clean = X.dropna(subset=feats)
                    if not X_clean.empty and len(X_clean) >= 20:
                        probas = model.predict_proba(X_clean[feats])[:, 1]
                        df_op = df_feats.loc[X_clean.index]
        except Exception as e:
            logger.warning(f"Backtest Sniper ({symbol}): Fallback a señales técnicas directas: {e}")
            probas = None
            df_op = df

    # Arrays para iteración ultra rápida
    close_arr = df_op["close"].values
    high_arr = df_op["high"].values
    low_arr = df_op["low"].values
    open_arr = df_op["open"].values
    atr_arr = df_op["atr"].values if "atr" in df_op.columns else np.zeros(len(df_op))
    times_arr = [idx.strftime("%Y-%m-%d %H:%M") if hasattr(idx, "strftime") else str(idx)[:16] for idx in df_op.index]

    currentCap = float(initialCapital or 1000.0)
    allocPct = float(allocationPct or 1.0)
    trades: List[Dict[str, Any]] = []
    equityCurve: List[Dict[str, Any]] = [{"datetime": times_arr[0], "equity": round(currentCap, 2)}]
    returns_pct_list: List[float] = []

    in_trade = False
    trade_entry = None
    peak_cap = currentCap
    max_dd_pct = 0.0

    for i in range(1, len(df_op)):
        if in_trade:
            trade_entry["durationBars"] += 1
            h = high_arr[i]
            l = low_arr[i]
            d = trade_entry["direction"]
            isLong = (d in ["LARGO", "COMPRA", "BUY"])
            ep = trade_entry["entryPrice"]
            sl = trade_entry["stopLoss"]
            tp = trade_entry["takeProfit"]
            atr_v = trade_entry["atr"]

            # Break-Even Adaptativo
            if usarBreakEven and not trade_entry["isBreakEven"]:
                dist_half = abs(tp - ep) * 0.50
                if isLong and h >= ep + dist_half:
                    trade_entry["stopLoss"] = ep
                    trade_entry["isBreakEven"] = True
                elif (not isLong) and l <= ep - dist_half:
                    trade_entry["stopLoss"] = ep
                    trade_entry["isBreakEven"] = True

            # Trailing Stop Loss Adaptativo
            if usarTSL:
                if isLong:
                    if h > trade_entry["peakPx"]:
                        trade_entry["peakPx"] = h
                        cand_sl = h - (atr_v * sl_mult)
                        if cand_sl > trade_entry["stopLoss"]:
                            trade_entry["stopLoss"] = cand_sl
                else:
                    if l < trade_entry["valleyPx"]:
                        trade_entry["valleyPx"] = l
                        cand_sl = l + (atr_v * sl_mult)
                        if cand_sl < trade_entry["stopLoss"]:
                            trade_entry["stopLoss"] = cand_sl

            sl = trade_entry["stopLoss"]
            exit_px = None
            exit_reason = None

            if isLong:
                if l <= sl:
                    exit_px = sl
                    exit_reason = "Break-Even" if (trade_entry["isBreakEven"] and abs(sl - ep) < pipSize * 5) else ("Trailing SL" if sl > ep else "Stop Loss")
                elif h >= tp:
                    exit_px = tp
                    exit_reason = "Take Profit"
            else:
                if h >= sl:
                    exit_px = sl
                    exit_reason = "Break-Even" if (trade_entry["isBreakEven"] and abs(sl - ep) < pipSize * 5) else ("Trailing SL" if sl < ep else "Stop Loss")
                elif l <= tp:
                    exit_px = tp
                    exit_reason = "Take Profit"

            if exit_px is not None:
                ret_pct = ((exit_px - ep) / ep * 100.0) if isLong else ((ep - exit_px) / ep * 100.0)
                pips = ((exit_px - ep) / pipSize) if isLong else ((ep - exit_px) / pipSize)
                alloc_cap = currentCap * (allocPct / 100.0)
                # Tamaño en unidades con apalancamiento institucional 30x (margen 3.33%)
                units = alloc_cap / (ep * 0.0333) if (ep * 0.0333) > 0 else 1000.0
                pnl = (exit_px - ep) * units if isLong else (ep - exit_px) * units
                currentCap += pnl

                if currentCap > peak_cap:
                    peak_cap = currentCap
                dd = ((peak_cap - currentCap) / peak_cap * 100.0) if peak_cap > 0 else 0.0
                if dd > max_dd_pct:
                    max_dd_pct = dd

                returns_pct_list.append(float(ret_pct))
                trade_entry.update({
                    "exitDate": times_arr[i],
                    "exitPrice": float(round(exit_px, 5)),
                    "exitPriceA": float(round(exit_px, 5)),
                    "returnPct": float(round(ret_pct, 2)),
                    "pnl": float(round(pnl, 2)),
                    "pnlA": float(round(pnl, 2)),
                    "pips": float(round(pips, 1)),
                    "pipsA": float(round(pips, 1)),
                    "isWin": bool(pnl > 0),
                    "exitReason": str(exit_reason or ""),
                    "accumCapital": float(round(currentCap, 2))
                })
                trades.append(trade_entry)
                equityCurve.append({"datetime": times_arr[i], "equity": float(round(currentCap, 2))})
                in_trade = False
                trade_entry = None
                continue

        # Evaluación de nueva entrada
        is_l = False
        is_s = False
        bar_allowed = True
        if filtroHorario:
            bar_dt = df_op.index[i - 1] if hasattr(df_op.index[i - 1], "hour") else pd.to_datetime(times_arr[i - 1])
            bar_allowed = isWithinOperatingHours(horaInicio, horaFin, bar_dt, horaInicio2, horaFin2)

        if bar_allowed:
            if probas is not None and i - 1 < len(probas):
                pr = float(probas[i - 1])
                is_l = pr >= thresh_long
                is_s = pr <= thresh_short
                conf = (pr * 100.0) if is_l else ((1.0 - pr) * 100.0 if is_s else 0.0)
                if conf < min_conf:
                    is_l = False
                    is_s = False
            else:
                # Fallback técnico (RSI + Momentum)
                if i >= 14:
                    curr_c = float(close_arr[i - 1])
                    prev_c = float(close_arr[i - 14])
                    if curr_c > prev_c * 1.002: is_l = True
                    elif curr_c < prev_c * 0.998: is_s = True

        if is_l or is_s:
            ep = float(close_arr[i - 1])
            atr_v = float(atr_arr[i - 1]) if i - 1 < len(atr_arr) and atr_arr[i - 1] > 0 else (ep * 0.002)
            if np.isnan(atr_v) or atr_v <= 0:
                atr_v = ep * 0.002

            if is_l:
                direction = "LARGO"
                sl_v = ep - (atr_v * sl_mult)
                tp_v = ep + (abs(ep - sl_v) * effective_rr)
            else:
                direction = "CORTO"
                sl_v = ep + (atr_v * sl_mult)
                tp_v = ep - (abs(ep - sl_v) * effective_rr)

            alloc_cap = float(currentCap * (allocPct / 100.0))
            trade_entry = {
                "tradeNum": int(len(trades) + 1),
                "signalType": "SNIPER ML",
                "direction": "COMPRA" if direction == "LARGO" else "VENTA",
                "entryDate": times_arr[i - 1],
                "entryPrice": float(round(ep, 5)),
                "entryPriceA": float(round(ep, 5)),
                "stopLoss": float(round(sl_v, 5)),
                "origSl": float(round(sl_v, 5)),
                "takeProfit": float(round(tp_v, 5)),
                "atr": float(atr_v),
                "peakPx": float(ep),
                "valleyPx": float(ep),
                "isBreakEven": False,
                "durationBars": 1,
                "allocatedCapital": float(round(alloc_cap, 2))
            }
            in_trade = True

    # Calcular métricas globales
    totalTrades = int(len(trades))
    winningTrades = int(len([t for t in trades if t.get("isWin")]))
    losingTrades = int(totalTrades - winningTrades)
    winRate = float(round((winningTrades / totalTrades * 100.0), 2)) if totalTrades > 0 else 0.0

    grossProfit = float(sum(t["pnl"] for t in trades if t.get("isWin")))
    grossLoss = float(abs(sum(t["pnl"] for t in trades if not t.get("isWin"))))
    profitFactor = float(round(grossProfit / grossLoss, 2)) if grossLoss > 0 else (99.0 if grossProfit > 0 else 1.0)
    netProfit = float(round(currentCap - initialCapital, 2))
    totalReturnPct = float(round((currentCap - initialCapital) / initialCapital * 100.0, 2)) if initialCapital > 0 else 0.0

    sharpeRatio = 0.0
    if returns_pct_list and len(returns_pct_list) > 1:
        mean_ret = float(np.mean(returns_pct_list))
        std_ret = float(np.std(returns_pct_list))
        if std_ret > 0:
            sharpeRatio = float(round((mean_ret / std_ret) * np.sqrt(252), 2))

    # Downsampling de la curva de equidad a max 350 puntos
    downsampled_curve = equityCurve
    if len(equityCurve) > 350:
        step = max(1, len(equityCurve) // 350)
        downsampled_curve = [equityCurve[k] for k in range(0, len(equityCurve), step)]
        if downsampled_curve[-1] != equityCurve[-1]:
            downsampled_curve.append(equityCurve[-1])

    # Limitar historia y trades para transmisión ligera
    trimmed_history = historyList[-1500:] if len(historyList) > 1500 else historyList
    trimmed_trades = trades[-300:] if len(trades) > 300 else trades

    return {
        "status": "success",
        "startDate": startDateStr,
        "endDate": endDateStr,
        "totalBars": int(len(df)),
        "totalDays": int(totalDays),
        "history": trimmed_history,
        "equityCurve": downsampled_curve,
        "trades": trimmed_trades,
        "metrics": {
            "totalTrades": totalTrades,
            "winningTrades": winningTrades,
            "losingTrades": losingTrades,
            "winRate": winRate,
            "netProfit": netProfit,
            "totalReturnPct": totalReturnPct,
            "profitFactor": profitFactor,
            "sharpeRatio": sharpeRatio,
            "maxDrawdown": float(round(max_dd_pct, 2)),
            "finalCapital": float(round(currentCap, 2))
        }
    }


def sendSniperWebhookOrder(
    dbSession,
    idCuenta: int,
    symbol: str,
    action: str,
    size: float,
    price: float,
    sl: Optional[float] = None,
    tp: Optional[float] = None,
    ticketId: Optional[str] = None,
    idTrade: Optional[int] = None,
    comment: str = "Orden SNIPER"
) -> Dict[str, Any]:
    brokerInfo = get_account_broker(dbSession, idCuenta)
    if not brokerInfo:
        logger.warning(f"⚠️ [SNIPER] No hay brokers activos en brokercuenta para idCuenta {idCuenta}.")
        return {"success": False, "error": f"Sin broker activo para idCuenta {idCuenta}"}
    return send_broker_order(
        strategy=STRATEGY_NAME,
        setup=f"ML SNIPER {symbol}",
        action=action,
        symbol=symbol,
        quantity=size,
        price=price,
        sl=sl or 0.0,
        tp=tp or 0.0,
        ticketId=ticketId,
        idTrade=idTrade,
        encryptedAccountToken=brokerInfo["encryptedAccountToken"],
        comment=comment
    )


def checkAccountMarginHealth(dbSession, idCuenta: int, accountCapital: float) -> Tuple[bool, float, float, float, float]:
    return check_account_margin_health(dbSession, idCuenta, accountCapital)


def evaluateSingleSniper(dbSession, sniperRecord: Dict[str, Any], botInstance: SniperBot) -> None:
    """
    Evalúa un par configurado en userSNIPER en tiempo real:
      - Monitorea trades activos (TP, SL, BreakEven, TSL).
      - Si no hay trade activo y operar=1, evalúa señales Sniper ML y ejecuta entradas institucionales.
    """
    idCuenta = int(sniperRecord["idCuenta"])
    symbol = str(sniperRecord["symbol"]).strip()
    timeframe = str(sniperRecord.get("Temporalidad") or "15min").strip()
    operar = bool(sniperRecord.get("operar", False))
    usarBreakEven = bool(sniperRecord.get("usarBreakEven", True))
    usarTSL = bool(sniperRecord.get("usarTSL", False))

    # 1. Verificar si hay posición abierta para este símbolo y cuenta en SNIPER
    openTrades = dbSession.execute(text("""
        SELECT idTrade, symbol, direction, size, entryPrice, stopLoss, takeProfit, isBreakEven, margin_used, ticketId, openTime
        FROM trades
        WHERE idCuenta = :idc
          AND strategy = :strat
          AND (symbol = :sym OR setup = :sym)
          AND status = 'OPEN'
        ORDER BY idTrade ASC
    """), {"idc": idCuenta, "strat": STRATEGY_NAME, "sym": symbol}).mappings().fetchall()

    if not openTrades and not operar:
        return

    if not isMarketOpenForSymbol(symbol):
        if openTrades:
            if shouldLogStatus(idCuenta, symbol, 300.0):
                logger.info(f"⏸️ [SNIPER] Mercado cerrado o en receso diario para {symbol}. Posición #{openTrades[0]['idTrade']} en pausa hasta reapertura.")
        else:
            if shouldLogStatus(idCuenta, symbol, 300.0):
                logger.info(f"⏸️ [SNIPER] Mercado cerrado o en receso diario para {symbol}.")
        return

    df = fetchSniperCandlesHistory(symbol, timeframe=timeframe, days=90)
    if df.empty or len(df) < 50:
        if shouldLogStatus(idCuenta, symbol, 60.0):
            logger.warning(f"⚠️ [SNIPER] Velas insuficientes para {symbol} ({timeframe}).")
        return

    currentPx = float(df["close"].iloc[-1])
    highVal = float(df["high"].iloc[-1])
    lowVal = float(df["low"].iloc[-1])

    if openTrades:
        firstTr = openTrades[0]
        direction = str(firstTr["direction"] or "LARGO").upper()
        isLong = "LARG" in direction or "BUY" in direction
        entryPx = float(firstTr["entryPrice"] or currentPx)
        slPx = float(firstTr["stopLoss"]) if firstTr.get("stopLoss") is not None else None
        tpPx = float(firstTr["takeProfit"]) if firstTr.get("takeProfit") is not None else None
        isBe = bool(firstTr.get("isBreakEven", 0))

        shouldClose = False
        exitReason = ""
        exitFillPx = currentPx

        # A. BreakEven Adaptativo: asegura posición a precio de entrada cuando avanza 50% hacia TP
        if usarBreakEven and not isBe and tpPx and entryPx:
            beTrigger = entryPx + (tpPx - entryPx) * 0.50 if isLong else entryPx - (entryPx - tpPx) * 0.50
            hitBe = (highVal >= beTrigger) if isLong else (lowVal <= beTrigger)
            if hitBe:
                newSl = entryPx
                dbSession.execute(text("""
                    UPDATE trades
                    SET isBreakEven = 1, stopLoss = :nsl
                    WHERE idTrade = :idt
                """), {"nsl": newSl, "idt": firstTr["idTrade"]})
                dbSession.commit()
                logger.info(f"🛡️ [SNIPER] Posición #{firstTr['idTrade']} en {symbol} ({direction}) movida a BreakEven @ {entryPx:.5f} (Trigger={beTrigger:.5f}). ¡Riesgo Cero!")
                isBe = True
                slPx = newSl

        # B. Trailing Stop Loss opcional (solo avanza hacia el TP y nunca retrocede)
        canTsl = usarTSL and slPx and (isBe or not usarBreakEven)
        if canTsl:
            adaptiveCfg = getAdaptiveConfigForSymbol(dbSession, symbol)
            atrVal = float((df["high"].iloc[-14:] - df["low"].iloc[-14:]).mean()) if len(df) >= 14 else 0.0010
            slDistance = atrVal * float(adaptiveCfg.get("slAtrMult", 1.5))

            # Anclar al extremo más favorable alcanzado por el trade desde su apertura
            trOpenTime = firstTr.get("openTime") or firstTr.get("candleTime")
            bestHigh = highVal
            bestLow = lowVal
            if trOpenTime and hasattr(df.index, "__ge__"):
                try:
                    tsOpen = pd.to_datetime(trOpenTime)
                    dfIdx = df.index.tz_localize(None) if (hasattr(df.index, "tz") and df.index.tz is not None) else df.index
                    dfSince = df[dfIdx >= tsOpen]
                    if len(dfSince) > 0:
                        bestHigh = max(float(dfSince["high"].max()), highVal)
                        bestLow = min(float(dfSince["low"].min()), lowVal)
                except Exception:
                    pass

            if isLong:
                candidateSl = bestHigh - slDistance
                if tpPx is not None:
                    candidateSl = min(candidateSl, tpPx)
                # El TSL solo avanza hacia el TP (hacia arriba) y nunca retrocede
                if candidateSl > slPx:
                    dbSession.execute(text("UPDATE trades SET stopLoss = :nsl WHERE idTrade = :idt"), {"nsl": candidateSl, "idt": firstTr["idTrade"]})
                    dbSession.commit()
                    logger.info(f"📈 [SNIPER] TSL avanzado para #{firstTr['idTrade']} en {symbol} (LARGO): SL movido de {slPx:.5f} a {candidateSl:.5f} hacia TP ({tpPx})")
                    slPx = candidateSl
            else:
                candidateSl = bestLow + slDistance
                if tpPx is not None:
                    candidateSl = max(candidateSl, tpPx)
                # El TSL solo avanza hacia el TP (hacia abajo) y nunca retrocede
                if candidateSl < slPx:
                    dbSession.execute(text("UPDATE trades SET stopLoss = :nsl WHERE idTrade = :idt"), {"nsl": candidateSl, "idt": firstTr["idTrade"]})
                    dbSession.commit()
                    logger.info(f"📉 [SNIPER] TSL avanzado para #{firstTr['idTrade']} en {symbol} (CORTO): SL movido de {slPx:.5f} a {candidateSl:.5f} hacia TP ({tpPx})")
                    slPx = candidateSl

        # C. Verificación de Cierre por Take Profit
        if tpPx is not None:
            hitTp = (highVal >= tpPx) if isLong else (lowVal <= tpPx)
            if hitTp:
                shouldClose = True
                exitReason = f"Take Profit Alcanzado (@ {tpPx:.5f})"
                exitFillPx = tpPx

        # D. Verificación de Cierre por Stop Loss / BreakEven
        if not shouldClose and slPx is not None:
            hitSl = (lowVal <= slPx) if isLong else (highVal >= slPx)
            if hitSl:
                shouldClose = True
                exitReason = f"{'BreakEven' if isBe and abs(slPx - entryPx) < 0.0001 else 'Stop Loss'} Ejecutado (@ {slPx:.5f})"
                exitFillPx = slPx

        # E. Cierre Preventivo por Fin de Semana
        if not shouldClose and isWeekendClosingWindow(symbol=symbol, dbSession=dbSession):
            shouldClose = True
            exitReason = "Cierre Preventivo Fin de Semana"
            exitFillPx = currentPx

        if shouldClose:
            logger.info(f"🎯 [SNIPER] Cierre de posición #{firstTr['idTrade']} {symbol} ({direction}): {exitReason}")
            for idxTr, tr in enumerate(openTrades):
                if idxTr > 0:
                    time.sleep(2.0)
                resClose = execute_single_asset_close(
                    dbSession=dbSession,
                    idCuenta=idCuenta,
                    tradeRow=dict(tr),
                    strategy=STRATEGY_NAME,
                    exitPrice=exitFillPx,
                    closeReason=f"Cierre SNIPER ({exitReason})"
                )
                if not resClose.get("success"):
                    logger.error(f"❌ [SNIPER] Error en cierre de Trade #{tr['idTrade']}: {resClose.get('error')}")

            setOrderCooldown(idCuenta, symbol, 180.0)
            if str(sniperRecord.get("accionCierre") or "Continua").strip().lower() == "para":
                dbSession.execute(text("UPDATE userSNIPER SET operar = 0 WHERE id = :sid"), {"sid": sniperRecord["id"]})
                dbSession.commit()
            return
        else:
            if shouldLogStatus(idCuenta, symbol, 30.0):
                logger.info(
                    f"🎖️ [SNIPER] Posición #{firstTr['idTrade']} en curso: Cuenta {idCuenta} | {symbol} ({direction}) | "
                    f"Px={currentPx:.5f} | Entrada={entryPx:.5f} | SL={slPx or '-'} | TP={tpPx or '-'} | BE={'SÍ' if isBe else 'NO'}"
                )
            return

    # 2. Si no hay posición abierta y operar=1, evaluar nueva entrada Sniper
    if not openTrades and operar:
        if not isMarketOpenForSymbol(symbol):
            if shouldLogStatus(idCuenta, symbol, 60.0):
                logger.info(f"⏸️ [SNIPER] Mercado cerrado para {symbol} (fin de semana).")
            return

        if isWeekendClosingWindow(symbol=symbol, dbSession=dbSession):
            return

        if isOrderInCooldown(idCuenta, symbol):
            return

        useHourFilter = bool(sniperRecord.get("filtroHorario", True))
        hInicio = str(sniperRecord.get("horaInicio") or "08:00")
        hFin = str(sniperRecord.get("horaFin") or "16:00")
        hInicio2 = sniperRecord.get("horaInicio2")
        hFin2 = sniperRecord.get("horaFin2")
        hInicio2Str = str(hInicio2) if hInicio2 else None
        hFin2Str = str(hFin2) if hFin2 else None
        if useHourFilter and not isWithinOperatingHours(hInicio, hFin, None, hInicio2Str, hFin2Str):
            if shouldLogStatus(idCuenta, symbol, 60.0):
                horarioDesc = f"{hInicio}-{hFin}" + (f" y {hInicio2Str}-{hFin2Str}" if hInicio2Str and hFin2Str else "")
                logger.info(f"⏳ [SNIPER] [{symbol}] Fuera de horario operativo configurado ({horarioDesc} CST).")
            return

        # Consultar configuración adaptativa jerárquica
        adaptiveCfg = getAdaptiveConfigForSymbol(dbSession, symbol)
        minConf = float(sniperRecord.get("minConfidence") or adaptiveCfg["minConfidence"])
        minRr = float(sniperRecord.get("minRr") or adaptiveCfg["minRr"])
        maxRr = float(sniperRecord.get("maxRr") or adaptiveCfg["maxRr"])

        symInfo = {
            "symbol": symbol,
            "intervalo": timeframe,
            "tipo": adaptiveCfg["tipo"],
            "minConfidence": minConf,
            "minRr": minRr,
            "maxRr": maxRr
        }

        # Ejecutar análisis en SniperBot
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            signalObj = loop.run_until_complete(botInstance.runAnalysisCycleForSymbol(symInfo, preloaded_data={symbol: df}))
            loop.close()
        except Exception as eSig:
            logger.error(f"Error evaluando señal Sniper para {symbol}: {eSig}")
            signalObj = None

        if signalObj is not None:
            directionStr = signalObj.direction.upper()
            isLong = "LARG" in directionStr or "BUY" in directionStr
            actionStr = "buy" if isLong else "sell"
            entryPx = signalObj.entry_price
            slPrice = signalObj.stop_loss
            tpPrice = signalObj.take_profit
            confidenceVal = signalObj.confidence
            rrVal = signalObj.rr_ratio

            # Restricción de dividendos en corto
            if not isLong:
                isDivRestr, divDetail = isDividendShortRestricted(dbSession, symbol)
                if isDivRestr:
                    logger.info(f"🚫 [SNIPER] [{symbol}] Entrada en CORTO bloqueada por periodo de dividendos ({divDetail}).")
                    return

            # Salud de margen y riesgo financiero
            rowAcc = dbSession.execute(text("SELECT Capital, riesgoPorOperacion FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).fetchone()
            cap = float(rowAcc[0] or 1000.0) if rowAcc else 1000.0
            riesgoPct = float(rowAcc[1] or 1.0) if rowAcc else 1.0

            # Multiplicador de riesgo específico de la emisora en userSNIPER
            multRiesgo = 1.0
            rowUsr = dbSession.execute(text("SELECT multiplicadorRiesgo FROM userSNIPER WHERE idCuenta = :idc AND symbol = :sym AND (borrado = 0 OR borrado IS NULL) LIMIT 1"), {"idc": idCuenta, "sym": symbol}).fetchone()
            if rowUsr and rowUsr[0] is not None:
                multRiesgo = max(0.1, float(rowUsr[0]))

            effectiveRiesgoPct = riesgoPct * multRiesgo
            allocationRate = (effectiveRiesgoPct / 100.0) if effectiveRiesgoPct >= 0.05 else effectiveRiesgoPct
            if allocationRate <= 0: allocationRate = 0.01

            canOperate, marginInd, totalMargin, equity, minMarginRequired = checkAccountMarginHealth(dbSession, idCuenta, cap)
            if not canOperate:
                logger.warning(f"⛔ [SNIPER] [{symbol}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: Indicador de Margen ({marginInd:.1f}%) < {minMarginRequired:.0f}%.")
                return

            cleanSym = symbol.replace("/", "").upper()
            rowSym = dbSession.execute(text("SELECT min_lots, margen, pip FROM symbols WHERE UPPER(REPLACE(symbol, '/', '')) = :s LIMIT 1"), {"s": cleanSym}).fetchone()
            minLots = float(rowSym[0] or 1000.0) if rowSym else 1000.0
            margenPct = float(rowSym[1] or 0.5) if rowSym else 0.5
            minMargen1Lot = max(0.01, calculate_margin(symbol, minLots, entryPx, margenPct))
            availableEquity = max(0.0, equity - totalMargin)

            entryBudget = availableEquity * allocationRate
            multA = max(1, int(entryBudget // minMargen1Lot)) if (entryBudget >= minMargen1Lot and minMargen1Lot > 0) else 1
            marginUsed = round(multA * minMargen1Lot, 2)
            projectedTotalMargin = totalMargin + marginUsed
            projectedIndicator = (equity / projectedTotalMargin * 100.0) if (projectedTotalMargin > 0 and equity > 0) else 0.0

            if (marginUsed > availableEquity) or (projectedIndicator < minMarginRequired):
                multA = 1
                marginUsed = round(minMargen1Lot, 2)
                projectedTotalMargin = totalMargin + marginUsed
                projectedIndicator = (equity / projectedTotalMargin * 100.0) if (projectedTotalMargin > 0 and equity > 0) else 0.0

            if (marginUsed > availableEquity) or (projectedIndicator < minMarginRequired):
                logger.warning(f"⛔ [SNIPER] [{symbol}] Margen proyectado ({projectedIndicator:.1f}%) < {minMarginRequired:.0f}%. Entrada omitida.")
                return

            units = round(multA * minLots, 4)

            candleDtRaw = getattr(signalObj, 'candleTime', None)
            candleDt = datetime.now()
            if candleDtRaw:
                for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
                    try:
                        candleDt = datetime.strptime(str(candleDtRaw)[:19], fmt)
                        break
                    except Exception:
                        pass

            logger.info(
                f"🎯 [SNIPER] DISPARO DE SEÑAL {directionStr} en {symbol} | "
                f"Px={entryPx:.5f} | SL={slPrice:.5f} | TP={tpPrice:.5f} | R:R={rrVal:.2f} | "
                f"Confianza={confidenceVal:.1f}% | Units={units:g} | Cuenta #{idCuenta}"
            )

            resEntry = execute_single_asset_entry(
                dbSession=dbSession,
                idCuenta=idCuenta,
                strategy=STRATEGY_NAME,
                setup=f"ML SNIPER {timeframe.upper()}",
                symbol=symbol,
                direction=directionStr,
                timeframe=timeframe,
                size=units,
                price=entryPx,
                sl=slPrice,
                tp=tpPrice,
                margenPct=margenPct,
                comment=f"SNIPER {directionStr} (R:R={rrVal:.2f} Conf={confidenceVal:.1f}%)",
                signalType=f"ML SNIPER (Conf: {confidenceVal:.1f}%)",
                candleDt=candleDt
            )

            if not resEntry.get("success"):
                setOrderCooldown(idCuenta, symbol, 120.0)
                logger.error(f"❌ [SNIPER] No se pudo ejecutar orden para Cuenta #{idCuenta} {symbol}: {resEntry.get('error')}")
                return
        else:
            if shouldLogStatus(idCuenta, symbol, 30.0):
                logger.info(f"🎖️ [SNIPER] Cuenta {idCuenta} | {symbol} ({timeframe}) | Px={currentPx:.5f} | Sin señal Sniper activa | Operar={operar}")


def processSniperForAccount(idCuenta: int, botInstance: SniperBot) -> None:
    """
    Función de trabajo para el hilo asignado a una cuenta específica.
    Abre su propia sesión de base de datos y evalúa todos los pares asignados a esa cuenta.
    """
    db = SessionLocal()
    try:
        sql = text("""
            SELECT s.id, s.idUsuario, s.idCuenta, s.symbol, s.Temporalidad, s.periodo,
                   s.minConfidence, s.minRr, s.maxRr, s.operar, s.accionCierre, s.tipoCierre,
                   s.usarBreakEven, s.usarTSL, s.filtroHorario, s.horaInicio, s.horaFin,
                   s.horaInicio2, s.horaFin2
            FROM userSNIPER s
            JOIN cuenta c ON s.idCuenta = c.idCuenta
            WHERE s.idCuenta = :idc AND (s.borrado = 0 OR s.borrado IS NULL) AND c.Activo = 1
              AND (s.operar = 1 OR EXISTS (
                  SELECT 1 FROM trades t 
                  WHERE t.idCuenta = s.idCuenta 
                    AND t.strategy = 'SNIPER' 
                    AND (t.symbol = s.symbol OR t.setup = s.symbol)
                    AND t.status = 'OPEN'
              ))
            ORDER BY s.operar DESC, s.id ASC
        """)
        records = db.execute(sql, {"idc": idCuenta}).mappings().fetchall()
        for rec in records:
            try:
                evaluateSingleSniper(db, dict(rec), botInstance)
            except Exception as exSingle:
                logger.error(f"❌ [SNIPER] Error evaluando {rec.get('symbol')} (Cuenta {idCuenta}): {exSingle}", exc_info=True)
            time.sleep(0.3)
    except Exception as exAcc:
        logger.error(f"❌ [SNIPER] Error procesando Cuenta #{idCuenta}: {exAcc}", exc_info=True)
    finally:
        db.close()


def main() -> None:
    import fcntl
    from concurrent.futures import ThreadPoolExecutor

    lock_file = open('/tmp/microSniper.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, BlockingIOError):
        print("⚠️ Ya existe una instancia de microSniper en ejecución. Abortando para garantizar instancia única.")
        sys.exit(0)

    logger.info("==========================================================")
    logger.info("🚀 Iniciando Microservicio SNIPER (microSniper.py)")
    logger.info("⚡ Arquitectura Concurrente: Pool de 5 Hilos por Cuenta")
    logger.info("==========================================================")
    botInstance = getSniperBotInstance()

    executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="SniperWorker")

    while True:
        db = SessionLocal()
        try:
            sqlAccounts = text("""
                SELECT DISTINCT s.idCuenta
                FROM userSNIPER s
                JOIN cuenta c ON s.idCuenta = c.idCuenta
                WHERE (s.borrado = 0 OR s.borrado IS NULL) AND c.Activo = 1
                  AND (s.operar = 1 OR EXISTS (
                      SELECT 1 FROM trades t 
                      WHERE t.idCuenta = s.idCuenta 
                        AND t.strategy = 'SNIPER' 
                        AND (t.symbol = s.symbol OR t.setup = s.symbol)
                        AND t.status = 'OPEN'
                  ))
                ORDER BY s.idCuenta ASC
            """)
            accountRows = db.execute(sqlAccounts).fetchall()
            activeAccountIds = [int(r[0]) for r in accountRows]
        except Exception as exMain:
            logger.error(f"❌ [SNIPER] Excepción general consultando cuentas activas: {exMain}", exc_info=True)
            activeAccountIds = []
        finally:
            db.close()

        if activeAccountIds:
            # Despachar las cuentas en paralelo en el pool de 5 hilos
            futures = [executor.submit(processSniperForAccount, accId, botInstance) for accId in activeAccountIds]
            for f in futures:
                try:
                    f.result(timeout=60.0)
                except Exception as exFut:
                    logger.error(f"❌ [SNIPER] Excepción en hilo de cuenta: {exFut}")

        time.sleep(6.0)


if __name__ == "__main__":
    main()
