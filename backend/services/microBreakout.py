"""
Microservicio Autónomo de Análisis, Monitoreo y Ejecución de BREAKOUT (microBreakout.py)
Estrategia: BREAKOUT (Ruptura de Rango de Apertura en Sesiones NY / Londres)

Reglas Institucionales:
  1. Identificación del Rango de Apertura:
     - Sesión Nueva York: Apertura oficial 08:30 NY (07:30 CDMX).
     - Sesión Londres: Apertura oficial 08:00 Londres / 03:00 NY (02:00 CDMX).
     - Duración del Rango: 15 min (por defecto) o 30 min.
     - Se extrae el Máximo (rangeHigh) y Mínimo (rangeLow) del rango de apertura.
  2. Ventana de Operación y Horario Límite ("definir un horario: para que despues de ese horario ya no entre"):
     - Monitorea velas M5 cerradas después de concluir el rango.
     - Si la hora actual de la vela supera la horaFin / hora límite (ej. 11:00 NY), NO se permite nueva entrada.
  3. Confirmación de Ruptura y Filtros de Seguridad:
     - Ruptura Alcista: Cierre de vela M5 > rangeHigh.
     - Ruptura Bajista: Cierre de vela M5 < rangeLow.
     - Filtro Macro EMA 200: En compras, precio >= EMA 200; en ventas, precio <= EMA 200.
     - Filtro de Calidad de Vela: Cuerpo de vela >= 60% del rango total (evita mechas o fakeouts).
     - Filtro Impulse MACD / Momentum: Confirmación direccional del histograma.
  4. Gestión de Riesgo (R:R) y Salidas:
     - Stop Loss estructural: Al extremo opuesto del rango de apertura.
     - Take Profit: Multiplicador R:R sobre la distancia de riesgo (mínimo 1:1, configurable a 1.5 o 2.0).
     - BreakEven adaptativo: Protege a 0 una vez alcanzado el 50%-75% del recorrido hacia TP.
  5. Zoom de Horario:
     - Proporciona coordenadas de tiempo (minTs, maxTs) para enfocar la gráfica en la ventana matutina.
"""

import os
import sys
import time
import math
import json
import logging
import asyncio
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple, Set

import numpy as np
import pandas as pd
import pytz
import talib as ta
from sqlalchemy import text

# Asegurar raíz de ATALAia en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal
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
from Sentinel.analysis import technical
from Sentinel.core.models import Signal
from middleware.config import constants as config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [microBreakout] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("microBreakout")

STRATEGY_NAME = "BREAKOUT"


def fetchBreakoutCandlesHistory(
    symbol: str,
    timeframe: str = "5min",
    days: int = 60,
    limit: int = 2500
) -> pd.DataFrame:
    """
    Obtiene historial de velas en temporalidad intradiaria (5min por defecto)
    desde la tabla candles unificada con velas recientes de 1min hasta el instante actual.
    """
    from backend.services.microSniper import fetchSniperCandlesHistory
    return fetchSniperCandlesHistory(symbol=symbol, timeframe=timeframe, days=days, limitBars=limit)


def getAdaptiveConfigForBreakout(db, symbol: str) -> Dict[str, Any]:
    """
    Obtiene la calibración adaptativa para BREAKOUT desde symbolstrategyconfig o defaults.
    """
    cleanSym = (symbol or "").replace("/", "").strip().upper()
    defaultCfg = {
        "sesion": "NY",
        "horaInicio": "09:30",
        "duracionRangoMin": 15,
        "horaFin": "12:00",
        "minRr": 2.0,
        "maxRr": 2.0,
        "usarEma200": False,
        "usarImpulseMacd": False,
        "cuerpoVelaMin": 0.50,
        "usarBreakEven": True,
        "usarTSL": False,
        "multiplicadorRiesgo": 1.0
    }
    try:
        sql = text("""
            SELECT parametersJson
            FROM symbolstrategyconfig
            WHERE strategy IN ('BreakoutNY', 'BREAKOUT')
              AND UPPER(REPLACE(symbol, '/', '')) = :sym
            LIMIT 1
        """)
        row = db.execute(sql, {"sym": cleanSym}).fetchone()
        if row and row[0]:
            params = json.loads(row[0]) if isinstance(row[0], str) else row[0]
            if "range_duration" in params:
                defaultCfg["duracionRangoMin"] = int(params["range_duration"])
            if "min_rr" in params:
                defaultCfg["minRr"] = float(params["min_rr"])
            if "start_hour" in params:
                h = int(params["start_hour"])
                m = int(params.get("start_minute", 30))
                defaultCfg["horaInicio"] = f"{h:02d}:{m:02d}"
            if "trading_window" in params:
                # Si trading_window está en minutos desde rangeEnd
                twMin = int(params["trading_window"])
                hIni, mIni = map(int, defaultCfg["horaInicio"].split(":"))
                dur = defaultCfg["duracionRangoMin"]
                totMin = hIni * 60 + mIni + dur + twMin
                hFin = min(23, totMin // 60)
                mFin = totMin % 60
                defaultCfg["horaFin"] = f"{hFin:02d}:{mFin:02d}"
    except Exception as e:
        logger.warn(f"No se pudo cargar config de symbolstrategyconfig para {symbol}: {e}")

    return defaultCfg


def calculateBreakoutRangeAndSignals(
    df: pd.DataFrame,
    symbol: str,
    sesion: str = "NY",
    horaInicio: str = "09:30",
    duracionRangoMin: int = 15,
    horaFin: str = "12:00",
    minRr: float = 1.0,
    usarEma200: bool = True,
    usarImpulseMacd: bool = True,
    cuerpoVelaMin: float = 0.60
) -> Dict[str, Any]:
    """
    Calcula los rangos de apertura matutina (NY o Londres), detecta rupturas
    válidas respetando la hora límite ('horaFin'), aplica filtros y simula resultados.
    
    Retorna:
      - currentRange: rango actual o más reciente (rangeHigh, rangeLow, rangeStart, rangeEnd, cutoffTime, status)
      - sessionZoom: coordenadas de inicio y fin (minTs, maxTs) para hacer zoom en la gráfica
      - signals: lista de señales detectadas
      - trades: lista de operaciones históricas simuladas
      - metrics: métricas de rendimiento (winRate, profitFactor, netPnl, etc.)
    """
    if df.empty or len(df) < 20:
        return {
            "currentRange": None,
            "sessionZoom": None,
            "signals": [],
            "trades": [],
            "metrics": {}
        }

    # Asegurar orden cronológico
    df = df.sort_index()

    # Zona horaria de referencia: la base de datos y la gráfica operan en America/Mexico_City.
    # El usuario configura horaInicio y horaFin en el horario nativo de la sesión seleccionada (NY o LDN).
    # Se realiza el traslado automático a America/Mexico_City para sincronizar exactamente con las velas intradiarias.
    localTz = pytz.timezone("America/Mexico_City")

    sessionUpper = str(sesion or "NY").upper().strip()
    if any(k in sessionUpper for k in ["LON", "LDN"]):
        sessionTz = pytz.timezone("Europe/London")
    else:
        sessionTz = pytz.timezone("America/New_York")

    # Localizar / convertir index si es naive (las velas en BD están en America/Mexico_City)
    if df.index.tz is None:
        df_tz = df.copy()
        df_tz.index = df_tz.index.tz_localize(localTz)
    else:
        df_tz = df.copy()
        df_tz.index = df_tz.index.tz_convert(localTz)

    # Indicadores técnicos en toda la serie
    closeVals = df_tz["close"].values
    highVals = df_tz["high"].values
    lowVals = df_tz["low"].values
    openVals = df_tz["open"].values

    ema200 = ta.EMA(closeVals, timeperiod=200) if len(closeVals) >= 200 else np.full_like(closeVals, np.nan)
    macd, macdsignal, macdhist = ta.MACD(closeVals, fastperiod=12, slowperiod=26, signalperiod=9) if len(closeVals) >= 35 else (np.zeros_like(closeVals), np.zeros_like(closeVals), np.zeros_like(closeVals))

    # Parsear horaInicio y horaFin (formato HH:MM)
    defaultHIni, defaultMIni = (8, 0) if any(k in sessionUpper for k in ["LON", "LDN"]) else (9, 30)
    defaultHFin, defaultMFin = (11, 30) if any(k in sessionUpper for k in ["LON", "LDN"]) else (12, 0)

    try:
        hIni, mIni = map(int, str(horaInicio or f"{defaultHIni:02d}:{defaultMIni:02d}").strip().split(":"))
    except Exception:
        hIni, mIni = defaultHIni, defaultMIni

    try:
        hFin, mFin = map(int, str(horaFin or f"{defaultHFin:02d}:{defaultMFin:02d}").strip().split(":"))
    except Exception:
        hFin, mFin = defaultHFin, defaultMFin

    durRange = max(5, int(duracionRangoMin or 15))

    # Multiplicador de pip
    cleanSym = (symbol or "").replace("/", "").strip().upper()
    isJpy = "JPY" in cleanSym
    isCrypto = any(k in cleanSym for k in ["BTC", "ETH", "SOL", "LTC", "XRP"])
    pipMult = 100.0 if isJpy else (1.0 if isCrypto else 10000.0)

    # Agrupar por días en la zona de referencia
    df_tz["session_date"] = df_tz.index.date
    uniqueDays = df_tz["session_date"].unique()

    trades: List[Dict[str, Any]] = []
    signals: List[Dict[str, Any]] = []

    lastRangeInfo: Optional[Dict[str, Any]] = None
    lastZoomBounds: Optional[Dict[str, Any]] = None

    for day in uniqueDays:
        df_day = df_tz[df_tz["session_date"] == day]
        if len(df_day) < 3:
            continue

        # Fechas límite del rango de apertura localizadas en la zona nativa de la sesión
        # y trasladadas a la zona horaria local de las velas (America/Mexico_City)
        startNative = sessionTz.localize(datetime.combine(day, dt_time(hIni, mIni)))
        startTs = startNative.astimezone(localTz)
        endRangeTs = startTs + timedelta(minutes=durRange)

        cutoffNative = sessionTz.localize(datetime.combine(day, dt_time(hFin, mFin)))
        cutoffTs = cutoffNative.astimezone(localTz)

        # Zoom bounds: desde 45 min antes de la apertura hasta 60 min después del cutoff
        zoomStart = startTs - timedelta(minutes=45)
        zoomEnd = cutoffTs + timedelta(minutes=60)

        # Velas dentro del rango de apertura
        df_range = df_day[(df_day.index >= startTs) & (df_day.index < endRangeTs)]
        if df_range.empty:
            continue

        rangeHigh = float(df_range["high"].max())
        rangeLow = float(df_range["low"].min())
        rangeSize = rangeHigh - rangeLow
        if rangeSize <= 0:
            continue

        # Guardar como último rango evaluado
        statusRange = "RANGO DEFINIDO"
        nowInRef = datetime.now(localTz)
        if day == nowInRef.date():
            if nowInRef < endRangeTs:
                statusRange = "FORMANDO RANGO DE APERTURA"
            elif nowInRef <= cutoffTs:
                statusRange = "RANGO ACTIVO - ESPERANDO RUPTURA"
            else:
                statusRange = "SESIÓN CONCLUIDA (HORA LÍMITE ALCANZADA)"

        lastRangeInfo = {
            "session": sesion.upper(),
            "timeZone": "America/Mexico_City",
            "sessionDate": str(day),
            "rangeStart": startTs.isoformat(),
            "rangeEnd": endRangeTs.isoformat(),
            "cutoffTime": cutoffTs.isoformat(),
            "rangeStartMs": int(startTs.timestamp() * 1000),
            "rangeEndMs": int(endRangeTs.timestamp() * 1000),
            "cutoffTimeMs": int(cutoffTs.timestamp() * 1000),
            "rangeHigh": round(rangeHigh, 5),
            "rangeLow": round(rangeLow, 5),
            "rangeSizePips": round(rangeSize * pipMult, 1),
            "status": statusRange
        }

        # Guardar zoom bounds para la sesión más reciente
        lastZoomBounds = {
            "zoomStart": zoomStart.isoformat(),
            "zoomEnd": zoomEnd.isoformat(),
            "startTsMs": int(zoomStart.timestamp() * 1000),
            "endTsMs": int(zoomEnd.timestamp() * 1000)
        }

        # Velas en la ventana operativa (después de cerrar el rango y ANTES de horaFin)
        df_window = df_day[(df_day.index >= endRangeTs) & (df_day.index <= cutoffTs)]
        if df_window.empty:
            continue

        # =====================================================================
        # ESTRATEGIA YOUTUBE: BREAKOUT + ORDER BLOCK (5m) + RETEST (R:R 1:2)
        # =====================================================================
        # 1. Detectar si una vela posterior a las 9:30 NY (08:30 CDMX) cierra fuera del rango de 15m.
        # 2. Identificar el Order Block más cercano en 5 minutos.
        # 3. Esperar que el precio regrese (Retest) al Order Block para entrar al mercado.
        # 4. Colocar Stop Loss detrás del Order Block y Take Profit 1:2 (2%).
        # 5. Máximo 1 trade al día.
        # =====================================================================
        breakoutDir: Optional[str] = None
        breakoutCandleTs = None
        obCandleTs = None
        obHigh: Optional[float] = None
        obLow: Optional[float] = None

        for idx, row in df_window.iterrows():
            closePx = float(row["close"])
            openPx = float(row["open"])
            highPx = float(row["high"])
            lowPx = float(row["low"])
            candleTs = idx

            body = abs(closePx - openPx)
            totSize = highPx - lowPx
            bodyRatio = body / totSize if totSize > 0 else 0.0
            if cuerpoVelaMin > 0 and bodyRatio < cuerpoVelaMin:
                continue

            locPos = df_tz.index.get_loc(idx)
            curEma = ema200[locPos] if not np.isnan(ema200[locPos]) else None
            curMacdHist = macdhist[locPos] if not np.isnan(macdhist[locPos]) else 0.0

            # Ruptura Alcista (Cierre por encima del High de 15m)
            if closePx > rangeHigh:
                if usarEma200 and curEma is not None and closePx < curEma:
                    continue
                if usarImpulseMacd and curMacdHist < 0:
                    continue

                breakoutDir = "LARGO"
                breakoutCandleTs = candleTs

                # Order Block (5m): última vela bajista (close < open) previa al impulso
                priorCandles = df_day[df_day.index < candleTs].tail(6)
                bearishOB = priorCandles[priorCandles["close"] < priorCandles["open"]]
                if not bearishOB.empty:
                    obCandle = bearishOB.iloc[-1]
                    obHigh = float(obCandle["high"])
                    obLow = float(obCandle["low"])
                    obCandleTs = obCandle.name
                else:
                    obHigh = rangeHigh
                    obLow = rangeHigh - (rangeSize * 0.25)
                    obCandleTs = candleTs

                # Asegurar coherencia geométrica
                if obHigh < rangeHigh:
                    obHigh = rangeHigh
                if obLow >= obHigh:
                    obLow = obHigh - (rangeSize * 0.25)
                break

            # Ruptura Bajista (Cierre por debajo del Low de 15m)
            elif closePx < rangeLow:
                if usarEma200 and curEma is not None and closePx > curEma:
                    continue
                if usarImpulseMacd and curMacdHist > 0:
                    continue

                breakoutDir = "CORTO"
                breakoutCandleTs = candleTs

                # Order Block (5m): última vela alcista (close > open) previa al impulso
                priorCandles = df_day[df_day.index < candleTs].tail(6)
                bullishOB = priorCandles[priorCandles["close"] > priorCandles["open"]]
                if not bullishOB.empty:
                    obCandle = bullishOB.iloc[-1]
                    obHigh = float(obCandle["high"])
                    obLow = float(obCandle["low"])
                    obCandleTs = obCandle.name
                else:
                    obLow = rangeLow
                    obHigh = rangeLow + (rangeSize * 0.25)
                    obCandleTs = candleTs

                # Asegurar coherencia geométrica
                if obLow > rangeLow:
                    obLow = rangeLow
                if obHigh <= obLow:
                    obHigh = obLow + (rangeSize * 0.25)
                break

        if not breakoutDir or breakoutCandleTs is None or obHigh is None or obLow is None:
            continue

        # Registrar Order Block en la info del rango para la sesión activa
        if lastRangeInfo:
            obCandleVal = obCandleTs if obCandleTs is not None else breakoutCandleTs
            lastRangeInfo["orderBlock"] = {
                "high": round(obHigh, 5),
                "low": round(obLow, 5),
                "direction": breakoutDir,
                "candleTime": obCandleVal.isoformat(),
                "candleTimeMs": int(obCandleVal.timestamp() * 1000),
                "breakoutTime": breakoutCandleTs.isoformat(),
                "breakoutTimeMs": int(breakoutCandleTs.timestamp() * 1000),
                "status": "ESPERANDO_RETEST"
            }

        # 2. Esperar RETEST al Order Block en las velas posteriores
        df_retest_window = df_day[(df_day.index > breakoutCandleTs) & (df_day.index <= cutoffTs)]
        tradeExecuted = False
        entryPx = 0.0
        slPx = 0.0
        tpPx = 0.0
        entryTs = None

        slBuffer = max(rangeSize * 0.05, 0.0003 if isJpy else (0.00005 if not isCrypto else 10.0))

        for rIdx, rRow in df_retest_window.iterrows():
            rH = float(rRow["high"])
            rL = float(rRow["low"])

            if breakoutDir == "LARGO":
                # Retest: el precio retrocede y toca la zona superior del Order Block
                if rL <= obHigh:
                    entryPx = obHigh
                    slPx = obLow - slBuffer
                    riskDist = entryPx - slPx
                    if riskDist <= 0:
                        continue
                    tpPx = entryPx + (float(minRr or 2.0) * riskDist)
                    entryTs = rIdx
                    tradeExecuted = True
                    break
            else:  # CORTO
                # Retest: el precio retrocede y toca la zona inferior del Order Block
                if rH >= obLow:
                    entryPx = obLow
                    slPx = obHigh + slBuffer
                    riskDist = slPx - entryPx
                    if riskDist <= 0:
                        continue
                    tpPx = entryPx - (float(minRr or 2.0) * riskDist)
                    entryTs = rIdx
                    tradeExecuted = True
                    break

        if not tradeExecuted or entryTs is None:
            # Si el día actual tuvo ruptura pero aún no retestea, se refleja en el estado del rango
            if lastRangeInfo and day == nowInRef.date():
                lastRangeInfo["status"] = f"RUPTURA {breakoutDir} CONFIRMADA - ESPERANDO RETEST EN ORDER BLOCK (5m)"
            continue

        # Ruptura y Retest completados con éxito -> Señal de entrada
        riskDist = abs(entryPx - slPx)
        sigObj = {
            "datetime": entryTs.isoformat(),
            "symbol": symbol,
            "direction": breakoutDir,
            "entry": round(entryPx, 5),
            "stopLoss": round(slPx, 5),
            "takeProfit": round(tpPx, 5),
            "rangeHigh": round(rangeHigh, 5),
            "rangeLow": round(rangeLow, 5),
            "orderBlockHigh": round(obHigh, 5),
            "orderBlockLow": round(obLow, 5),
            "riskPips": round(riskDist * pipMult, 1)
        }
        signals.append(sigObj)

        if lastRangeInfo and day == nowInRef.date():
            lastRangeInfo["status"] = f"OPERACIÓN {breakoutDir} EN VIVO (RETEST CONFIRMADO)"
            if "orderBlock" in lastRangeInfo and lastRangeInfo["orderBlock"]:
                lastRangeInfo["orderBlock"]["retestTime"] = entryTs.isoformat()
                lastRangeInfo["orderBlock"]["retestTimeMs"] = int(entryTs.timestamp() * 1000)
                lastRangeInfo["orderBlock"]["status"] = "RETEST_CONFIRMADO"

        # 3. Simular desenlace del trade vela a vela durante el resto del día (1 trade al día)
        df_after = df_day[df_day.index > entryTs]
        isClosed = False
        exitPx = entryPx
        exitReason = "CIERRE_SESION"
        exitTs = entryTs

        for aIdx, aRow in df_after.iterrows():
            aHigh = float(aRow["high"])
            aLow = float(aRow["low"])
            exitTs = aIdx

            if breakoutDir == "LARGO":
                if aHigh >= tpPx:
                    exitPx = tpPx
                    exitReason = "TAKE_PROFIT"
                    isClosed = True
                    break
                elif aLow <= slPx:
                    exitPx = slPx
                    exitReason = "STOP_LOSS"
                    isClosed = True
                    break
            else:  # CORTO
                if aLow <= tpPx:
                    exitPx = tpPx
                    exitReason = "TAKE_PROFIT"
                    isClosed = True
                    break
                elif aHigh >= slPx:
                    exitPx = slPx
                    exitReason = "STOP_LOSS"
                    isClosed = True
                    break

        if not isClosed and not df_after.empty:
            exitPx = float(df_after.iloc[-1]["close"])
            exitTs = df_after.index[-1]
            exitReason = "CIERRE_DIA"

        pnlPips = (exitPx - entryPx) * pipMult if breakoutDir == "LARGO" else (entryPx - exitPx) * pipMult
        isWin = pnlPips > 0

        # Actualizar metadatos de cierre en la señal
        sigObj["closeTime"] = exitTs.isoformat()
        sigObj["closeTimeMs"] = int(exitTs.timestamp() * 1000)
        sigObj["exitPrice"] = round(exitPx, 5)
        sigObj["exitReason"] = exitReason
        sigObj["isClosed"] = isClosed

        if lastRangeInfo and day == nowInRef.date():
            lastRangeInfo["tradeCloseTimeMs"] = int(exitTs.timestamp() * 1000)
            lastRangeInfo["exitReason"] = exitReason
            lastRangeInfo["isClosed"] = isClosed
            if "orderBlock" in lastRangeInfo and lastRangeInfo["orderBlock"]:
                lastRangeInfo["orderBlock"]["closeTime"] = exitTs.isoformat()
                lastRangeInfo["orderBlock"]["closeTimeMs"] = int(exitTs.timestamp() * 1000)
                lastRangeInfo["orderBlock"]["exitPrice"] = round(exitPx, 5)
                lastRangeInfo["orderBlock"]["exitReason"] = exitReason
                lastRangeInfo["orderBlock"]["isClosed"] = isClosed
                lastRangeInfo["orderBlock"]["status"] = f"CONCLUIDO ({exitReason})"
            if isClosed:
                lastRangeInfo["status"] = f"OPERACIÓN {breakoutDir} CONCLUIDA ({exitReason})"
            else:
                lastRangeInfo["status"] = f"OPERACIÓN {breakoutDir} EN VIVO (EN PROGRESO)"

        trades.append({
            "openTime": entryTs.strftime("%Y-%m-%d %H:%M:%S"),
            "closeTime": exitTs.strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": symbol,
            "direction": breakoutDir,
            "entryPrice": round(entryPx, 5),
            "exitPrice": round(exitPx, 5),
            "stopLoss": round(slPx, 5),
            "takeProfit": round(tpPx, 5),
            "pnlPips": round(pnlPips, 1),
            "isWin": isWin,
            "exitReason": exitReason
        })

    # Calcular Métricas del Backtest
    totalTrades = len(trades)
    winningTrades = sum(1 for t in trades if t["isWin"])
    losingTrades = totalTrades - winningTrades
    winRate = (winningTrades / totalTrades * 100.0) if totalTrades > 0 else 0.0

    grossProfitPips = sum(t["pnlPips"] for t in trades if t["pnlPips"] > 0)
    grossLossPips = abs(sum(t["pnlPips"] for t in trades if t["pnlPips"] < 0))
    profitFactor = (grossProfitPips / grossLossPips) if grossLossPips > 0 else (99.0 if grossProfitPips > 0 else 0.0)
    netPnlPips = sum(t["pnlPips"] for t in trades)

    # Curva de Equity acumulada
    equityCurve = []
    runningBalance = 1000.0  # Base estándar $1,000 USD
    peakBalance = runningBalance
    maxDrawdownPct = 0.0

    equityCurve.append({"time": "Inicio", "balance": runningBalance})
    for t in trades:
        # Estimación aproximada: $10 USD por cada 10 pips (1 lote estándar) o normalizado
        tradeDollarPnl = round(t["pnlPips"] * 1.5, 2)
        runningBalance += tradeDollarPnl
        if runningBalance > peakBalance:
            peakBalance = runningBalance
        dd = ((peakBalance - runningBalance) / peakBalance * 100.0) if peakBalance > 0 else 0.0
        if dd > maxDrawdownPct:
            maxDrawdownPct = dd

        equityCurve.append({
            "time": t["closeTime"],
            "balance": round(runningBalance, 2),
            "pnl": tradeDollarPnl
        })

    metrics = {
        "totalTrades": totalTrades,
        "winningTrades": winningTrades,
        "losingTrades": losingTrades,
        "winRate": round(winRate, 1),
        "profitFactor": round(min(profitFactor, 99.0), 2),
        "netPnlPips": round(netPnlPips, 1),
        "netProfit": round(runningBalance - 1000.0, 2),
        "maxDrawdownPct": round(maxDrawdownPct, 1),
        "initialCapital": 1000.0,
        "finalBalance": round(runningBalance, 2)
    }

    return {
        "currentRange": lastRangeInfo,
        "sessionZoom": lastZoomBounds,
        "signals": signals[-15:],  # Últimas señales
        "trades": trades,
        "metrics": metrics,
        "equityCurve": equityCurve
    }


# ==============================================================================
# EVALUACIÓN EN TIEMPO REAL Y MOTOR MULTI-HILO A NIVEL DE CUENTA
# ==============================================================================

_lastBreakoutLogTime: Dict[Tuple[int, str], float] = {}
_failedBreakoutOrderCooldown: Dict[Tuple[int, str], float] = {}


def shouldLogBreakoutStatus(idCuenta: int, symbol: str, intervalSec: float = 30.0) -> bool:
    key = (idCuenta, symbol)
    now = time.time()
    last = _lastBreakoutLogTime.get(key, 0.0)
    if now - last >= intervalSec:
        _lastBreakoutLogTime[key] = now
        return True
    return False


def isBreakoutOrderInCooldown(idCuenta: int, symbol: str) -> bool:
    key = (idCuenta, symbol)
    now = time.time()
    until = _failedBreakoutOrderCooldown.get(key, 0.0)
    return now < until


def setBreakoutOrderCooldown(idCuenta: int, symbol: str, seconds: float = 120.0) -> None:
    key = (idCuenta, symbol)
    _failedBreakoutOrderCooldown[key] = time.time() + seconds


def evaluateSingleBreakout(dbSession, breakoutRecord: Dict[str, Any]) -> None:
    """
    Evalúa un par configurado en userBREAKOUT en tiempo real:
      - Monitorea trades activos (TP, SL, BreakEven, TSL).
      - Si no hay trade activo y operar=1, evalúa señales de ruptura del rango matutino.
    """
    from backend.services.microSniper import (
        isMarketOpenForSymbol,
        isWeekendClosingWindow,
        isDividendShortRestricted
    )

    idCuenta = int(breakoutRecord["idCuenta"])
    symbol = str(breakoutRecord["symbol"]).strip()
    timeframe = str(breakoutRecord.get("Temporalidad") or "5min").strip()
    operar = bool(breakoutRecord.get("operar", False))
    sesion = str(breakoutRecord.get("sesion") or "NY").strip().upper()
    horaInicio = str(breakoutRecord.get("horaInicio") or "09:30").strip()
    duracionRangoMin = int(breakoutRecord.get("duracionRangoMin") or 15)
    horaFin = str(breakoutRecord.get("horaFin") or "12:00").strip()
    minRr = float(breakoutRecord.get("minRr") or 1.0)
    usarBreakEven = bool(breakoutRecord.get("usarBreakEven", True))
    usarTSL = bool(breakoutRecord.get("usarTSL", False))
    usarEma200 = bool(breakoutRecord.get("usarEma200", True))
    usarImpulseMacd = bool(breakoutRecord.get("usarImpulseMacd", True))
    cuerpoVelaMin = float(breakoutRecord.get("cuerpoVelaMin") or 0.60)

    # 1. Verificar si hay posición abierta para este símbolo y cuenta en BREAKOUT
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
            if shouldLogBreakoutStatus(idCuenta, symbol, 300.0):
                logger.info(f"⏸️ [BREAKOUT] Mercado cerrado para {symbol}. Posición #{openTrades[0]['idTrade']} en pausa.")
        else:
            if shouldLogBreakoutStatus(idCuenta, symbol, 300.0):
                logger.info(f"⏸️ [BREAKOUT] Mercado cerrado para {symbol}.")
        return

    df = fetchBreakoutCandlesHistory(symbol, timeframe=timeframe, days=60)
    if df.empty or len(df) < 50:
        if shouldLogBreakoutStatus(idCuenta, symbol, 60.0):
            logger.warning(f"⚠️ [BREAKOUT] Velas insuficientes para {symbol} ({timeframe}).")
        return

    currentPx = float(df["close"].iloc[-1])
    highVal = float(df["high"].iloc[-1])
    lowVal = float(df["low"].iloc[-1])

    # Gestión de Posiciones Abiertas
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

        # A. BreakEven Adaptativo: asegura posición a precio de entrada al avanzar 50% hacia TP
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
                logger.info(f"🛡️ [BREAKOUT] Posición #{firstTr['idTrade']} en {symbol} ({direction}) movida a BreakEven @ {entryPx:.5f}!")
                isBe = True
                slPx = newSl

        # B. Trailing Stop Loss opcional
        canTsl = usarTSL and slPx and (isBe or not usarBreakEven)
        if canTsl:
            atrVal = float((df["high"].iloc[-14:] - df["low"].iloc[-14:]).mean()) if len(df) >= 14 else 0.0010
            slDistance = atrVal * 1.5

            trOpenTime = firstTr.get("openTime")
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
                if candidateSl > slPx:
                    dbSession.execute(text("UPDATE trades SET stopLoss = :nsl WHERE idTrade = :idt"), {"nsl": candidateSl, "idt": firstTr["idTrade"]})
                    dbSession.commit()
                    logger.info(f"📈 [BREAKOUT] TSL avanzado para #{firstTr['idTrade']} en {symbol} (LARGO): {slPx:.5f} -> {candidateSl:.5f}")
                    slPx = candidateSl
            else:
                candidateSl = bestLow + slDistance
                if tpPx is not None:
                    candidateSl = max(candidateSl, tpPx)
                if candidateSl < slPx:
                    dbSession.execute(text("UPDATE trades SET stopLoss = :nsl WHERE idTrade = :idt"), {"nsl": candidateSl, "idt": firstTr["idTrade"]})
                    dbSession.commit()
                    logger.info(f"📉 [BREAKOUT] TSL avanzado para #{firstTr['idTrade']} en {symbol} (CORTO): {slPx:.5f} -> {candidateSl:.5f}")
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

        # E. Cierre Preventivo Fin de Semana
        if not shouldClose and isWeekendClosingWindow(symbol=symbol, dbSession=dbSession):
            shouldClose = True
            exitReason = "Cierre Preventivo Fin de Semana"
            exitFillPx = currentPx

        if shouldClose:
            logger.info(f"🎯 [BREAKOUT] Cierre de posición #{firstTr['idTrade']} {symbol} ({direction}): {exitReason}")
            for tr in openTrades:
                resClose = execute_single_asset_close(
                    dbSession=dbSession,
                    idCuenta=idCuenta,
                    tradeRow=dict(tr),
                    strategy=STRATEGY_NAME,
                    exitPrice=exitFillPx,
                    closeReason=f"Cierre BREAKOUT ({exitReason})"
                )
                if not resClose.get("success"):
                    logger.error(f"❌ [BREAKOUT] Error cerrando Trade #{tr['idTrade']}: {resClose.get('error')}")

            setBreakoutOrderCooldown(idCuenta, symbol, 180.0)
            if str(breakoutRecord.get("accionCierre") or "Continua").strip().lower() == "para":
                dbSession.execute(text("UPDATE userBREAKOUT SET operar = 0 WHERE id = :bid"), {"bid": breakoutRecord["id"]})
                dbSession.commit()
            return
        else:
            if shouldLogBreakoutStatus(idCuenta, symbol, 30.0):
                logger.info(
                    f"⚡ [BREAKOUT] Posición #{firstTr['idTrade']} en curso: Cuenta {idCuenta} | {symbol} ({direction}) | "
                    f"Px={currentPx:.5f} | Entrada={entryPx:.5f} | SL={slPx or '-'} | TP={tpPx or '-'} | BE={'SÍ' if isBe else 'NO'}"
                )
            return

    # 2. Si no hay posición abierta y operar=1, evaluar nuevas señales de Breakout
    if not openTrades and operar:
        if isWeekendClosingWindow(symbol=symbol, dbSession=dbSession):
            return

        if isBreakoutOrderInCooldown(idCuenta, symbol):
            return

        resAnalysis = calculateBreakoutRangeAndSignals(
            df=df,
            symbol=symbol,
            sesion=sesion,
            horaInicio=horaInicio,
            duracionRangoMin=duracionRangoMin,
            horaFin=horaFin,
            minRr=minRr,
            usarEma200=usarEma200,
            usarImpulseMacd=usarImpulseMacd,
            cuerpoVelaMin=cuerpoVelaMin
        )

        signals = resAnalysis.get("signals", [])
        if not signals:
            if shouldLogBreakoutStatus(idCuenta, symbol, 30.0):
                curR = resAnalysis.get("currentRange")
                st = curR.get("status", "Sin señal") if curR else "Sin rango hoy"
                logger.info(f"⚡ [BREAKOUT] Cuenta {idCuenta} | {symbol} ({timeframe}) | Px={currentPx:.5f} | Estado: {st} | Operar={operar}")
            return

        lastSig = signals[-1]
        sigDtStr = lastSig.get("datetime")
        # Verificar que la señal corresponda a la última vela cerrada (dentro de los últimos 20 minutos)
        try:
            sigDt = pd.to_datetime(sigDtStr)
            nowUtc = datetime.now(timezone.utc)
            if hasattr(sigDt, "tz") and sigDt.tz is not None:
                diffSec = (nowUtc - sigDt.tz_convert(timezone.utc)).total_seconds()
            else:
                diffSec = (datetime.now() - sigDt).total_seconds()

            if diffSec > 1500:  # Señal de hace más de 25 min -> ya pasó
                if shouldLogBreakoutStatus(idCuenta, symbol, 60.0):
                    logger.info(f"⏳ [BREAKOUT] Última señal para {symbol} ocurrió hace {int(diffSec/60)} min. En espera de nuevo rango.")
                return
        except Exception:
            pass

        directionStr = "LARGO" if lastSig.get("direction") == "LARGO" else "CORTO"
        isLong = (directionStr == "LARGO")
        entryPx = float(lastSig["entry"])
        slPrice = float(lastSig["stopLoss"])
        tpPrice = float(lastSig["takeProfit"])

        if not isLong:
            isDivRestr, divDetail = isDividendShortRestricted(dbSession, symbol)
            if isDivRestr:
                logger.info(f"🚫 [BREAKOUT] [{symbol}] Entrada en CORTO bloqueada por dividendos ({divDetail}).")
                return

        # Salud de Margen y Riesgo Financiero
        rowAcc = dbSession.execute(text("SELECT Capital, riesgoPorOperacion FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).fetchone()
        cap = float(rowAcc[0] or 1000.0) if rowAcc else 1000.0
        riesgoPct = float(rowAcc[1] or 1.0) if rowAcc else 1.0

        multRiesgo = 1.0
        rowUsr = dbSession.execute(text("SELECT multiplicadorRiesgo FROM userBREAKOUT WHERE idCuenta = :idc AND symbol = :sym AND (borrado = 0 OR borrado IS NULL) LIMIT 1"), {"idc": idCuenta, "sym": symbol}).fetchone()
        if rowUsr and rowUsr[0] is not None:
            multRiesgo = max(0.1, float(rowUsr[0]))

        effectiveRiesgoPct = riesgoPct * multRiesgo
        allocationRate = (effectiveRiesgoPct / 100.0) if effectiveRiesgoPct >= 0.05 else effectiveRiesgoPct
        if allocationRate <= 0: allocationRate = 0.01

        canOperate, marginInd, totalMargin, equity, minMarginRequired = check_account_margin_health(dbSession, idCuenta, cap)
        if not canOperate:
            logger.warning(f"⛔ [BREAKOUT] [{symbol}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: Margen ({marginInd:.1f}%) < {minMarginRequired:.0f}%.")
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
            logger.warning(f"⛔ [BREAKOUT] [{symbol}] Margen proyectado ({projectedIndicator:.1f}%) < {minMarginRequired:.0f}%. Entrada omitida.")
            return

        units = round(multA * minLots, 4)

        logger.info(
            f"🎯 [BREAKOUT] DISPARO DE RUPTURA {directionStr} en {symbol} | "
            f"Px={entryPx:.5f} | SL={slPrice:.5f} | TP={tpPrice:.5f} | R:R={minRr:.2f} | "
            f"Units={units:g} | Cuenta #{idCuenta}"
        )

        resEntry = execute_single_asset_entry(
            dbSession=dbSession,
            idCuenta=idCuenta,
            strategy=STRATEGY_NAME,
            setup=f"BREAKOUT {sesion} {timeframe.upper()}",
            symbol=symbol,
            direction=directionStr,
            timeframe=timeframe,
            size=units,
            price=entryPx,
            sl=slPrice,
            tp=tpPrice,
            margenPct=margenPct,
            comment=f"BREAKOUT {sesion} {directionStr} (R:R={minRr:.2f})",
            signalType=f"BREAKOUT {sesion}",
            candleDt=datetime.now()
        )

        if not resEntry.get("success"):
            setBreakoutOrderCooldown(idCuenta, symbol, 120.0)
            logger.error(f"❌ [BREAKOUT] No se pudo ejecutar orden para Cuenta #{idCuenta} {symbol}: {resEntry.get('error')}")
            return


def processBreakoutForAccount(idCuenta: int) -> None:
    """
    Función de trabajo para el hilo asignado a una cuenta específica.
    Abre su propia sesión de base de datos y evalúa todos los pares asignados a esa cuenta.
    """
    db = SessionLocal()
    try:
        sql = text("""
            SELECT b.id, b.idUsuario, b.idCuenta, b.symbol, b.Temporalidad, b.periodo,
                   b.sesion, b.horaInicio, b.duracionRangoMin, b.horaFin,
                   b.minRr, b.maxRr, b.operar, b.accionCierre, b.tipoCierre,
                   b.usarBreakEven, b.usarTSL, b.usarEma200, b.usarImpulseMacd,
                   b.cuerpoVelaMin, b.multiplicadorRiesgo
            FROM userBREAKOUT b
            JOIN cuenta c ON b.idCuenta = c.idCuenta
            WHERE b.idCuenta = :idc AND (b.borrado = 0 OR b.borrado IS NULL) AND c.Activo = 1
              AND (b.operar = 1 OR EXISTS (
                  SELECT 1 FROM trades t 
                  WHERE t.idCuenta = b.idCuenta 
                    AND t.strategy = 'BREAKOUT' 
                    AND (t.symbol = b.symbol OR t.setup = b.symbol)
                    AND t.status = 'OPEN'
              ))
            ORDER BY b.operar DESC, b.id ASC
        """)
        records = db.execute(sql, {"idc": idCuenta}).mappings().fetchall()
        for rec in records:
            try:
                evaluateSingleBreakout(db, dict(rec))
            except Exception as exSingle:
                logger.error(f"❌ [BREAKOUT] Error evaluando {rec.get('symbol')} (Cuenta {idCuenta}): {exSingle}", exc_info=True)
            time.sleep(0.3)
    except Exception as exAcc:
        logger.error(f"❌ [BREAKOUT] Error procesando Cuenta #{idCuenta}: {exAcc}", exc_info=True)
    finally:
        db.close()


def main() -> None:
    import fcntl
    from concurrent.futures import ThreadPoolExecutor

    lock_file = open('/tmp/microBreakout.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (IOError, BlockingIOError):
        print("⚠️ Ya existe una instancia de microBreakout en ejecución. Abortando.")
        sys.exit(0)

    logger.info("==========================================================")
    logger.info("🚀 Iniciando Microservicio BREAKOUT (microBreakout.py)")
    logger.info("⚡ Arquitectura Concurrente: Pool de 5 Hilos por Cuenta")
    logger.info("==========================================================")

    executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix="BreakoutWorker")

    while True:
        db = SessionLocal()
        try:
            sqlAccounts = text("""
                SELECT DISTINCT b.idCuenta
                FROM userBREAKOUT b
                JOIN cuenta c ON b.idCuenta = c.idCuenta
                WHERE (b.borrado = 0 OR b.borrado IS NULL) AND c.Activo = 1
                  AND (b.operar = 1 OR EXISTS (
                      SELECT 1 FROM trades t 
                      WHERE t.idCuenta = b.idCuenta 
                        AND t.strategy = 'BREAKOUT' 
                        AND (t.symbol = b.symbol OR t.setup = b.symbol)
                        AND t.status = 'OPEN'
                  ))
                ORDER BY b.idCuenta ASC
            """)
            accountRows = db.execute(sqlAccounts).fetchall()
            activeAccountIds = [int(r[0]) for r in accountRows]
        except Exception as exMain:
            logger.error(f"❌ [BREAKOUT] Error consultando cuentas activas: {exMain}", exc_info=True)
            activeAccountIds = []
        finally:
            db.close()

        if activeAccountIds:
            # Despachar las cuentas en paralelo en el pool de 5 hilos
            futures = [executor.submit(processBreakoutForAccount, accId) for accId in activeAccountIds]
            for f in futures:
                try:
                    f.result(timeout=60.0)
                except Exception as exFut:
                    logger.error(f"❌ [BREAKOUT] Excepción en hilo de cuenta: {exFut}")

        time.sleep(6.0)


if __name__ == "__main__":
    main()
