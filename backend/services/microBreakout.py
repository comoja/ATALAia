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
    desde la tabla candles o ConnectionPool.
    """
    db = SessionLocal()
    try:
        cleanSym = (symbol or "").replace("/", "").strip().upper()
        sql = text("""
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE UPPER(REPLACE(symbol, '/', '')) = :sym
              AND timeframe = :tf
              AND timestamp >= DATE_SUB(NOW(), INTERVAL :days DAY)
            ORDER BY timestamp ASC
            LIMIT :lim
        """)
        rows = db.execute(sql, {
            "sym": cleanSym,
            "tf": timeframe,
            "days": int(days),
            "lim": int(limit)
        }).fetchall()

        if not rows:
            # Fallback sin filtro de días
            sqlFallback = text("""
                SELECT timestamp as datetime, open, high, low, close, volume
                FROM (
                    SELECT timestamp, open, high, low, close, volume
                    FROM candles
                    WHERE UPPER(REPLACE(symbol, '/', '')) = :sym
                      AND timeframe = :tf
                    ORDER BY timestamp DESC
                    LIMIT :lim
                ) sub
                ORDER BY timestamp ASC
            """)
            rows = db.execute(sqlFallback, {"sym": cleanSym, "tf": timeframe, "lim": int(limit)}).fetchall()

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows, columns=["datetime", "open", "high", "low", "close", "volume"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.set_index("datetime", inplace=True)
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        df = df[~df.index.duplicated(keep="last")]
        return df
    except Exception as e:
        logger.error(f"Error en fetchBreakoutCandlesHistory para {symbol}: {e}")
        return pd.DataFrame()
    finally:
        db.close()


def getAdaptiveConfigForBreakout(db, symbol: str) -> Dict[str, Any]:
    """
    Obtiene la calibración adaptativa para BREAKOUT desde symbolstrategyconfig o defaults.
    """
    cleanSym = (symbol or "").replace("/", "").strip().upper()
    defaultCfg = {
        "sesion": "NY",
        "horaInicio": "08:30",
        "duracionRangoMin": 15,
        "horaFin": "11:00",
        "minRr": 1.0,
        "maxRr": 2.0,
        "usarEma200": True,
        "usarImpulseMacd": True,
        "cuerpoVelaMin": 0.60,
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
    horaInicio: str = "08:30",
    duracionRangoMin: int = 15,
    horaFin: str = "11:00",
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

    # Timezones: convertimos velas a la zona horaria de referencia para la sesión
    # NY -> America/New_York
    # LONDRES -> Europe/London
    refTzName = "America/New_York" if sesion.upper() in ["NY", "NEW YORK", "NUEVA YORK"] else "Europe/London"
    refTz = pytz.timezone(refTzName)

    # Localizar / convertir index si es naive
    if df.index.tz is None:
        # Asumimos que las velas en BD están en UTC
        df_tz = df.copy()
        df_tz.index = df_tz.index.tz_localize("UTC").tz_convert(refTz)
    else:
        df_tz = df.copy()
        df_tz.index = df_tz.index.tz_convert(refTz)

    # Indicadores técnicos en toda la serie
    closeVals = df_tz["close"].values
    highVals = df_tz["high"].values
    lowVals = df_tz["low"].values
    openVals = df_tz["open"].values

    ema200 = ta.EMA(closeVals, timeperiod=200) if len(closeVals) >= 200 else np.full_like(closeVals, np.nan)
    macd, macdsignal, macdhist = ta.MACD(closeVals, fastperiod=12, slowperiod=26, signalperiod=9) if len(closeVals) >= 35 else (np.zeros_like(closeVals), np.zeros_like(closeVals), np.zeros_like(closeVals))

    # Parsear horaInicio y horaFin (formato HH:MM)
    try:
        hIni, mIni = map(int, str(horaInicio or "08:30").strip().split(":"))
    except Exception:
        hIni, mIni = 8, 30

    try:
        hFin, mFin = map(int, str(horaFin or "11:00").strip().split(":"))
    except Exception:
        hFin, mFin = 11, 0

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

        # Fechas límite del rango de apertura
        startTs = refTz.localize(datetime.combine(day, dt_time(hIni, mIni)))
        endRangeTs = startTs + timedelta(minutes=durRange)
        cutoffTs = refTz.localize(datetime.combine(day, dt_time(hFin, mFin)))

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
        nowInRef = datetime.now(refTz)
        if day == nowInRef.date():
            if nowInRef < endRangeTs:
                statusRange = "FORMANDO RANGO DE APERTURA"
            elif nowInRef <= cutoffTs:
                statusRange = "RANGO ACTIVO - ESPERANDO RUPTURA"
            else:
                statusRange = "SESIÓN CONCLUIDA (HORA LÍMITE ALCANZADA)"

        lastRangeInfo = {
            "session": sesion.upper(),
            "timeZone": refTzName,
            "sessionDate": str(day),
            "rangeStart": startTs.isoformat(),
            "rangeEnd": endRangeTs.isoformat(),
            "cutoffTime": cutoffTs.isoformat(),
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

        tradeActive = False
        for idx, row in df_window.iterrows():
            if tradeActive:
                break

            closePx = float(row["close"])
            openPx = float(row["open"])
            highPx = float(row["high"])
            lowPx = float(row["low"])
            candleTs = idx

            # Filtro 1: Calidad de vela (cuerpo >= 60%)
            body = abs(closePx - openPx)
            totSize = highPx - lowPx
            bodyRatio = body / totSize if totSize > 0 else 0.0
            if bodyRatio < cuerpoVelaMin:
                continue

            # Obtener posición en array original para EMA y MACD
            locPos = df_tz.index.get_loc(idx)
            curEma = ema200[locPos] if not np.isnan(ema200[locPos]) else None
            curMacdHist = macdhist[locPos] if not np.isnan(macdhist[locPos]) else 0.0

            direction: Optional[str] = None
            entryPx = closePx
            slPx = 0.0
            tpPx = 0.0
            riskDist = 0.0

            # Ruptura Alcista
            if closePx > rangeHigh:
                # Filtro EMA 200
                if usarEma200 and curEma is not None and closePx < curEma:
                    continue
                # Filtro MACD
                if usarImpulseMacd and curMacdHist < 0:
                    continue

                direction = "LARGO"
                slPx = rangeLow
                riskDist = entryPx - slPx
                tpPx = entryPx + (riskDist * minRr)

            # Ruptura Bajista
            elif closePx < rangeLow:
                # Filtro EMA 200
                if usarEma200 and curEma is not None and closePx > curEma:
                    continue
                # Filtro MACD
                if usarImpulseMacd and curMacdHist > 0:
                    continue

                direction = "CORTO"
                slPx = rangeHigh
                riskDist = slPx - entryPx
                tpPx = entryPx - (riskDist * minRr)

            if not direction or riskDist <= 0:
                continue

            # Ruptura válida confirmada
            tradeActive = True
            sigObj = {
                "datetime": candleTs.isoformat(),
                "symbol": symbol,
                "direction": direction,
                "entry": round(entryPx, 5),
                "stopLoss": round(slPx, 5),
                "takeProfit": round(tpPx, 5),
                "rangeHigh": round(rangeHigh, 5),
                "rangeLow": round(rangeLow, 5),
                "riskPips": round(riskDist * pipMult, 1)
            }
            signals.append(sigObj)

            # Simular desenlace del trade vela a vela durante el resto del día
            df_after = df_day[df_day.index > candleTs]
            isClosed = False
            exitPx = entryPx
            exitReason = "CIERRE_SESION"
            exitTs = candleTs

            for aIdx, aRow in df_after.iterrows():
                aHigh = float(aRow["high"])
                aLow = float(aRow["low"])
                aClose = float(aRow["close"])
                exitTs = aIdx

                if direction == "LARGO":
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

            # Calcular PnL en pips y valor nominal
            pnlPips = (exitPx - entryPx) * pipMult if direction == "LARGO" else (entryPx - exitPx) * pipMult
            isWin = pnlPips > 0

            trades.append({
                "openTime": candleTs.strftime("%Y-%m-%d %H:%M:%S"),
                "closeTime": exitTs.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "direction": direction,
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
