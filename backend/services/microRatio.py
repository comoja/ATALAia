#!/usr/bin/env python3
"""
Microservicio: microRatio.py
===========================
Evalúa periódicamente (cada hora) las combinaciones activas en 'user_ratios'
con 'operar = 1' para la cuenta correspondiente.

Lógica de Operación:
1. ENTRADA (Triángulo Coincidente):
   Se abren exactamente 2 registros (las 2 patas) cuando ambos pares se encuentran fuera
   de su desviación estándar (+1σ o -1σ) y cruzan simultáneamente su respectiva EMA rápida.
2. CIERRE (Convergencia de Precios ●):
   Se cierran y liquidan las posiciones ÚNICAMENTE cuando la curva de precios normalizados
   del Par A cruza a la curva de precios normalizados del Par B.

Todas las funciones y variables utilizan nomenclatura camelCase.
"""

import os
import sys
import time
import math
import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple

import pandas as pd
import numpy as np
from sqlalchemy import text

# Configurar path raíz del proyecto
projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from backend.database.models import SessionLocal
from backend.core.pnl_calculator import calculate_unrealized_pnl, calculate_margin
from backend.services.signal_engine import signalEngine
from middleware.utils.communications import alertaInmediata

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [microRatio] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("microRatio")


def ensureStrategyRegistered(dbSession) -> None:
    """Asegura que la estrategia RATIO ATALAia esté registrada en strategyconfig."""
    try:
        dbSession.execute(text("INSERT IGNORE INTO strategyconfig (strategy, enabled) VALUES ('RATIO ATALAia', 1)"))
        dbSession.commit()
    except Exception as e:
        dbSession.rollback()
        logger.warning(f"Error asegurando estrategia en strategyconfig: {e}")


def getDatabaseSession():
    """Retorna una sesión activa de SQLAlchemy."""
    return SessionLocal()


def fetchActiveUserRatios(dbSession) -> List[Dict[str, Any]]:
    """
    Obtiene todas las configuraciones de user_ratios donde operar = 1,
    o que tengan posiciones abiertas (status = 'OPEN') en trades para poder evaluarlas y cerrarlas.
    """
    sqlQuery = text("""
        SELECT ur.id, ur.idUsuario, ur.idCuenta, ur.numerador, ur.denominador, ur.Temporalidad, ur.periodo, ur.EMARapida, ur.EMALenta, ur.operar, COALESCE(ur.cierreDivergencia, 2) AS cierreDivergencia, COALESCE(ur.tipoEntrada, 'Selectiva') AS tipoEntrada, ur.fixedMinA, ur.fixedMaxA, ur.fixedMinB, ur.fixedMaxB, COALESCE(ur.accionCierre, 'Continua') AS accionCierre, COALESCE(ur.tipoCierre, 'Diferencia') AS tipoCierre, COALESCE(ur.umbralDiferencia, 0.2000) AS umbralDiferencia, COALESCE(ur.salirBreakeven, 0) AS salirBreakeven
        FROM user_ratios ur
        WHERE (ur.borrado = 0 OR ur.borrado IS NULL)
          AND (
              ur.operar = 1 
              OR EXISTS (
                  SELECT 1 FROM trades t 
                  WHERE t.idCuenta = ur.idCuenta 
                    AND (t.setup = CONCAT(ur.numerador, ' - ', ur.denominador) OR t.setup = CONCAT(ur.denominador, ' - ', ur.numerador))
                    AND t.status = 'OPEN'
              )
          )
        ORDER BY ur.id ASC
    """)
    rows = dbSession.execute(sqlQuery).fetchall()
    activeList = []
    for r in rows:
        cierreDiv = bool(r[10]) if len(r) > 10 and r[10] is not None else True
        activeList.append({
            "id": r[0],
            "idUsuario": r[1],
            "idCuenta": r[2],
            "numerador": str(r[3]),
            "denominador": str(r[4]),
            "Temporalidad": str(r[5]) if r[5] else "1d",
            "periodo": int(r[6]) if r[6] else 180,
            "dias": int(r[6]) if r[6] else 180,
            "EMARapida": int(r[7]) if r[7] else 2,
            "EMALenta": int(r[8]) if r[8] else 20,
            "operar": bool(r[9]),
            "cierreDivergencia": cierreDiv,
            "tipoEntrada": str(r[11]) if len(r) > 11 and r[11] else "Selectiva",
            "fixedMinA": float(r[12]) if len(r) > 12 and r[12] is not None else None,
            "fixedMaxA": float(r[13]) if len(r) > 13 and r[13] is not None else None,
            "fixedMinB": float(r[14]) if len(r) > 14 and r[14] is not None else None,
            "fixedMaxB": float(r[15]) if len(r) > 15 and r[15] is not None else None,
            "accionCierre": str(r[16]) if len(r) > 16 and r[16] else "Continua",
            "tipoCierre": str(r[17]) if len(r) > 17 and r[17] else "Diferencia",
            "umbralDiferencia": float(r[18]) if len(r) > 18 and r[18] is not None else 0.2,
            "salirBreakeven": bool(r[19]) if len(r) > 19 and r[19] is not None else False
        })
    return activeList


def fetchSymbolData(dbSession, symbol: str) -> Dict[str, Any]:
    """
    Obtiene los parámetros operativos de un símbolo en la tabla 'symbols', incluyendo multiplo.
    """
    sqlQuery = text("""
        SELECT min_lots, margen, pip, quote_currency, multiplo
        FROM symbols
        WHERE symbol = :sym
        LIMIT 1
    """)
    row = dbSession.execute(sqlQuery, {"sym": symbol}).fetchone()
    if row:
        margenRaw = float(row[1]) if row[1] is not None else 0.25
        margenRate = (margenRaw / 100.0) if margenRaw >= 0.05 else margenRaw
        multiploVal = int(row[4]) if (len(row) > 4 and row[4] is not None and int(row[4]) > 0) else None
        return {
            "minLots": float(row[0]) if row[0] is not None else 1000.0,
            "margenRate": margenRate,
            "margenPct": margenRaw,
            "pip": float(row[2]) if row[2] is not None else 0.0001,
            "quoteCurrency": str(row[3]) if row[3] else "USD",
            "multiplo": multiploVal
        }
    return {
        "minLots": 1000.0,
        "margenRate": 0.0025,
        "margenPct": 0.25,
        "pip": 0.0001,
        "quoteCurrency": "USD",
        "multiplo": None
    }


def fetchAccountData(dbSession, idCuenta: int) -> Dict[str, Any]:
    """
    Obtiene el balance de capital actual, nombre, riesgo por operación, comisión y concentradora de la cuenta.
    """
    sqlQuery = text("""
        SELECT idCuenta, Nombre, Capital, Activo, riesgoPorOperacion, comision, Concentradora, COALESCE(indicadorMargen, 200.0)
        FROM cuenta
        WHERE idCuenta = :idc
        LIMIT 1
    """)
    row = dbSession.execute(sqlQuery, {"idc": idCuenta}).fetchone()
    if row:
        riesgoVal = float(row[4]) if (len(row) > 4 and row[4] is not None and float(row[4]) > 0) else 3.0
        comisionVal = float(row[5]) if (len(row) > 5 and row[5] is not None and float(row[5]) >= 0) else 0.0
        concentradoraVal = bool(row[6]) if (len(row) > 6 and row[6] is not None) else False
        indMargenVal = float(row[7]) if (len(row) > 7 and row[7] is not None) else 200.0
        return {
            "idCuenta": row[0],
            "nombre": str(row[1]),
            "capital": float(row[2]) if row[2] is not None else 0.0,
            "activo": bool(row[3]),
            "riesgoPorOperacion": riesgoVal,
            "comision": comisionVal,
            "concentradora": concentradoraVal,
            "indicadorMargen": indMargenVal
        }
    return {
        "idCuenta": idCuenta,
        "nombre": f"Cuenta #{idCuenta}",
        "capital": 0.0,
        "activo": False,
        "riesgoPorOperacion": 3.0,
        "comision": 0.0,
        "concentradora": False
    }


def fetchPriceHistory(dbSession, symbol: str, daysLimit: int = 365, timeframe: str = "1d") -> pd.DataFrame:
    """
    Carga el historial de precios: si es intradía (1h, 4h, etc.), consulta 'candles' (5min).
    Si es diario (1d, 1w), consulta 'stockprices'.
    """
    pLower = str(timeframe).lower().strip()
    isIntraday = pLower in ["1h", "4h", "15min", "15m", "30min", "30m", "5min", "5m"]
    if isIntraday:
        cutoff = datetime.now() - timedelta(days=daysLimit)
        sqlQuery = text("""
            SELECT timestamp as priceDate, close as closePrice
            FROM candles
            WHERE symbol = :sym AND timeframe = '5min' AND timestamp >= :cutoff
            ORDER BY timestamp ASC
        """)
        rows = dbSession.execute(sqlQuery, {"sym": symbol, "cutoff": cutoff}).fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=["priceDate", "closePrice"])
            df["priceDate"] = pd.to_datetime(df["priceDate"])
            df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce")
            return df.dropna().drop_duplicates(subset=["priceDate"]).sort_values("priceDate").set_index("priceDate")

    sqlQuery = text("""
        SELECT priceDate, closePrice
        FROM stockprices
        WHERE symbol = :sym
        ORDER BY priceDate ASC
    """)
    rows = dbSession.execute(sqlQuery, {"sym": symbol}).fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=["priceDate", "closePrice"])
    df["priceDate"] = pd.to_datetime(df["priceDate"])
    df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce")
    df = df.dropna().drop_duplicates(subset=["priceDate"]).sort_values("priceDate").set_index("priceDate")
    return df


def resamplePriceData(df: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """
    Aplica el resample adecuado según la temporalidad configurada en user_ratios.
    """
    if df.empty:
        return df

    ruleMap = {
        "1d": "D",
        "1D": "D",
        "1h": "1h",
        "1H": "1h",
        "4h": "4h",
        "4H": "4h",
        "15min": "15min",
        "15m": "15min",
        "30min": "30min",
        "30m": "30min",
        "1week": "W",
        "1W": "W",
        "1month": "ME",
        "1M": "ME"
    }
    rule = ruleMap.get(timeframe, None)
    if rule:
        dfResampled = df.resample(rule).agg({"closePrice": "last"}).dropna()
        if len(dfResampled) > 1:
            return dfResampled
    return df


def parseCandleDateTime(dtVal: Any) -> datetime:
    """Parsea de forma robusta la fecha y hora de la vela preservando horas y minutos."""
    s = str(dtVal).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            pass
    return datetime.now()


def isCandleSignalFresh(candleDt: datetime, timeframe: str) -> bool:
    """
    Valida estrictamente que la señal de entrada ocurra ÚNICAMENTE en su ventana inicial:
    - 1h: Máximo 15 minutos (900s + 60s tolerancia) desde la apertura de la vela.
          Si ya pasaron más de 15 minutos de la hora, la entrada se descarta para evitar entrar tarde con el precio ya corrido.
    - 4h: Máximo 30 minutos desde la apertura.
    - 30m: Máximo 7.5 minutos desde la apertura.
    - 15m: Máximo 5 minutos desde la apertura.
    - 1d: Solo en las primeras 4 horas del día.
    """
    tf = str(timeframe).lower().strip()
    now = datetime.now()
    deltaSecs = (now - candleDt).total_seconds()

    if deltaSecs < 0:
        return abs(deltaSecs) <= 180  # Tolerancia máxima de 3 min por desfase leve de reloj

    if tf in ["1h", "1H"]:
        return deltaSecs <= 960  # Máximo 15 minutos (+60s de margen para ciclo) desde la apertura de la vela
    elif tf in ["4h", "4H"]:
        return deltaSecs <= 1860  # Máximo 30 minutos desde apertura
    elif tf in ["15min", "15m"]:
        return deltaSecs <= 360   # Máximo 5 minutos
    elif tf in ["30min", "30m"]:
        return deltaSecs <= 540   # Máximo 7.5 minutos
    elif tf in ["1d", "1D"]:
        return deltaSecs <= (4 * 3600) # Primeras 4h del día
    elif tf in ["1week", "1w", "1W"]:
        return deltaSecs <= 2 * 86400
    elif tf in ["1month", "1m", "1M"]:
        return deltaSecs <= 7 * 86400

    return deltaSecs <= 960

def checkTradeExistsForCandle(dbSession, idCuenta: int, setupName: str, candleDt: datetime, timeframe: str = "1d") -> bool:
    """
    Verifica si ya existe un trade abierto para el mismo periodo según la temporalidad:
    - En horas (1h, 4h, 15m): busca coincidencia exacta de fecha y hora (candleTime = :cdt).
    - En días (1d): busca coincidencia por día (DATE(candleTime) = DATE(:cdt)).
    - En semanas (1w): busca coincidencia por semana (YEARWEEK(candleTime, 1) = YEARWEEK(:cdt, 1)).
    - En meses (1m): busca coincidencia por mes (YEAR/MONTH).
    """
    tf = str(timeframe).lower().strip()
    if tf in ["1h", "4h", "15min", "15m", "30min", "30m", "5min", "5m"]:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND setup = :stp
              AND candleTime = :cdt
        """)
    elif tf in ["1week", "1w"]:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND setup = :stp
              AND YEARWEEK(candleTime, 1) = YEARWEEK(:cdt, 1)
        """)
    elif tf in ["1month", "1m"]:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND setup = :stp
              AND YEAR(candleTime) = YEAR(:cdt)
              AND MONTH(candleTime) = MONTH(:cdt)
        """)
    else:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND setup = :stp
              AND DATE(candleTime) = DATE(:cdt)
        """)
    cnt = dbSession.execute(sqlQuery, {"idc": idCuenta, "stp": setupName, "cdt": candleDt}).scalar()
    return bool(cnt and cnt > 0)


def checkAccountMarginHealth(dbSession, idCuenta: int, accountCapital: float) -> Tuple[bool, float, float, float]:
    """
    Evalúa la salud de margen de la cuenta considerando todas las operaciones abiertas.
    Calcula:
    - totalMargin: sumatoria de margen retenido de todos los trades OPEN
    - totalFloatingPnl: sumatoria de PnL no realizado de todos los trades OPEN
    - equidad: accountCapital + totalFloatingPnl
    - marginIndicator: (equidad / totalMargin) * 100.0 si totalMargin > 0 sino 9999.0
    
    Regla: Permite operar si marginIndicator >= 200.0 (o si no hay margen retenido).
    Retorna: (canOperate: bool, marginIndicator: float, totalMargin: float, equidad: float)
    """
    sqlTrades = text("""
        SELECT t.idTrade, t.symbol, t.direction, t.size, t.entryPrice, t.margin_used,
               s.pip, s.quote_currency
        FROM trades t
        LEFT JOIN symbols s ON t.symbol = s.symbol
        WHERE t.idCuenta = :idc AND t.status = 'OPEN'
    """)
    rows = dbSession.execute(sqlTrades, {"idc": idCuenta}).fetchall()
    
    if not rows:
        return True, 9999.0, 0.0, accountCapital

    totalMargin = 0.0
    totalFloatingPnl = 0.0
    symbolPrices = {}

    for r in rows:
        sym = str(r[1])
        direction = str(r[2]).upper()
        size = float(r[3])
        entryPx = float(r[4])
        marginUsed = float(r[5]) if r[5] is not None else 0.0
        pipVal = float(r[6]) if r[6] is not None and float(r[6]) > 0 else 0.0001
        quoteCurr = str(r[7]) if r[7] else "USD"

        totalMargin += marginUsed

        if sym not in symbolPrices:
            sqlPx = text("""
                SELECT closePrice 
                FROM stockprices 
                WHERE symbol = :sym 
                ORDER BY priceDate DESC 
                LIMIT 1
            """)
            pxRow = dbSession.execute(sqlPx, {"sym": sym}).fetchone()
            symbolPrices[sym] = float(pxRow[0]) if pxRow and pxRow[0] is not None else entryPx

        currentPx = symbolPrices[sym]

        # Cálculo unificado de PnL no realizado institucional (sin costos artificiales)
        tradePnl, _pips, _pipMoneyValue, _mov = calculate_unrealized_pnl(
            symbol=sym,
            direction=direction,
            entry_price=entryPx,
            current_price=currentPx,
            size=size,
            pip_size=pipVal
        )
        totalFloatingPnl += tradePnl

    equity = accountCapital + totalFloatingPnl
    marginIndicator = (equity / totalMargin * 100.0) if totalMargin > 0 else 9999.0
    
    # Consulta de indicador de margen personalizado para esta cuenta
    sqlIndMargen = text("SELECT COALESCE(indicadorMargen, 200.0) FROM cuenta WHERE idCuenta = :idc")
    rowInd = dbSession.execute(sqlIndMargen, {"idc": idCuenta}).fetchone()
    minMarginRequired = float(rowInd[0]) if rowInd and rowInd[0] is not None else 200.0
    canOperate = bool(marginIndicator >= minMarginRequired)

    return canOperate, marginIndicator, totalMargin, equity


def checkActiveOpenTrades(dbSession, idCuenta: int, setupName: str) -> List[Dict[str, Any]]:
    """
    Verifica si existen operaciones actualmente en estado 'OPEN' para este setup y cuenta,
    incluyendo su ticketId y strategy para la gestión de órdenes en bróker y coberturas.
    """
    sqlQuery = text("""
        SELECT idTrade, symbol, direction, size, entryPrice, margin_used, candleTime, openTime, ticketId, strategy
        FROM trades
        WHERE idCuenta = :idc
          AND setup = :stp
          AND status = 'OPEN'
        ORDER BY idTrade ASC
    """)
    rows = dbSession.execute(sqlQuery, {"idc": idCuenta, "stp": setupName}).fetchall()
    openTrades = []
    for r in rows:
        openTrades.append({
            "idTrade": r[0],
            "symbol": str(r[1]),
            "direction": str(r[2]),
            "size": float(r[3]),
            "entryPrice": float(r[4]),
            "margin_used": float(r[5]) if r[5] is not None else 0.0,
            "candleTime": r[6],
            "openTime": r[7],
            "ticketId": str(r[8]) if r[8] else None,
            "strategy": str(r[9]) if len(r) > 9 and r[9] else "RATIO ATALAia"
        })
    return openTrades


def buildRatioLotEntryAlertMessage(
    accountName: str,
    numerador: str,
    denominador: str,
    periodo: str,
    symbol: str,
    direction: str,
    size: float,
    fillPrice: float,
    ticketId: str,
    chunkMargin: float,
    currentLotIndex: int,
    totalLots: int,
    signalType: str,
    accountCapitalRemaining: float
) -> str:
    """Construye el mensaje formal de Telegram para un lote confirmado en FOREX.com."""
    nowStr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dirBadge = "🟢 COMPRA" if ("LARG" in str(direction).upper() or "BUY" in str(direction).upper()) else "🔴 VENTA"

    lines = [
        f"<center>🟩 <b>ATALA.ia ORDEN EJECUTADA</b> 🟩</center>",
        f"<center><b> {numerador} ⇄ {denominador} ({periodo.upper()}) </b></center>",
        f"<center><b> {accountName} </b></center>",
        f"<center>{nowStr}</center>",
        "━━━━━━━━━━━━━━━━━",
        f"🎯 <b>Señal:</b> {signalType} | <b>Lote {currentLotIndex}/{totalLots}</b>",
        f"📍 <b>Orden:</b> {dirBadge} <b>{symbol}</b>",
        f"🔹 <b>Precio Ejecución:</b> <b>{fillPrice:,.5f}</b>",
        f"📦 <b>Volumen:</b> <b>{size:,.0f} unidades</b>",
        f"🎟️ <b>Ticket ID (Broker):</b> <code>{ticketId}</code>",
        f"💼 <b>Margen Retenido:</b> <b>${chunkMargin:,.2f} USD</b>",
        f"💰 <b>Capital Restante:</b> <b>${accountCapitalRemaining:,.2f} USD</b>",
        "━━━━━━━━━━━━━━━━━"
    ]
    return "\n".join(lines)


def buildRatioEntryAlertMessage(
    accountName: str,
    accountCapital: float,
    numerador: str,
    denominador: str,
    periodo: str,
    signalType: str,
    directionA: str,
    directionB: str,
    unitsA: float,
    unitsB: float,
    priceA: float,
    priceB: float,
    marginA: float,
    marginB: float,
    totalMargin: float,
    entryDate: str
) -> str:
    """Construye el mensaje formal de entrada para Telegram."""
    nowStr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entryDateDisplay = entryDate if entryDate else nowStr[:10]
    dirBadgeA = "🟢 COMPRA" if directionA == "LARGO" else "🔴 VENTA"
    dirBadgeB = "🟢 COMPRA" if directionB == "LARGO" else "🔴 VENTA"
      

    lines = [
        f"<center>🟩🟥 <b>ATALA.ia CRUCE EMA</b> 🟥🟩</center>",
        f"<center><b> {numerador} ⇄ {denominador} ({periodo.upper()}) </b></center>",
        f"<center><b> {accountName} </b></center>",
        f"<center>{nowStr}</center>",
        "━━━━━━━━━━━━━━━━━",   
        f"<center>Fecha del cruce: <b>{entryDateDisplay}</b></center>",
        "",   
        f"📍 <b>PATA 1 {dirBadgeA} {numerador}</b>",
        f"   🔹 Entrada: <b>{priceA:,.5f}</b>",
        f"   📦 Volumen: <b>{unitsA:,.0f} lotes</b>",
        f"   💼 Margen:  <b>${marginA:,.2f} USD</b>",
        "",
        f"📍 <b>PATA 2 {dirBadgeB} {denominador}</b>",
        f"   🔹 Entrada: <b>{priceB:,.5f}</b>",
        f"   📦 Volumen: <b>{unitsB:,.0f} lotes</b>",
        f"   💼 Margen:  <b>${marginB:,.2f} USD</b>",
        "━━━━━━━━━━━━━━━━━",
        f" Margen total: <b>${totalMargin:,.2f} USD</b>",
        "━━━━━━━━━━━━━━━━━"
    ]
    return "\n".join(lines)


def buildRatioExitAlertMessage(
    accountName: str,
    newAccountCapital: float,
    numerador: str,
    denominador: str,
    periodo: str,
    pnlA: float,
    pnlB: float,
    pipsA: float,
    pipsB: float,
    dirA: str,
    dirB: str,
    totalNetPnl: float,
    totalMarginReleased: float,
    closeReason: str = "CONVERGENCIA"
) -> str:
    """Construye el mensaje de liquidación por cruce de precios o solicitud manual para Telegram."""
    pnlSign = "+" if totalNetPnl >= 0 else ""
    retPct = ((totalNetPnl / totalMarginReleased) * 100.0) if totalMarginReleased > 0 else 0.0
    retSign = "+" if retPct >= 0 else ""
    nowStr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if "TRAILING" in closeReason.upper():
        header_reason = "POR TRAILING PROFIT"
        body_reason = closeReason
    elif "DIFERENCIAL" in closeReason.upper() or "DIF NORM" in closeReason.upper():
        header_reason = "POR DIFERENCIAL NORMALIZADO"
        body_reason = closeReason
    elif "CICLO" in closeReason.upper():
        header_reason = "POR LÍMITE DE CICLOS"
        body_reason = closeReason
    elif "TAKE PROFIT" in closeReason.upper():
        header_reason = "POR TAKE PROFIT DE CANASTA"
        body_reason = closeReason
    elif "BREAK-EVEN" in closeReason.upper():
        header_reason = "POR BREAK-EVEN PROTEGIDO"
        body_reason = closeReason
    elif closeReason == "POR SOLICITUD":
        header_reason = "POR SOLICITUD"
        body_reason = "POR SOLICITUD"
    elif "CAMBIO" in closeReason.upper():
        header_reason = "POR GIRO DE DIRECCIÓN"
        body_reason = closeReason
    elif "CRUCE" in closeReason.upper():
        header_reason = "POR CRUCE DE PRECIOS"
        body_reason = "POR CONVERGENCIA (●)"
    else:
        header_reason = f"POR {closeReason.upper()}"
        body_reason = closeReason

    lines = [
        f"<center>🟥<b>ATALA.ia CIERRE {header_reason}</b>🟥</center>",
        f"<center><b>   {numerador} ⇄ {denominador} ({periodo.upper()})</b></center>",
        f"<center><b>   {accountName} </b></center>",
        f"<center>{nowStr}</center>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"<center> <b>LIQUIDACIÓN {body_reason}:</b></center>",
        f"   <b>{numerador} ({dirA}):</b> {pipsA:+,.1f} pips  |  <b>{pnlA:+,.2f} USD</b>",
        f"   <b>{denominador} ({dirB}):</b> {pipsB:+,.1f} pips  |  <b>{pnlB:+,.2f} USD</b>",
        "━━━━━━━━━━━━━━━━━━━━",
        f"💵 <b>PnL NETO TOTAL:       {pnlSign}${totalNetPnl:,.2f} USD ({retSign}{retPct:,.1f}%)</b>",
        f"💼 Margen Liberado:       <b>${totalMarginReleased:,.2f} USD</b>",
        f"💰 Capital:  <b>${newAccountCapital:,.2f} USD</b>",
        "━━━━━━━━━━━━━━━━━━━━"
    ]
    return "\n".join(lines)


async def sendRatioTelegramAlert(idCuenta: int, messageText: str):
    """Envía la alerta por Telegram utilizando la infraestructura de communications.py."""
    try:
        await alertaInmediata(idCuenta, messageText, prioridad=True)
        logger.info(f"📱 Alerta de Telegram despachada exitosamente para Cuenta #{idCuenta}.")
    except Exception as exTel:
        logger.error(f"⚠️ Error enviando alerta de Telegram para Cuenta #{idCuenta}: {exTel}")


def dispatchRatioTelegramAlert(idCuenta: int, messageText: str):
    """Despacha la corrutina de alerta de Telegram de manera segura con o sin event loop activo."""
    try:
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            asyncio.ensure_future(sendRatioTelegramAlert(idCuenta, messageText), loop=loop)
        else:
            asyncio.run(sendRatioTelegramAlert(idCuenta, messageText))
    except Exception as ex:
        logger.error(f"Error despachando alerta Telegram para Cuenta #{idCuenta}: {ex}")


def sendRatioWebhookOrders(dbSession, idCuenta: int, orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Envía órdenes de ejecución/cierre al Webhook del bróker para todas las cuentas
    activas en 'brokercuenta' asociadas a idCuenta.
    Retorna la lista con los resultados de ejecución real (order_id, fill_price, idTrade).
    """
    executionResults = []
    try:
        query = text("""
            SELECT bc.idBrokerCuenta, bc.idCuenta, bc.loginUsuario, bc.tokenAcceso, bc.activo, b.nombre AS nombreBroker
            FROM brokercuenta bc
            LEFT JOIN broker b ON bc.idBroker = b.idBroker
            WHERE bc.idCuenta = :idc AND bc.activo = 1
        """)
        activeBrokers = dbSession.execute(query, {"idc": idCuenta}).mappings().fetchall()
        if not activeBrokers:
            logger.info(f"ℹ️ [Webhook] No hay cuentas de bróker activas en brokercuenta para Cuenta #{idCuenta}. Se omite envío a Webhook.")
            return executionResults

        import os, time, json, requests
        from middleware.utils.cryptoUtils import buildEncryptedAccountToken, encryptValue
        
        webhookUrl = os.getenv("WEBHOOK_URL") or "http://127.0.0.1:8002/webhook/tradingview"
        rawPassphrase = os.getenv("WEBHOOK_VERIFY_TOKEN", "")
        encryptedPassphrase = encryptValue(rawPassphrase) if rawPassphrase else ""

        for bc in activeBrokers:
            loginUsuario = bc.get("loginUsuario", "")
            tokenAcceso = bc.get("tokenAcceso", "")
            nombreBroker = bc.get("nombreBroker", "Broker")

            encryptedAccountToken = buildEncryptedAccountToken(
                idCuenta=idCuenta,
                loginUsuario=loginUsuario,
                tokenAcceso=tokenAcceso
            )

            for idx, ordInfo in enumerate(orders):
                if idx > 0:
                    logger.info(f"⏳ [Webhook] Pausando 5.0s antes de enviar la siguiente orden ({ordInfo.get('symbol')}) para respetar candado anti-spam...")
                    time.sleep(5.0)

                symbol = ordInfo.get("symbol")
                action = ordInfo.get("action")
                size = float(ordInfo.get("size", 0.0))
                entryPx = float(ordInfo.get("entryPrice", 0.0))
                strategy = ordInfo.get("strategy", "RATIO ATALAia")
                ticketId = ordInfo.get("ticketId")
                idTrade = ordInfo.get("idTrade")

                orderPayload = {
                    "strategy": strategy,
                    "setup": ordInfo.get("setup"),
                    "passphrase": encryptedPassphrase,
                    "time": time.time(),
                    "action": action,
                    "ticker": symbol,
                    "entry": entryPx,
                    "quantity": size,
                    "tp": 0.0,
                    "sl": 0.0,
                    "sync": True,
                    "ticketId": ticketId,
                    "idTrade": idTrade,
                    "FOREX_USERNAME": encryptedAccountToken
                }

                logger.info(f"🌐 [Webhook] Enviando orden {action.upper()} {symbol} ({size:,.0f} lotes) para cuenta {loginUsuario} ({nombreBroker})...")
                
                # Si patas anteriores ya cerraron pero esta falla, permitir reintentos automáticos para no dejar el ratio cojo
                hasPriorSuccess = any(r.get("success") for r in executionResults)
                maxAttempts = 3 if (action == "close" and hasPriorSuccess) else 1
                isSuccess = False
                resData = {}
                errMsg = ""
                
                for attempt in range(1, maxAttempts + 1):
                    if attempt > 1:
                        logger.warning(f"🔄 [REINTENTO CIERRE {attempt}/{maxAttempts}] Reintentando {action.upper()} {symbol} en FOREX.com para no dejar el ratio cojo...")
                        time.sleep(3.0)
                    try:
                        resp = requests.post(webhookUrl, json=orderPayload, timeout=35)
                        if resp.status_code == 200:
                            resData = resp.json() if resp.text else {}
                            isSuccess = (resData.get("status") == "success") or (resData.get("success") is True)
                            if isSuccess:
                                break
                            errMsg = resData.get("message") or resData.get("error") or "Orden rechazada por el broker"
                        else:
                            errMsg = f"Error HTTP {resp.status_code}: {resp.text}"
                    except Exception as exReq:
                        errMsg = f"Excepción de conexión: {exReq}"
                
                if not isSuccess:
                    logger.error(f"❌ [Webhook] Orden {action.upper()} {symbol} RECHAZADA por FOREX.com: {errMsg}")
                    executionResults.append({
                        "idTrade": idTrade,
                        "symbol": symbol,
                        "action": action,
                        "success": False,
                        "error": errMsg
                    })
                    # TRANSACCIÓN A NIVEL DE RATIO: Si la primera pata fue rechazada y no hay patas previas cerradas,
                    # abortar el envío de las patas restantes para no desbalancear ni dejar el ratio cojo
                    if not hasPriorSuccess:
                        logger.error(f"🛑 [ABORTAR RATIO] La pata {symbol} fue rechazada por el broker. Abortando patas restantes del ratio para evitar operación coja.")
                        break
                else:
                    orderId = resData.get("order_id")
                    fillPrice = resData.get("fill_price") or entryPx
                    logger.info(f"✅ [Webhook] Orden {action.upper()} {symbol} confirmada en FOREX.com -> OrderId: {orderId}, FillPrice: {fillPrice}")
                    executionResults.append({
                        "idTrade": idTrade,
                        "symbol": symbol,
                        "action": action,
                        "order_id": orderId,
                        "fill_price": fillPrice,
                        "success": True
                    })
    except Exception as e:
        logger.error(f"⚠️ Error general en despacho de Webhook para Cuenta #{idCuenta}: {e}", exc_info=True)
    return executionResults


def openSingleRatioTradePair(
    dbSession,
    idCuenta: int,
    setupName: str,
    numerador: str,
    denominador: str,
    periodo: str,
    candleDateStr: str,
    signalType: str,
    directionA: str,
    directionB: str,
    entryPxA: float,
    entryPxB: float,
    accountData: Dict[str, Any],
    symDataA: Dict[str, Any],
    symDataB: Dict[str, Any]
) -> bool:
    """
    Ejecuta el ciclo de apertura de órdenes:
    1. Se calcula el dimensionamiento y fragmentación de lotes.
    2. Si la cuenta tiene broker activo en brokercuenta:
       - Se envía a FOREX.com lote por lote (con 5s de pausa).
       - Se confirma con el broker.
       - Si hay confirmación -> se inserta en trades con TicketID real, se descuenta margen y se alerta por Telegram.
       - Si no hay confirmación -> NO se inserta en BD.
    3. Si la cuenta NO tiene broker activo en brokercuenta:
       - NO OPERA: Se cancela la apertura y no se registra ninguna operación.
    """
    import requests
    from middleware.utils.cryptoUtils import buildEncryptedAccountToken, encryptValue

    accountCapital = float(accountData.get("capital", 300.0))
    accountName = str(accountData.get("nombre", f"Cuenta #{idCuenta}"))

    # Porcentaje de riesgo dinámico tomado de cuenta.riesgoPorOperacion (ej. 3.0 -> 3.0% del capital)
    riesgoPct = float(accountData.get("riesgoPorOperacion", 3.0))
    allocationRate = (riesgoPct / 100.0) if riesgoPct >= 0.05 else riesgoPct
    if allocationRate <= 0:
        allocationRate = 0.03

    budget = max(0.0, accountCapital) * allocationRate
    budgetA = budget / 2.0
    budgetB = budget / 2.0

    minLotsA = float(symDataA.get("minLots", 1000.0))
    minLotsB = float(symDataB.get("minLots", 1000.0))
    margenRateA = float(symDataA.get("margenRate", 0.0025))
    margenRateB = float(symDataB.get("margenRate", 0.0025))

    margen1LotA = calculate_margin(numerador, minLotsA, entryPxA, symDataA.get("margenPct", 0.5))
    margen1LotB = calculate_margin(denominador, minLotsB, entryPxB, symDataB.get("margenPct", 0.5))

    multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA and margen1LotA > 0) else 1
    multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB and margen1LotB > 0) else 1

    unitsA = multA * minLotsA
    unitsB = multB * minLotsB

    candleDt = parseCandleDateTime(candleDateStr)

    nowStr = datetime.now()

    # Fragmentar unidades según symbols.multiplo para evitar incrementos de margen escalonado
    multiploA = symDataA.get("multiplo")
    chunksA = []
    if multiploA and multiploA > 0 and unitsA > multiploA:
        remA = float(unitsA)
        while remA > 0:
            cA = min(remA, float(multiploA))
            chunksA.append(cA)
            remA -= cA
    else:
        chunksA = [float(unitsA)]

    multiploB = symDataB.get("multiplo")
    chunksB = []
    if multiploB and multiploB > 0 and unitsB > multiploB:
        remB = float(unitsB)
        while remB > 0:
            cB = min(remB, float(multiploB))
            chunksB.append(cB)
            remB -= cB
    else:
        chunksB = [float(unitsB)]

    # Construir lista ordenada de todos los lotes a despachar
    lotsToExecute = []
    for cA in chunksA:
        lotsToExecute.append({
            "symbol": numerador,
            "direction": directionA,
            "action": "buy" if directionA == "LARGO" else "sell",
            "size": cA,
            "entryPrice": entryPxA,
            "margenRate": margenRateA,
            "margenPct": symDataA.get("margenPct", 0.5),
            "leg": "1"
        })
    for cB in chunksB:
        lotsToExecute.append({
            "symbol": denominador,
            "direction": directionB,
            "action": "buy" if directionB == "LARGO" else "sell",
            "size": cB,
            "entryPrice": entryPxB,
            "margenRate": margenRateB,
            "margenPct": symDataB.get("margenPct", 0.5),
            "leg": "2"
        })

    # Consultar brokers activos para la cuenta
    queryBrokers = text("""
        SELECT bc.idBrokerCuenta, bc.idCuenta, bc.loginUsuario, bc.tokenAcceso, bc.activo, b.nombre AS nombreBroker
        FROM brokercuenta bc
        LEFT JOIN broker b ON bc.idBroker = b.idBroker
        WHERE bc.idCuenta = :idc AND bc.activo = 1
    """)
    activeBrokers = dbSession.execute(queryBrokers, {"idc": idCuenta}).mappings().fetchall()

    webhookUrl = os.getenv("WEBHOOK_URL") or "http://127.0.0.1:8002/webhook/tradingview"
    rawPassphrase = os.getenv("WEBHOOK_VERIFY_TOKEN", "")
    encryptedPassphrase = encryptValue(rawPassphrase) if rawPassphrase else ""

    insertTradeSql = text("""
        INSERT INTO trades (
            idCuenta, strategy, setup, symbol, status, direction,
            intervalo, pnl, candleTime, openTime, size, entryPrice,
            stopLoss, takeProfit, margin_used, ticketId
        ) VALUES (
            :idCuenta, 'RATIO ATALAia', :setup, :symbol, 'OPEN', :direction,
            :intervalo, 0.0, :candleTime, :openTime, :size, :entryPrice,
            NULL, NULL, :margin_used, :ticketId
        )
    """)

    totalLotsCount = len(lotsToExecute)
    insertedTrades = []

    logger.info(f"🧩 [{setupName}] Iniciando secuencia de {totalLotsCount} lote(s) para Cuenta #{idCuenta} ({accountName})...")

    # CASO 1: Cuenta con Broker Conectado (FOREX.com / Webhook) - EJECUCIÓN TRANSACCIONAL ESTRICTA
    if activeBrokers:
        confirmedBrokerLots = []
        allConfirmed = True

        for idx, lot in enumerate(lotsToExecute):
            if idx > 0:
                logger.info(f"⏳ [FOREX] Esperando 5.0s antes de despachar lote {idx+1}/{totalLotsCount} ({lot['symbol']})...")
                time.sleep(5.0)

            sym = lot["symbol"]
            act = lot["action"]
            dirStr = lot["direction"]
            sz = float(lot["size"])
            px = float(lot["entryPrice"])
            rate = float(lot["margenRate"])
            chunkMargin = calculate_margin(sym, sz, px, lot.get("margenPct", 0.5))

            lotConfirmedForAccount = False
            for bc in activeBrokers:
                loginUsuario = bc.get("loginUsuario", "")
                tokenAcceso = bc.get("tokenAcceso", "")
                nombreBroker = bc.get("nombreBroker", "Broker")

                encryptedAccountToken = buildEncryptedAccountToken(
                    idCuenta=idCuenta,
                    loginUsuario=loginUsuario,
                    tokenAcceso=tokenAcceso
                )

                orderPayload = {
                    "strategy": "RATIO ATALAia",
                    "setup": setupName,
                    "passphrase": encryptedPassphrase,
                    "time": time.time(),
                    "action": act,
                    "ticker": sym,
                    "entry": px,
                    "quantity": sz,
                    "tp": 0.0,
                    "sl": 0.0,
                    "sync": True,
                    "ticketId": None,
                    "FOREX_USERNAME": encryptedAccountToken
                }

                logger.info(f"🌐 [Lote {idx+1}/{totalLotsCount}] Enviando a FOREX.com: {act.upper()} {sym} ({sz:,.0f} u) para {loginUsuario} ({nombreBroker})...")

                isConfirmed = False
                orderId = None
                fillPrice = px

                try:
                    resp = requests.post(webhookUrl, json=orderPayload, timeout=35)
                    if resp.status_code == 200:
                        resData = resp.json() if resp.text else {}
                        if (resData.get("status") == "success" or resData.get("success") is True) and resData.get("order_id"):
                            isConfirmed = True
                            orderId = str(resData.get("order_id"))
                            fillPrice = float(resData.get("fill_price") or px)
                            realMargin = float(resData.get("margin") or chunkMargin)
                            chunkMargin = realMargin
                            logger.info(f"✅ [FOREX] Lote {idx+1}/{totalLotsCount} confirmado -> OrderId: {orderId}, FillPrice: {fillPrice}, Margen: {realMargin}")
                        elif resData.get("status") == "ignored":
                            logger.warning(f"⚠️ [FOREX] Orden ignorada por candado anti-spam: {resData.get('message')}")
                        else:
                            logger.error(f"❌ [FOREX] Error devuelto por FOREX.com: {resData}")
                    else:
                        logger.error(f"❌ [FOREX] Error HTTP {resp.status_code} al despachar lote: {resp.text}")
                except Exception as exReq:
                    logger.error(f"❌ [FOREX] Excepción de conexión enviando lote a {webhookUrl}: {exReq}")

                if isConfirmed and orderId:
                    confirmedBrokerLots.append({
                        "symbol": sym,
                        "direction": dirStr,
                        "size": sz,
                        "fillPrice": fillPrice,
                        "chunkMargin": chunkMargin,
                        "ticketId": orderId,
                        "loginUsuario": loginUsuario,
                        "encryptedAccountToken": encryptedAccountToken,
                        "lotIdx": idx + 1
                    })
                    lotConfirmedForAccount = True
                else:
                    logger.error(f"⛔ [FALLO FOREX] Lote {idx+1}/{totalLotsCount} ({act.upper()} {sym}, {sz:,.0f} u) NO confirmado por FOREX.com.")
                    allConfirmed = False
                    break

            if not allConfirmed or not lotConfirmedForAccount:
                allConfirmed = False
                break

        # EVALUACIÓN DE TRANSACCIONALIDAD:
        # Si falló cualquier lote, ejecutar ROLLBACK completo de las patas que sí se confirmaron
        if not allConfirmed or len(confirmedBrokerLots) < totalLotsCount:
            logger.error(f"🚨 [ROLLBACK TRANSACCIONAL] Secuencia para {setupName} INCOMPLETA ({len(confirmedBrokerLots)}/{totalLotsCount}). Ejecutando reversión/cierre de patas abiertas en FOREX.com...")
            
            for rbLot in confirmedBrokerLots:
                rbSym = rbLot["symbol"]
                rbTicket = rbLot["ticketId"]
                rbSz = rbLot["size"]
                rbToken = rbLot["encryptedAccountToken"]
                
                closePayload = {
                    "strategy": "RATIO ATALAia",
                    "passphrase": encryptedPassphrase,
                    "time": time.time(),
                    "action": "close",
                    "ticker": rbSym,
                    "entry": 0.0,
                    "quantity": rbSz,
                    "tp": 0.0,
                    "sl": 0.0,
                    "sync": True,
                    "ticketId": rbTicket,
                    "FOREX_USERNAME": rbToken
                }
                try:
                    logger.warning(f"🔄 [ROLLBACK FOREX] Cerrando lote previo {rbSym} (Ticket: {rbTicket}, {rbSz:,.0f} u) en FOREX.com...")
                    respRb = requests.post(webhookUrl, json=closePayload, timeout=25)
                    logger.info(f"🔄 [ROLLBACK FOREX] Respuesta de cierre para {rbTicket}: {respRb.status_code}")
                except Exception as exRb:
                    logger.error(f"❌ [ROLLBACK FOREX] Error cerrando lote {rbTicket} en FOREX.com: {exRb}")

            rollbackMsg = (
                f"🚨 <b>[ROLLBACK TRANSACCIONAL RATIO]</b>\n\n"
                f"Cuenta: <b>{accountName}</b> (#{idCuenta})\n"
                f"Ratio: <b>{setupName}</b>\n"
                f"Causa: Falló la confirmación en FOREX.com del lote {len(confirmedBrokerLots)+1}/{totalLotsCount}.\n"
                f"Acción: Se ejecutó cierre de emergencia para {len(confirmedBrokerLots)} lote(s) en Forex. La Base de Datos permanece intacta (sin posiciones huérfanas)."
            )
            try:
                dispatchRatioTelegramAlert(idCuenta, rollbackMsg)
            except Exception as exTel:
                logger.error(f"Error despachando alerta Telegram de rollback: {exTel}")
                
            return False

        # FASE 2: PERSISTENCIA ATÓMICA EN BASE DE DATOS (MYSQL)
        # Solo se ejecuta si TODOS los lotes del ratio están 100% confirmados por Forex
        try:
            for cLot in confirmedBrokerLots:
                resInsert = dbSession.execute(insertTradeSql, {
                    "idCuenta": idCuenta,
                    "setup": setupName,
                    "symbol": cLot["symbol"],
                    "direction": cLot["direction"],
                    "intervalo": periodo,
                    "candleTime": candleDt,
                    "openTime": nowStr,
                    "size": cLot["size"],
                    "entryPrice": cLot["fillPrice"],
                    "margin_used": cLot["chunkMargin"],
                    "ticketId": cLot["ticketId"]
                })
                tId = resInsert.lastrowid
                insertedTrades.append(tId)
                cLot["tId"] = tId

                dbSession.execute(text("""
                    UPDATE cuenta
                    SET Capital = Capital - :chunkMargin
                    WHERE idCuenta = :idc
                """), {"chunkMargin": cLot["chunkMargin"], "idc": idCuenta})

            dbSession.commit()
            logger.info(f"💾 [TRANSACCIÓN BD COMMIT] {len(insertedTrades)} trades de ratio guardados y capital actualizado en MySQL.")

            capRow = dbSession.execute(text("SELECT Capital FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).mappings().fetchone()
            curCapital = float(capRow["Capital"]) if capRow else accountCapital

            for cLot in confirmedBrokerLots:
                alertMsg = buildRatioLotEntryAlertMessage(
                    accountName=accountName,
                    numerador=numerador,
                    denominador=denominador,
                    periodo=periodo,
                    symbol=cLot["symbol"],
                    direction=cLot["direction"],
                    size=cLot["size"],
                    fillPrice=cLot["fillPrice"],
                    ticketId=cLot["ticketId"],
                    chunkMargin=cLot["chunkMargin"],
                    currentLotIndex=cLot["lotIdx"],
                    totalLots=totalLotsCount,
                    signalType=signalType,
                    accountCapitalRemaining=curCapital
                )
                try:
                    dispatchRatioTelegramAlert(idCuenta, alertMsg)
                except Exception as exTel:
                    logger.error(f"Error despachando alerta Telegram para lote #{cLot.get('tId')}: {exTel}")

        except Exception as exDb:
            dbSession.rollback()
            logger.error(f"❌ [TRANSACCIÓN BD ROLLBACK] Falló la inserción en MySQL. Rollback ejecutado: {exDb}", exc_info=True)
            dbFailMsg = (
                f"🚨 <b>[ALERTA CRÍTICA - ERROR BASE DE DATOS]</b>\n\n"
                f"Cuenta: <b>{accountName}</b> (#{idCuenta})\n"
                f"Ratio: <b>{setupName}</b>\n"
                f"Las órdenes fueron confirmadas en FOREX.com ({[c['ticketId'] for c in confirmedBrokerLots]}), pero falló el registro en MySQL. Error: {exDb}"
            )
            try:
                dispatchRatioTelegramAlert(idCuenta, dbFailMsg)
            except Exception:
                pass
            return False

    # CASO 2: Cuenta Sin Bróker activo en brokercuenta -> NO OPERA
    else:
        logger.warning(
            f"🚫 [SIN BRÓKER] Cuenta #{idCuenta} ({accountName}) no tiene bróker activo configurado en brokercuenta. "
            f"NO opera: No se abrirán operaciones ni se registrarán órdenes."
        )
        return False

    logger.info(f"🎯 [{setupName}] Secuencia de lotes finalizada para Cuenta #{idCuenta}. Total registros creados en BD: {len(insertedTrades)}/{totalLotsCount}.")
    return len(insertedTrades) > 0


TRAILING_STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "logs", "trailing_profit_state.json")

import json

def getTrailingProfitState(key: str) -> Dict[str, Any]:
    try:
        if os.path.exists(TRAILING_STATE_FILE):
            with open(TRAILING_STATE_FILE, "r") as f:
                data = json.load(f)
                return data.get(key, {})
    except Exception as e:
        logger.warning(f"Error al leer trailing state: {e}")
    return {}

def saveTrailingProfitState(key: str, state: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(TRAILING_STATE_FILE), exist_ok=True)
        data = {}
        if os.path.exists(TRAILING_STATE_FILE):
            with open(TRAILING_STATE_FILE, "r") as f:
                data = json.load(f)
        data[key] = state
        with open(TRAILING_STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Error al guardar trailing state: {e}")

def clearTrailingProfitState(key: str) -> None:
    try:
        if os.path.exists(TRAILING_STATE_FILE):
            with open(TRAILING_STATE_FILE, "r") as f:
                data = json.load(f)
            if key in data:
                del data[key]
                with open(TRAILING_STATE_FILE, "w") as f:
                    json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Error al limpiar trailing state: {e}")


COVERAGE_STATE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "logs", "coverage_state.json")

def getCoverageState(key: str) -> Dict[str, Any]:
    try:
        if os.path.exists(COVERAGE_STATE_FILE):
            with open(COVERAGE_STATE_FILE, "r") as f:
                data = json.load(f)
                return data.get(key, {})
    except Exception as e:
        logger.warning(f"Error al leer coverage state: {e}")
    return {}

def saveCoverageState(key: str, state: Dict[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(COVERAGE_STATE_FILE), exist_ok=True)
        data = {}
        if os.path.exists(COVERAGE_STATE_FILE):
            with open(COVERAGE_STATE_FILE, "r") as f:
                data = json.load(f)
        data[key] = state
        with open(COVERAGE_STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Error al guardar coverage state: {e}")

def clearCoverageState(key: str) -> None:
    try:
        if os.path.exists(COVERAGE_STATE_FILE):
            with open(COVERAGE_STATE_FILE, "r") as f:
                data = json.load(f)
            if key in data:
                del data[key]
                with open(COVERAGE_STATE_FILE, "w") as f:
                    json.dump(data, f, indent=2)
    except Exception as e:
        logger.warning(f"Error al limpiar coverage state: {e}")


def closeRatioTrades(
    dbSession,
    idCuenta: int,
    setupName: str,
    openTrades: List[Dict[str, Any]],
    symbolDataMap: Dict[str, Dict[str, Any]],
    latestPricesMap: Dict[str, float],
    accountData: Dict[str, Any],
    periodo: str = "1d",
    closeReason: str = "POR CRUCE DE PRECIOS"
) -> bool:
    """
    Cierra todas las operaciones abiertas del setup al ocurrir el cruce de precios (●) o rebalanceo,
    liquida el PnL neto por pips (sin comisión por trade, commission = 0.0),
    reincorpora el margen y la ganancia/pérdida a 'cuenta.Capital' y envía la alerta de cierre a Telegram y la orden de cierre al Webhook.
    """
    if not openTrades:
        return False

    import calendar
    nowDt = datetime.now()
    nowStr = nowDt
    totalMarginToRelease = 0.0
    totalNetPnl = 0.0
    totalCommissionCharged = 0.0
    pnlA, pnlB = 0.0, 0.0
    pipsA, pipsB = 0.0, 0.0
    dirA, dirB = "", ""
    numerador, denominador = setupName.split(" - ")[0], setupName.split(" - ")[1]

    # Comisión porcentual configurada en la cuenta (ej. 20.0 = 20%)
    comision_pct = float(accountData.get("comision", 0.0) or 0.0)

    # =========================================================================
    # FASE 1: DESPACHO PREVIO AL BROKER (VALIDACIÓN DE CIERRE REAL)
    # =========================================================================
    webhookCloseOrders = []
    hasBrokerPositions = False
    for tr in openTrades:
        tId = tr.get("ticketId")
        if tId and str(tId).strip() not in ["", "None", "0", "CLOSED"]:
            hasBrokerPositions = True
        sym = tr.get("symbol")
        sz = float(tr.get("size", 0.0) or 0.0)
        webhookCloseOrders.append({
            "strategy": tr.get("strategy") or "RATIO ATALAia",
            "setup": tr.get("setup") or setupName,
            "symbol": sym,
            "action": "close",
            "size": sz,
            "entryPrice": 0.0,
            "ticketId": tId,
            "idTrade": tr.get("idTrade")
        })

    brokerPricesMap = {}
    if hasBrokerPositions and webhookCloseOrders:
        logger.info(f"🔒 [closeRatioTrades] Despachando {len(webhookCloseOrders)} orden(es) de cierre al broker antes de alterar la BD...")
        execResults = sendRatioWebhookOrders(dbSession, idCuenta, webhookCloseOrders)
        failedResults = [r for r in execResults if not r.get("success")]
        if failedResults:
            errorDetails = ", ".join([f"{r.get('symbol')}: {r.get('error', 'Rechazado')}" for r in failedResults])
            logger.error(f"❌ [ABORTAR CIERRE] El broker rechazó el cierre de {setupName} en Cuenta #{idCuenta}: {errorDetails}. La Base de Datos permanece intacta.")
            return False
        for r in execResults:
            tId = r.get("idTrade")
            sym = r.get("symbol")
            fp = float(r.get("fill_price", 0.0) or 0.0)
            if fp > 0:
                if tId:
                    brokerPricesMap[tId] = fp
                if sym:
                    brokerPricesMap[sym] = fp

    # Identificar la cuenta concentradora del usuario que administra la cuenta
    idCuentaConcentradora = None
    try:
        userRow = dbSession.execute(text("""
            SELECT idUsuario FROM usuarioCuenta 
            WHERE idCuenta = :idc AND activo = 1 
            ORDER BY idUsuarioCuenta ASC LIMIT 1
        """), {"idc": idCuenta}).fetchone()
        idUsuario = userRow[0] if userRow else None

        if idUsuario:
            concAccRow = dbSession.execute(text("""
                SELECT c.idCuenta FROM cuenta c
                JOIN usuarioCuenta uc ON c.idCuenta = uc.idCuenta
                WHERE uc.idUsuario = :idu AND c.Concentradora = 1 AND c.Activo = 1
                LIMIT 1
            """), {"idu": idUsuario}).fetchone()
            if concAccRow:
                idCuentaConcentradora = concAccRow[0]
        
        if not idCuentaConcentradora:
            fallbackConc = dbSession.execute(text("""
                SELECT idCuenta FROM cuenta 
                WHERE Concentradora = 1 AND Activo = 1 
                LIMIT 1
            """)).fetchone()
            if fallbackConc:
                idCuentaConcentradora = fallbackConc[0]
    except Exception as exConc:
        logger.error(f"Error al determinar cuenta concentradora para idCuenta={idCuenta}: {exConc}")

    updateTradeSql = text("""
        UPDATE trades
        SET status = 'CLOSED',
            exitPrice = :exitPrice,
            closeTime = :closeTime,
            pnl = :pnl,
            commission = :commission
        WHERE idTrade = :idt
    """)

    insertCommTradeSql = text("""
        INSERT INTO trades (
            idCuenta, strategy, setup, symbol, status, direction,
            intervalo, size, entryPrice, exitPrice, stopLoss, takeProfit,
            isBreakEven, pnl, slippage, commission, margin_used,
            openTime, closeTime, candleTime, sentAt, ticketId
        ) VALUES (
            :idCuenta, :strategy, :setup, :symbol, 'CLOSED', :direction,
            :intervalo, :size, :entryPrice, :exitPrice, NULL, NULL,
            0, :pnl, 0.0, 0.0, 0.0,
            :openTime, :closeTime, :candleTime, :sentAt, NULL
        )
    """)

    openTimeMes = datetime(nowDt.year, nowDt.month, 1, 0, 0, 0)
    lastDayOfMonth = calendar.monthrange(nowDt.year, nowDt.month)[1]
    closeTimeMes = datetime(nowDt.year, nowDt.month, lastDayOfMonth, 23, 59, 59)

    for tr in openTrades:
        tradeId = tr["idTrade"]
        sym = tr["symbol"]
        direction = tr["direction"]
        entryPx = tr["entryPrice"]
        size = tr["size"]
        marginUsed = tr["margin_used"]
        exitPx = brokerPricesMap.get(tradeId) or brokerPricesMap.get(sym) or latestPricesMap.get(sym, entryPx)

        symInfo = symbolDataMap.get(sym, {"pip": 0.0001, "quoteCurrency": "USD"})
        pipVal = symInfo["pip"]
        quoteCurr = symInfo["quoteCurrency"]

        # Cálculo unificado de PnL con la función centralizada (sin costos artificiales)
        rawPnl, pips, pipMoneyValue, _mov = calculate_unrealized_pnl(
            symbol=sym,
            direction=direction,
            entry_price=entryPx,
            current_price=exitPx,
            size=size,
            pip_size=pipVal
        )
        tradePnl = round(rawPnl, 2)

        # No se calcula comisión por trade
        tradeCommission = 0.0

        if sym == numerador:
            pnlA += tradePnl
            pipsA += pips
            dirA = direction
        else:
            pnlB += tradePnl
            pipsB += pips
            dirB = direction

        dbSession.execute(updateTradeSql, {
            "exitPrice": exitPx,
            "closeTime": nowStr,
            "pnl": tradePnl,
            "commission": tradeCommission,
            "idt": tradeId
        })

        totalMarginToRelease += marginUsed
        totalNetPnl += tradePnl
        totalCommissionCharged += tradeCommission

        # Si se cobró comisión y la cuenta no es la misma concentradora, registrar en cuenta concentradora
        if tradeCommission > 0 and idCuentaConcentradora and idCuentaConcentradora != idCuenta:
            origStrategy = tr.get("strategy") or "RATIO ATALAia"
            strategyComision = f"{origStrategy} Comision {idCuenta} {tradeId}"
            origSetup = tr.get("setup") or setupName

            try:
                dbSession.execute(text("""
                    INSERT IGNORE INTO strategyconfig (strategy, enabled, created_at, updated_at)
                    VALUES (:strategy, 1, NOW(), NOW())
                """), {"strategy": strategyComision})
            except Exception as exStrat:
                pass

            tradeOpenTime = tr.get("openTime") or tr.get("candleTime") or nowStr
            tradeCloseTime = nowStr

            dbSession.execute(insertCommTradeSql, {
                "idCuenta": idCuentaConcentradora,
                "strategy": strategyComision,
                "setup": origSetup,
                "symbol": sym,
                "direction": direction,
                "intervalo": tr.get("intervalo") or periodo or "1d",
                "size": size,
                "entryPrice": entryPx,
                "exitPrice": exitPx,
                "pnl": tradeCommission,
                "openTime": tradeOpenTime,
                "closeTime": tradeCloseTime,
                "candleTime": tr.get("candleTime") or tradeOpenTime,
                "sentAt": nowStr
            })

            # Acreditar la comisión cobrada en el capital de la cuenta concentradora
            dbSession.execute(text("""
                UPDATE cuenta
                SET Capital = Capital + :comm
                WHERE idCuenta = :idConc
            """), {
                "comm": tradeCommission,
                "idConc": idCuentaConcentradora
            })

            logger.info(
                f"   💰 COMISIÓN GENERADA:  USD ({comision_pct}%) del Trade #{tradeId} "
                f"registrada en Cuenta Concentradora #{idCuentaConcentradora} (Estrategia: '{strategyComision}')"
            )

        logger.info(
            f"   ● Trade #{tradeId} [{sym}] CERRADO @ {exitPx} "
            f"| PnL:  USD ({pips:+,.1f} pips) | Comision:  USD | Margen Liberado: "
        )

    # Capital a reintegrar en la cuenta: Capital + margen + pnl - comision
    netReintegration = totalMarginToRelease + totalNetPnl - totalCommissionCharged
    updateAccountSql = text("""
        UPDATE cuenta
        SET Capital = Capital + :netReintegration
        WHERE idCuenta = :idc
    """)
    dbSession.execute(updateAccountSql, {
        "netReintegration": netReintegration,
        "idc": idCuenta
    })

    dbSession.commit()
    accountName = str(accountData.get("nombre", f"Cuenta #{idCuenta}"))
    newCapital = float(accountData.get("capital", 0.0)) + netReintegration

    logger.info(
        f"🎯 [{setupName}] {closeReason}: {len(openTrades)} trades liquidados en Cuenta #{idCuenta} "
        f"| PnL Total:  USD | Comision Total:  USD "
        f"| Margen Reintegrado:  | Impacto Neto en Cuenta:  USD"
    )

    # El despacho a brókers se ejecutó exitosamente en la FASE 1 previa al commit.

    # Enviar alerta de cierre por Telegram
    exitAlertMsg = buildRatioExitAlertMessage(
        accountName=accountName,
        newAccountCapital=newCapital,
        numerador=numerador,
        denominador=denominador,
        periodo=periodo,
        pnlA=pnlA,
        pnlB=pnlB,
        pipsA=pipsA,
        pipsB=pipsB,
        dirA=dirA,
        dirB=dirB,
        totalNetPnl=totalNetPnl,
        totalMarginReleased=totalMarginToRelease,
        closeReason=closeReason
    )
    dispatchRatioTelegramAlert(idCuenta, exitAlertMsg)
    clearTrailingProfitState(f"{idCuenta}_{setupName}")
    clearCoverageState(f"{idCuenta}_{setupName}_{numerador}")
    clearCoverageState(f"{idCuenta}_{setupName}_{denominador}")

    return True



def openSingleCoverageTrade(
    dbSession,
    idCuenta: int,
    setupName: str,
    symbol: str,
    direction: str,
    size: float,
    entryPrice: float,
    accountData: Dict[str, Any],
    symData: Dict[str, Any],
    periodo: str = "1h",
    legReason: str = ""
) -> Optional[Dict[str, Any]]:
    """
    Abre una orden de cobertura para una pata específica cuando supera el -25% de pérdida en modo protección.
    1. Si hay bróker conectado, despacha al bróker por Webhook.
    2. Registra en BD con strategy = 'COBERTURA'.
    3. Inicializa el estado de seguimiento de la cobertura.
    4. Envía notificación por Telegram.
    """
    accountName = str(accountData.get("nombre", f"Cuenta #{idCuenta}"))
    margenRate = float(symData.get("margenRate", 0.0025))
    margenPct = symData.get("margenPct", 0.5)
    chunkMargin = calculate_margin(symbol, size, entryPrice, margenPct)

    nowDt = datetime.now()
    nowStr = nowDt

    query = text("""
        SELECT bc.idBrokerCuenta, bc.idCuenta, bc.loginUsuario, bc.tokenAcceso, bc.activo, b.nombre AS nombreBroker
        FROM brokercuenta bc
        LEFT JOIN broker b ON bc.idBroker = b.idBroker
        WHERE bc.idCuenta = :idc AND bc.activo = 1
    """)
    activeBrokers = dbSession.execute(query, {"idc": idCuenta}).mappings().fetchall()

    ticketId = None
    fillPrice = entryPrice

    if activeBrokers:
        covOrder = [{
            "symbol": symbol,
            "direction": direction,
            "action": "buy" if direction == "LARGO" else "sell",
            "size": size,
            "entryPrice": entryPrice,
            "margenRate": margenRate,
            "margenPct": margenPct,
            "strategy": "COBERTURA",
            "setup": setupName
        }]
        logger.info(f"🛡️ [{setupName}] Despachando COBERTURA al bróker: {symbol} {direction} {size} unidades para Cuenta #{idCuenta}...")
        execResults = sendRatioWebhookOrders(dbSession, idCuenta, covOrder)
        if not execResults or not execResults[0].get("success"):
            err = execResults[0].get("error", "Rechazado") if execResults else "Sin respuesta"
            logger.error(f"❌ [COBERTURA RECHAZADA] Bróker rechazó la cobertura para {symbol} en Cuenta #{idCuenta}: {err}")
            return None
        
        ticketId = execResults[0].get("order_id")
        fp = float(execResults[0].get("fill_price", 0.0) or 0.0)
        if fp > 0:
            fillPrice = fp

    dbSession.commit()
    existingTrade = None
    if ticketId:
        existingTrade = dbSession.execute(text("""
            SELECT idTrade FROM trades 
            WHERE idCuenta = :idc AND ticketId = :tid AND status = 'OPEN' 
            ORDER BY idTrade ASC LIMIT 1
        """), {"idc": idCuenta, "tid": str(ticketId)}).fetchone()

    if existingTrade:
        newTradeId = existingTrade[0]
        dbSession.execute(text("""
            UPDATE trades 
            SET strategy = 'COBERTURA',
                setup = :setup,
                margin_used = :margin_used
            WHERE idTrade = :idt
        """), {"setup": setupName, "margin_used": chunkMargin, "idt": newTradeId})
        dbSession.commit()
    else:
        insertSql = text("""
            INSERT INTO trades (
                idCuenta, strategy, setup, symbol, status, direction,
                intervalo, pnl, candleTime, openTime, size, entryPrice,
                stopLoss, takeProfit, margin_used, ticketId
            ) VALUES (
                :idCuenta, 'COBERTURA', :setup, :symbol, 'OPEN', :direction,
                :intervalo, 0.0, :candleTime, :openTime, :size, :entryPrice,
                NULL, NULL, :margin_used, :ticketId
            )
        """)
        res = dbSession.execute(insertSql, {
            "idCuenta": idCuenta,
            "setup": setupName,
            "symbol": symbol,
            "direction": direction,
            "intervalo": periodo,
            "candleTime": nowStr,
            "openTime": nowStr,
            "size": size,
            "entryPrice": fillPrice,
            "margin_used": chunkMargin,
            "ticketId": str(ticketId) if ticketId else None
        })
        newTradeId = res.lastrowid
        dbSession.commit()

    stateKey = f"{idCuenta}_{setupName}_{symbol}"
    saveCoverageState(stateKey, {
        "tradeId": newTradeId,
        "symbol": symbol,
        "direction": direction,
        "size": size,
        "entryPrice": fillPrice,
        "marginUsed": chunkMargin,
        "entryTime": nowDt.strftime("%Y-%m-%d %H:%M:%S"),
        "peakPxMovePct": 0.0,
        "target05Reached": False,
        "safeZoneRecovered": False
    })

    msg = (
        f"🛡️ <b>[COBERTURA ACTIVADA]</b>\n"
        f"<b>Cuenta:</b> #{idCuenta} ({accountName})\n"
        f"<b>Ratio:</b> {setupName}\n"
        f"<b>Símbolo:</b> {symbol}\n"
        f"<b>Operación:</b> {direction} {size:,.0f} unidades @ {fillPrice}\n"
        f"<b>Margen:</b> ${chunkMargin:.2f} USD\n"
        f"<b>Motivo:</b> Pérdida en pata supera -25.0% ({legReason})\n"
        f"<i>Posición congelada para proteger capital.</i>"
    )
    dispatchRatioTelegramAlert(idCuenta, msg)
    logger.info(f"✅ [{setupName}] Cobertura activada con éxito para {symbol} (Trade #{newTradeId} | Ticket {ticketId}).")
    return {
        "idTrade": newTradeId,
        "symbol": symbol,
        "direction": direction,
        "size": size,
        "entryPrice": fillPrice,
        "margin_used": chunkMargin,
        "ticketId": str(ticketId) if ticketId else None,
        "strategy": "COBERTURA"
    }


def closeSingleCoverageTrade(
    dbSession,
    idCuenta: int,
    setupName: str,
    coverageTrade: Dict[str, Any],
    latestPrice: float,
    symData: Dict[str, Any],
    accountData: Dict[str, Any],
    closeReason: str = "POR REGLA DE COBERTURA"
) -> bool:
    """
    Liquida y cierra una sola posición de cobertura en el bróker y en la base de datos.
    Reintegra el margen y registra el PnL obtenido.
    """
    accountName = str(accountData.get("nombre", f"Cuenta #{idCuenta}"))
    sym = coverageTrade["symbol"]
    tId = coverageTrade.get("ticketId")
    idTrade = coverageTrade["idTrade"]
    size = float(coverageTrade["size"])
    entryPx = float(coverageTrade["entryPrice"])
    direction = coverageTrade["direction"]
    pipSize = symData.get("pip", 0.0001)

    if tId and str(tId).strip() not in ["", "None", "0", "CLOSED"]:
        closeOrder = [{
            "strategy": "COBERTURA",
            "setup": setupName,
            "symbol": sym,
            "action": "close",
            "size": size,
            "entryPrice": 0.0,
            "ticketId": tId,
            "idTrade": idTrade
        }]
        logger.info(f"🔒 [closeSingleCoverageTrade] Despachando orden de cierre de cobertura al bróker para Trade #{idTrade} (Ticket {tId})...")
        execResults = sendRatioWebhookOrders(dbSession, idCuenta, closeOrder)
        failedResults = [r for r in execResults if not r.get("success")]
        if failedResults:
            err = failedResults[0].get("error", "Rechazado")
            logger.error(f"❌ [ABORTAR CIERRE COBERTURA] El bróker rechazó el cierre de cobertura {sym} en Cuenta #{idCuenta}: {err}")
            return False
        if execResults and execResults[0].get("fill_price"):
            fp = float(execResults[0]["fill_price"])
            if fp > 0:
                latestPrice = fp

    pnl, pips, _, _ = calculate_unrealized_pnl(
        symbol=sym,
        direction=direction,
        entry_price=entryPx,
        current_price=latestPrice,
        size=size,
        pip_size=pipSize
    )
    marginUsed = float(coverageTrade.get("margin_used") or 0.0)
    returnPct = (pnl / marginUsed * 100.0) if marginUsed > 0 else 0.0

    nowDt = datetime.now()
    nowStr = nowDt

    dbSession.execute(text("""
        UPDATE trades
        SET status = 'CLOSED',
            exitPrice = :exitPrice,
            closeTime = :closeTime,
            pnl = :pnl,
            commission = 0.0
        WHERE idTrade = :idt
    """), {
        "exitPrice": latestPrice,
        "closeTime": nowStr,
        "pnl": pnl,
        "idt": idTrade
    })

    netReintegration = marginUsed + pnl
    dbSession.execute(text("""
        UPDATE cuenta
        SET Capital = Capital + :netReintegration
        WHERE idCuenta = :idc
    """), {
        "netReintegration": netReintegration,
        "idc": idCuenta
    })
    dbSession.commit()

    saveCoverageState(f"{idCuenta}_{setupName}_{sym}", {
        "safeZoneRecovered": False,
        "lastClosedPrice": latestPrice,
        "lastCloseTime": nowDt.strftime("%Y-%m-%d %H:%M:%S")
    })

    msg = (
        f"🎯 <b>[COBERTURA CERRADA]</b>\n"
        f"<b>Cuenta:</b> #{idCuenta} ({accountName})\n"
        f"<b>Ratio:</b> {setupName}\n"
        f"<b>Símbolo:</b> {sym} ({direction})\n"
        f"<b>PnL Cobertura:</b> {pnl:+.2f} USD ({returnPct:+.2f}%)\n"
        f"<b>Precio Salida:</b> {latestPrice}\n"
        f"<b>Motivo:</b> {closeReason}\n"
        f"<i>Margen liberado: ${marginUsed:.2f} USD</i>"
    )
    dispatchRatioTelegramAlert(idCuenta, msg)
    logger.info(f"✅ [{setupName}] Cobertura cerrada con éxito para {sym}: PnL {pnl:+.2f} USD ({returnPct:+.2f}%).")
    return True


def processSingleUserRatio(dbSession, ratioRecord: Dict[str, Any]) -> None:
    """
    Evalúa el user_ratio activo (operar = 1) para la cuenta especificada.
    Utiliza el motor centralizado signalEngine:
    - Abre operaciones cuando ambos pares coinciden en señal de Triángulo (hasSignalBoth).
    - Cierra operaciones ÚNICAMENTE cuando ocurre el cruce real entre los precios de Par A y Par B (isPriceCross).
    """
    idCuenta = ratioRecord["idCuenta"]
    numerador = ratioRecord["numerador"]
    denominador = ratioRecord["denominador"]
    setupName = f"{numerador} - {denominador}"
    periodo = ratioRecord.get("Temporalidad") or ratioRecord.get("periodo")
    dias = ratioRecord.get("periodo") if isinstance(ratioRecord.get("periodo"), int) else ratioRecord.get("dias") or 180
    emaRapida = ratioRecord["EMARapida"] or 2
    emaLenta = ratioRecord["EMALenta"] or 20

    import pandas as pd
    from datetime import datetime, timedelta
    createdAtStr = ratioRecord.get("createdAt")
    createdAtDt = None
    if createdAtStr:
        try:
            if isinstance(createdAtStr, datetime):
                base_dt = createdAtStr
            else:
                base_dt = pd.to_datetime(createdAtStr).tz_localize(None)
            
            tf_clean = str(periodo).lower().strip()
            num_periods = dias
            
            if "mo" in tf_clean or "month" in tf_clean:
                createdAtDt = base_dt - pd.DateOffset(months=num_periods)
            elif "w" in tf_clean:
                createdAtDt = base_dt - timedelta(weeks=num_periods)
            elif "d" in tf_clean:
                createdAtDt = base_dt - timedelta(days=num_periods)
            elif "15m" in tf_clean:
                createdAtDt = base_dt - timedelta(minutes=15 * num_periods)
            elif "30m" in tf_clean:
                createdAtDt = base_dt - timedelta(minutes=30 * num_periods)
            elif "5m" in tf_clean:
                createdAtDt = base_dt - timedelta(minutes=5 * num_periods)
            elif "4h" in tf_clean:
                createdAtDt = base_dt - timedelta(hours=4 * num_periods)
            else: # fallback 1h
                createdAtDt = base_dt - timedelta(hours=num_periods)
                
            logger.info(f"⏳ Fecha efectiva calculada: base={base_dt}, tf={tf_clean}, periodos={num_periods}, efectiva={createdAtDt}")
            
        except Exception as e:
            logger.error(f"Error parseando o calculando createdAt: {e}")
            pass

    # 2. Datos de símbolos y cuenta
    symDataA = fetchSymbolData(dbSession, numerador)
    symDataB = fetchSymbolData(dbSession, denominador)
    accountData = fetchAccountData(dbSession, idCuenta)
    accountName = accountData.get("nombre", f"Cuenta #{idCuenta}")
    accHeader = f"Cuenta #{idCuenta} ({accountName}) | {setupName} ({periodo})"

    # 2.1 Verificación de Bróker Activo en brokercuenta: Si la cuenta no tiene bróker activo, NO opera
    queryBrokers = text("""
        SELECT bc.idBrokerCuenta, bc.idCuenta, bc.loginUsuario, b.nombre AS nombreBroker
        FROM brokercuenta bc
        LEFT JOIN broker b ON bc.idBroker = b.idBroker
        WHERE bc.idCuenta = :idc AND bc.activo = 1
    """)
    activeBrokers = dbSession.execute(queryBrokers, {"idc": idCuenta}).mappings().fetchall()

    if not activeBrokers:
        openTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)
        if createdAtDt:
            openTrades = [t for t in openTrades if (t.get("openTime") or t.get("candleTime")) and parseCandleDateTime(t.get("openTime") or t.get("candleTime")) >= createdAtDt]
        if not openTrades:
            logger.info(f"🚫 [{accHeader}] Cuenta #{idCuenta} ({accountName}) no tiene bróker activo configurado en brokercuenta. NO opera (evaluación omitida).")
            return
        else:
            logger.info(f"⚠️ [{accHeader}] Cuenta #{idCuenta} ({accountName}) sin bróker activo en brokercuenta, pero con {len(openTrades)} trade(s) abierto(s). Se evaluará únicamente para cierre por cruce de precios.")

    # 1. Historial de precios: filtrar a los últimos 'dias' periodos (velas) configurados
    pLower = str(periodo).lower()
    if "month" in pLower or "1m" == pLower:
        neededDays = max((dias * 32) + 60, 730)
    elif "week" in pLower or "1w" == pLower:
        neededDays = max((dias * 8) + 60, 365)
    elif "1d" in pLower:
        neededDays = max(dias + 60, 365)
    else:  # 1h, 4h, 15m, 30m
        neededDays = max((dias // 24) + 15, 60)

    dfA = fetchPriceHistory(dbSession, numerador, daysLimit=neededDays, timeframe=periodo)
    dfB = fetchPriceHistory(dbSession, denominador, daysLimit=neededDays, timeframe=periodo)

    if dfA.empty or dfB.empty:
        logger.warning(f"⚠️ [{accHeader}] Historial insuficiente para {numerador} o {denominador}.")
        return

    dfATf = resamplePriceData(dfA, periodo).tail(dias)
    dfBTf = resamplePriceData(dfB, periodo).tail(dias)

    # 3. Evaluación matemática con signalEngine (sin EMA lenta, ventana fija 20 para sigma)
    evalResult = signalEngine.evaluateRatioSignals(
        dfA=dfATf,
        dfB=dfBTf,
        pairA=numerador,
        pairB=denominador,
        smaPeriod=emaRapida,
        sigmaWindow=20,
        includeBoxes=True,
        fixedMinA=ratioRecord.get("fixedMinA"),
        fixedMaxA=ratioRecord.get("fixedMaxA"),
        fixedMinB=ratioRecord.get("fixedMinB"),
        fixedMaxB=ratioRecord.get("fixedMaxB")
    )

    normInfo = evalResult.get("normData", {})
    signalsList = evalResult.get("signals", [])
    latestSig = evalResult.get("latestSignal", {})

    if not latestSig:
        logger.warning(f"⚠️ [{accHeader}] No se obtuvieron señales del motor signalEngine.")
        return

    # Evaluar ÚNICAMENTE la vela actual (sin retroceder a velas del pasado)
    targetSignal = latestSig

    pA = targetSignal.get("priceA", 0.0)
    pB = targetSignal.get("priceB", 0.0)
    nA = targetSignal.get("normA", 0.0)
    nB = targetSignal.get("normB", 0.0)
    eA = targetSignal.get("emaA", 0.0)
    eB = targetSignal.get("emaB", 0.0)
    stdUpA = normInfo.get("stdAboveA", 0.0)
    stdDownA = normInfo.get("stdBelowA", 0.0)
    stdUpB = normInfo.get("stdAboveB", 0.0)
    stdDownB = normInfo.get("stdBelowB", 0.0)
    hasSigA = targetSignal.get("hasSignalA", False)
    hasSigB = targetSignal.get("hasSignalB", False)
    hasBoth = targetSignal.get("hasSignalBoth", False)
    isPriceCross = targetSignal.get("isPriceCross", False)
    sameSide = targetSignal.get("sameSideDeviation", False)
    sameSideMean = targetSignal.get("sameSideMean", False)

    logger.info(
        f"📊 [{accHeader}] EVALUACIÓN: "
        f"{numerador} [Px: {pA:.5f} | Norm: {nA:.4f} | EMA: {eA:.4f} | +1σ: {stdUpA:.4f} | -1σ: {stdDownA:.4f} | Sig: {hasSigA}] ⇄ "
        f"{denominador} [Px: {pB:.5f} | Norm: {nB:.4f} | EMA: {eB:.4f} | +1σ: {stdUpB:.4f} | -1σ: {stdDownB:.4f} | Sig: {hasSigB}] "
        f"| Coincidente (Triángulo): {hasBoth} | Cruce Precios (●): {isPriceCross}"
    )

    dbOpenTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)
    if createdAtDt:
        dbOpenTrades = [t for t in dbOpenTrades if (t.get("openTime") or t.get("candleTime")) and parseCandleDateTime(t.get("openTime") or t.get("candleTime")) >= createdAtDt]
    latestPriceA = float(dfATf["closePrice"].iloc[-1])
    latestPriceB = float(dfBTf["closePrice"].iloc[-1])
    tipoCierre = str(ratioRecord.get("tipoCierre", "Tolerante")).strip().capitalize()
    umbralDiferencia = float(ratioRecord.get("umbralDiferencia") or 0.2)
    salirBreakeven = bool(ratioRecord.get("salirBreakeven", False))
    cierreDivergencia = (tipoCierre in ["Divergencia", "Reactivo"])

    # Si el ratio tiene operar = 0 y no tiene posiciones abiertas, no hay nada que cerrar ni abrir
    if not ratioRecord.get("operar", True) and not dbOpenTrades:
        logger.info(f"ℹ️ [{accHeader}] Ratio con operar=0 y sin posiciones abiertas. Omitiendo evaluación.")
        return

    # Evaluación de Cierre por Modo Síncrono (Cruce de Precios: tipoCierre == 'Sincrono' o 'Cruce')
    if (tipoCierre in ["Sincrono", "Cruce"]) and isPriceCross and dbOpenTrades:
        logger.info(
            f"🔄 [{accHeader}] ¡CRUCE DE PRECIOS DETECTADO (●) en modo Cruce! "
            f"Cerrando {len(dbOpenTrades)} posiciones abiertas por cruce entre {numerador} y {denominador}..."
        )
        symMap = {numerador: symDataA, denominador: symDataB}
        priceMap = {numerador: latestPriceA, denominador: latestPriceB}
        closeRatioTrades(
            dbSession=dbSession,
            idCuenta=idCuenta,
            setupName=setupName,
            openTrades=dbOpenTrades,
            symbolDataMap=symMap,
            latestPricesMap=priceMap,
            accountData=accountData,
            periodo=periodo,
            closeReason="POR CRUCE DE PRECIOS"
        )
        accountData = fetchAccountData(dbSession, idCuenta)
        dbOpenTrades = []
        accionCierre = str(ratioRecord.get("accionCierre", "Continua")).strip().capitalize()
        if accionCierre == "Para":
            logger.info(f"🛑 [{accHeader}] Acción 'Cierra y Para' ejecutada por cruce: desactivando operar = 0 en BD.")
            dbSession.execute(text("UPDATE user_ratios SET operar = 0 WHERE id = :rid"), {"rid": ratioRecord["id"]})
            dbSession.commit()
            return

    # 3.1 EVALUACIÓN DE SALIDA POR CICLOS Y TAKE PROFIT DE CANASTA (Consecuencia natural de RIDE sin pérdidas)
    detectedWaves = 0
    hoursOpen = 0.0
    maxAllowedCycles = int(ratioRecord.get("EMARapida") or 2)
    maxHoursAllowed = maxAllowedCycles * dias

    if dbOpenTrades:
        regularTrades = [t for t in dbOpenTrades if str(t.get("strategy") or "").upper() != "COBERTURA"]
        coverageTrades = [t for t in dbOpenTrades if str(t.get("strategy") or "").upper() == "COBERTURA"]

        basketFloatingPnl = 0.0
        basketMarginUsed = 0.0
        distinctCandleTimes = set()
        openTimestamps = []

        for tr in dbOpenTrades:
            tSym = tr.get("symbol")
            tDir = tr.get("direction")
            tEntryPx = float(tr.get("entryPrice", 0.0))
            tSize = float(tr.get("size", 0.0))
            tCurPx = latestPriceA if tSym == numerador else latestPriceB
            sInfo = symDataA if tSym == numerador else symDataB
            tPip = sInfo.get("pip", 0.0001)

            tPnl, _pips, _pipVal, _mov = calculate_unrealized_pnl(
                symbol=tSym,
                direction=tDir,
                entry_price=tEntryPx,
                current_price=tCurPx,
                size=tSize,
                pip_size=tPip
            )
            basketFloatingPnl += tPnl
            basketMarginUsed += float(tr.get("margin_used") or 0.0)

        # Olas y tiempos basados estrictamente en entradas regulares
        for tr in regularTrades:
            cTime = tr.get("candleTime") or tr.get("openTime")
            if cTime:
                cDt = parseCandleDateTime(cTime)
                distinctCandleTimes.add(cDt)
                openTimestamps.append(cDt)

        returnOnMarginPct = (basketFloatingPnl / basketMarginUsed * 100.0) if basketMarginUsed > 0 else 0.0

        # Conteo de olas/ciclos de entrada (separadas por >= 12h)
        sortedCandles = sorted(list(distinctCandleTimes))
        lastWaveDt = None
        for cDt in sortedCandles:
            if lastWaveDt is None or (cDt - lastWaveDt).total_seconds() >= 12 * 3600:
                detectedWaves += 1
                lastWaveDt = cDt

        earliestDt = min(openTimestamps) if openTimestamps else None
        hoursOpen = (datetime.now() - earliestDt).total_seconds() / 3600.0 if earliestDt else 0.0

        normDiff = abs(nA - nB)

        logger.info(
            f"💼 [{accHeader}] GESTIÓN CANASTA: {len(dbOpenTrades)} ops | "
            f"PnL Flotante: {basketFloatingPnl:+.2f} USD ({returnOnMarginPct:+.1f}%) | "
            f"Margen: ${basketMarginUsed:.2f} | Dif Norm: {normDiff:.4f} (Meta <= 0.20) | "
            f"Olas: {detectedWaves}/{maxAllowedCycles} | "
            f"Tiempo Abierto: {hoursOpen:.1f}h/{maxHoursAllowed}h"
        )

        shouldCloseByExitRule = False
        exitRuleReason = ""

        # Regla 1: Cierre por Diferencial de Precios Normalizados (Modo Diferencia con ganancia)
        if (tipoCierre in ["Tolerante", "Diferencia"]) and (normDiff <= umbralDiferencia) and basketFloatingPnl > 0.0:
            shouldCloseByExitRule = True
            exitRuleReason = f"POR DIFERENCIAL NORMALIZADO <= {umbralDiferencia:.2f} (Dif: {normDiff:.4f} | PnL: +${basketFloatingPnl:.2f} USD)"

        # Regla 2: Trailing Profit 7.5% en Modo Reactivo / Divergencia (Activo ÚNICAMENTE tras ocurrir el cruce de precios ●)
        elif (tipoCierre in ["Reactivo", "Divergencia"]) and basketFloatingPnl > 0.0:
            trailingKey = f"{idCuenta}_{setupName}"
            tState = getTrailingProfitState(trailingKey)

            hasCrossed = bool(tState.get("hasCrossed", False))
            firstTradeA = next((t for t in regularTrades if t.get("symbol") == numerador), (regularTrades[0] if regularTrades else dbOpenTrades[0]))
            firstDirA = firstTradeA.get("direction", "LARGO")

            # Detectar si el cruce ocurrió en este censo o si las curvas normalizadas ya se cruzaron
            if not hasCrossed:
                if isPriceCross:
                    hasCrossed = True
                elif firstDirA == "LARGO" and nA >= nB:
                    hasCrossed = True
                elif firstDirA == "CORTO" and nA <= nB:
                    hasCrossed = True

                if hasCrossed:
                    tState["hasCrossed"] = True
                    tState["crossTime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    tState["peakPnl"] = round(basketFloatingPnl, 2)
                    saveTrailingProfitState(trailingKey, tState)
                    logger.info(
                        f"🔄 [{accHeader}] ¡CRUCE DE PRECIOS DETECTADO (●)! "
                        f"Iniciando fase Reactiva post-cruce. Trailing Profit 7.5% armado con PnL inicial: +${basketFloatingPnl:.2f} USD."
                    )

            if hasCrossed:
                prevPeak = float(tState.get("peakPnl", 0.0))
                if basketFloatingPnl > prevPeak:
                    tState["peakPnl"] = round(basketFloatingPnl, 2)
                    tState["peakTime"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    saveTrailingProfitState(trailingKey, tState)
                    prevPeak = tState["peakPnl"]

                # Si el pico acumulado post-cruce sobrepasó PPP con un colchón mínimo (>= 2.00 USD), evaluar retroceso del 7.5%
                if prevPeak >= 2.0:
                    pullbackFloor = round(prevPeak * 0.925, 2)  # 7.5% de retroceso permitido
                    logger.info(
                        f"🎯 [{accHeader}] TRAILING REACTIVO (Post-Cruce): PnL Actual: +${basketFloatingPnl:.2f} USD | "
                        f"Pico Máximo: +${prevPeak:.2f} USD | Umbral Salida (-7.5%): +${pullbackFloor:.2f} USD"
                    )
                    if basketFloatingPnl <= pullbackFloor:
                        shouldCloseByExitRule = True
                        exitRuleReason = (
                            f"POR TRAILING PROFIT 7.5% EN REACTIVO "
                            f"(Pico: +${prevPeak:.2f} | Cierre: +${basketFloatingPnl:.2f} USD | Retorno: {returnOnMarginPct:+.1f}%)"
                        )
                        clearTrailingProfitState(trailingKey)
            else:
                logger.info(
                    f"⏳ [{accHeader}] MODO REACTIVO: Canasta en camino al cruce (flotante: {basketFloatingPnl:+.2f} USD). "
                    f"Trailing Profit inactivo hasta que ocurra el cruce de precios (●)."
                )

        # Regla 2: [COMENTADA TEMPORALMENTE] Límite de Ciclos Alcanzado con PnL Positivo
        # Desactivado: no cerrar por límite de olas ni por primera ganancia flotante.
        # elif (detectedWaves >= maxAllowedCycles or hoursOpen >= maxHoursAllowed) and basketFloatingPnl > 0.50:
        #     shouldCloseByExitRule = True
        #     exitRuleReason = f"POR LÍMITE DE {maxAllowedCycles} CICLOS EN GANANCIA (+${basketFloatingPnl:.2f} USD)"

        # Regla 3: Break-Even Dinámico al Vencer Ciclos (Solo si salirBreakeven está activo y con PnL positivo)
        elif salirBreakeven and (detectedWaves >= maxAllowedCycles or hoursOpen >= maxHoursAllowed):
            if basketFloatingPnl > 0.0:
                shouldCloseByExitRule = True
                exitRuleReason = f"POR BREAK-EVEN AL VENCER {maxAllowedCycles} CICLOS (PnL: +${basketFloatingPnl:.2f} USD)"
            else:
                logger.info(
                    f"⏳ [{accHeader}] Límite de {maxAllowedCycles} ciclos alcanzado con flotante negativo ({basketFloatingPnl:+.2f} USD). "
                    f"Modo Break-Even activo: no se cierra a pérdida, esperando retorno a >= $0.00."
                )
        elif (not salirBreakeven) and (detectedWaves >= maxAllowedCycles or hoursOpen >= maxHoursAllowed):
            logger.info(
                f"ℹ️ [{accHeader}] Límite de {maxAllowedCycles} ciclos alcanzado, pero Salida en Break-Even está desactivada (salirBreakeven=False). "
                f"Posición continúa abierta esperando regla estándar de {tipoCierre}."
            )

        if shouldCloseByExitRule:
            logger.info(
                f"🎯 [{accHeader}] ¡DISPARO DE SALIDA DE CANASTA ({exitRuleReason})! "
                f"Cerrando {len(dbOpenTrades)} posiciones para asegurar capital..."
            )
            symMap = {numerador: symDataA, denominador: symDataB}
            priceMap = {numerador: latestPriceA, denominador: latestPriceB}
            closeSuccess = closeRatioTrades(
                dbSession=dbSession,
                idCuenta=idCuenta,
                setupName=setupName,
                openTrades=dbOpenTrades,
                symbolDataMap=symMap,
                latestPricesMap=priceMap,
                accountData=accountData,
                periodo=periodo,
                closeReason=exitRuleReason
            )
            if closeSuccess:
                accountData = fetchAccountData(dbSession, idCuenta)
                dbOpenTrades = []
                accionCierre = str(ratioRecord.get("accionCierre", "Continua")).strip().capitalize()
                if accionCierre == "Para":
                    logger.info(f"🛑 [{accHeader}] Acción 'Cierra y Para' ejecutada por {exitRuleReason}: desactivando operar = 0 en BD.")
                    dbSession.execute(text("UPDATE user_ratios SET operar = 0 WHERE id = :rid"), {"rid": ratioRecord["id"]})
                    dbSession.commit()
                return

        # 3.2 GESTIÓN DE COBERTURAS POR PRECIO (+1% ENTRADA / TRAILING 0.5% / STOP 0.5%)
        # REGLA ESTRICTA: Cuenta #3 (Raúl Colin) completamente excluida y protegida.
        ENABLE_COBERTURAS = True
        isProtectionMode = bool(regularTrades) and (detectedWaves >= maxAllowedCycles) and ENABLE_COBERTURAS and (idCuenta != 3)
        if isProtectionMode:
            legsToEvaluate = [
                ("A", numerador, symDataA, latestPriceA),
                ("B", denominador, symDataB, latestPriceB)
            ]
            for legName, legSym, legSymData, legCurPx in legsToEvaluate:
                legRegTrades = [t for t in regularTrades if t.get("symbol") == legSym]
                legCobTrades = [t for t in coverageTrades if t.get("symbol") == legSym]

                if not legRegTrades:
                    continue

                lastRegTrade = legRegTrades[-1]
                lastEntryPx = float(lastRegTrade.get("entryPrice", 0.0))
                legDir = lastRegTrade.get("direction", "LARGO")
                oppDir = "CORTO" if legDir == "LARGO" else "LARGO"
                legTotSize = sum(float(tr.get("size", 0.0)) for tr in legRegTrades)
                pipSize = legSymData.get("pip", 0.0001)

                stateKey = f"{idCuenta}_{setupName}_{legSym}"
                cobState = getCoverageState(stateKey)

                # Nivel de disparo al 1% respecto a la última entrada
                if legDir == "LARGO":
                    triggerPx = lastEntryPx * 0.99
                    isAdverse = (legCurPx <= triggerPx)
                    isSafeZone = (legCurPx > triggerPx)
                else:
                    triggerPx = lastEntryPx * 1.01
                    isAdverse = (legCurPx >= triggerPx)
                    isSafeZone = (legCurPx < triggerPx)

                # CASO 1: NO HAY COBERTURA ACTIVA -> EVALUAR ENTRADA AL 1% ADVERSO
                if not legCobTrades:
                    # Si el precio recuperó a la zona segura, habilitar la siguiente re-entrada
                    if isSafeZone and not cobState.get("safeZoneRecovered", True):
                        cobState["safeZoneRecovered"] = True
                        saveCoverageState(stateKey, cobState)
                        logger.info(
                            f"🛡️ [{accHeader}] Pata {legName} ({legSym}) recuperó zona segura "
                            f"(Precio {legCurPx:.5f} volvió dentro del 1% respecto a última entrada {lastEntryPx:.5f}). "
                            f"Re-entrada de cobertura rearmada."
                        )

                    canEnter = cobState.get("safeZoneRecovered", True)
                    if isAdverse and canEnter and legTotSize > 0:
                        logger.warning(
                            f"🛡️ [{accHeader}] DISPARO DE COBERTURA PATA {legName} ({legSym}): "
                            f"Precio {legCurPx:.5f} alcanzó/superó el 1% adverso desde última entrada {lastEntryPx:.5f} "
                            f"(Umbral: {triggerPx:.5f}). Abriendo cobertura en {oppDir} por {legTotSize:.0f} unidades "
                            f"(volumen total de la pata)..."
                        )
                        newCob = openSingleCoverageTrade(
                            dbSession=dbSession,
                            idCuenta=idCuenta,
                            setupName=setupName,
                            symbol=legSym,
                            direction=oppDir,
                            size=legTotSize,
                            entryPrice=legCurPx,
                            accountData=accountData,
                            symData=legSymData,
                            periodo=periodo,
                            legReason=f"Pata {legName} {legSym} {legDir} superó 1% adverso desde última entrada ({lastEntryPx:.5f} -> {legCurPx:.5f})"
                        )
                        if newCob:
                            coverageTrades.append(newCob)
                            dbOpenTrades.append(newCob)
                            cobState["safeZoneRecovered"] = False
                            cobState["peakPxMovePct"] = 0.0
                            cobState["target05Reached"] = False
                            saveCoverageState(stateKey, cobState)

                # CASO 2: YA HAY COBERTURA ACTIVA -> EVALUAR CIERRE (TRAILING 0.5% / STOP -0.5%)
                else:
                    activeCob = legCobTrades[0]
                    cobPx = float(activeCob.get("entryPrice", 0.0))
                    cobSz = float(activeCob.get("size", 0.0))
                    cobDir = activeCob.get("direction", oppDir)

                    # Variación porcentual en precio de la cobertura
                    if cobDir in ["LARGO", "BUY"]:
                        cobPxMovePct = ((legCurPx - cobPx) / cobPx) * 100.0
                    else:
                        cobPxMovePct = ((cobPx - legCurPx) / cobPx) * 100.0

                    cobPnl, _, _, _ = calculate_unrealized_pnl(
                        symbol=legSym,
                        direction=cobDir,
                        entry_price=cobPx,
                        current_price=legCurPx,
                        size=cobSz,
                        pip_size=pipSize
                    )

                    prevPeak = float(cobState.get("peakPxMovePct", 0.0))
                    peakPxMove = max(prevPeak, cobPxMovePct)
                    target05Reached = bool(cobState.get("target05Reached", False)) or (cobPxMovePct >= 0.5)

                    cobState["peakPxMovePct"] = peakPxMove
                    cobState["target05Reached"] = target05Reached
                    saveCoverageState(stateKey, cobState)

                    logger.info(
                        f"🛡️ [{accHeader}] COBERTURA ACTIVA Pata {legName} ({legSym} {cobDir}): "
                        f"Precio Ent: {cobPx:.5f} | Act: {legCurPx:.5f} | Mov: {cobPxMovePct:+.2f}% | "
                        f"Pico: {peakPxMove:+.2f}% | Meta +0.5% alcanzada: {target05Reached} | PnL: ${cobPnl:+.2f}"
                    )

                    shouldCloseCoverage = False
                    cobCloseReason = ""

                    # Regla A: Superó +0.5% en precio y retrocede a <= +0.5% (asegurar ganancia)
                    if target05Reached and cobPxMovePct <= 0.5:
                        shouldCloseCoverage = True
                        cobCloseReason = f"POR RETROCESO A +0.5% EN PRECIO TRAS PICO DE {peakPxMove:+.2f}% (Movimiento: {cobPxMovePct:+.2f}% | PnL: ${cobPnl:+.2f})"

                    # Regla B: Stop de protección si el precio se mueve en contra de la cobertura <= -0.5%
                    elif cobPxMovePct <= -0.5:
                        shouldCloseCoverage = True
                        cobCloseReason = f"POR STOP DE PROTECCIÓN -0.5% EN PRECIO (Movimiento: {cobPxMovePct:+.2f}% | PnL: ${cobPnl:+.2f})"

                    if shouldCloseCoverage:
                        logger.info(
                            f"🎯 [{accHeader}] DISPARO DE SALIDA DE COBERTURA Pata {legName} ({legSym}) {cobCloseReason}! "
                            f"Liquidando posición de cobertura..."
                        )
                        closeSuccess = closeSingleCoverageTrade(
                            dbSession=dbSession,
                            idCuenta=idCuenta,
                            setupName=setupName,
                            coverageTrade=activeCob,
                            latestPrice=legCurPx,
                            symData=legSymData,
                            accountData=accountData,
                            closeReason=cobCloseReason
                        )
                        if closeSuccess:
                            accountData = fetchAccountData(dbSession, idCuenta)
                            coverageTrades = [t for t in coverageTrades if t.get("idTrade") != activeCob.get("idTrade")]
                            dbOpenTrades = [t for t in dbOpenTrades if t.get("idTrade") != activeCob.get("idTrade")]

    if sameSide:
        logger.info(
            f"🛡️ [{accHeader}] Ambos precios normalizados en el mismo lado de la desviación estándar "
            f"({numerador}: {nA:.4f} vs {denominador}: {nB:.4f}). Entradas restringidas (sin cuadros ni triángulos)."
        )
    if sameSideMean:
        logger.info(
            f"🛡️ [{accHeader}] Par contrario en el mismo lado de la línea recta negra / promedio de la media "
            f"({numerador}: {nA:.4f} vs {denominador}: {nB:.4f} | Media: {normInfo.get('avgOfMean', 0.5):.4f}). Entrada restringida."
        )

    # 4. EVALUACIÓN DE ENTRADA, REVERSAL (CAMBIO DE DIRECCIÓN) O ACUMULACIÓN
    sigTypeRaw = targetSignal.get("signalType")
    hasEntrySignal = bool(sigTypeRaw) and not sameSide and not sameSideMean
    latestSig = targetSignal

    if hasEntrySignal:
        entryDateStr = str(latestSig.get("date", "")).strip()
        candleDt = parseCandleDateTime(entryDateStr)
        
        # Para temporalidades intradía, redondear candleDt al bloque correspondiente para que el candado de orden sea exacto por vela
        tf_clean = str(periodo).lower().strip()
        if tf_clean in ["1h", "1H"]:
            candleDt = candleDt.replace(minute=0, second=0, microsecond=0)
            entryDateStr = candleDt.strftime("%Y-%m-%d %H:00:00")
        elif tf_clean in ["30min", "30m"]:
            rounded_minute = (candleDt.minute // 30) * 30
            candleDt = candleDt.replace(minute=rounded_minute, second=0, microsecond=0)
            entryDateStr = candleDt.strftime("%Y-%m-%d %H:%M:00")
        elif tf_clean in ["15min", "15m"]:
            rounded_minute = (candleDt.minute // 15) * 15
            candleDt = candleDt.replace(minute=rounded_minute, second=0, microsecond=0)
            entryDateStr = candleDt.strftime("%Y-%m-%d %H:%M:00")
        elif tf_clean in ["1d", "1D"]:
            candleDt = candleDt.replace(hour=0, minute=0, second=0, microsecond=0)
            entryDateStr = candleDt.strftime("%Y-%m-%d")

        # Filtrar si la vela es anterior a la fecha de creación
        if createdAtDt and candleDt < createdAtDt:
            logger.info(f"⏳ [{accHeader}] Vela actual ({entryDateStr}) es anterior a la creación del ratio ({createdAtDt}). Se ignora la evaluación.")
            return

        sigType = latestSig.get("signalType", "TRIANGULO")
        dirA = "CORTO" if latestSig.get("symbolAAction") == "SELL" else "LARGO"
        dirB = "CORTO" if latestSig.get("symbolBAction") == "SELL" else "LARGO"

        # Validación estricta por temporalidad: solo insertar dentro de los primeros 15 min de la vela
        # EXCEPCIÓN (Opción A): Si ya existen operaciones abiertas en el ciclo y el precio actual de AMBAS patas
        # continúa en máxima expansión (más favorable que la última entrada abierta: más bajo para compra y más alto para venta),
        # se permite la entrada tardía porque el precio no se devolvió hacia el cruce sino que amplió la divergencia.
        isFresh = isCandleSignalFresh(candleDt, periodo)
        if not isFresh:
            minsOld = int(abs((datetime.now() - candleDt).total_seconds()) / 60)
            isBetterPriceExpansion = False
            regularTrades = [t for t in dbOpenTrades if str(t.get("strategy") or "").upper() != "COBERTURA"]
            if regularTrades:
                openTradesA = [t for t in regularTrades if t.get("symbol") == numerador]
                openTradesB = [t for t in regularTrades if t.get("symbol") == denominador]
                if openTradesA and openTradesB:
                    lastTradeA = openTradesA[-1]
                    lastTradeB = openTradesB[-1]
                    lastPxA = float(lastTradeA.get("entryPrice", 0.0))
                    lastPxB = float(lastTradeB.get("entryPrice", 0.0))

                    betterA = (latestPriceA < lastPxA) if dirA == "LARGO" else (latestPriceA > lastPxA)
                    betterB = (latestPriceB < lastPxB) if dirB == "LARGO" else (latestPriceB > lastPxB)

                    if betterA and betterB:
                        isBetterPriceExpansion = True
                        logger.info(
                            f"✨ [{accHeader}] Excepción de frescura concedida ({minsOld} min en {periodo}): "
                            f"Precios en máxima expansión respecto a la última entrada "
                            f"({numerador} [{dirA}]: actual {latestPriceA:.5f} vs última {lastPxA:.5f} | "
                            f"{denominador} [{dirB}]: actual {latestPriceB:.5f} vs última {lastPxB:.5f}). "
                            f"Permitiendo evaluación tardía en vela {entryDateStr}."
                        )

            if not isBetterPriceExpansion:
                logger.info(
                    f"⏳ [{accHeader}] Señal de fecha/hora {entryDateStr} tiene {minsOld} min desde apertura (> 15 min permitidos en {periodo}). "
                    f"Omitiendo entrada tardía para no entrar con el precio ya desplazado hacia el cruce."
                )
                return

        # REGLA DE LÍMITE DE CICLOS / MODO PROTECCIÓN: Si ya se alcanzó el límite de ciclos, NO acumular ni girar por señales
        regularTrades = [t for t in dbOpenTrades if str(t.get("strategy") or "").upper() != "COBERTURA"]
        if regularTrades and (detectedWaves >= maxAllowedCycles or hoursOpen >= maxHoursAllowed):
            logger.info(
                f"🛡️ [{accHeader}] Modo Protección activo: Ya se alcanzó el límite de {maxAllowedCycles} ciclos "
                f"({detectedWaves} olas / {hoursOpen:.1f}h abiertas). Nuevas entradas y giros bloqueados mientras gestiona cobertura/salida."
            )
            return

        # Verificar si hay posiciones abiertas en dirección contraria (REVERSAL / FLIP) basado EXCLUSIVAMENTE en trades regulares
        if regularTrades:
            tradesA = [t for t in regularTrades if t.get("symbol") == numerador]
            lastTradeA = tradesA[-1] if tradesA else regularTrades[-1]
            lastDirA = lastTradeA.get("direction", "")
            isReversal = (dirA != lastDirA)

            if isReversal:
                logger.info(
                    f"🔄 [{accHeader}] ¡CAMBIO DE DIRECCIÓN DETECTADO ({sigType})! "
                    f"La última entrada fue {lastDirA} y la siguiente señal es {dirA}. "
                    f"Cerrando {len(dbOpenTrades)} posiciones anteriores para girar a {dirA}..."
                )
                symMap = {numerador: symDataA, denominador: symDataB}
                priceMap = {numerador: latestPriceA, denominador: latestPriceB}
                closeRatioTrades(
                    dbSession=dbSession,
                    idCuenta=idCuenta,
                    setupName=setupName,
                    openTrades=dbOpenTrades,
                    symbolDataMap=symMap,
                    latestPricesMap=priceMap,
                    accountData=accountData,
                    periodo=periodo,
                    closeReason=f"POR CAMBIO DE DIRECCIÓN ({dirA})"
                )
                accountData = fetchAccountData(dbSession, idCuenta)
                dbOpenTrades = []
                accionCierre = str(ratioRecord.get("accionCierre", "Continua")).strip().capitalize()
                if accionCierre == "Para":
                    logger.info(f"🛑 [{accHeader}] Acción 'Cierra y Para' ejecutada por giro de dirección: desactivando operar = 0 en BD.")
                    dbSession.execute(text("UPDATE user_ratios SET operar = 0 WHERE id = :rid"), {"rid": ratioRecord["id"]})
                    dbSession.commit()
                    return

        # REGLA ESTRICTA DE PRIMERA ENTRADA: Ambos pares deben estar fuera de sus desviaciones estándar en lados opuestos
        if not regularTrades:
            if dirA == "CORTO":
                isOutA = bool(nA >= stdUpA)
                isOutB = bool(nB <= stdDownB)
            else:
                isOutA = bool(nA <= stdDownA)
                isOutB = bool(nB >= stdUpB)
            if not (isOutA and isOutB):
                logger.info(
                    f"🛡️ [{accHeader}] Primera entrada ({sigType}) RECHAZADA: ambos pares deben estar fuera de sus bandas en lados opuestos "
                    f"({numerador}: {nA:.4f} [fuera: {isOutA}] vs {denominador}: {nB:.4f} [fuera: {isOutB}])."
                )
                return

        # Si el ratio tiene operar = 0 (generación automática desactivada), no abrir nuevas operaciones ni acumular
        if not ratioRecord.get("operar", True):
            logger.info(
                f"🛑 [{accHeader}] Ratio tiene 'operar = 0' (generación automática desactivada). "
                f"Se evaluaron/procesaron cierres si correspondía, pero NO se abrirán nuevas posiciones."
            )
            return

        # REGLA SELECTIVA DE ENTRADAS: Si hay operaciones abiertas del mismo ciclo, validar selectividad (solo si tipoEntrada es Selectiva)
        tipoEntrada = str(ratioRecord.get("tipoEntrada", "Selectiva")).strip().capitalize()
        if tipoEntrada == "Selectiva" and regularTrades:
            openTradesA = [t for t in regularTrades if t["symbol"] == numerador]
            openTradesB = [t for t in regularTrades if t["symbol"] == denominador]
            if openTradesA and openTradesB:
                rangeA = normInfo.get("rangeA", 0.0)
                rangeB = normInfo.get("rangeB", 0.0)
                minA = normInfo.get("minA", 0.0)
                minB = normInfo.get("minB", 0.0)

                # Comparar contra la última entrada abierta de cada pata (Opción A)
                lastPxA = float(openTradesA[-1]["entryPrice"])
                lastPxB = float(openTradesB[-1]["entryPrice"])

                lastNormA = (lastPxA - minA) / rangeA if rangeA > 0 else 0.5
                lastNormB = (lastPxB - minB) / rangeB if rangeB > 0 else 0.5

                isValidSelective, updA, updB, rejectReason = signalEngine.isSelectiveEntryValid(
                    sig=latestSig,
                    currNormA=nA,
                    currNormB=nB,
                    lastEntryA=lastNormA,
                    lastEntryB=lastNormB,
                    stdAboveA=stdUpA,
                    stdBelowA=stdDownA,
                    stdAboveB=stdUpB,
                    stdBelowB=stdDownB
                )
                if not isValidSelective:
                    logger.info(
                        f"🛡️ [{accHeader}] Entrada sucesiva ({sigType}) RECHAZADA por regla selectiva: {rejectReason}."
                    )
                    return

        # REGLA DE LÍMITE DE CICLOS: Si ya se alcanzó el límite de ciclos (RIDE), NO acumular más
        if dbOpenTrades and (detectedWaves >= maxAllowedCycles or hoursOpen >= maxHoursAllowed):
            logger.info(
                f"🛡️ [{accHeader}] Acumulación bloqueada: Ya se alcanzó el límite de {maxAllowedCycles} ciclos "
                f"({detectedWaves} olas / {hoursOpen:.1f}h abiertas). Esperando resolución de salida."
            )
            return

        alreadyEntered = checkTradeExistsForCandle(dbSession, idCuenta, setupName, candleDt, timeframe=periodo)
        if not alreadyEntered:
            # Control de Gestión de Riesgo: Indicador de Margen >= 200%
            canOperate, marginInd, totalMargin, equity = checkAccountMarginHealth(
                dbSession, idCuenta, float(accountData.get("capital", 0.0))
            )
            if not canOperate:
                logger.warning(
                    f"⛔ [{setupName}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: "
                    f"Indicador de Margen {marginInd:.1f}% es menor al {accountData.get('indicadorMargen', 200.0):.0f}% requerido "
                    f"(Capital: ${accountData.get('capital', 0.0):,.2f} | Margen: ${totalMargin:,.2f} | Equidad: ${equity:,.2f})."
                )
                return

            logger.info(
                f"🎯 [{setupName}] NUEVA SEÑAL ({sigType}, fecha {entryDateStr} | "
                f"Dirección: {numerador} {dirA} + {denominador} {dirB} | "
                f"Ind. Margen: {marginInd:.1f}% >= {accountData.get('indicadorMargen', 200.0):.0f}%). Registrando en trades..."
            )
            openSingleRatioTradePair(
                dbSession=dbSession,
                idCuenta=idCuenta,
                setupName=setupName,
                numerador=numerador,
                denominador=denominador,
                periodo=periodo,
                candleDateStr=entryDateStr,
                signalType=sigType,
                directionA=dirA,
                directionB=dirB,
                entryPxA=latestPriceA,
                entryPxB=latestPriceB,
                accountData=accountData,
                symDataA=symDataA,
                symDataB=symDataB
            )
        else:
            logger.info(f"ℹ️ [{accHeader}] Señal ({sigType}) de fecha {entryDateStr} ya fue registrada previamente en trades.")
    else:
        if dbOpenTrades:
            logger.info(
                f"⏳ [{accHeader}] Posición activa en curso ({len(dbOpenTrades)} trades). "
                f"Precios normalizados: {numerador}={nA:.4f} vs {denominador}={nB:.4f} (Dif: {abs(nA - nB):.4f}). Corriendo inercia/divergencia hasta cambio de dirección."
            )
        else:
            logger.info(f"🔍 [{accHeader}] Sin señales de entrada (Triángulos/Cuadros) en la vela actual.")


def processAllActiveRatios(is_hourly_tick: bool = True) -> int:
    """
    Punto de entrada para la evaluación de ratios:
    - Ratios en 1h (o menor): se evalúan en cada censo de 5 minutos.
    - Ratios en 1d o mayor: solo se evalúan en el censo horario (is_hourly_tick=True).
    """
    cycle_type = "HORARIO (1h y Diario)" if is_hourly_tick else "INTRA-HORA (5 min para 1h)"
    logger.info("=================================================================")
    logger.info(f"📡 [microRatio] INICIANDO CENSO {cycle_type} DE RATIOS...")
    startTime = time.time()
    processedCount = 0

    with getDatabaseSession() as dbSession:
        try:
            ensureStrategyRegistered(dbSession)
            activeRatios = fetchActiveUserRatios(dbSession)
            logger.info(f"📋 Ratios activos totales con operar=1: {len(activeRatios)}")

            for r in activeRatios:
                tf = str(r.get("Temporalidad") or r.get("timeframe") or "1h").lower().strip()
                is_intraday = tf in ["1h", "4h", "15min", "15m", "30min", "30m", "5min", "5m"]

                # Si el ratio es diario o mayor y NO es el censo horario (:00), se omite para evitar sobre-operar
                if not is_intraday and not is_hourly_tick:
                    continue

                try:
                    processSingleUserRatio(dbSession, r)
                    processedCount += 1
                except Exception as exRatio:
                    logger.error(f"❌ Error procesando ratio ID #{r.get('id')} ({r.get('numerador')}/{r.get('denominador')}): {exRatio}", exc_info=True)

        except Exception as exGlobal:
            logger.error(f"❌ Error global en ciclo de microRatio: {exGlobal}", exc_info=True)

    elapsed = time.time() - startTime
    logger.info(f"✅ [microRatio] CICLO COMPLETADO: {processedCount} ratios evaluados en {elapsed:.2f}s")
    logger.info("=================================================================")
    return processedCount


async def runAdaptiveScheduler():
    """
    Bucle asíncrono adaptable:
    - Censa cada 5 minutos para ratios en temporalidad horaria (1h).
    - En el minuto :00 (hora en punto), censa además los ratios en temporalidad diaria (1d) o mayor.
    - Candado estricto: máximo 1 orden por temporalidad (1 por hora en 1h, 1 por día en 1d).
    """
    logger.info("🚀 [microRatio] Demonio adaptativo iniciado: censo cada 5 min (1h) y cada hora (1d).")
    while True:
        now = datetime.now()
        # Se considera censo horario si estamos en los primeros 4 minutos de la hora
        is_hourly_tick = (now.minute < 5)

        try:
            processAllActiveRatios(is_hourly_tick=is_hourly_tick)
        except Exception as e:
            logger.error(f"Error en ejecución de microRatio: {e}", exc_info=True)

        now = datetime.now()
        # Calcular segundos restantes hasta el próximo bloque de 5 minutos (:00, :05, :10, :15, etc.)
        secondsUntilNext5Min = (5 - (now.minute % 5) - 1) * 60 + (60 - now.second)
        nextMin = (now.minute + (secondsUntilNext5Min // 60) + 1) % 60
        logger.info(f"💤 Próximo censo en {secondsUntilNext5Min // 60}m {secondsUntilNext5Min % 60}s (en el minuto :{nextMin:02d}).")
        await asyncio.sleep(max(5, secondsUntilNext5Min))


def main():
    parser = argparse.ArgumentParser(description="Microservicio microRatio para arbitraje de ratios EMA.")
    parser.add_argument("--once", action="store_true", help="Ejecuta una sola iteración y termina.")
    args = parser.parse_args()

    if args.once:
        logger.info("▶️ Modo de ejecución única activado (--once).")
        processAllActiveRatios(is_hourly_tick=True)
    else:
        try:
            asyncio.run(runAdaptiveScheduler())
        except KeyboardInterrupt:
            logger.info("🛑 Detención manual del microservicio microRatio.")


if __name__ == "__main__":
    main()
