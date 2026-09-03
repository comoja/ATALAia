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
    Obtiene todas las configuraciones activas de user_ratios donde operar = 1.
    """
    sqlQuery = text("""
        SELECT id, idUsuario, idCuenta, numerador, denominador, periodo, dias, EMARapida, EMALenta, operar
        FROM user_ratios
        WHERE operar = 1
        ORDER BY id ASC
    """)
    rows = dbSession.execute(sqlQuery).fetchall()
    activeList = []
    for r in rows:
        activeList.append({
            "id": r[0],
            "idUsuario": r[1],
            "idCuenta": r[2],
            "numerador": str(r[3]),
            "denominador": str(r[4]),
            "periodo": str(r[5]) if r[5] else "1d",
            "dias": int(r[6]) if r[6] else 180,
            "EMARapida": int(r[7]) if r[7] else 2,
            "EMALenta": int(r[8]) if r[8] else 20,
            "operar": bool(r[9])
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
        SELECT idCuenta, Nombre, Capital, Activo, riesgoPorOperacion, comision, Concentradora
        FROM cuenta
        WHERE idCuenta = :idc
        LIMIT 1
    """)
    row = dbSession.execute(sqlQuery, {"idc": idCuenta}).fetchone()
    if row:
        riesgoVal = float(row[4]) if (len(row) > 4 and row[4] is not None and float(row[4]) > 0) else 3.0
        comisionVal = float(row[5]) if (len(row) > 5 and row[5] is not None and float(row[5]) >= 0) else 0.0
        concentradoraVal = bool(row[6]) if (len(row) > 6 and row[6] is not None) else False
        return {
            "idCuenta": row[0],
            "nombre": str(row[1]),
            "capital": float(row[2]) if row[2] is not None else 0.0,
            "activo": bool(row[3]),
            "riesgoPorOperacion": riesgoVal,
            "comision": comisionVal,
            "concentradora": concentradoraVal
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
    Valida estrictamente que el cruce haya sucedido en el periodo actual:
    - 1h: solo cruces de esa hora (máx 1.5 horas).
    - 4h: solo cruces de esa barra de 4h (máx 4.5 horas).
    - 15m/30m: solo cruces de esa fracción horaria.
    - 1d: solo cruces de ese día.
    - 1week: solo cruces de esa semana.
    - 1month: solo cruces de ese mes.
    """
    tf = str(timeframe).lower().strip()
    now = datetime.now()
    deltaSecs = abs((now - candleDt).total_seconds())

    if tf in ["1h", "1h"]:
        return deltaSecs <= 5400  # Máximo 1 hora y media
    elif tf in ["4h", "4h"]:
        return deltaSecs <= 18000  # Máximo 5 horas
    elif tf in ["15min", "15m"]:
        return deltaSecs <= 1800   # Máximo 30 min
    elif tf in ["30min", "30m"]:
        return deltaSecs <= 3600   # Máximo 1 hora
    elif tf in ["1d", "1d"]:
        return deltaSecs <= 36 * 3600  # Cruces de hoy o cierre inmediato
    elif tf in ["1week", "1w", "1w"]:
        return deltaSecs <= 8 * 86400  # Cruces de la semana actual
    elif tf in ["1month", "1m", "1m"]:
        return deltaSecs <= 35 * 86400 # Cruces del mes actual

    return deltaSecs <= 36 * 3600

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
              AND status = 'OPEN'
              AND candleTime = :cdt
        """)
    elif tf in ["1week", "1w"]:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND setup = :stp
              AND status = 'OPEN'
              AND YEARWEEK(candleTime, 1) = YEARWEEK(:cdt, 1)
        """)
    elif tf in ["1month", "1m"]:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND setup = :stp
              AND status = 'OPEN'
              AND YEAR(candleTime) = YEAR(:cdt)
              AND MONTH(candleTime) = MONTH(:cdt)
        """)
    else:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND setup = :stp
              AND status = 'OPEN'
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

        if direction in ["LARGO", "BUY", "LONG"]:
            delta = currentPx - entryPx
        else:
            delta = entryPx - currentPx

        pips = (delta / pipVal) if pipVal > 0 else 0.0

        if quoteCurr == "USD":
            pipMoneyValue = size * pipVal
        else:
            pipMoneyValue = (size * pipVal) / currentPx if currentPx > 0 else (size * pipVal)

        costs = (size * currentPx) * 0.0003
        tradePnl = (pips * pipMoneyValue) - costs
        totalFloatingPnl += tradePnl

    equity = accountCapital + totalFloatingPnl
    marginIndicator = (equity / totalMargin * 100.0) if totalMargin > 0 else 9999.0
    canOperate = bool(marginIndicator >= 200.0)

    return canOperate, marginIndicator, totalMargin, equity


def checkActiveOpenTrades(dbSession, idCuenta: int, setupName: str) -> List[Dict[str, Any]]:
    """
    Verifica si existen operaciones actualmente en estado 'OPEN' para este setup y cuenta,
    incluyendo su ticketId para la gestión de órdenes en bróker.
    """
    sqlQuery = text("""
        SELECT idTrade, symbol, direction, size, entryPrice, margin_used, candleTime, openTime, ticketId
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
            "ticketId": str(r[8]) if r[8] else None
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

    header_reason = "POR SOLICITUD" if closeReason == "POR SOLICITUD" else "POR CRUCE DE PRECIOS"
    body_reason = "POR SOLICITUD" if closeReason == "POR SOLICITUD" else "POR CONVERGENCIA (●)"

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
                    "FOREX_USERNAME": encryptedAccountToken
                }

                logger.info(f"🌐 [Webhook] Enviando orden {action.upper()} {symbol} ({size:,.0f} lotes) para cuenta {loginUsuario} ({nombreBroker})...")
                try:
                    resp = requests.post(webhookUrl, json=orderPayload, timeout=35)
                    if resp.status_code == 200:
                        resData = resp.json() if resp.text else {}
                        if resData.get("status") == "ignored":
                            logger.warning(f"⚠️ [Webhook] Orden ignorada por candado: {resData.get('message')}")
                            executionResults.append({
                                "idTrade": idTrade,
                                "symbol": symbol,
                                "action": action,
                                "success": False
                            })
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
                    else:
                        logger.error(f"❌ [Webhook] Error {resp.status_code} para {loginUsuario} en {symbol}: {resp.text}")
                        executionResults.append({
                            "idTrade": idTrade,
                            "symbol": symbol,
                            "action": action,
                            "success": False
                        })
                except Exception as exReq:
                    logger.error(f"❌ [Webhook] Excepción enviando orden a {webhookUrl} para {loginUsuario}: {exReq}")
                    executionResults.append({
                        "idTrade": idTrade,
                        "symbol": symbol,
                        "action": action,
                        "success": False
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
    3. Si la cuenta NO tiene broker en brokercuenta (cuenta interna/simulada):
       - Se inserta en trades con ticketId=NULL, se descuenta margen de cuenta.Capital y se envía alerta a Telegram.
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

    margen1LotA = minLotsA * margenRateA
    margen1LotB = minLotsB * margenRateB

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

    # CASO 1: Cuenta con Broker Conectado (FOREX.com / Webhook)
    if activeBrokers:
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
            chunkMargin = sz * rate

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
                        if resData.get("status") == "success" and resData.get("order_id"):
                            isConfirmed = True
                            orderId = str(resData.get("order_id"))
                            fillPrice = float(resData.get("fill_price") or px)
                            logger.info(f"✅ [FOREX] Lote {idx+1}/{totalLotsCount} confirmado -> OrderId: {orderId}, FillPrice: {fillPrice}")
                        elif resData.get("status") == "ignored":
                            logger.warning(f"⚠️ [FOREX] Orden ignorada por candado anti-spam: {resData.get('message')}")
                        else:
                            logger.error(f"❌ [FOREX] Error devuelto por FOREX.com: {resData}")
                    else:
                        logger.error(f"❌ [FOREX] Error HTTP {resp.status_code} al despachar lote: {resp.text}")
                except Exception as exReq:
                    logger.error(f"❌ [FOREX] Excepción de conexión enviando lote a {webhookUrl}: {exReq}")

                if isConfirmed and orderId:
                    resInsert = dbSession.execute(insertTradeSql, {
                        "idCuenta": idCuenta,
                        "setup": setupName,
                        "symbol": sym,
                        "direction": dirStr,
                        "intervalo": periodo,
                        "candleTime": candleDt,
                        "openTime": nowStr,
                        "size": sz,
                        "entryPrice": fillPrice,
                        "margin_used": chunkMargin,
                        "ticketId": orderId
                    })
                    tId = resInsert.lastrowid
                    insertedTrades.append(tId)

                    dbSession.execute(text("""
                        UPDATE cuenta
                        SET Capital = Capital - :chunkMargin
                        WHERE idCuenta = :idc
                    """), {"chunkMargin": chunkMargin, "idc": idCuenta})
                    dbSession.commit()

                    capRow = dbSession.execute(text("SELECT Capital FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).mappings().fetchone()
                    curCapital = float(capRow["Capital"]) if capRow else accountCapital

                    logger.info(f"💾 [BD] Trade #{tId} guardado exitosamente en BD (TicketId: {orderId}, Margen: ${chunkMargin:,.2f} USD).")

                    alertMsg = buildRatioLotEntryAlertMessage(
                        accountName=accountName,
                        numerador=numerador,
                        denominador=denominador,
                        periodo=periodo,
                        symbol=sym,
                        direction=dirStr,
                        size=sz,
                        fillPrice=fillPrice,
                        ticketId=orderId,
                        chunkMargin=chunkMargin,
                        currentLotIndex=idx + 1,
                        totalLots=totalLotsCount,
                        signalType=signalType,
                        accountCapitalRemaining=curCapital
                    )
                    try:
                        asyncio.run(sendRatioTelegramAlert(idCuenta, alertMsg))
                    except Exception as exTel:
                        logger.error(f"Error despachando alerta Telegram para lote #{tId}: {exTel}")
                else:
                    logger.error(f"⛔ [RECHAZADO] Lote {idx+1}/{totalLotsCount} ({act.upper()} {sym}, {sz:,.0f} u) NO confirmado por FOREX.com. NO se inserta en BD.")

    # CASO 2: Cuenta Interna / Simulada (Sin Broker en brokercuenta)
    else:
        logger.info(f"ℹ️ [Simulado/Interno] Cuenta #{idCuenta} ({accountName}) sin broker conectado. Registrando operaciones directamente en BD...")
        for idx, lot in enumerate(lotsToExecute):
            sym = lot["symbol"]
            dirStr = lot["direction"]
            sz = float(lot["size"])
            px = float(lot["entryPrice"])
            rate = float(lot["margenRate"])
            chunkMargin = sz * rate

            resInsert = dbSession.execute(insertTradeSql, {
                "idCuenta": idCuenta,
                "setup": setupName,
                "symbol": sym,
                "direction": dirStr,
                "intervalo": periodo,
                "candleTime": candleDt,
                "openTime": nowStr,
                "size": sz,
                "entryPrice": px,
                "margin_used": chunkMargin,
                "ticketId": None
            })
            tId = resInsert.lastrowid
            insertedTrades.append(tId)

            dbSession.execute(text("""
                UPDATE cuenta
                SET Capital = Capital - :chunkMargin
                WHERE idCuenta = :idc
            """), {"chunkMargin": chunkMargin, "idc": idCuenta})
            dbSession.commit()

            capRow = dbSession.execute(text("SELECT Capital FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).mappings().fetchone()
            curCapital = float(capRow["Capital"]) if capRow else accountCapital

            logger.info(f"💾 [BD] Trade simulado #{tId} ({dirStr} {sz:,.0f} {sym} @ {px}) guardado exitosamente en BD.")

            alertMsg = buildRatioLotEntryAlertMessage(
                accountName=accountName,
                numerador=numerador,
                denominador=denominador,
                periodo=periodo,
                symbol=sym,
                direction=dirStr,
                size=sz,
                fillPrice=px,
                ticketId="INTERNO",
                chunkMargin=chunkMargin,
                currentLotIndex=idx + 1,
                totalLots=totalLotsCount,
                signalType=signalType,
                accountCapitalRemaining=curCapital
            )
            try:
                asyncio.run(sendRatioTelegramAlert(idCuenta, alertMsg))
            except Exception as exTel:
                logger.error(f"Error despachando alerta Telegram para lote #{tId}: {exTel}")

    logger.info(f"🎯 [{setupName}] Secuencia de lotes finalizada para Cuenta #{idCuenta}. Total registros creados en BD: {len(insertedTrades)}/{totalLotsCount}.")
    return len(insertedTrades) > 0


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
    liquida el PnL neto por pips, calcula la comisión (%) sobre ganancias positivas y la guarda en trades.commission,
    reincorpora el margen y la ganancia/pérdida a 'cuenta.Capital', genera el registro de comisión en la cuenta
    concentradora del usuario que administra la cuenta y envía la alerta de cierre a Telegram y la orden de cierre al Webhook.
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
        exitPx = latestPricesMap.get(sym, entryPx)

        symInfo = symbolDataMap.get(sym, {"pip": 0.0001, "quoteCurrency": "USD"})
        pipVal = symInfo["pip"]
        quoteCurr = symInfo["quoteCurrency"]

        if direction.upper() in ["LARGO", "BUY", "LONG"]:
            delta = exitPx - entryPx
        else:
            delta = entryPx - exitPx

        pips = (delta / pipVal) if pipVal > 0 else 0.0

        if quoteCurr == "USD":
            pipMoneyValue = size * pipVal
        else:
            pipMoneyValue = (size * pipVal) / exitPx if exitPx > 0 else (size * pipVal)

        costs = (size * exitPx) * 0.0003
        tradePnl = round((pips * pipMoneyValue) - costs, 2)

        # Cálculo de comisión en $ sobre ganancia positiva sólo si la estrategia contiene 'ATALAia'
        tradeStrategy = str(tr.get("strategy") or "RATIO ATALAia")
        isAtalaiaStrategy = "ATALAIA" in tradeStrategy.upper()

        tradeCommission = 0.0
        if isAtalaiaStrategy and tradePnl > 0 and comision_pct > 0:
            tradeCommission = round(tradePnl * (comision_pct / 100.0), 2)

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
            strategyComision = f"{origStrategy} Comision"
            origSetup = tr.get("setup") or setupName

            try:
                dbSession.execute(text("""
                    INSERT IGNORE INTO strategyconfig (strategy, enabled, created_at, updated_at)
                    VALUES (:strategy, 1, NOW(), NOW())
                """), {"strategy": strategyComision})
            except Exception as exStrat:
                pass

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
                "openTime": openTimeMes,
                "closeTime": closeTimeMes,
                "candleTime": tr.get("candleTime") or openTimeMes,
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
        f"🎯 [{setupName}] CRUCE DE PRECIOS (●): {len(openTrades)} trades liquidados en Cuenta #{idCuenta} "
        f"| PnL Total:  USD | Comision Total:  USD "
        f"| Margen Reintegrado:  | Impacto Neto en Cuenta:  USD"
    )

    # Despachar Órdenes de Cierre al Webhook de Brókers (1 orden por símbolo único del ratio)
    webhookCloseOrders = []
    uniqueSymbols = list(dict.fromkeys([tr.get("symbol") for tr in openTrades if tr.get("symbol")]))
    for sym in uniqueSymbols:
        webhookCloseOrders.append({
            "strategy": "RATIO ATALAia",
            "symbol": sym,
            "action": "close",
            "size": 0.0,
            "entryPrice": 0.0
        })
    sendRatioWebhookOrders(dbSession, idCuenta, webhookCloseOrders)

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
    try:
        asyncio.run(sendRatioTelegramAlert(idCuenta, exitAlertMsg))
    except Exception as exTel:
        logger.error(f"Error despachando alerta Telegram de cierre para Cuenta #{idCuenta}: {exTel}")

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
    periodo = ratioRecord["periodo"]
    dias = ratioRecord["dias"] or 180
    emaRapida = ratioRecord["EMARapida"] or 2
    emaLenta = ratioRecord["EMALenta"] or 20

    # 2. Datos de símbolos y cuenta
    symDataA = fetchSymbolData(dbSession, numerador)
    symDataB = fetchSymbolData(dbSession, denominador)
    accountData = fetchAccountData(dbSession, idCuenta)
    accountName = accountData.get("nombre", f"Cuenta #{idCuenta}")
    accHeader = f"Cuenta #{idCuenta} ({accountName}) | {setupName} ({periodo})"

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
        includeBoxes=True
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

    logger.info(
        f"📊 [{accHeader}] EVALUACIÓN: "
        f"{numerador} [Px: {pA:.5f} | Norm: {nA:.4f} | EMA: {eA:.4f} | +1σ: {stdUpA:.4f} | -1σ: {stdDownA:.4f} | Sig: {hasSigA}] ⇄ "
        f"{denominador} [Px: {pB:.5f} | Norm: {nB:.4f} | EMA: {eB:.4f} | +1σ: {stdUpB:.4f} | -1σ: {stdDownB:.4f} | Sig: {hasSigB}] "
        f"| Coincidente (Triángulo): {hasBoth} | Cruce Precios (●): {isPriceCross}"
    )

    dbOpenTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)
    latestPriceA = float(dfATf["closePrice"].iloc[-1])
    latestPriceB = float(dfBTf["closePrice"].iloc[-1])

    # 4. EVALUACIÓN DE ENTRADA, REVERSAL (CAMBIO DE DIRECCIÓN) O ACUMULACIÓN
    sigTypeRaw = targetSignal.get("signalType")
    hasEntrySignal = bool(sigTypeRaw)
    latestSig = targetSignal

    if hasEntrySignal:
        entryDateStr = str(latestSig.get("date", "")).strip()
        candleDt = parseCandleDateTime(entryDateStr)
        
        # Para temporalidad horaria, redondear candleDt a la hora en punto para que el candado de 1 orden por hora sea exacto
        tf_clean = str(periodo).lower().strip()
        if tf_clean in ["1h", "1H"]:
            candleDt = candleDt.replace(minute=0, second=0, microsecond=0)
            entryDateStr = candleDt.strftime("%Y-%m-%d %H:00:00")
        elif tf_clean in ["1d", "1D"]:
            candleDt = candleDt.replace(hour=0, minute=0, second=0, microsecond=0)
            entryDateStr = candleDt.strftime("%Y-%m-%d")

        # Validación estricta por temporalidad: solo insertar si el cruce sucedió en esa hora / día
        if not isCandleSignalFresh(candleDt, periodo):
            logger.info(
                f"⏳ [{accHeader}] Cruce de fecha/hora {entryDateStr} no pertenece al periodo actual ({periodo}). "
                f"Solo se insertan cruces ocurridos en esa hora/día. Omitiendo entrada."
            )
            return

        sigType = latestSig.get("signalType", "TRIANGULO")
        dirA = "CORTO" if latestSig.get("symbolAAction") == "SELL" else "LARGO"
        dirB = "CORTO" if latestSig.get("symbolBAction") == "SELL" else "LARGO"

        # Verificar si hay posiciones abiertas en dirección contraria (REVERSAL / FLIP)
        if dbOpenTrades:
            firstTrade = dbOpenTrades[0]
            currDirA = firstTrade.get("direction", "")
            isReversal = (dirA != currDirA)

            if isReversal:
                logger.info(
                    f"🔄 [{accHeader}] ¡CAMBIO DE DIRECCIÓN DETECTADO ({sigType})! "
                    f"Cerrando {len(dbOpenTrades)} posiciones anteriores ({currDirA}) para girar a {dirA}..."
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

        alreadyEntered = checkTradeExistsForCandle(dbSession, idCuenta, setupName, candleDt, timeframe=periodo)
        if not alreadyEntered:
            # Control de Gestión de Riesgo: Indicador de Margen >= 200%
            canOperate, marginInd, totalMargin, equity = checkAccountMarginHealth(
                dbSession, idCuenta, float(accountData.get("capital", 0.0))
            )
            if not canOperate:
                logger.warning(
                    f"⛔ [{setupName}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: "
                    f"Indicador de Margen {marginInd:.1f}% es menor al 200% requerido "
                    f"(Capital: ${accountData.get('capital', 0.0):,.2f} | Margen: ${totalMargin:,.2f} | Equidad: ${equity:,.2f})."
                )
                return

            logger.info(
                f"🎯 [{setupName}] NUEVA SEÑAL ({sigType}, fecha {entryDateStr} | "
                f"Dirección: {numerador} {dirA} + {denominador} {dirB} | "
                f"Ind. Margen: {marginInd:.1f}% >= 200%). Registrando en trades..."
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
                tf = str(r.get("timeframe", "1h")).lower().strip()
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
