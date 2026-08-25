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
from datetime import datetime
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
    Obtiene los parámetros operativos de un símbolo en la tabla 'symbols'.
    """
    sqlQuery = text("""
        SELECT min_lots, margen, pip, quote_currency
        FROM symbols
        WHERE symbol = :sym
        LIMIT 1
    """)
    row = dbSession.execute(sqlQuery, {"sym": symbol}).fetchone()
    if row:
        margenRaw = float(row[1]) if row[1] is not None else 0.25
        margenRate = (margenRaw / 100.0) if margenRaw >= 0.05 else margenRaw
        return {
            "minLots": float(row[0]) if row[0] is not None else 1000.0,
            "margenRate": margenRate,
            "margenPct": margenRaw,
            "pip": float(row[2]) if row[2] is not None else 0.0001,
            "quoteCurrency": str(row[3]) if row[3] else "USD"
        }
    return {
        "minLots": 1000.0,
        "margenRate": 0.0025,
        "margenPct": 0.25,
        "pip": 0.0001,
        "quoteCurrency": "USD"
    }


def fetchAccountData(dbSession, idCuenta: int) -> Dict[str, Any]:
    """
    Obtiene el balance de capital actual y nombre de la cuenta.
    """
    sqlQuery = text("""
        SELECT idCuenta, Nombre, Capital, Activo
        FROM cuenta
        WHERE idCuenta = :idc
        LIMIT 1
    """)
    row = dbSession.execute(sqlQuery, {"idc": idCuenta}).fetchone()
    if row:
        return {
            "idCuenta": row[0],
            "nombre": str(row[1]),
            "capital": float(row[2]) if row[2] is not None else 0.0,
            "activo": bool(row[3])
        }
    return {
        "idCuenta": idCuenta,
        "nombre": f"Cuenta #{idCuenta}",
        "capital": 0.0,
        "activo": False
    }


def fetchPriceHistory(dbSession, symbol: str, daysLimit: int = 365) -> pd.DataFrame:
    """
    Carga el historial de precios desde la tabla 'stockprices' indexado por fecha.
    """
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


def checkTradeExistsForCandle(dbSession, idCuenta: int, setupName: str, candleDt: datetime) -> bool:
    """Verifica si ya existe un trade abierto registrado para la misma fecha de vela."""
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
        WHERE t.idCuenta = :idc AND t.status = OPEN
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
    totalMarginReleased: float
) -> str:
    """Construye el mensaje de liquidación por cruce de precios (convergencia) para Telegram."""
    pnlSign = "+" if totalNetPnl >= 0 else ""
    retPct = ((totalNetPnl / totalMarginReleased) * 100.0) if totalMarginReleased > 0 else 0.0
    retSign = "+" if retPct >= 0 else ""
    nowStr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "<center>🟥<b>ATALA.ia CIERRE POR CRUCE DE PRECIOS</b>🟥</center>",
        f"<center><b>   {numerador} ⇄ {denominador} ({periodo.upper()})</b></center>",
        f"<center><b>   {accountName} </b></center>",
        f"<center>{nowStr}</center>",
        "━━━━━━━━━━━━━━━━━━━━",
        "<center> <b>LIQUIDACIÓN POR CONVERGENCIA (●):</b></center>",
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
        from middleware.utils.cryptoUtils import buildEncryptedAccountToken
        
        webhookUrl = os.getenv("WEBHOOK_URL") or "http://127.0.0.1:8002/webhook/tradingview"
        passphrase = os.getenv("WEBHOOK_VERIFY_TOKEN", "")

        for bc in activeBrokers:
            loginUsuario = bc.get("loginUsuario", "")
            tokenAcceso = bc.get("tokenAcceso", "")
            nombreBroker = bc.get("nombreBroker", "Broker")

            encryptedAccountToken = buildEncryptedAccountToken(
                idCuenta=idCuenta,
                loginUsuario=loginUsuario,
                tokenAcceso=tokenAcceso
            )

            for ordInfo in orders:
                symbol = ordInfo.get("symbol")
                action = ordInfo.get("action")
                size = float(ordInfo.get("size", 0.0))
                entryPx = float(ordInfo.get("entryPrice", 0.0))
                strategy = ordInfo.get("strategy", "RATIO ATALAia")
                ticketId = ordInfo.get("ticketId")
                idTrade = ordInfo.get("idTrade")

                orderPayload = {
                    "strategy": strategy,
                    "passphrase": passphrase,
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
                    resp = requests.post(webhookUrl, json=orderPayload, timeout=30)
                    if resp.status_code == 200:
                        resData = resp.json() if resp.text else {}
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
    Registra EXACTAMENTE 2 registros (Pata 1: Numerador y Pata 2: Denominador) en 'trades',
    calcula el dimensionamiento proporcional exacto (3% del capital disponible de la cuenta),
    descuenta el margen retenido de 'cuenta.Capital' y envía la alerta de Cruces EMA por Telegram.
    """
    accountCapital = float(accountData.get("capital", 300.0))
    accountName = str(accountData.get("nombre", f"Cuenta #{idCuenta}"))

    budget = max(0.0, accountCapital) * 0.03
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
    marginA = multA * margen1LotA
    marginB = multB * margen1LotB
    totalMargin = marginA + marginB

    try:
        candleDt = datetime.strptime(str(candleDateStr)[:10], "%Y-%m-%d")
    except Exception:
        candleDt = datetime.now()

    nowStr = datetime.now()

    insertTradeSql = text("""
        INSERT INTO trades (
            idCuenta, strategy, setup, symbol, status, direction,
            intervalo, pnl, candleTime, openTime, size, entryPrice,
            stopLoss, takeProfit, margin_used
        ) VALUES (
            :idCuenta, :strategy, :setup, :symbol, :status, :direction,
            :intervalo, :pnl, :candleTime, :openTime, :size, :entryPrice,
            :stopLoss, :takeProfit, :margin_used
        )
    """)

    # 1. Pata 1: Numerador
    resA = dbSession.execute(insertTradeSql, {
        "idCuenta": idCuenta,
        "strategy": "RATIO ATALAia",
        "setup": setupName,
        "symbol": numerador,
        "status": "OPEN",
        "direction": directionA,
        "intervalo": periodo,
        "pnl": 0.0,
        "candleTime": candleDt,
        "openTime": nowStr,
        "size": unitsA,
        "entryPrice": entryPxA,
        "stopLoss": None,
        "takeProfit": None,
        "margin_used": marginA
    })
    idTradeA = resA.lastrowid

    # 2. Pata 2: Denominador
    resB = dbSession.execute(insertTradeSql, {
        "idCuenta": idCuenta,
        "strategy": "RATIO ATALAia",
        "setup": setupName,
        "symbol": denominador,
        "status": "OPEN",
        "direction": directionB,
        "intervalo": periodo,
        "pnl": 0.0,
        "candleTime": candleDt,
        "openTime": nowStr,
        "size": unitsB,
        "entryPrice": entryPxB,
        "stopLoss": None,
        "takeProfit": None,
        "margin_used": marginB
    })
    idTradeB = resB.lastrowid

    # 3. Descontar margen retenido de cuenta.Capital
    updateCapitalSql = text("""
        UPDATE cuenta
        SET Capital = Capital - :margenRetenido
        WHERE idCuenta = :idc
    """)
    dbSession.execute(updateCapitalSql, {
        "margenRetenido": totalMargin,
        "idc": idCuenta
    })
    dbSession.commit()

    # 4. Despachar Órdenes al Webhook y recibir ID de Transacción (ticketId) y Precio Real
    webhookOrders = [
        {
            "idTrade": idTradeA,
            "strategy": "RATIO ATALAia",
            "symbol": numerador,
            "action": "buy" if directionA == "LARGO" else "sell",
            "size": unitsA,
            "entryPrice": entryPxA
        },
        {
            "idTrade": idTradeB,
            "strategy": "RATIO ATALAia",
            "symbol": denominador,
            "action": "buy" if directionB == "LARGO" else "sell",
            "size": unitsB,
            "entryPrice": entryPxB
        }
    ]
    execResults = sendRatioWebhookOrders(dbSession, idCuenta, webhookOrders)

    # 5. Corregir precio real de ejecución y guardar TicketID en la BD MySQL
    realPxA, realPxB = entryPxA, entryPxB
    for res in execResults:
        tId = res.get("idTrade")
        ordId = res.get("order_id")
        fPx = res.get("fill_price")
        if tId and (ordId or fPx):
            updateTradeSyncSql = text("""
                UPDATE trades
                SET ticketId = COALESCE(:ticketId, ticketId),
                    entryPrice = COALESCE(:fillPrice, entryPrice)
                WHERE idTrade = :idt
            """)
            dbSession.execute(updateTradeSyncSql, {
                "ticketId": str(ordId) if ordId else None,
                "fillPrice": float(fPx) if fPx else None,
                "idt": tId
            })
            if tId == idTradeA and fPx:
                realPxA = float(fPx)
            elif tId == idTradeB and fPx:
                realPxB = float(fPx)

    dbSession.commit()
    logger.info(
        f"🚀 [{setupName}] SEÑAL EJECUTADA (2 REGISTROS): {signalType} en Cuenta #{idCuenta} "
        f"| Pata 1 ({numerador}, Trade #{idTradeA}): {directionA} {unitsA:,.0f} lotes @ {realPxA} (Margen: ${marginA:,.2f}) "
        f"| Pata 2 ({denominador}, Trade #{idTradeB}): {directionB} {unitsB:,.0f} lotes @ {realPxB} (Margen: ${marginB:,.2f}) "
        f"| Margen Total Retenido: ${totalMargin:,.2f} USD (Cap. restante: ${(accountCapital - totalMargin):,.2f})"
    )

    # 5. Despachar Alerta por Telegram
    alertMsg = buildRatioEntryAlertMessage(
        accountName=accountName,
        accountCapital=accountCapital,
        numerador=numerador,
        denominador=denominador,
        periodo=periodo,
        signalType=signalType,
        directionA=directionA,
        directionB=directionB,
        unitsA=unitsA,
        unitsB=unitsB,
        priceA=entryPxA,
        priceB=entryPxB,
        marginA=marginA,
        marginB=marginB,
        totalMargin=totalMargin,
        entryDate=str(candleDateStr)[:10]
    )
    try:
        asyncio.run(sendRatioTelegramAlert(idCuenta, alertMsg))
    except Exception as exTel:
        logger.error(f"Error despachando alerta Telegram para Cuenta #{idCuenta}: {exTel}")

    return True


def closeRatioTrades(
    dbSession,
    idCuenta: int,
    setupName: str,
    openTrades: List[Dict[str, Any]],
    symbolDataMap: Dict[str, Dict[str, Any]],
    latestPricesMap: Dict[str, float],
    accountData: Dict[str, Any],
    periodo: str = "1d"
) -> bool:
    """
    Cierra todas las operaciones abiertas del setup al ocurrir el cruce de precios (●),
    liquida el PnL neto por pips, reincorpora el margen y la ganancia/pérdida a 'cuenta.Capital'
    y envía la alerta de cierre a Telegram y la orden de cierre al Webhook.
    """
    if not openTrades:
        return False

    nowStr = datetime.now()
    totalMarginToRelease = 0.0
    totalNetPnl = 0.0
    pnlA, pnlB = 0.0, 0.0
    pipsA, pipsB = 0.0, 0.0
    dirA, dirB = "", ""
    numerador, denominador = setupName.split(" - ")[0], setupName.split(" - ")[1]

    updateTradeSql = text("""
        UPDATE trades
        SET status = 'CLOSED',
            exitPrice = :exitPrice,
            closeTime = :closeTime,
            pnl = :pnl
        WHERE idTrade = :idt
    """)

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
            "idt": tradeId
        })

        totalMarginToRelease += marginUsed
        totalNetPnl += tradePnl

        logger.info(
            f"   ● Trade #{tradeId} [{sym}] CERRADO @ {exitPx} "
            f"| PnL: ${tradePnl:+,.2f} USD ({pips:+,.1f} pips) | Margen Liberado: ${marginUsed:,.2f}"
        )

    netReintegration = totalMarginToRelease + totalNetPnl
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
        f"| PnL Total: ${totalNetPnl:+,.2f} USD | Margen Reintegrado: ${totalMarginToRelease:,.2f} "
        f"| Impacto Neto en Cuenta: ${netReintegration:+,.2f} USD"
    )

    # Despachar Órdenes de Cierre al Webhook de Brókers
    webhookCloseOrders = []
    for tr in openTrades:
        webhookCloseOrders.append({
            "strategy": "RATIO ATALAia",
            "symbol": tr.get("symbol"),
            "action": "close",
            "size": float(tr.get("size", 0.0)),
            "entryPrice": float(tr.get("entryPrice", 0.0))
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
        totalMarginReleased=totalMarginToRelease
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

    # 1. Historial de precios
    dfA = fetchPriceHistory(dbSession, numerador, daysLimit=max(dias, 180))
    dfB = fetchPriceHistory(dbSession, denominador, daysLimit=max(dias, 180))

    if dfA.empty or dfB.empty:
        logger.warning(f"⚠️ [{setupName}] Historial insuficiente para {numerador} o {denominador}.")
        return

    dfATf = resamplePriceData(dfA, periodo).tail(dias)
    dfBTf = resamplePriceData(dfB, periodo).tail(dias)

    # 2. Datos de símbolos y cuenta
    symDataA = fetchSymbolData(dbSession, numerador)
    symDataB = fetchSymbolData(dbSession, denominador)
    accountData = fetchAccountData(dbSession, idCuenta)

    # 3. Evaluación matemática con signalEngine
    evalResult = signalEngine.evaluateRatioSignals(
        dfA=dfATf,
        dfB=dfBTf,
        pairA=numerador,
        pairB=denominador,
        smaPeriod=emaRapida,
        sigmaWindow=emaLenta,
        includeBoxes=False
    )

    normInfo = evalResult.get("normData", {})
    latestSig = evalResult.get("latestSignal", {})

    if not latestSig:
        logger.warning(f"⚠️ [{setupName}] No se obtuvieron señales del motor signalEngine.")
        return

    pA = latestSig.get("priceA", 0.0)
    pB = latestSig.get("priceB", 0.0)
    nA = latestSig.get("normA", 0.0)
    nB = latestSig.get("normB", 0.0)
    eA = latestSig.get("emaA", 0.0)
    eB = latestSig.get("emaB", 0.0)
    stdUpA = normInfo.get("stdAboveA", 0.0)
    stdDownA = normInfo.get("stdBelowA", 0.0)
    stdUpB = normInfo.get("stdAboveB", 0.0)
    stdDownB = normInfo.get("stdBelowB", 0.0)
    hasSigA = latestSig.get("hasSignalA", False)
    hasSigB = latestSig.get("hasSignalB", False)
    hasBoth = latestSig.get("hasSignalBoth", False)
    isPriceCross = latestSig.get("isPriceCross", False)

    logger.info(
        f"📊 [{setupName}] EVALUACIÓN: "
        f"{numerador} [Px: {pA:.5f} | Norm: {nA:.4f} | EMA: {eA:.4f} | +1σ: {stdUpA:.4f} | -1σ: {stdDownA:.4f} | Sig: {hasSigA}] ⇄ "
        f"{denominador} [Px: {pB:.5f} | Norm: {nB:.4f} | EMA: {eB:.4f} | +1σ: {stdUpB:.4f} | -1σ: {stdDownB:.4f} | Sig: {hasSigB}] "
        f"| Coincidente (Triángulo): {hasBoth} | Cruce Precios (●): {isPriceCross}"
    )

    dbOpenTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)
    latestPriceA = float(dfATf["closePrice"].iloc[-1])
    latestPriceB = float(dfBTf["closePrice"].iloc[-1])

    # 4. EVALUACIÓN DE CIERRE: ¿Ocurrió el cruce entre los dos precios (convergencia ●)?
    if dbOpenTrades and isPriceCross:
        logger.info(f"🔄 [{setupName}] ¡CRUCE DE PRECIOS CONFIRMADO (●)! Cerrando {len(dbOpenTrades)} posiciones abiertas en BD...")
        symMap = {numerador: symDataA, denominador: symDataB}
        priceMap = {numerador: latestPriceA, denominador: latestPriceB}
        closeRatioTrades(dbSession, idCuenta, setupName, dbOpenTrades, symMap, priceMap, accountData, periodo)
        return

    # 5. EVALUACIÓN DE ENTRADA: Permite acumulación de posiciones si hay nueva señal coincidente en nueva vela
    if hasBoth:
        entryDateStr = str(latestSig.get("date", ""))[:10]
        try:
            candleDt = datetime.strptime(entryDateStr, "%Y-%m-%d")
        except Exception:
            candleDt = datetime.now()

        alreadyEntered = checkTradeExistsForCandle(dbSession, idCuenta, setupName, candleDt)
        if not alreadyEntered:
            # Control de Gestión de Riesgo: Indicador de Margen >= 200%
            canOperate, marginInd, totalMargin, equity = checkAccountMarginHealth(
                dbSession, idCuenta, float(accountData.get("capital", 0.0))
            )
            if not canOperate:
                logger.warning(
                    f"⛔ [{setupName}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: "
                    f"Indicador de Margen {marginInd:.1f}% es menor al 200% requerido "
                    f"(Capital: ${accountData.get("capital", 0.0):,.2f} | Margen: ${totalMargin:,.2f} | Equidad: ${equity:,.2f})."
                )
                return

            sigType = latestSig.get("signalType", "TRIANGULO")
            dirA = "CORTO" if latestSig.get("symbolAAction") == "SELL" else "LARGO"
            dirB = "CORTO" if latestSig.get("symbolBAction") == "SELL" else "LARGO"

            logger.info(
                f"🎯 [{setupName}] NUEVA SEÑAL COINCIDENTE ({sigType}, fecha {entryDateStr} | "
                f"Ind. Margen: {marginInd:.1f}% >= 200%). Generando 2 registros en trades..."
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
            logger.info(f"ℹ️ [{setupName}] Señal coincidente de fecha {entryDateStr} ya fue registrada previamente en trades.")
    else:
        if dbOpenTrades:
            logger.info(
                f"⏳ [{setupName}] Posición activa en curso ({len(dbOpenTrades)} trades en Cuenta #{idCuenta}). "
                f"Precios normalizados: {numerador}={nA:.4f} vs {denominador}={nB:.4f} (Dif: {abs(nA - nB):.4f}). Esperando cruce de precios (●)."
            )
        else:
            logger.info(f"🔍 [{setupName}] Sin señales de entrada coincidentes (Triángulos) en la vela actual.")


def processAllActiveRatios() -> int:
    """
    Punto de entrada principal para un ciclo de evaluación de los ratios activos con operar=1.
    """
    logger.info("=================================================================")
    logger.info("📡 [microRatio] INICIANDO CICLO HORARIO DE EVALUACIÓN DE RATIOS...")
    startTime = time.time()
    processedCount = 0

    with getDatabaseSession() as dbSession:
        try:
            ensureStrategyRegistered(dbSession)
            activeRatios = fetchActiveUserRatios(dbSession)
            logger.info(f"📋 Ratios activos encontrados con operar=1: {len(activeRatios)}")

            for r in activeRatios:
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


async def runHourlyScheduler():
    """
    Bucle asíncrono para ejecutar la evaluación cada hora en punto.
    """
    logger.info("🚀 [microRatio] Demonio de ejecución horaria iniciado.")
    while True:
        try:
            processAllActiveRatios()
        except Exception as e:
            logger.error(f"Error en ejecución horaria de microRatio: {e}", exc_info=True)

        now = datetime.now()
        secondsUntilNextHour = (60 - now.minute - 1) * 60 + (60 - now.second)
        logger.info(f"💤 Próxima evaluación en {secondsUntilNextHour // 60}m {secondsUntilNextHour % 60}s (en la siguiente hora en punto).")
        await asyncio.sleep(max(10, secondsUntilNextHour))


def main():
    parser = argparse.ArgumentParser(description="Microservicio microRatio para arbitraje de ratios EMA.")
    parser.add_argument("--once", action="store_true", help="Ejecuta una sola iteración y termina.")
    args = parser.parse_args()

    if args.once:
        logger.info("▶️ Modo de ejecución única activado (--once).")
        processAllActiveRatios()
    else:
        try:
            asyncio.run(runHourlyScheduler())
        except KeyboardInterrupt:
            logger.info("🛑 Detención manual del microservicio microRatio.")


if __name__ == "__main__":
    main()
