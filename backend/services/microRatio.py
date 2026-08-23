#!/usr/bin/env python3
"""
Microservicio: microRatio.py
===========================
Evalúa periódicamente (cada hora) las combinaciones activas en 'user_ratios'
con 'operar = 1' para la cuenta correspondiente.
Al detectar señales de Análisis de Cruces EMA (Triángulos, Cuadros y Círculo de Media),
genera EXACTAMENTE 2 registros (las 2 patas: Numerador y Denominador) en la tabla 'trades',
calcula el dimensionamiento real (3% del capital de la cuenta en múltiplos de min_lots),
descuenta el margen retenido de 'cuenta.Capital' y despacha la alerta de Cruces EMA por Telegram.

Estrategia: RATIO ATALAia
Setup: {numerador} - {denominador} (ej: USD/MXN - GBP/USD)
Patas: 
  - Pata 1: Símbolo Numerador
  - Pata 2: Símbolo Denominador
"""

import os
import sys
import time
import math
import asyncio
import argparse
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
import numpy as np
from sqlalchemy import text

# Configurar path raíz del proyecto
projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from backend.database.models import SessionLocal
from backend.services.quant_pair_engine import quantEngine
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
            "EMARapida": int(r[7]) if r[7] else 3,
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


def checkActiveOpenTrades(dbSession, idCuenta: int, setupName: str) -> List[Dict[str, Any]]:
    """
    Verifica si existen órdenes abiertas para el setup y cuenta en la tabla 'trades'.
    """
    sqlQuery = text("""
        SELECT idTrade, idCuenta, strategy, setup, symbol, status, direction, size, entryPrice, margin_used, candleTime, openTime
        FROM trades
        WHERE idCuenta = :idc AND strategy = 'RATIO ATALAia' AND setup = :setup AND status = 'OPEN'
        ORDER BY idTrade ASC
    """)
    rows = dbSession.execute(sqlQuery, {"idc": idCuenta, "setup": setupName}).fetchall()
    openTrades = []
    for r in rows:
        openTrades.append({
            "idTrade": r[0],
            "idCuenta": r[1],
            "strategy": r[2],
            "setup": r[3],
            "symbol": r[4],
            "status": r[5],
            "direction": r[6],
            "size": float(r[7]) if r[7] is not None else 0.0,
            "entryPrice": float(r[8]) if r[8] is not None else 0.0,
            "margin_used": float(r[9]) if r[9] is not None else 0.0,
            "candleTime": r[10],
            "openTime": r[11]
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
    entryDate: str = ""
) -> str:
    """Construye el mensaje de alerta de entrada para Telegram."""
    dirBadgeA = "🟢 COMPRA" if directionA == "LARGO" else "🔴 VENTA"
    dirBadgeB = "🟢 COMPRA" if directionB == "LARGO" else "🔴 VENTA"
    
    if any(s in signalType for s in ["▲", "▼", "■", "●"]):
        sigEmoji = ""
    elif "CUADRO" in signalType:
        sigEmoji = "■ "
    elif "VERDE" in signalType:
        sigEmoji = "▲ "
    else:
        sigEmoji = "▼ "

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entry_date_display = entryDate if entryDate else now_str[:10]
    lines = [
        "<center>🟩🟥🟩 <b>ATALA.ia CRUCE EMA</b> 🟩🟥🟩</center>",
        f"<center><b>   {numerador} ⇄ {denominador} ({periodo.upper()})</b></center>",
        f"<center><b>   {accountName} (${accountCapital:,.2f} USD)</b></center>",
        f"<center>{now_str}</center>",
        "━━━━━━━━━━━━━━━━━",   
        f"<center>Fecha del cruce: <b>{entry_date_display}</b></center>",
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
    """Construye el mensaje de liquidación y cierre en media para Telegram."""
    pnlSign = "+" if totalNetPnl >= 0 else ""
    retPct = ((totalNetPnl / totalMarginReleased) * 100.0) if totalMarginReleased > 0 else 0.0
    retSign = "+" if retPct >= 0 else ""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = [
        "<center>🟩🟥🟩 <b>ATALA.ia CIERRE CRUCE EMA </b> 🟩🟥🟩</center>",
        f"<center><b>   {numerador} ⇄ {denominador} ({periodo.upper()})</b></center>",
        f"<center><b>   {accountName} </b></center>",
        f"<center>{now_str}</center>",
        "━━━━━━━━━━━━━━━━━━━━",
        "<center> <b>LIQUIDACIÓN POR PATA:</b></center>",
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


def openSingleRatioTradePair(
    dbSession,
    idCuenta: int,
    setupName: str,
    numerador: str,
    denominador: str,
    periodo: str,
    op: Dict[str, Any],
    accountData: Dict[str, Any],
    symDataA: Dict[str, Any],
    symDataB: Dict[str, Any]
) -> bool:
    """
    Registra EXACTAMENTE 2 registros (Pata 1: Numerador y Pata 2: Denominador) en 'trades',
    calcula el dimensionamiento proporcional exacto (3% del capital disponible de la cuenta),
    descuenta el margen retenido de 'cuenta.Capital' y envía la alerta de Cruces EMA por Telegram.
    """
    dirStr = str(op.get("direction", ""))
    if "SHORT " + numerador in dirStr or "SHORT A" in dirStr:
        directionA = "CORTO"
        directionB = "LARGO"
    else:
        directionA = "LARGO"
        directionB = "CORTO"

    # Dimensionamiento real basado en el 3% del capital real de la cuenta
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

    entryPxA = float(op.get("entryPriceA", 0.0))
    entryPxB = float(op.get("entryPriceB", 0.0))
    candleDateStr = str(op.get("entryDate", ""))[:19]
    try:
        candleDt = datetime.strptime(candleDateStr[:10], "%Y-%m-%d")
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
    dbSession.execute(insertTradeSql, {
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

    # 2. Pata 2: Denominador
    dbSession.execute(insertTradeSql, {
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
    logger.info(
        f"🚀 [{setupName}] SEÑAL EJECUTADA (2 REGISTROS): {op.get('signalType')} en Cuenta #{idCuenta} "
        f"| Pata 1 ({numerador}): {directionA} {unitsA:,.0f} lotes @ {entryPxA} (Margen: ${marginA:,.2f}) "
        f"| Pata 2 ({denominador}): {directionB} {unitsB:,.0f} lotes @ {entryPxB} (Margen: ${marginB:,.2f}) "
        f"| Margen Total Retenido: ${totalMargin:,.2f} USD (Cap. restante: ${(accountCapital - totalMargin):,.2f})"
    )

    # 4. Despachar Alerta por Telegram
    alertMsg = buildRatioEntryAlertMessage(
        accountName=accountName,
        accountCapital=accountCapital,
        numerador=numerador,
        denominador=denominador,
        periodo=periodo,
        signalType=str(op.get("signalType", "SEÑAL DE CRUCE EMA")),
        directionA=directionA,
        directionB=directionB,
        unitsA=unitsA,
        unitsB=unitsB,
        priceA=entryPxA,
        priceB=entryPxB,
        marginA=marginA,
        marginB=marginB,
        totalMargin=totalMargin,
        entryDate=candleDateStr[:10]
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
    Cierra todas las operaciones abiertas del setup al llegar al Círculo de Media (●),
    liquida el PnL neto por pips, reincorpora el margen y la ganancia/pérdida a 'cuenta.Capital'
    y envía la alerta de cierre a Telegram.
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
        f"🎯 [{setupName}] CRUCE DE MEDIA (●): {len(openTrades)} trades liquidados en Cuenta #{idCuenta} "
        f"| PnL Total: ${totalNetPnl:+,.2f} USD | Margen Reintegrado: ${totalMarginToRelease:,.2f} "
        f"| Impacto Neto en Cuenta: ${netReintegration:+,.2f} USD"
    )

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
    Genera EXACTAMENTE 2 registros para la señal activa más reciente.
    """
    idCuenta = ratioRecord["idCuenta"]
    numerador = ratioRecord["numerador"]
    denominador = ratioRecord["denominador"]
    setupName = f"{numerador} - {denominador}"
    periodo = ratioRecord["periodo"]
    dias = ratioRecord["dias"] or 180
    emaRapida = ratioRecord["EMARapida"] or 3
    emaLenta = ratioRecord["EMALenta"] or 20

    # 1. Historial de precios
    dfA = fetchPriceHistory(dbSession, numerador, daysLimit=max(dias, 180))
    dfB = fetchPriceHistory(dbSession, denominador, daysLimit=max(dias, 180))

    if dfA.empty or dfB.empty:
        logger.warning(f"⚠️ [{setupName}] Historial insuficiente para {numerador} o {denominador}.")
        return

    dfA_tf = resamplePriceData(dfA, periodo).tail(dias)
    dfB_tf = resamplePriceData(dfB, periodo).tail(dias)

    # 2. Datos de símbolos y cuenta
    symDataA = fetchSymbolData(dbSession, numerador)
    symDataB = fetchSymbolData(dbSession, denominador)
    accountData = fetchAccountData(dbSession, idCuenta)
    capitalActual = accountData.get("capital", 300.0)

    # 3. Ejecutar Backtest del Ciclo de Cruces EMA
    bt = quantEngine.runSignalBacktest(
        dfA_tf, dfB_tf,
        pairA=numerador, pairB=denominador,
        smaPeriod=emaRapida, sigmaWindow=emaLenta,
        includeBoxes=True, initialCapital=capitalActual,
        minLotsA=symDataA["minLots"], minLotsB=symDataB["minLots"],
        margenPctA=symDataA["margenPct"], margenPctB=symDataB["margenPct"]
    )

    activeCycleTrades = [t for t in bt["trades"] if t.get("isOpen") and not t.get("isSubtotal")]
    dbOpenTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)

    latestPriceA = float(dfA_tf["closePrice"].iloc[-1])
    latestPriceB = float(dfB_tf["closePrice"].iloc[-1])

    # Caso A: Si el ciclo ya cerró en media (●) pero en BD hay órdenes abiertas
    if not activeCycleTrades and dbOpenTrades:
        logger.info(f"🔄 [{setupName}] Ciclo completado en la media (●). Cerrando {len(dbOpenTrades)} posiciones abiertas en BD...")
        symMap = {numerador: symDataA, denominador: symDataB}
        priceMap = {numerador: latestPriceA, denominador: latestPriceB}
        closeRatioTrades(dbSession, idCuenta, setupName, dbOpenTrades, symMap, priceMap, accountData, periodo)
        return

    # Caso B: Hay operaciones activas en el ciclo -> Tomar ÚNICAMENTE la señal activa más reciente
    if activeCycleTrades:
        latestOp = activeCycleTrades[-1]
        entryDateStr = str(latestOp.get("entryDate", ""))[:10]
        
        # Verificar si la señal activa más reciente ya está en trades
        isAlreadyInDb = any(
            str(tr["candleTime"])[:10] == entryDateStr for tr in dbOpenTrades
        )

        if not isAlreadyInDb and not dbOpenTrades:
            logger.info(f"📊 [{setupName}] Señal activa detectada para Cuenta #{idCuenta} ({latestOp.get('signalType')}, fecha {entryDateStr}). Generando 2 registros...")
            openSingleRatioTradePair(
                dbSession=dbSession,
                idCuenta=idCuenta,
                setupName=setupName,
                numerador=numerador,
                denominador=denominador,
                periodo=periodo,
                op=latestOp,
                accountData=accountData,
                symDataA=symDataA,
                symDataB=symDataB
            )
        else:
            logger.info(f"ℹ️ [{setupName}] Posición activa ya registrada en trades ({len(dbOpenTrades)} registros presentes en Cuenta #{idCuenta}). Sin nuevas entradas.")
    else:
        logger.info(f"🔍 [{setupName}] Sin operaciones activas en el ciclo actual para Cuenta #{idCuenta}.")


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
        secondsToNextHour = 3600 - (now.minute * 60 + now.second)
        logger.info(f"⏳ Próxima evaluación en {secondsToNextHour // 60} min {secondsToNextHour % 60} seg...")
        await asyncio.sleep(secondsToNextHour)


def main():
    """Manejo de argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(description="Microservicio de Señales y Operación de Ratios ATALAia")
    parser.add_argument("--once", "--run-now", action="store_true", help="Ejecuta un único ciclo de evaluación y termina.")
    args = parser.parse_args()

    if args.once:
        logger.info("Modo de ejecución única activado (--once).")
        processAllActiveRatios()
    else:
        asyncio.run(runHourlyScheduler())


if __name__ == "__main__":
    main()
