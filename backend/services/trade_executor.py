#!/usr/bin/env python3
"""
Módulo Centralizado de Ejecución de Órdenes e Integridad Transaccional
======================================================================
Servicio unificado para todas las estrategias de ATALA.ia (RATIO, CART, SNIPER):
  1. Validación de Salud y Proyección de Margen Mínimo (%) configurable por cuenta.
  2. Resolución y autenticación cifrada por cuenta en brokercuenta (FOREX.com / MT5).
  3. Despacho estandarizado de órdenes al Webhook con confirmación síncrona.
  4. Prevención estricta de órdenes huérfanas:
     - Mapeo 1 a 1 de sub-órdenes por ticket en MySQL 'trades'.
     - Mecanismo de Rollback automático en bróker si una secuencia falla.
  5. Sincronización atómica de Capital y Margen en MySQL 'cuenta'.
  6. Despacho unificado de alertas institucionales por Telegram.
"""

import os
import sys
import time
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple

import requests
from sqlalchemy import text

from backend.core.pnl_calculator import calculate_unrealized_pnl, calculate_margin
from middleware.utils.cryptoUtils import buildEncryptedAccountToken, encryptValue
from middleware.utils.communications import alertaInmediata

logger = logging.getLogger("trade_executor")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] [TradeExecutor] %(message)s"))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


# ============================================================================
# 1. SALUD Y PROYECCIÓN DE MARGEN MÍNIMO (%) (TODAS LAS ESTRATEGIAS)
# ============================================================================

def check_account_margin_health(
    dbSession,
    idCuenta: int,
    accountCapital: float
) -> Tuple[bool, float, float, float, float]:
    """
    Evalúa la salud de margen de la cuenta considerando todas las operaciones abiertas.
    Calcula:
      - totalMargin: suma de margin_used de todos los trades OPEN
      - totalFloatingPnl: suma de PnL no realizado de todos los trades OPEN
      - equity: accountCapital + totalFloatingPnl
      - marginIndicator: (equity / totalMargin) * 100.0 si totalMargin > 0 sino 9999.0
      - minMarginRequired: cuenta.indicadorMargen (por defecto 200.0%)

    Retorna: (canOperate: bool, marginIndicator: float, totalMargin: float, equity: float, minMarginRequired: float)
    """
    sqlTrades = text("""
        SELECT t.idTrade, t.symbol, t.direction, t.size, t.entryPrice, t.margin_used,
               s.pip, s.quote_currency
        FROM trades t
        LEFT JOIN symbols s ON UPPER(REPLACE(t.symbol, '/', '')) = UPPER(REPLACE(s.symbol, '/', ''))
        WHERE t.idCuenta = :idc AND t.status = 'OPEN'
    """)
    rows = dbSession.execute(sqlTrades, {"idc": idCuenta}).fetchall()

    sqlIndMargen = text("SELECT COALESCE(indicadorMargen, 200.0), COALESCE(Capital, :cap) FROM cuenta WHERE idCuenta = :idc LIMIT 1")
    rowInd = dbSession.execute(sqlIndMargen, {"idc": idCuenta, "cap": accountCapital}).fetchone()
    minMarginRequired = float(rowInd[0]) if rowInd and rowInd[0] is not None else 200.0
    effectiveCapital = float(rowInd[1]) if rowInd and rowInd[1] is not None else accountCapital

    if not rows:
        return True, 9999.0, 0.0, effectiveCapital, minMarginRequired

    totalMargin = 0.0
    totalFloatingPnl = 0.0
    symbolPrices = {}

    for r in rows:
        sym = str(r[1])
        direction = str(r[2]).upper()
        size = float(r[3] or 0.0)
        entryPx = float(r[4] or 0.0)
        marginUsed = float(r[5] or 0.0)
        pipVal = float(r[6]) if r[6] is not None and float(r[6]) > 0 else 0.0001

        totalMargin += marginUsed

        if sym not in symbolPrices:
            sqlPx = text("SELECT closePrice FROM stockprices WHERE symbol = :sym ORDER BY priceDate DESC LIMIT 1")
            pxRow = dbSession.execute(sqlPx, {"sym": sym}).fetchone()
            symbolPrices[sym] = float(pxRow[0]) if pxRow and pxRow[0] is not None else entryPx

        currentPx = symbolPrices[sym]
        tradePnl, _pips, _pipMoney, _mov = calculate_unrealized_pnl(
            symbol=sym,
            direction=direction,
            entry_price=entryPx,
            current_price=currentPx,
            size=size,
            pip_size=pipVal
        )
        totalFloatingPnl += tradePnl

    equity = effectiveCapital + totalFloatingPnl
    marginIndicator = (equity / totalMargin * 100.0) if totalMargin > 0 else 9999.0
    canOperate = bool(marginIndicator >= minMarginRequired)

    return canOperate, marginIndicator, totalMargin, equity, minMarginRequired


def check_projected_margin(
    equity: float,
    totalMargin: float,
    newMargin: float,
    minMarginRequired: float
) -> Tuple[bool, float]:
    """
    Verifica si una nueva operación mantendría el Indicador de Margen por encima del umbral mínimo requerido.
    Retorna: (isAllowed: bool, projectedIndicator: float)
    """
    if equity <= 0:
        return False, 0.0

    projectedTotalMargin = totalMargin + newMargin
    if projectedTotalMargin <= 0:
        return True, 9999.0

    projectedIndicator = (equity / projectedTotalMargin) * 100.0
    availableEquity = max(0.0, equity - totalMargin)
    isAllowed = (newMargin <= availableEquity) and (projectedIndicator >= minMarginRequired)
    return isAllowed, projectedIndicator


# ============================================================================
# 2. RESOLUCIÓN DE BRÓKER Y CANALES CIFRADOS (FOREX.com / MT5)
# ============================================================================

def get_account_broker(dbSession, idCuenta: int) -> Optional[Dict[str, Any]]:
    """
    Consulta la cuenta de bróker activa en 'brokercuenta'.
    Retorna credenciales con token cifrado 'FOREX_USERNAME' o None si no tiene bróker activo.
    """
    query = text("""
        SELECT bc.idBrokerCuenta, bc.idCuenta, bc.loginUsuario, bc.tokenAcceso, bc.activo,
               b.nombre AS nombreBroker, b.idBroker
        FROM brokercuenta bc
        JOIN broker b ON b.idBroker = bc.idBroker
        WHERE bc.idCuenta = :idc AND bc.activo = 1
        LIMIT 1
    """)
    bcRow = dbSession.execute(query, {"idc": idCuenta}).mappings().fetchone()
    if not bcRow:
        return None

    loginUsuario = bcRow.get("loginUsuario", "")
    tokenAcceso = bcRow.get("tokenAcceso", "")
    encryptedToken = buildEncryptedAccountToken(
        idCuenta=idCuenta,
        loginUsuario=loginUsuario,
        tokenAcceso=tokenAcceso
    )

    return {
        "idBrokerCuenta": bcRow.get("idBrokerCuenta"),
        "idCuenta": idCuenta,
        "loginUsuario": loginUsuario,
        "tokenAcceso": tokenAcceso,
        "nombreBroker": bcRow.get("nombreBroker", "Broker"),
        "idBroker": bcRow.get("idBroker"),
        "encryptedAccountToken": encryptedToken
    }


# ============================================================================
# 3. ENVÍO DE ÓRDENES AL WEBHOOK (CANAL HOMOLOGADO)
# ============================================================================

def send_broker_order(
    strategy: str,
    setup: str,
    action: str,
    symbol: str,
    quantity: float,
    price: float = 0.0,
    sl: float = 0.0,
    tp: float = 0.0,
    ticketId: Optional[str] = None,
    idTrade: Optional[int] = None,
    encryptedAccountToken: str = "",
    comment: str = ""
) -> Dict[str, Any]:
    """
    Envía una orden al servidor Webhook (/webhook/tradingview) de forma síncrona.
    Estandariza los nombres de campos requeridos por ForexClient y MT5.
    Retorna resultado estandarizado con order_id, order_ids y sub_orders.
    """
    webhookUrl = os.getenv("WEBHOOK_URL") or "http://127.0.0.1:8002/webhook/tradingview"
    rawPassphrase = os.getenv("WEBHOOK_VERIFY_TOKEN", "")
    encryptedPassphrase = encryptValue(rawPassphrase) if rawPassphrase else ""

    orderPayload = {
        "strategy": strategy,
        "setup": setup,
        "passphrase": encryptedPassphrase,
        "time": time.time(),
        "action": action.lower().strip(),
        "ticker": symbol,
        "entry": float(price or 0.0),
        "quantity": float(quantity or 1.0),
        "tp": float(tp or 0.0),
        "sl": float(sl or 0.0),
        "sync": True,
        "ticketId": str(ticketId) if ticketId else None,
        "idTrade": idTrade,
        "FOREX_USERNAME": encryptedAccountToken,
        "comment": comment
    }

    try:
        resp = requests.post(webhookUrl, json=orderPayload, timeout=35)
        if resp.status_code == 200:
            resData = resp.json() if resp.text else {}
            isSuccess = (resData.get("status") == "success") or (resData.get("success") is True)
            orderId = str(resData.get("order_id") or resData.get("ticketId") or resData.get("OrderId") or "")
            fillPrice = float(resData.get("fill_price") or resData.get("Price") or price)
            execQty = resData.get("total_qty") or resData.get("quantity") or resData.get("totalQty") or quantity
            execMargin = resData.get("margin")
            raw_sub_orders = resData.get("sub_orders") or []
            order_ids = resData.get("order_ids") or ([orderId] if orderId else [])

            isCloseAction = (action.lower().strip() == "close")
            if isSuccess and (isCloseAction or orderId or order_ids or raw_sub_orders):
                return {
                    "success": True,
                    "order_id": orderId or (order_ids[0] if order_ids else ""),
                    "order_ids": order_ids,
                    "sub_orders": raw_sub_orders,
                    "fill_price": fillPrice,
                    "quantity": float(execQty),
                    "margin": float(execMargin) if execMargin is not None else None,
                    "pnl": float(resData.get("pnl") or 0.0) if resData.get("pnl") is not None else None,
                    "raw": resData
                }
            else:
                errMsg = resData.get("message") or resData.get("error") or "Orden rechazada por bróker"
                return {"success": False, "error": errMsg, "raw": resData}
        else:
            return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
    except Exception as exReq:
        return {"success": False, "error": f"Excepción de conexión: {exReq}"}


# ============================================================================
# 4. PREVENCIÓN DE ÓRDENES HUÉRFANAS: ROLLBACK TRANSACCIONAL
# ============================================================================

def rollback_broker_orders(
    encryptedAccountToken: str,
    ordersToRollback: List[Dict[str, Any]],
    strategy: str = "ATALAia"
) -> None:
    """
    Ejecuta cierre inmediato de emergencia en el bróker para todas las órdenes/tickets
    que se abrieron parcialmente si falló algún lote de la secuencia.
    Garantiza CERO órdenes huérfanas en FOREX.com / MT5.
    """
    webhookUrl = os.getenv("WEBHOOK_URL") or "http://127.0.0.1:8002/webhook/tradingview"
    rawPassphrase = os.getenv("WEBHOOK_VERIFY_TOKEN", "")
    encryptedPassphrase = encryptValue(rawPassphrase) if rawPassphrase else ""

    for rb in ordersToRollback:
        rbSym = rb.get("symbol")
        rbTicket = str(rb.get("ticketId") or rb.get("order_id") or "")
        rbQty = float(rb.get("size") or rb.get("quantity") or 0.0)
        if not rbTicket:
            continue

        closePayload = {
            "strategy": strategy,
            "passphrase": encryptedPassphrase,
            "time": time.time(),
            "action": "close",
            "ticker": rbSym,
            "entry": 0.0,
            "quantity": rbQty,
            "tp": 0.0,
            "sl": 0.0,
            "sync": True,
            "ticketId": rbTicket,
            "FOREX_USERNAME": encryptedAccountToken
        }
        try:
            logger.warning(f"🔄 [ROLLBACK DE EMERGENCIA] Cerrando posición en bróker {rbSym} (Ticket: {rbTicket}, Qty: {rbQty:g})...")
            resp = requests.post(webhookUrl, json=closePayload, timeout=25)
            logger.info(f"🔄 [ROLLBACK RESPUESTA] Ticket {rbTicket}: HTTP {resp.status_code}")
        except Exception as exRb:
            logger.error(f"❌ [ROLLBACK ERROR] No se pudo cerrar ticket {rbTicket} en bróker: {exRb}")


# ============================================================================
# 5. ALERTAS UNIFICADAS POR TELEGRAM
# ============================================================================

def dispatch_strategy_telegram_alert(idCuenta: int, messageHtml: str) -> None:
    """
    Despacha alertas enriquecidas por Telegram con prioridad inmediata.
    """
    try:
        loop = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            asyncio.ensure_future(alertaInmediata(idCuenta, messageHtml, prioridad=True), loop=loop)
        else:
            asyncio.run(alertaInmediata(idCuenta, messageHtml, prioridad=True))
        logger.info(f"📱 [Telegram] Alerta despachada para Cuenta #{idCuenta}.")
    except Exception as ex:
        logger.error(f"⚠️ [Telegram] Error despachando alerta para Cuenta #{idCuenta}: {ex}")


# ============================================================================
# 6. EJECUCIÓN ESTANDARIZADA DE ENTRADA (SINGLE ASSET: CART / SNIPER)
# ============================================================================

def execute_single_asset_entry(
    dbSession,
    idCuenta: int,
    strategy: str,
    setup: str,
    symbol: str,
    direction: str,
    timeframe: str,
    size: float,
    price: float,
    sl: Optional[float] = None,
    tp: Optional[float] = None,
    margenPct: float = 0.5,
    comment: str = "",
    signalType: str = "PRECISION",
    candleDt: Optional[datetime] = None
) -> Dict[str, Any]:
    """
    Rutina maestra unificada para aperturas de 1 solo activo (CART / SNIPER):
      1. Valida Salud de Margen y Margen Proyectado contra cuenta.indicadorMargen.
      2. Valida conexión activa con bróker en brokercuenta (no opera si no hay bróker).
      3. Envía orden síncrona al Webhook.
      4. Si el bróker confirma:
         - Si hubo división por multiplo: inserta 1 registro en 'trades' por cada sub-ticket.
         - Si fue orden única: inserta el trade con su ticketId.
         - Descuenta margen_used de cuenta.Capital.
         - Despacha alerta de Telegram.
      5. Si el bróker rechaza: no modifica BD y retorna error.
    """
    now = datetime.now()
    candleDt = candleDt or now

    # 1. Validar salud de margen actual
    accRow = dbSession.execute(
        text("SELECT Nombre, Capital, COALESCE(indicadorMargen, 200.0) FROM cuenta WHERE idCuenta = :idc LIMIT 1"),
        {"idc": idCuenta}
    ).mappings().fetchone()
    if not accRow:
        return {"success": False, "error": f"Cuenta #{idCuenta} no existe"}

    accountName = accRow["Nombre"]
    accountCapital = float(accRow["Capital"] or 0.0)
    minMarginRequired = float(accRow["COALESCE(indicadorMargen, 200.0)"])

    canOperate, marginInd, totalMargin, equity, minMarginReq = check_account_margin_health(
        dbSession, idCuenta, accountCapital
    )
    if not canOperate:
        msg = f"⛔ [{strategy}] Apertura SUSPENDIDA para {symbol}: Indicador de Margen ({marginInd:.1f}%) < {minMarginReq:.0f}% requerido."
        logger.warning(msg)
        return {"success": False, "error": msg}

    plannedMargin = calculate_margin(symbol, size, price, margenPct)
    isAllowed, projInd = check_projected_margin(equity, totalMargin, plannedMargin, minMarginReq)
    if not isAllowed:
        msg = f"⛔ [{strategy}] Apertura SUSPENDIDA para {symbol}: Margen proyectado ({projInd:.1f}%) caería debajo de {minMarginReq:.0f}% (Margen req: ${plannedMargin:.2f})."
        logger.warning(msg)
        return {"success": False, "error": msg}

    # 2. Validar bróker conectado
    brokerInfo = get_account_broker(dbSession, idCuenta)
    if not brokerInfo:
        msg = f"🚫 [{strategy}] Cuenta #{idCuenta} ({accountName}) no tiene bróker activo configurado en brokercuenta. NO OPERA."
        logger.warning(msg)
        return {"success": False, "error": msg}

    # 3. Enviar orden a Webhook
    action = "buy" if ("LARG" in direction.upper() or "BUY" in direction.upper()) else "sell"
    logger.info(f"🌐 [{strategy}] Despachando {action.upper()} {symbol} ({size:g} u) Px={price} a FOREX.com para {brokerInfo['loginUsuario']} ({brokerInfo['nombreBroker']})...")

    whRes = send_broker_order(
        strategy=strategy,
        setup=setup,
        action=action,
        symbol=symbol,
        quantity=size,
        price=price,
        sl=sl or 0.0,
        tp=tp or 0.0,
        encryptedAccountToken=brokerInfo["encryptedAccountToken"],
        comment=comment
    )

    if not whRes.get("success"):
        logger.error(f"❌ [{strategy}] Orden rechazada por bróker para {symbol}: {whRes.get('error')}")
        return {"success": False, "error": whRes.get("error")}

    # 4. Inserción atómica en BD (1 registro por ticket real)
    raw_sub_orders = whRes.get("sub_orders") or []
    insertedTradeIds = []
    totalRealMargin = 0.0

    # Ajuste de SL/TP si el precio de llenado difiere del precio esperado
    primaryFillPx = float(whRes.get("fill_price") or price)
    realSl = sl
    realTp = tp
    if price > 0 and primaryFillPx > 0 and abs(primaryFillPx - price) > 1e-4:
        if sl is not None:
            slDist = abs(price - sl)
            realSl = round((primaryFillPx - slDist) if action == "buy" else (primaryFillPx + slDist), 5)
        if tp is not None:
            tpDist = abs(price - tp)
            realTp = round((primaryFillPx + tpDist) if action == "buy" else (primaryFillPx - tpDist), 5)

    insertSql = text("""
        INSERT INTO trades (
            idCuenta, strategy, setup, symbol, status, direction, intervalo,
            size, entryPrice, stopLoss, takeProfit, isBreakEven, margin_used,
            candleTime, candle_time, openTime, sentAt, ticketId
        ) VALUES (
            :idc, :strat, :setup, :sym, 'OPEN', :dir, :tf,
            :sz, :epx, :sl, :tp, 0, :mrg,
            :cdt, :cdt, NOW(), NOW(), :tid
        )
    """)

    if raw_sub_orders and len(raw_sub_orders) > 1:
        for sub in raw_sub_orders:
            subTicket = str(sub.get("order_id") or "").strip()
            if not subTicket:
                continue
            subPx = float(sub.get("fill_price") or primaryFillPx)
            subQty = float(sub.get("quantity") or (size / len(raw_sub_orders)))
            subMrg = float(sub.get("margin") or (plannedMargin / len(raw_sub_orders)))
            totalRealMargin += subMrg

            resIns = dbSession.execute(insertSql, {
                "idc": idCuenta,
                "strat": strategy,
                "setup": setup,
                "sym": symbol,
                "dir": direction.upper(),
                "tf": timeframe,
                "sz": subQty,
                "epx": subPx,
                "sl": realSl,
                "tp": realTp,
                "mrg": subMrg,
                "cdt": candleDt,
                "tid": subTicket
            })
            insertedTradeIds.append(resIns.lastrowid)
            logger.info(f"✅ [{strategy}] Sub-orden guardada: Trade #{resIns.lastrowid}, Ticket={subTicket}, {subQty:g} u @ {subPx:.5f}, Margen=${subMrg:.2f}")
    else:
        orderId = str(whRes.get("order_id") or "")
        fillPx = primaryFillPx
        confQty = float(whRes.get("quantity") or size)
        confMrg = float(whRes.get("margin") or plannedMargin)
        totalRealMargin = confMrg

        resIns = dbSession.execute(insertSql, {
            "idc": idCuenta,
            "strat": strategy,
            "setup": setup,
            "sym": symbol,
            "dir": direction.upper(),
            "tf": timeframe,
            "sz": confQty,
            "epx": fillPx,
            "sl": realSl,
            "tp": realTp,
            "mrg": confMrg,
            "cdt": candleDt,
            "tid": orderId
        })
        insertedTradeIds.append(resIns.lastrowid)
        logger.info(f"✅ [{strategy}] Trade guardado: Trade #{resIns.lastrowid}, Ticket={orderId}, {confQty:g} u @ {fillPx:.5f}, Margen=${confMrg:.2f}")

    # 5. Descontar margen retenido de cuenta.Capital
    dbSession.execute(text("""
        UPDATE cuenta
        SET Capital = Capital - :mrg
        WHERE idCuenta = :idc
    """), {"mrg": totalRealMargin, "idc": idCuenta})
    dbSession.commit()

    # 6. Alerta de Telegram enriquecida
    dirEmoji = "🟢 COMPRA" if action == "buy" else "🔴 VENTA"
    alertMsg = (
        f"🎯 <b>[{strategy} - ENTRADA CONFIRMADA]</b>\n\n"
        f"Cuenta: <b>{accountName}</b> (#{idCuenta})\n"
        f"Activo: <b>{symbol}</b> ({timeframe})\n"
        f"Operación: <b>{dirEmoji}</b> ({size:g} unidades)\n"
        f"Precio Entrada: <b>{price:.5f}</b>\n"
        f"Tickets Bróker: <code>{whRes.get('order_ids') or [whRes.get('order_id')]}</code>\n"
        f"Margen Retenido: <b>${totalRealMargin:,.2f} USD</b>\n"
        f"Ind. Margen: <b>{projInd:.1f}%</b> (Mín Req: {minMarginReq:.0f}%)\n"
        f"Señal: <i>{signalType}</i>\n"
        f"Hora: {now.strftime('%Y-%m-%d %H:%M:%S')} CST"
    )
    dispatch_strategy_telegram_alert(idCuenta, alertMsg)

    return {
        "success": True,
        "trade_ids": insertedTradeIds,
        "ticket_ids": whRes.get("order_ids") or [whRes.get("order_id")],
        "margin_used": totalRealMargin
    }


# ============================================================================
# 7. EJECUCIÓN ESTANDARIZADA DE CIERRE (SINGLE ASSET: CART / SNIPER)
# ============================================================================

def execute_single_asset_close(
    dbSession,
    idCuenta: int,
    tradeRow: Dict[str, Any],
    strategy: str,
    exitPrice: float,
    closeReason: str = "Take Profit / Stop Loss"
) -> Dict[str, Any]:
    """
    Rutina maestra unificada para cierres de 1 solo activo (CART / SNIPER):
      1. Obtiene credenciales de bróker en brokercuenta.
      2. Despacha acción 'close' al Webhook con reintentos para no dejar órdenes huérfanas.
      3. Si el bróker confirma:
         - Actualiza trades: status = 'CLOSED', exitPrice, pnl, closeTime.
         - Devuelve el margen + PnL a cuenta.Capital.
         - Despacha alerta de Telegram.
      4. Si el bróker rechaza: la posición permanece OPEN para no desincronizar con el bróker.
    """
    symbol = tradeRow["symbol"]
    tId = str(tradeRow.get("ticketId") or "")
    idTrade = int(tradeRow["idTrade"])
    sz = float(tradeRow.get("size") or 1.0)
    direction = str(tradeRow.get("direction") or "LARGO").upper()
    entryPx = float(tradeRow.get("entryPrice") or exitPrice)
    marginUsed = float(tradeRow.get("margin_used") or 0.0)

    brokerInfo = get_account_broker(dbSession, idCuenta)
    if not brokerInfo or not tId or tId.lower() == "none":
        reason = "Sin bróker activo" if not brokerInfo else "Sin ticketId registrado"
        logger.warning(f"⚠️ [{strategy}] {reason} para cerrar Trade #{idTrade} ({symbol}). Cerrando solo en BD local.")
        # Cierre solo local si la cuenta no tiene bróker configurado o no hay ticketId
        pnlVal, _pips, _, _ = calculate_unrealized_pnl(symbol, direction, entryPx, exitPrice, sz)
        dbSession.execute(text("""
            UPDATE trades
            SET status = 'CLOSED', exitPrice = :epx, pnl = :pnl, closeTime = NOW()
            WHERE idTrade = :idt
        """), {"epx": exitPrice, "pnl": round(pnlVal, 2), "idt": idTrade})
        dbSession.execute(text("""
            UPDATE cuenta SET Capital = Capital + :mrg + :pnl WHERE idCuenta = :idc
        """), {"mrg": marginUsed, "pnl": round(pnlVal, 2), "idc": idCuenta})
        dbSession.commit()
        return {"success": True, "local_only": True}

    logger.info(f"🌐 [{strategy}] Solicitando CIERRE a FOREX.com para {symbol} (Ticket: {tId}, Qty: {sz:g})...")

    # Reintentos automáticos para garantizar el cierre sin huérfanas
    whRes = None
    maxAttempts = 3
    for attempt in range(1, maxAttempts + 1):
        whRes = send_broker_order(
            strategy=strategy,
            setup=symbol,
            action="close",
            symbol=symbol,
            quantity=sz,
            price=exitPrice,
            ticketId=tId,
            idTrade=idTrade,
            encryptedAccountToken=brokerInfo["encryptedAccountToken"],
            comment=f"Cierre {strategy}: {closeReason}"
        )
        if whRes.get("success"):
            break
        logger.warning(f"🔄 [{strategy}] Reintento cierre {attempt}/{maxAttempts} para {symbol} (Ticket: {tId}): {whRes.get('error')}")
        time.sleep(2.0)

    if not whRes or not whRes.get("success"):
        logger.error(f"❌ [{strategy}] No se pudo cerrar posición {symbol} en FOREX.com: {whRes.get('error') if whRes else 'Timeout'}")
        return {"success": False, "error": whRes.get("error") if whRes else "Fallo de comunicación"}

    realExitPx = float(whRes.get("fill_price") or exitPrice)

    # Consultar pip size
    rowSym = dbSession.execute(
        text("SELECT pip FROM symbols WHERE UPPER(REPLACE(symbol, '/', '')) = UPPER(REPLACE(:s, '/', '')) LIMIT 1"),
        {"s": symbol}
    ).fetchone()
    pipSz = float(rowSym[0]) if rowSym and rowSym[0] else (0.01 if "JPY" in symbol else 0.0001)

    pnlVal, pipsVal, _, _ = calculate_unrealized_pnl(
        symbol=symbol,
        direction=direction,
        entry_price=entryPx,
        current_price=realExitPx,
        size=sz,
        pip_size=pipSz
    )

    # Actualizar estado cerrado y devolver capital + margen + pnl
    dbSession.execute(text("""
        UPDATE trades
        SET status = 'CLOSED',
            exitPrice = :epx,
            pnl = :pnl,
            closeTime = NOW()
        WHERE idTrade = :idt
    """), {"epx": realExitPx, "pnl": round(pnlVal, 2), "idt": idTrade})

    dbSession.execute(text("""
        UPDATE cuenta
        SET Capital = Capital + :mrg + :pnl
        WHERE idCuenta = :idc
    """), {"mrg": marginUsed, "pnl": round(pnlVal, 2), "idc": idCuenta})
    dbSession.commit()

    logger.info(f"✅ [{strategy}] Posición {symbol} (Trade #{idTrade}, Ticket: {tId}) CERRADA en Forex y BD. PnL: ${pnlVal:.2f} USD ({pipsVal:.1f} pips).")

    # Alerta de Telegram
    accRow = dbSession.execute(text("SELECT Nombre FROM cuenta WHERE idCuenta = :idc LIMIT 1"), {"idc": idCuenta}).fetchone()
    accountName = accRow[0] if accRow else f"Cuenta #{idCuenta}"
    pnlColor = "🟢" if pnlVal >= 0 else "🔴"
    alertMsg = (
        f"🏁 <b>[{strategy} - CIERRE DE POSICIÓN]</b>\n\n"
        f"Cuenta: <b>{accountName}</b> (#{idCuenta})\n"
        f"Activo: <b>{symbol}</b>\n"
        f"Ticket Bróker: <code>{tId}</code>\n"
        f"Motivo: <i>{closeReason}</i>\n"
        f"Entrada: <b>{entryPx:.5f}</b> | Salida: <b>{realExitPx:.5f}</b>\n"
        f"Resultado: <b>{pnlColor} ${pnlVal:,.2f} USD</b> ({pipsVal:+.1f} pips)\n"
        f"Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} CST"
    )
    dispatch_strategy_telegram_alert(idCuenta, alertMsg)

    return {"success": True, "pnl": round(pnlVal, 2), "exit_price": realExitPx}
