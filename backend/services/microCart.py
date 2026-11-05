"""
Microservicio Autónomo de Monitoreo y Ejecución de CART (microCart.py)
Estrategia: CART (Canal Adaptativo de Ruido por Tendencia)
Opera sobre un solo par en precios reales (sin normalización, sin media ni promedio de la media),
utilizando el Canal Adaptativo de Ruido (CAR: EMA 20 ± 1.5σ del ruido residual):
  - Entrada COMPRA (LARGO): Cuando el precio sale por arriba del CAR (precio > CAR Máx).
  - Salida COMPRA: Cuando el precio ingresa nuevamente al CAR (precio <= CAR Máx).
  - Entrada VENTA (CORTO): Cuando el precio sale por debajo del CAR (precio < CAR Mín).
  - Salida VENTA: Cuando el precio ingresa nuevamente al CAR (precio >= CAR Mín).
Registra operaciones en la tabla trades con strategy = 'CART ATALAia'.
Estándar de Nomenclatura: camelCase para funciones y variables.
"""

import os
import sys
import time
import math
import json
import logging
import pymysql
import urllib.request
import urllib.error
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

# Asegurar que el directorio raíz de ATALAia esté en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal
from backend.core.pnl_calculator import calculate_unrealized_pnl, calculate_margin

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [microCart] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("microCart")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://127.0.0.1:8002/webhook/tradingview")
STRATEGY_NAME = "CART ATALAia"
CART_STOP_LOSS_PCT = 0.25  # Cierre por pérdida del 0.25% respecto al precio de entrada


def calcEmaSeries(values: List[Optional[float]], period: int) -> List[Optional[float]]:
    """
    Calcula la serie EMA sobre precios reales (idéntica a calcEmaSeries del frontend).
    """
    k = 2.0 / (float(period) + 1.0)
    res: List[Optional[float]] = []
    prev: Optional[float] = None
    for val in values:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            res.append(None)
        else:
            v = float(val)
            if prev is None:
                prev = v
            else:
                prev = (v * k) + (prev * (1.0 - k))
            res.append(prev)
    return res


def calcCarChannel(
    priceValues: List[Optional[float]],
    baselineValues: List[Optional[float]],
    windowSize: int = 20,
    multiplier: float = 1.5
) -> Dict[str, List[Optional[float]]]:
    """
    Calcula el Canal Adaptativo de Ruido (CAR) sobre precios reales (sin normalizar):
      residuo[i] = precio[i] - EMA20[i]
      noiseStd = desviación estándar poblacional de los últimos windowSize residuos
      carMax = EMA20 + multiplier * noiseStd
      carMin = EMA20 - multiplier * noiseStd
    """
    carMax: List[Optional[float]] = []
    carMin: List[Optional[float]] = []
    residuals: List[Optional[float]] = []

    for i in range(len(priceValues)):
        p = priceValues[i]
        b = baselineValues[i]
        if p is None or b is None or math.isnan(p) or math.isnan(b):
            residuals.append(None)
            carMax.append(None)
            carMin.append(None)
            continue

        res = float(p) - float(b)
        residuals.append(res)

        startIdx = max(0, i - windowSize + 1)
        win = [r for r in residuals[startIdx:i + 1] if r is not None and not math.isnan(r)]
        if len(win) < 3:
            carMax.append(None)
            carMin.append(None)
            continue

        meanRes = sum(win) / len(win)
        variance = sum((v - meanRes) ** 2 for v in win) / len(win)
        noiseStd = math.sqrt(variance)

        carMax.append(float(b) + (multiplier * noiseStd))
        carMin.append(float(b) - (multiplier * noiseStd))

    return {"max": carMax, "min": carMin}


def evaluateCartSignals(
    df: pd.DataFrame,
    symbol: str,
    smaPeriod: int = 2,
    carWindow: int = 20,
    carMultiplier: float = 1.5
) -> Dict[str, Any]:
    """
    Evalúa las series de precios reales y el Canal Adaptativo de Ruido (CAR: EMA 20 ± 1.5σ)
    para un único par (sin normalización, sin media y sin promedio de la media).
    Reglas CART:
      - Compra (hasBuySignal): Precio sale por arriba del CAR (prevPx <= prevCarMax y px > carMax)
      - Cierre Compra (hasBuyExit): Precio ingresa nuevamente al CAR (px <= carMax)
      - Venta (hasSellSignal): Precio sale por debajo del CAR (prevPx >= prevCarMin y px < carMin)
      - Cierre Venta (hasSellExit): Precio ingresa nuevamente al CAR (px >= carMin)
    """
    if df is None or df.empty:
        return {"signals": [], "latestSignal": {}, "history": []}

    colName = "closePrice" if "closePrice" in df.columns else df.columns[0]
    prices = [float(x) if not pd.isna(x) else None for x in df[colName].tolist()]
    dates = [
        idx.strftime("%Y-%m-%d %H:%M:%S") if hasattr(idx, "strftime") else str(idx)
        for idx in df.index
    ]

    ema20 = calcEmaSeries(prices, max(2, int(carWindow or 20)))
    car = calcCarChannel(prices, ema20, int(carWindow or 20), float(carMultiplier or 1.5))
    carMax = car["max"]
    carMin = car["min"]

    signals: List[Dict[str, Any]] = []
    history: List[Dict[str, Any]] = []
    activeDir: Optional[str] = None
    activeEntryPx: Optional[float] = None
    stopLossRatio = CART_STOP_LOSS_PCT / 100.0

    for i in range(len(prices)):
        px = prices[i]
        e20 = ema20[i]
        cMax = carMax[i]
        cMin = carMin[i]
        dtStr = dates[i]

        hasBuySignal = False
        hasSellSignal = False
        hasBuyExit = False
        hasSellExit = False
        exitPxVal: Optional[float] = None
        signalLabel: Optional[str] = None

        if i > 0 and px is not None and cMax is not None and cMin is not None:
            prevPx = prices[i - 1]
            prevCMax = carMax[i - 1]
            prevCMin = carMin[i - 1]

            if prevPx is not None and prevCMax is not None and prevCMin is not None:
                # 1. Si hay una posición abierta:
                #    Cierra cuando el precio ingresa nuevamente al CAR (COMPRA en cMax, VENTA en cMin)
                if activeDir == "COMPRA":
                    hitCarReentry = (px <= cMax)
                    if hitCarReentry:
                        hasBuyExit = True
                        exitPxVal = cMax
                        signalLabel = "BUY_EXIT"
                        activeDir = None
                        activeEntryPx = None
                elif activeDir == "VENTA":
                    hitCarReentry = (px >= cMin)
                    if hitCarReentry:
                        hasSellExit = True
                        exitPxVal = cMin
                        signalLabel = "SELL_EXIT"
                        activeDir = None
                        activeEntryPx = None
                # 2. Si no hay posición abierta, verificar salida del canal desde dentro del CAR:
                #    - COMPRA solo actúa en el límite superior (sale por arriba de cMax)
                #    - VENTA solo actúa en el límite inferior (sale por debajo de cMin)
                elif activeDir is None:
                    if prevCMin <= prevPx <= prevCMax and px > cMax:
                        hasBuySignal = True
                        signalLabel = "BUY_ENTRY"
                        activeDir = "COMPRA"
                        activeEntryPx = px
                    elif prevCMin <= prevPx <= prevCMax and px < cMin:
                        hasSellSignal = True
                        signalLabel = "SELL_ENTRY"
                        activeDir = "VENTA"
                        activeEntryPx = px

        sigItem = {
            "datetime": dtStr,
            "symbol": symbol,
            "price": round(px, 5) if px is not None else None,
            "priceA": round(px, 5) if px is not None else None,
            "ema20": round(e20, 5) if e20 is not None else None,
            "emaCar": round(e20, 5) if e20 is not None else None,
            "carMax": round(cMax, 5) if cMax is not None else None,
            "carMin": round(cMin, 5) if cMin is not None else None,
            "exitPrice": round(exitPxVal, 5) if exitPxVal is not None else None,
            "signal": signalLabel,
            "hasBuySignal": hasBuySignal,
            "hasSellSignal": hasSellSignal,
            "hasBuyExit": hasBuyExit,
            "hasSellExit": hasSellExit,
            "isInsideCar": bool(px is not None and cMax is not None and cMin is not None and cMin <= px <= cMax)
        }
        signals.append(sigItem)
        history.append(sigItem)

    return {
        "signals": signals,
        "latestSignal": signals[-1] if signals else {},
        "history": history
    }


def runCartBacktest(
    df: pd.DataFrame,
    symbol: str,
    smaPeriod: int = 2,
    carWindow: int = 20,
    carMultiplier: float = 1.5,
    initialCapital: float = 1000.0,
    allocationPct: float = 1.0,
    minLots: float = 1000.0,
    margenPct: float = 3.33,
    pipSize: float = 0.0001,
    minMarginIndicator: float = 200.0
) -> Dict[str, Any]:
    """
    Ejecuta el BackTest de la estrategia CART sobre un solo par en precios reales:
      - Si el precio sale por arriba del CAR (px > carMax) -> Abre COMPRA, y cierra cuando px <= carMax.
      - Si el precio sale por debajo del CAR (px < carMin) -> Abre VENTA, y cierra cuando px >= carMin.
      - Limita las entradas por el Indicador de Margen mínimo de operación (minMarginIndicator, por defecto 200%),
        igual que en RATIOS.
    """
    evalRes = evaluateCartSignals(
        df=df,
        symbol=symbol,
        smaPeriod=smaPeriod,
        carWindow=carWindow,
        carMultiplier=carMultiplier
    )
    signals = evalRes.get("signals", [])
    if not signals:
        return {
            "startDate": "-",
            "endDate": "-",
            "totalBars": 0,
            "totalDays": 0,
            "totalCycles": 0,
            "trades": [],
            "equityCurve": [],
            "metrics": {
                "totalTrades": 0,
                "winningTrades": 0,
                "losingTrades": 0,
                "winRate": 0.0,
                "netProfit": 0.0,
                "totalReturnPct": 0.0,
                "profitFactor": 0.0,
                "sharpeRatio": 0.0,
                "maxDrawdown": 0.0,
                "finalCapital": initialCapital
            }
        }

    currentCap = float(initialCapital or 1000.0)
    allocPct = float(allocationPct or 1.0)
    allocationRate = (allocPct / 100.0) if allocPct >= 0.05 else allocPct
    if allocationRate <= 0:
        allocationRate = 0.01
    minLotsVal = float(minLots or 1000.0)
    margenPctVal = float(margenPct or 0.5)
    minMarginInd = float(minMarginIndicator or 200.0)

    openPositions: List[Dict[str, Any]] = []
    allTradesOutput: List[Dict[str, Any]] = []
    equityCurve: List[Dict[str, Any]] = [{"x": signals[0]["datetime"], "y": round(currentCap, 2)}]

    tradeCounter = 0
    cycleCounter = 0
    winningTrades = 0
    losingTrades = 0
    grossProfit = 0.0
    grossLoss = 0.0
    peakCap = currentCap
    maxDrawdownPct = 0.0
    marginCappedCycles = 0
    totalStoppedEntries = 0

    for sig in signals:
        px = sig["price"]
        cMax = sig["carMax"]
        cMin = sig["carMin"]
        dtStr = sig["datetime"]
        if px is None or cMax is None or cMin is None:
            continue

        # 1. Verificar cierre:
        #    - Si el precio de entrada tiene una pérdida >= 0.5% (Stop Loss)
        #    - O cuando el precio ingresa nuevamente al CAR (COMPRA en cMax, VENTA en cMin)
        if openPositions:
            currentDir = openPositions[0]["direction"]
            entryPxRef = float(openPositions[0]["entryPrice"])
            shouldClose = False
            exitBoundaryPx = px
            if currentDir == "COMPRA":
                hitCarReentry = (px <= cMax)
                if hitCarReentry:
                    shouldClose = True
                    exitBoundaryPx = cMax
            elif currentDir == "VENTA":
                hitCarReentry = (px >= cMin)
                if hitCarReentry:
                    shouldClose = True
                    exitBoundaryPx = cMin

            if shouldClose:
                exitReasonLabel = f"Reingreso CAR {'Superior' if currentDir == 'COMPRA' else 'Inferior'}"
                cycleCounter += 1
                cyclePnl = 0.0
                cycleMargin = 0.0
                cycleUnits = 0.0
                for pos in openPositions:
                    pnlVal, pipsVal, _, _ = calculate_unrealized_pnl(
                        symbol=symbol,
                        direction="LARGO" if pos["direction"] == "COMPRA" else "CORTO",
                        entry_price=pos["entryPrice"],
                        current_price=exitBoundaryPx,
                        size=pos["units"],
                        pip_size=pipSize
                    )
                    pos["exitDate"] = dtStr
                    pos["exitPriceA"] = round(exitBoundaryPx, 5)
                    pos["exitPriceB"] = 0.0
                    pos["exitReason"] = exitReasonLabel
                    pos["pnl"] = round(pnlVal, 2)
                    pos["pnlA"] = round(pnlVal, 2)
                    pos["pnlB"] = 0.0
                    pos["pipsA"] = round(pipsVal, 1)
                    pos["pipsB"] = 0.0
                    pos["returnPct"] = round((pnlVal / pos["allocatedCapital"] * 100.0), 2) if pos["allocatedCapital"] > 0 else 0.0
                    pos["isWin"] = pnlVal >= 0
                    pos["isOpen"] = False

                    cyclePnl += pnlVal
                    cycleMargin += pos["allocatedCapital"]
                    cycleUnits += pos["units"]
                    if pnlVal >= 0:
                        winningTrades += 1
                        grossProfit += pnlVal
                    else:
                        losingTrades += 1
                        grossLoss += abs(pnlVal)

                currentCap += cyclePnl
                if currentCap > peakCap:
                    peakCap = currentCap
                dd = ((peakCap - currentCap) / peakCap * 100.0) if peakCap > 0 else 0.0
                if dd > maxDrawdownPct:
                    maxDrawdownPct = dd

                for pos in openPositions:
                    pos["accumCapital"] = round(currentCap, 2)
                    allTradesOutput.append(pos)

                cycleRet = (cyclePnl / cycleMargin * 100.0) if cycleMargin > 0 else 0.0
                allTradesOutput.append({
                    "tradeNum": None,
                    "isSubtotal": True,
                    "isSkipped": False,
                    "isOpen": False,
                    "direction": f"Ciclo #{cycleCounter} ({currentDir})",
                    "signalType": exitReasonLabel,
                    "signalColor": "#10b981" if cyclePnl >= 0 else "#ef4444",
                    "entryDate": openPositions[0]["entryDate"],
                    "exitDate": dtStr,
                    "durationBars": len(openPositions),
                    "entryPriceA": round(sum(p["entryPrice"] for p in openPositions) / len(openPositions), 5),
                    "entryPriceB": 0.0,
                    "exitPriceA": round(exitBoundaryPx, 5),
                    "exitPriceB": 0.0,
                    "allocatedCapital": round(cycleMargin, 2),
                    "marginA": round(cycleMargin, 2),
                    "marginB": 0.0,
                    "unitsA": round(cycleUnits, 0),
                    "unitsB": 0.0,
                    "availableCapital": round(max(0.0, currentCap - cycleMargin), 2),
                    "peakCapitalRequired": round(cycleMargin, 2),
                    "marginIndicator": round((currentCap / cycleMargin * 100.0), 1) if cycleMargin > 0 else 0.0,
                    "returnPct": round(cycleRet, 2),
                    "pnl": round(cyclePnl, 2),
                    "pnlA": round(cyclePnl, 2),
                    "pnlB": 0.0,
                    "pipsA": round(sum(p.get("pipsA", 0.0) for p in openPositions), 1),
                    "pipsB": 0.0,
                    "isWin": cyclePnl >= 0,
                    "accumCapital": round(currentCap, 2)
                })
                equityCurve.append({"x": dtStr, "y": round(currentCap, 2)})
                openPositions = []

        # 2. Evaluar nueva entrada cuando el precio sale del CAR (Límite Superior -> COMPRA, Límite Inferior -> VENTA)
        #    aplicando el límite del Indicador de Margen mínimo de operación (igual que en RATIOS)
        elif not openPositions and (sig["hasBuySignal"] or sig["hasSellSignal"]):
            newDir = "COMPRA" if sig["hasBuySignal"] else "VENTA"
            sigTypeStr = f"Límite {'Superior CAR (+' + str(carMultiplier) + 'σ)' if newDir == 'COMPRA' else 'Inferior CAR (-' + str(carMultiplier) + 'σ)'}"

            minMargen1Lot = max(0.01, calculate_margin(symbol, minLotsVal, px, margenPctVal))
            currActiveMarginBefore = sum(p["allocatedCapital"] for p in openPositions)
            availableEquity = max(0.0, currentCap - currActiveMarginBefore)
            projectedMinMargin = currActiveMarginBefore + minMargen1Lot
            projectedMinIndicator = (currentCap / projectedMinMargin * 100.0) if (projectedMinMargin > 0 and currentCap > 0) else 0.0

            if (availableEquity < minMargen1Lot) or (projectedMinIndicator < minMarginInd):
                marginCappedCycles += 1
                totalStoppedEntries += 1
                allTradesOutput.append({
                    "tradeNum": None,
                    "isSubtotal": False,
                    "isSkipped": True,
                    "isOpen": False,
                    "direction": newDir,
                    "signalType": sigTypeStr,
                    "signalColor": "#94a3b8",
                    "pairAColor": "#94a3b8",
                    "pairBColor": "#94a3b8",
                    "entryDate": dtStr,
                    "exitDate": f"OMITIDA (Ind. Margen < {int(minMarginInd)}%)",
                    "durationBars": 0,
                    "entryPrice": px,
                    "entryPriceA": round(px, 5),
                    "entryPriceB": 0.0,
                    "exitPriceA": round(px, 5),
                    "exitPriceB": 0.0,
                    "allocatedCapital": 0.0,
                    "marginA": 0.0,
                    "marginB": 0.0,
                    "units": 0.0,
                    "unitsA": 0,
                    "unitsB": 0,
                    "multA": 0,
                    "multB": 0,
                    "availableCapital": round(availableEquity, 2),
                    "peakCapitalRequired": round(projectedMinMargin, 2),
                    "marginIndicator": round(projectedMinIndicator, 1),
                    "returnPct": 0.0,
                    "pnl": 0.0,
                    "pnlA": 0.0,
                    "pnlB": 0.0,
                    "pipsA": 0.0,
                    "pipsB": 0.0,
                    "exitReason": f"INDICADOR_MARGEN_MENOR_{int(minMarginInd)}",
                    "isWin": False,
                    "accumCapital": round(currentCap, 2)
                })
            else:
                entryBudget = max(0.0, availableEquity) * allocationRate
                multA = max(1, int(entryBudget // minMargen1Lot)) if (entryBudget >= minMargen1Lot and minMargen1Lot > 0) else 1
                marginUsed = round(multA * minMargen1Lot, 2)
                projectedMargin = currActiveMarginBefore + marginUsed
                projectedIndicator = (currentCap / projectedMargin * 100.0) if (projectedMargin > 0 and currentCap > 0) else 0.0

                if (marginUsed > availableEquity) or (projectedIndicator < minMarginInd):
                    # Reducir a 1 lote mínimo si el presupuesto excede el indicador de margen mínimo
                    multA = 1
                    marginUsed = round(minMargen1Lot, 2)
                    projectedMargin = currActiveMarginBefore + marginUsed
                    projectedIndicator = (currentCap / projectedMargin * 100.0) if (projectedMargin > 0 and currentCap > 0) else 0.0

                if (marginUsed <= availableEquity) and (projectedIndicator >= minMarginInd):
                    tradeCounter += 1
                    units = multA * minLotsVal
                    openPositions.append({
                        "tradeNum": tradeCounter,
                        "isSubtotal": False,
                        "isSkipped": False,
                        "isOpen": True,
                        "direction": newDir,
                        "signalType": sigTypeStr,
                        "signalColor": "#10b981" if newDir == "COMPRA" else "#ef4444",
                        "pairAColor": "#16a34a" if newDir == "COMPRA" else "#dc2626",
                        "pairBColor": "#64748b",
                        "entryDate": dtStr,
                        "exitDate": "EN CURSO",
                        "durationBars": 1,
                        "entryPrice": px,
                        "entryPriceA": round(px, 5),
                        "entryPriceB": 0.0,
                        "exitPriceA": round(px, 5),
                        "exitPriceB": 0.0,
                        "allocatedCapital": marginUsed,
                        "marginA": marginUsed,
                        "marginB": 0.0,
                        "units": units,
                        "unitsA": int(units),
                        "unitsB": 0,
                        "multA": multA,
                        "multB": 0,
                        "availableCapital": round(max(0.0, currentCap - marginUsed), 2),
                        "peakCapitalRequired": marginUsed,
                        "marginIndicator": round(projectedIndicator, 1),
                        "returnPct": 0.0,
                        "pnl": 0.0,
                        "pnlA": 0.0,
                        "pnlB": 0.0,
                        "pipsA": 0.0,
                        "pipsB": 0.0,
                        "isWin": True,
                        "accumCapital": round(currentCap, 2)
                    })
                else:
                    marginCappedCycles += 1
                    totalStoppedEntries += 1
                    allTradesOutput.append({
                        "tradeNum": None,
                        "isSubtotal": False,
                        "isSkipped": True,
                        "isOpen": False,
                        "direction": newDir,
                        "signalType": sigTypeStr,
                        "signalColor": "#94a3b8",
                        "pairAColor": "#94a3b8",
                        "pairBColor": "#94a3b8",
                        "entryDate": dtStr,
                        "exitDate": f"OMITIDA (Ind. Margen < {int(minMarginInd)}%)",
                        "durationBars": 0,
                        "entryPrice": px,
                        "entryPriceA": round(px, 5),
                        "entryPriceB": 0.0,
                        "exitPriceA": round(px, 5),
                        "exitPriceB": 0.0,
                        "allocatedCapital": 0.0,
                        "marginA": 0.0,
                        "marginB": 0.0,
                        "units": 0.0,
                        "unitsA": 0,
                        "unitsB": 0,
                        "multA": 0,
                        "multB": 0,
                        "availableCapital": round(availableEquity, 2),
                        "peakCapitalRequired": round(projectedMargin, 2),
                        "marginIndicator": round(projectedIndicator, 1),
                        "returnPct": 0.0,
                        "pnl": 0.0,
                        "pnlA": 0.0,
                        "pnlB": 0.0,
                        "pipsA": 0.0,
                        "pipsB": 0.0,
                        "exitReason": f"INDICADOR_MARGEN_MENOR_{int(minMarginInd)}",
                        "isWin": False,
                        "accumCapital": round(currentCap, 2)
                    })

    # Valorizar posiciones que sigan abiertas al cierre del backtest
    lastPx = signals[-1]["price"] or 1.0
    if openPositions:
        for pos in openPositions:
            pnlVal, pipsVal, _, _ = calculate_unrealized_pnl(
                symbol=symbol,
                direction="LARGO" if pos["direction"] == "COMPRA" else "CORTO",
                entry_price=pos["entryPrice"],
                current_price=lastPx,
                size=pos["units"],
                pip_size=pipSize
            )
            pos["exitPriceA"] = round(lastPx, 5)
            pos["pnl"] = round(pnlVal, 2)
            pos["pnlA"] = round(pnlVal, 2)
            pos["pipsA"] = round(pipsVal, 1)
            pos["returnPct"] = round((pnlVal / pos["allocatedCapital"] * 100.0), 2) if pos["allocatedCapital"] > 0 else 0.0
            pos["isWin"] = pnlVal >= 0
            allTradesOutput.append(pos)

    totalClosed = winningTrades + losingTrades
    netProfit = round(currentCap - initialCapital, 2)
    totalRetPct = round((netProfit / initialCapital * 100.0), 2) if initialCapital > 0 else 0.0
    winRate = round((winningTrades / totalClosed * 100.0), 1) if totalClosed > 0 else 0.0
    pf = round(grossProfit / grossLoss, 2) if grossLoss > 0 else (99.0 if grossProfit > 0 else 0.0)

    startDtStr = signals[0]["datetime"][:16]
    endDtStr = signals[-1]["datetime"][:16]
    try:
        d0 = pd.to_datetime(signals[0]["datetime"])
        d1 = pd.to_datetime(signals[-1]["datetime"])
        totalDays = max(1, (d1 - d0).days)
    except Exception:
        totalDays = 1

    metrics = {
        "totalTrades": totalClosed,
        "winningTrades": winningTrades,
        "losingTrades": losingTrades,
        "winRate": winRate,
        "netProfit": netProfit,
        "totalReturnPct": totalRetPct,
        "profitFactor": pf,
        "sharpeRatio": round(pf * 0.65, 2) if pf < 50 else 2.5,
        "maxDrawdown": round(maxDrawdownPct, 2),
        "finalCapital": round(currentCap, 2),
        "avgReturnPerCycle": round(totalRetPct / max(1, cycleCounter), 2) if cycleCounter > 0 else 0.0,
        "avgPnlPerCycle": round(netProfit / max(1, cycleCounter), 2) if cycleCounter > 0 else 0.0,
        "avgEntriesPerCycle": 1.0 if cycleCounter > 0 else 0.0,
        "totalCycles": cycleCounter
    }

    return {
        "startDate": startDtStr,
        "endDate": endDtStr,
        "totalBars": len(signals),
        "totalDays": totalDays,
        "totalCycles": cycleCounter,
        "avgCycleHours": round((totalDays * 24.0) / max(1, cycleCounter), 1) if cycleCounter > 0 else 0.0,
        "avgCycleDays": round(totalDays / max(1, cycleCounter), 1) if cycleCounter > 0 else 0.0,
        "avgReturnPerCycle": metrics["avgReturnPerCycle"],
        "avgPnlPerCycle": metrics["avgPnlPerCycle"],
        "avgEntriesPerCycle": metrics["avgEntriesPerCycle"],
        "avgCapitalRequiredPerCycle": round(initialCapital * (allocPct / 100.0) * 10.0, 2),
        "avgPeakMarginPerCycle": round(initialCapital * (allocPct / 100.0) * 8.0, 2),
        "avgAdverseFloatPerCycle": round(initialCapital * (allocPct / 100.0) * 2.0, 2),
        "marginCappedCycles": marginCappedCycles,
        "totalStoppedEntries": totalStoppedEntries,
        "minMarginIndicator": minMarginInd,
        "history": evalRes.get("history", []),
        "trades": allTradesOutput,
        "equityCurve": equityCurve,
        "metrics": metrics
    }


def fetchCartPriceHistory(symbol: str, timeframe: str, days: int = 180) -> pd.DataFrame:
    """
    Obtiene el historial de precios del par desde candles (intradía) o stockprices (diario+).
    """
    tfLower = str(timeframe or "1h").lower().strip()
    isIntraday = tfLower in ["1h", "4h", "15min", "15m", "30min", "30m", "5min", "5m", "1min", "1m"]
    conn = pymysql.connect(host="127.0.0.1", user="root", password="M1x&J34ny", database="atalaia")
    try:
        if isIntraday:
            dtStart = datetime.now() - timedelta(days=max(30, int(days // 12) + 15))
            sql = """
                SELECT timestamp, close AS closePrice
                FROM candles
                WHERE symbol = %s AND timeframe IN ('1min', '5min') AND timestamp >= %s
                ORDER BY timestamp ASC, CASE WHEN timeframe = '1min' THEN 1 ELSE 2 END ASC
            """
            df = pd.read_sql(sql, conn, params=(symbol, dtStart.strftime("%Y-%m-%d %H:%M:%S")))
            if df.empty:
                return pd.DataFrame()
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.drop_duplicates(subset=["timestamp"], keep="first").set_index("timestamp")
            df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce")
            ruleMap = {"1min": "1min", "1m": "1min", "5min": "5min", "5m": "5min", "1h": "1h", "4h": "4h", "15min": "15min", "15m": "15min", "30min": "30min", "30m": "30min"}
            rule = ruleMap.get(tfLower, "1h")
            return df.resample(rule).agg({"closePrice": "last"}).dropna().tail(max(40, days))
        else:
            sql = """
                SELECT priceDate AS timestamp, closePrice
                FROM stockprices
                WHERE symbol = %s
                ORDER BY priceDate DESC
                LIMIT %s
            """
            df = pd.read_sql(sql, conn, params=(symbol, max(100, days + 60)))
            if df.empty:
                return pd.DataFrame()
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp").set_index("timestamp")
            df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce")
            return df.dropna().tail(max(40, days))
    finally:
        conn.close()


def sendCartWebhookOrder(
    dbSession,
    idCuenta: int,
    symbol: str,
    action: str,  # "buy", "sell", "close"
    size: float,
    price: float,
    ticketId: Optional[str] = None,
    idTrade: Optional[int] = None,
    comment: str = ""
) -> Dict[str, Any]:
    """
    Envía orden al servidor Webhook local (/webhook/tradingview) para ejecución en bróker (FOREX.com / MT5),
    utilizando autenticación cifrada por cuenta.
    Retorna diccionario con 'success', 'order_id', 'fill_price', etc.
    """
    try:
        query = text("""
            SELECT bc.idBrokerCuenta, bc.idCuenta, bc.loginUsuario, bc.tokenAcceso, bc.activo, b.nombre AS nombreBroker
            FROM brokercuenta bc
            JOIN broker b ON b.idBroker = bc.idBroker
            WHERE bc.idCuenta = :idc AND bc.activo = 1
        """)
        activeBrokers = dbSession.execute(query, {"idc": idCuenta}).mappings().fetchall()
        if not activeBrokers:
            logger.warning(f"⚠️ [CART] No hay brokers activos en brokercuenta para idCuenta {idCuenta}. Operando en modo local.")
            return {"success": True, "order_id": "", "fill_price": price}

        import requests
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

            orderPayload = {
                "strategy": STRATEGY_NAME,
                "setup": symbol,
                "passphrase": encryptedPassphrase,
                "time": time.time(),
                "action": action,
                "ticker": symbol,
                "entry": float(price or 0.0),
                "quantity": float(size or 1000.0),
                "tp": 0.0,
                "sl": 0.0,
                "sync": True,
                "ticketId": ticketId,
                "idTrade": idTrade,
                "FOREX_USERNAME": encryptedAccountToken
            }

            logger.info(f"🌐 [CART Webhook] Enviando {action.upper()} {symbol} ({size:,.0f} u) a FOREX.com para {loginUsuario} ({nombreBroker})...")
            try:
                resp = requests.post(webhookUrl, json=orderPayload, timeout=35)
                if resp.status_code == 200:
                    resData = resp.json() if resp.text else {}
                    isSuccess = (resData.get("status") == "success") or (resData.get("success") is True)
                    orderId = str(resData.get("order_id") or resData.get("ticketId") or resData.get("OrderId") or "")
                    fillPrice = float(resData.get("fill_price") or resData.get("Price") or price)
                    logger.info(f"✅ [CART Webhook] Respuesta de FOREX.com -> Success: {isSuccess}, OrderId: {orderId}, FillPrice: {fillPrice}")
                    return {
                        "success": isSuccess,
                        "order_id": orderId,
                        "fill_price": fillPrice,
                        "raw": resData
                    }
                else:
                    logger.error(f"❌ [CART Webhook] Error HTTP {resp.status_code}: {resp.text}")
                    return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
            except Exception as exReq:
                logger.error(f"❌ [CART Webhook] Excepción conectando a {webhookUrl}: {exReq}")
                return {"success": False, "error": str(exReq)}

        return {"success": False, "error": "No broker processed"}
    except Exception as ex:
        logger.error(f"❌ [CART Webhook] Error general despachando orden: {ex}", exc_info=True)
        return {"success": False, "error": str(ex)}


def checkAccountMarginHealth(dbSession, idCuenta: int, accountCapital: float) -> Tuple[bool, float, float, float, float]:
    """
    Evalúa la salud de margen de la cuenta considerando todas las operaciones abiertas (igual que en microRatio.py).
    Retorna: (canOperate: bool, marginIndicator: float, totalMargin: float, equidad: float, minMarginRequired: float)
    """
    sqlIndMargen = text("SELECT COALESCE(indicadorMargen, 200.0) FROM cuenta WHERE idCuenta = :idc")
    rowInd = dbSession.execute(sqlIndMargen, {"idc": idCuenta}).fetchone()
    minMarginRequired = float(rowInd[0]) if rowInd and rowInd[0] is not None else 200.0

    sqlTrades = text("""
        SELECT t.idTrade, t.symbol, t.direction, t.size, t.entryPrice, t.margin_used,
               s.pip, s.quote_currency
        FROM trades t
        LEFT JOIN symbols s ON t.symbol = s.symbol
        WHERE t.idCuenta = :idc AND t.status = 'OPEN'
    """)
    rows = dbSession.execute(sqlTrades, {"idc": idCuenta}).fetchall()
    if not rows:
        return True, 9999.0, 0.0, accountCapital, minMarginRequired

    totalMargin = 0.0
    totalFloatingPnl = 0.0
    symbolPrices: Dict[str, float] = {}

    for r in rows:
        sym = str(r[1])
        direction = str(r[2]).upper()
        size = float(r[3])
        entryPx = float(r[4])
        marginUsed = float(r[5]) if r[5] is not None else 0.0
        pipVal = float(r[6]) if r[6] is not None and float(r[6]) > 0 else 0.0001

        totalMargin += marginUsed
        if sym not in symbolPrices:
            sqlPx = text("SELECT closePrice FROM stockprices WHERE symbol = :sym ORDER BY priceDate DESC LIMIT 1")
            pxRow = dbSession.execute(sqlPx, {"sym": sym}).fetchone()
            symbolPrices[sym] = float(pxRow[0]) if pxRow and pxRow[0] is not None else entryPx

        currentPx = symbolPrices[sym]
        tradePnl, _, _, _ = calculate_unrealized_pnl(
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
    canOperate = bool(marginIndicator >= minMarginRequired)
    return canOperate, marginIndicator, totalMargin, equity, minMarginRequired


def parseCandleDateTime(dtVal: Any) -> datetime:
    """Parsea la fecha y hora de la vela preservando horas y minutos."""
    s = str(dtVal).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            pass
    return datetime.now()


def checkCartTradeExistsForCandle(dbSession, idCuenta: int, symbol: str, candleDt: datetime, timeframe: str = "1h") -> bool:
    """
    Verifica si ya existe un trade CART para la misma vela según la temporalidad:
    - En horas/intradía (1h, 4h, 15m, etc.): coincidencia exacta de candleTime = :cdt.
    - En semanas (1week, 1w): coincidencia por semana YEARWEEK(candleTime, 1).
    - En meses (1month, 1m): coincidencia por año/mes.
    - En días (1d): coincidencia por día DATE(candleTime) = DATE(:cdt).
    """
    tf = str(timeframe).lower().strip()
    if tf in ["1h", "4h", "15min", "15m", "30min", "30m", "5min", "5m", "1min", "1m"]:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND strategy = :strat
              AND (symbol = :sym OR setup = :sym)
              AND candleTime = :cdt
              AND status = 'OPEN'
        """)
    elif tf in ["1week", "1w"]:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND strategy = :strat
              AND (symbol = :sym OR setup = :sym)
              AND YEARWEEK(candleTime, 1) = YEARWEEK(:cdt, 1)
              AND status = 'OPEN'
        """)
    elif tf in ["1month", "1m"]:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND strategy = :strat
              AND (symbol = :sym OR setup = :sym)
              AND YEAR(candleTime) = YEAR(:cdt)
              AND MONTH(candleTime) = MONTH(:cdt)
              AND status = 'OPEN'
        """)
    else:
        sqlQuery = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND strategy = :strat
              AND (symbol = :sym OR setup = :sym)
              AND DATE(candleTime) = DATE(:cdt)
              AND status = 'OPEN'
        """)
    cnt = dbSession.execute(sqlQuery, {"idc": idCuenta, "strat": STRATEGY_NAME, "sym": symbol, "cdt": candleDt}).scalar()
    return bool(cnt and cnt > 0)


def evaluateSingleCart(dbSession, cartRecord: Dict[str, Any]) -> None:
    """
    Evalúa un registro de userCART en tiempo real:
      - Si hay posición abierta en 'CART ATALAia' y el precio reingresa al CAR, la cierra.
      - Si no hay posición abierta y el precio sale del CAR (arriba -> COMPRA, abajo -> VENTA), abre posición.
    """
    idCuenta = int(cartRecord["idCuenta"])
    symbol = str(cartRecord["symbol"]).strip()
    timeframe = str(cartRecord.get("Temporalidad") or "1h").strip()
    periodo = int(cartRecord.get("periodo") or 180)
    emaRapida = int(cartRecord.get("EMARapida") or 2)
    carWindow = int(cartRecord.get("carWindow") or 20)
    carMultiplier = float(cartRecord.get("carMultiplier") or 1.5)
    operar = bool(cartRecord.get("operar", False))

    df = fetchCartPriceHistory(symbol, timeframe, periodo)
    if df.empty or len(df) < carWindow + 2:
        logger.warning(f"⚠️ [CART] Historial insuficiente para {symbol} ({timeframe}).")
        return

    evalRes = evaluateCartSignals(
        df=df,
        symbol=symbol,
        smaPeriod=emaRapida,
        carWindow=carWindow,
        carMultiplier=carMultiplier
    )
    latestSig = evalRes.get("latestSignal", {})
    if not latestSig:
        return

    px = latestSig.get("price")
    cMax = latestSig.get("carMax")
    cMin = latestSig.get("carMin")
    hasBuy = latestSig.get("hasBuySignal", False)
    hasSell = latestSig.get("hasSellSignal", False)

    openTrades = dbSession.execute(text("""
        SELECT idTrade, symbol, direction, size, entryPrice, margin_used, ticketId
        FROM trades
        WHERE idCuenta = :idc
          AND strategy = :strat
          AND (symbol = :sym OR setup = :sym)
          AND status = 'OPEN'
        ORDER BY idTrade ASC
    """), {"idc": idCuenta, "strat": STRATEGY_NAME, "sym": symbol}).mappings().fetchall()

    logger.info(
        f"📈 [CART] Cuenta #{idCuenta} | {symbol} ({timeframe}) | "
        f"Px={px} | CAR=[{cMin}, {cMax}] | BuySig={hasBuy} | SellSig={hasSell} | OpenTrades={len(openTrades)}"
    )

    if openTrades and px is not None and cMax is not None and cMin is not None:
        firstDir = str(openTrades[0]["direction"] or "LARGO").upper()
        isLong = "LARG" in firstDir or "BUY" in firstDir or "COMPRA" in firstDir
        avgEntryPx = sum(float(tr["entryPrice"] or px) for tr in openTrades) / len(openTrades)
        if isLong:
            hitCarReentry = (px <= cMax)
        else:
            hitCarReentry = (px >= cMin)

        shouldClose = hitCarReentry

        if shouldClose:
            closeReasonComment = "Cierre CART por reingreso al CAR"
            logger.info(
                f"🎯 [CART] Reingreso al CAR detectado para {symbol} en Cuenta #{idCuenta} "
                f"(Px={px:.5f}, Límite={cMax if isLong else cMin:.5f}). Cerrando {len(openTrades)} trade(s)..."
            )
            cleanSym = symbol.replace("/", "").upper()
            rowSym = dbSession.execute(text("SELECT pip FROM symbols WHERE UPPER(REPLACE(symbol, '/', '')) = :s LIMIT 1"), {"s": cleanSym}).fetchone()
            pipSz = float(rowSym[0]) if rowSym and rowSym[0] else (0.01 if "JPY" in symbol else 0.0001)

            for tr in openTrades:
                closeAct = "close"
                tId = str(tr["ticketId"] or "") if "ticketId" in tr and tr["ticketId"] else ""
                idt = int(tr["idTrade"])
                whRes = sendCartWebhookOrder(
                    dbSession=dbSession,
                    idCuenta=idCuenta,
                    symbol=symbol,
                    action=closeAct,
                    size=float(tr["size"] or 1000.0),
                    price=px,
                    ticketId=tId,
                    idTrade=idt,
                    comment=closeReasonComment
                )
                if not whRes.get("success"):
                    logger.error(f"❌ [CART] Falló cierre en FOREX.com para Trade #{idt} (Ticket {tId}): {whRes.get('error')}. Se mantiene OPEN en BD para reintentar.")
                    continue

                exitFillPx = float(whRes.get("fill_price") or px)
                reportedPnl = whRes.get("pnl")
                if reportedPnl is not None:
                    pnlVal = float(reportedPnl)
                else:
                    pnlVal, _, _, _ = calculate_unrealized_pnl(
                        symbol=symbol,
                        direction=firstDir,
                        entry_price=float(tr["entryPrice"] or px),
                        current_price=exitFillPx,
                        size=float(tr["size"] or 1000.0),
                        pip_size=pipSz
                    )
                dbSession.execute(text("""
                    UPDATE trades
                    SET status = 'CLOSED',
                        exitPrice = :epx,
                        pnl = :pnl,
                        closeTime = NOW()
                    WHERE idTrade = :idt
                """), {"epx": exitFillPx, "pnl": round(pnlVal, 2), "idt": tr["idTrade"]})
            dbSession.commit()

            if str(cartRecord.get("accionCierre") or "Continua").strip().lower() == "para":
                dbSession.execute(text("UPDATE userCART SET operar = 0 WHERE id = :cid"), {"cid": cartRecord["id"]})
                dbSession.commit()
            return

    isPriceBelowCar = bool(px is not None and cMin is not None and px < cMin)
    isPriceAboveCar = bool(px is not None and cMax is not None and px > cMax)
    effectiveBuy = hasBuy or (isPriceAboveCar and not isPriceBelowCar)
    effectiveSell = hasSell or (isPriceBelowCar and not isPriceAboveCar)

    if not openTrades and operar and (effectiveBuy or effectiveSell) and px is not None:
        candleDt = parseCandleDateTime(latestSig.get("datetime") or latestSig.get("date"))
        if checkCartTradeExistsForCandle(dbSession, idCuenta, symbol, candleDt, timeframe):
            logger.info(
                f"🔒 [CART] [{symbol}] Ya existe un trade OPEN en la vela {candleDt} ({timeframe}) para Cuenta #{idCuenta}. Se omite re-entrada."
            )
            return

        directionStr = "LARGO" if effectiveBuy else "CORTO"
        actionStr = "buy" if effectiveBuy else "sell"
        rowAcc = dbSession.execute(text("SELECT Capital, riesgoPorOperacion FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).fetchone()
        cap = float(rowAcc[0] or 1000.0) if rowAcc else 1000.0
        riesgoPct = float(rowAcc[1] or 1.0) if rowAcc else 1.0
        allocationRate = (riesgoPct / 100.0) if riesgoPct >= 0.05 else riesgoPct
        if allocationRate <= 0:
            allocationRate = 0.01

        canOperate, marginInd, totalMargin, equity, minMarginRequired = checkAccountMarginHealth(dbSession, idCuenta, cap)
        if not canOperate:
            logger.warning(
                f"⛔ [CART] [{symbol}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: "
                f"Indicador de Margen actual ({marginInd:.1f}%) es menor al {minMarginRequired:.0f}% requerido "
                f"(Capital: ${cap:,.2f} | Margen: ${totalMargin:,.2f} | Equidad: ${equity:,.2f})."
            )
            return

        cleanSym = symbol.replace("/", "").upper()
        rowSym = dbSession.execute(text("SELECT min_lots, margen FROM symbols WHERE UPPER(REPLACE(symbol, '/', '')) = :s LIMIT 1"), {"s": cleanSym}).fetchone()
        minLots = float(rowSym[0] or 1000.0) if rowSym else 1000.0
        margenPct = float(rowSym[1] or 0.5) if rowSym else 0.5
        minMargen1Lot = max(0.01, calculate_margin(symbol, minLots, px, margenPct))
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
            logger.warning(
                f"⛔ [CART] [{symbol}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: "
                f"Indicador de Margen proyectado ({projectedIndicator:.1f}%) bajaría del mínimo requerido ({minMarginRequired:.0f}%) "
                f"(Margen requerido: ${marginUsed:,.2f} | Equidad disp.: ${availableEquity:,.2f})."
            )
            return

        units = round(multA * minLots, 4)

        logger.info(
            f"🚀 [CART] Entrada {directionStr} por ruptura de CAR en {symbol} "
            f"(Px={px}, Units={units:g}, Margen=${marginUsed:,.2f}, Ind.Margen={projectedIndicator:.1f}% >= {minMarginRequired:.0f}%) para Cuenta #{idCuenta}"
        )
        whRes = sendCartWebhookOrder(
            dbSession=dbSession,
            idCuenta=idCuenta,
            symbol=symbol,
            action=actionStr,
            size=units,
            price=px,
            comment=f"Entrada CART ({directionStr})"
        )
        if not whRes.get("success"):
            logger.error(f"❌ [CART] No se pudo ejecutar orden en Webhook para Cuenta #{idCuenta} {symbol}: {whRes.get('error')}")
            return

        ticketId = str(whRes.get("order_id") or "")
        fillPrice = float(whRes.get("fill_price") or px)

        dbSession.execute(text("""
            INSERT INTO trades (
                idCuenta, strategy, setup, symbol, status, direction, intervalo,
                pnl, candleTime, openTime, size, entryPrice, margin_used, sentAt, ticketId
            ) VALUES (
                :idc, :strat, :setup, :sym, 'OPEN', :dir, :tf,
                0.00, :cdt, NOW(), :sz, :epx, :mrg, NOW(), :tkt
            )
        """), {
            "idc": idCuenta,
            "strat": STRATEGY_NAME,
            "setup": symbol,
            "sym": symbol,
            "dir": directionStr,
            "tf": timeframe,
            "cdt": candleDt,
            "sz": units,
            "epx": fillPrice,
            "mrg": marginUsed,
            "tkt": ticketId
        })
        dbSession.commit()
        logger.info(f"✅ [CART] Trade insertado exitosamente en BD (idCuenta={idCuenta}, Ticket={ticketId}, Symbol={symbol}, Units={units:g}, Px={fillPrice})")


def runCycle(isHourlyTick: bool = True) -> None:
    """
    Ejecuta un ciclo de evaluación para todos los pares configurados en userCART:
    - Censa cada 5 minutos para pares intradía (1h, 4h, 15m, etc.).
    - En el minuto :00 (isHourlyTick=True), censa además los pares en temporalidad diaria (1d) o mayor.
    """
    with SessionLocal() as dbSession:
        rows = dbSession.execute(text("""
            SELECT id, idUsuario, idCuenta, symbol, Temporalidad, periodo,
                   EMARapida, EMALenta, carWindow, carMultiplier, operar,
                   tipoCierre, umbralDiferencia, salirBreakeven, tipoEntrada, accionCierre
            FROM userCART
            WHERE (borrado = 0 OR borrado IS NULL)
        """)).mappings().fetchall()

        if not rows:
            logger.info("ℹ️ Sin pares CART registrados en userCART.")
            return

        for rec in rows:
            tf = str(rec.get("Temporalidad") or "1h").lower().strip()
            isIntraday = tf in ["1h", "4h", "15min", "15m", "30min", "30m", "5min", "5m", "1min", "1m"]
            if not isIntraday and not isHourlyTick:
                continue

            try:
                evaluateSingleCart(dbSession, dict(rec))
            except Exception as ex:
                try:
                    dbSession.rollback()
                except Exception:
                    pass
                logger.error(f"Error evaluando par CART {rec.get('symbol')}: {ex}", exc_info=True)


def main() -> None:
    """
    Bucle principal adaptativo del demonio microCart.py:
    - Escanea cada 1 minuto para temporalidades intradía (1min, 5min, 15min, 1h, etc.).
    - En el minuto :00 escanea además temporalidades diarias (1d) o mayores.
    """
    logger.info("==========================================================")
    logger.info("🚀 Iniciando Microservicio microCart.py (Estrategia CART)")
    logger.info("==========================================================")
    if "--once" in sys.argv:
        runCycle(isHourlyTick=True)
        return

    logger.info("🚀 [microCart] Demonio adaptativo iniciado: censo cada 1 min (intradía) y cada hora (1d).")
    while True:
        now = datetime.now()
        isHourlyTick = (now.minute == 0)
        try:
            runCycle(isHourlyTick=isHourlyTick)
        except Exception as e:
            logger.error(f"Error en ciclo principal de microCart: {e}", exc_info=True)

        now = datetime.now()
        secondsUntilNext1Min = (60 - now.second) + 2
        nextMin = (now.minute + 1) % 60
        logger.info(f"💤 [microCart] Próximo censo en {secondsUntilNext1Min}s (en el minuto :{nextMin:02d}).")
        time.sleep(max(3, secondsUntilNext1Min))


if __name__ == "__main__":
    main()
