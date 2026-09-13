"""
Motor de Análisis de Cruces EMA para Pair Trading y Arbitraje Estadístico de Ratios.
Diseñado para cálculo de Precios Normalizados, EMAs, Cruces en Zonas Extremas (Triángulos y Cuadros),
Cierre en Media (●) y Bitácora de Trades con Dimensionamiento Institucional al 3%.

Centralizado sobre backend.services.signal_engine.signalEngine.
Todas las funciones, variables y parámetros utilizan nomenclatura camelCase.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import numpy as np
import pandas as pd

from backend.services.signal_engine import signalEngine, SignalEngine

logger = logging.getLogger("cruceEmaEngine")


class CruceEmaEngine:
    """
    Motor de backtest y ejecución de análisis de cruces EMA.
    """

    def __init__(self, riskFreeRate: float = 0.03):
        self.riskFreeRate = riskFreeRate

    def runSignalBacktest(
        self,
        dfA: pd.DataFrame,
        dfB: pd.DataFrame,
        pairA: str = "",
        pairB: str = "",
        smaPeriod: int = 2,
        sigmaWindow: int = 30,
        includeBoxes: bool = False,
        initialCapital: float = 10000.0,
        allocationPct: float = 3.0,
        minLotsA: float = 1000.0,
        minLotsB: float = 1000.0,
        margenPctA: float = 0.25,
        margenPctB: float = 1.0,
        pipA: float = 0.00010,
        pipB: float = 0.01000,
        quoteA: str = "USD",
        quoteB: str = "USD",
        commissionBps: float = 2.0,
        slippageBps: float = 1.0
    ) -> Dict[str, Any]:
        """
        Ejecuta el backtest calculando el PnL exacto mediante el valor del pip
        (symbols.pip, quote_currency), asignando un 3% de capital en múltiplos
        enteros de symbols.min_lots y reinvirtiendo al cierre de todas las posiciones.
        """
        commonIdx = dfA.index.intersection(dfB.index)
        if len(commonIdx) < max(20, sigmaWindow + 5):
            return {
                "totalTrades": 0, "winRate": 0.0, "profitFactor": 0.0,
                "sharpeRatio": 0.0, "maxDrawdown": 0.0, "totalReturnPct": 0.0,
                "allocationPct": allocationPct,
                "minLotsA": minLotsA,
                "minLotsB": minLotsB,
                "margenPctA": margenPctA,
                "margenPctB": margenPctB,
                "reqMarginPerMinLot": 20.0,
                "initialCapital": float(initialCapital),
                "finalCapital": float(initialCapital),
                "netProfit": 0.0,
                "equityCurve": [],
                "trades": []
            }

        sA = dfA.loc[commonIdx, 'closePrice'] if 'closePrice' in dfA.columns else dfA.loc[commonIdx].iloc[:, 0]
        sB = dfB.loc[commonIdx, 'closePrice'] if 'closePrice' in dfB.columns else dfB.loc[commonIdx].iloc[:, 0]

        # 1. Utilizar el motor centralizado signalEngine
        normData = signalEngine.calculateNormalizedSeries(sA, sB)
        normA = normData["normA"]
        normB = normData["normB"]
        stdAboveA = normData["stdAboveA"]
        stdBelowA = normData["stdBelowA"]
        stdAboveB = normData["stdAboveB"]
        stdBelowB = normData["stdBelowB"]

        emaA = signalEngine.calculateEmaSeries(normA, smaPeriod)
        emaB = signalEngine.calculateEmaSeries(normB, smaPeriod)

        # Formatear fechas con horas y minutos si es intradía para que cada vela sea única
        has_time = any(hasattr(d, "hour") and (d.hour != 0 or d.minute != 0) for d in commonIdx[:10])
        if has_time:
            dates = [d.strftime("%Y-%m-%d %H:%M") if hasattr(d, "strftime") else str(d)[:16] for d in commonIdx]
        else:
            dates = [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d)[:10] for d in commonIdx]
        pricesA = sA.values
        pricesB = sB.values
        normAVals = normA.values
        normBVals = normB.values
        emaAVals = emaA.values
        emaBVals = emaB.values
        totalBars = len(dates)

        nameA = pairA if pairA else "Par A"
        nameB = pairB if pairB else "Par B"

        activeTrades: List[Dict[str, Any]] = []
        cycleSkippedTrades: List[Dict[str, Any]] = []
        finishedTrades: List[Dict[str, Any]] = []
        rawTradesCount = 0
        cycleCounter = 0
        equity = float(initialCapital) if initialCapital > 0 else 800.0
        cycleStartEquity = equity
        cycleAvailableEquity = equity
        peakEquity = equity
        maxDrawdown = 0.0
        equityCurve = []
        costRate = (commissionBps + slippageBps) / 10000.0
        allocationRate = allocationPct / 100.0

        # Ratios de margen institucional directo desde symbols.margen
        margenRateA = (margenPctA / 100.0) if margenPctA >= 0.05 else margenPctA
        margenRateB = (margenPctB / 100.0) if margenPctB >= 0.05 else margenPctB
        sampleReqMarginLot = 20.0

        # Control de tope de margen y requerimiento de capital por ciclo (Margen + Separación Flotante)
        isCycleMarginCapped = False
        cycleStoppedEntries = 0
        cyclePeakCapitalReq = 0.0
        cyclePeakMargin = 0.0
        cycleMaxAdverseFloat = 0.0
        has_time = any(hasattr(d, "hour") and (d.hour != 0 or d.minute != 0) for d in commonIdx[:20])

        for i in range(1, totalBars):
            d = dates[i]
            pxA = pricesA[i]
            pxB = pricesB[i]

            # Calcular PnL no realizado de los trades activos mediante pips
            unrealizedTotalPnl = 0.0
            if activeTrades:
                for t in activeTrades:
                    if "LONG " + nameA in t["direction"] or "LONG A" in t["direction"]:
                        dA = (pxA - t["entryPxA"])
                        dB = (t["entryPxB"] - pxB)
                    else:
                        dA = (t["entryPxA"] - pxA)
                        dB = (pxB - t["entryPxB"])

                    pipsANow = dA / pipA if pipA > 0 else 0.0
                    pipsBNow = dB / pipB if pipB > 0 else 0.0

                    pipValANow = t["unitsA"] * pipA if quoteA == "USD" else ((t["unitsA"] * pipA) / pxA if pxA > 0 else t["unitsA"] * pipA)
                    pipValBNow = t["unitsB"] * pipB if quoteB == "USD" else ((t["unitsB"] * pipB) / pxB if pxB > 0 else t["unitsB"] * pipB)

                    nomA = t["unitsA"] * pxA if quoteA == "USD" else t["unitsA"]
                    nomB = t["unitsB"] if quoteB != "USD" else t["unitsB"] * pxB
                    cNow = (nomA + nomB) * costRate

                    unrealizedTotalPnl += (pipsANow * pipValANow) + (pipsBNow * pipValBNow) - cNow

            currEquity = equity + unrealizedTotalPnl
            if currEquity > peakEquity:
                peakEquity = currEquity
            dd = (peakEquity - currEquity) / peakEquity if peakEquity > 0 else 0.0
            if dd > maxDrawdown:
                maxDrawdown = dd

            equityCurve.append({
                "x": d,
                "y": round(float(currEquity), 2)
            })

            # Rastrear barra a barra el capital requerido en el ciclo actual: Margen + Separación del precio promedio de las entradas
            if activeTrades:
                currActiveMargin = sum(t["margin"] for t in activeTrades)
                cycleUnitsA = sum(t["unitsA"] for t in activeTrades)
                cycleUnitsB = sum(t["unitsB"] for t in activeTrades)
                avgEntryPxA = (sum(t["entryPxA"] * t["unitsA"] for t in activeTrades) / cycleUnitsA) if cycleUnitsA > 0 else pxA
                avgEntryPxB = (sum(t["entryPxB"] * t["unitsB"] for t in activeTrades) / cycleUnitsB) if cycleUnitsB > 0 else pxB

                isLongA = ("LONG " + nameA in activeTrades[0]["direction"]) or ("LONG A" in activeTrades[0]["direction"]) or activeTrades[0]["direction"].startswith("LONG")
                if isLongA:
                    adversePxA = max(0.0, avgEntryPxA - pxA)
                    adversePxB = max(0.0, pxB - avgEntryPxB)
                else:
                    adversePxA = max(0.0, pxA - avgEntryPxA)
                    adversePxB = max(0.0, avgEntryPxB - pxB)

                pipsAdvA = adversePxA / pipA if pipA > 0 else 0.0
                pipsAdvB = adversePxB / pipB if pipB > 0 else 0.0

                pipValAdvA = cycleUnitsA * pipA if quoteA == "USD" else ((cycleUnitsA * pipA) / pxA if pxA > 0 else cycleUnitsA * pipA)
                pipValAdvB = cycleUnitsB * pipB if quoteB == "USD" else ((cycleUnitsB * pipB) / pxB if pxB > 0 else cycleUnitsB * pipB)

                currAdverseFloat = (pipsAdvA * pipValAdvA) + (pipsAdvB * pipValAdvB)
                currCapitalReq = currActiveMargin + currAdverseFloat
                if currCapitalReq > cyclePeakCapitalReq:
                    cyclePeakCapitalReq = currCapitalReq
                if currActiveMargin > cyclePeakMargin:
                    cyclePeakMargin = currActiveMargin
                if currAdverseFloat > cycleMaxAdverseFloat:
                    cycleMaxAdverseFloat = currAdverseFloat

            # Detectar señales con el módulo centralizado signalEngine
            candleSig = signalEngine.detectCandleSignals(
                prevPriceA=normAVals[i - 1],
                currPriceA=normAVals[i],
                prevEmaA=emaAVals[i - 1],
                currEmaA=emaAVals[i],
                stdAboveA=stdAboveA,
                stdBelowA=stdBelowA,
                prevPriceB=normBVals[i - 1],
                currPriceB=normBVals[i],
                prevEmaB=emaBVals[i - 1],
                currEmaB=emaBVals[i],
                stdAboveB=stdAboveB,
                stdBelowB=stdBelowB,
                prevNormA=normAVals[i - 1],
                currNormA=normAVals[i],
                prevNormB=normBVals[i - 1],
                currNormB=normBVals[i],
                pairA=nameA,
                pairB=nameB,
                includeBoxes=includeBoxes
            )

            isMeanCross = candleSig.get("isMeanCross", False)
            sigType = candleSig.get("signalType")
            direction = candleSig.get("direction")

            # 1. EVALUAR ENTRADA, REVERSAL (CAMBIO DE DIRECCIÓN) O ACUMULACIÓN
            if sigType and direction:
                # Verificar si existen posiciones activas o skipped en DIRECCIÓN OPUESTA (Reversal)
                isOpposite = False
                if activeTrades or cycleSkippedTrades:
                    prevDir = activeTrades[0]["direction"] if activeTrades else cycleSkippedTrades[0]["direction"]
                    if direction != prevDir:
                        isOpposite = True

                if isOpposite:
                    cycleCounter += 1
                    cycleTrades = []
                    cyclePnl = 0.0
                    cycleMargin = sum(t["margin"] for t in activeTrades)
                    cycleMarginA = sum(t.get("marginA", 0.0) for t in activeTrades)
                    cycleMarginB = sum(t.get("marginB", 0.0) for t in activeTrades)
                    cycleUnitsA = sum(t["unitsA"] for t in activeTrades)
                    cycleUnitsB = sum(t["unitsB"] for t in activeTrades)

                    # Cerrar operaciones operadas
                    for t in activeTrades:
                        rawTradesCount += 1
                        if ("LONG " + nameA in t["direction"]) or ("LONG A" in t["direction"]) or t["direction"].startswith("LONG"):
                            deltaA = (pxA - t["entryPxA"])
                            deltaB = (t["entryPxB"] - pxB)
                        else:
                            deltaA = (t["entryPxA"] - pxA)
                            deltaB = (pxB - t["entryPxB"])

                        pipsA = deltaA / pipA if pipA > 0 else 0.0
                        pipsB = deltaB / pipB if pipB > 0 else 0.0

                        if quoteA == "USD":
                            pipValA = t["unitsA"] * pipA
                        else:
                            pipValA = (t["unitsA"] * pipA) / pxA if pxA > 0 else (t["unitsA"] * pipA)

                        if quoteB == "USD":
                            pipValB = t["unitsB"] * pipB
                        else:
                            pipValB = (t["unitsB"] * pipB) / pxB if pxB > 0 else (t["unitsB"] * pipB)

                        pnlA = pipsA * pipValA
                        pnlB = pipsB * pipValB

                        tradePnl = pnlA + pnlB
                        tradeNetRet = (tradePnl / t["margin"]) if t["margin"] > 0 else 0.0

                        equity += tradePnl
                        cyclePnl += tradePnl

                        cycleTrades.append({
                            "tradeNum": rawTradesCount,
                            "isSubtotal": False,
                            "isSkipped": False,
                            "isOpen": False,
                            "cycleNum": cycleCounter,
                            "signalType": t["signalType"],
                            "direction": t["direction"],
                            "entryDate": t["entryDate"],
                            "entryIndex": t.get("entryIndex", i),
                            "exitDate": d,
                            "allocatedCapital": round(float(t["margin"]), 2),
                            "marginA": round(float(t.get("marginA", 0.0)), 2),
                            "marginB": round(float(t.get("marginB", 0.0)), 2),
                            "marginAccountPct": t.get("marginAccountPct", 0.0),
                            "marginIndicator": t.get("marginIndicator", 0.0),
                            "availableCapital": round(float(t.get("availableCapital", 0.0)), 2),
                            "accumCapital": round(float(equity), 2),
                            "multA": t["multA"],
                            "multB": t["multB"],
                            "unitsA": int(t["unitsA"]),
                            "unitsB": int(t["unitsB"]),
                            "pipsA": round(float(pipsA), 1),
                            "pipsB": round(float(pipsB), 1),
                            "pnlA": round(float(pnlA), 2),
                            "pnlB": round(float(pnlB), 2),
                            "entryPriceA": round(float(t["entryPxA"]), 5),
                            "exitPriceA": round(float(pxA), 5),
                            "entryPriceB": round(float(t["entryPxB"]), 5),
                            "exitPriceB": round(float(pxB), 5),
                            "durationBars": i - t["entryIndex"],
                            "returnPct": round(float(tradeNetRet * 100.0), 2),
                            "pnl": round(float(tradePnl), 2),
                            "exitReason": "CAMBIO_DIRECCION",
                            "isWin": bool(tradePnl > 0)
                        })

                    # Incorporar los registros que se estuvo perdiendo (no operados por indicador < 200)
                    for st in cycleSkippedTrades:
                        cycleTrades.append({
                            "tradeNum": None,
                            "isSubtotal": False,
                            "isSkipped": True,
                            "isOpen": False,
                            "cycleNum": cycleCounter,
                            "signalType": st["signalType"],
                            "direction": st["direction"],
                            "entryDate": st["entryDate"],
                            "entryIndex": st.get("entryIndex", i),
                            "exitDate": d,
                            "allocatedCapital": None,
                            "marginA": None,
                            "marginB": None,
                            "marginAccountPct": None,
                            "marginIndicator": None,
                            "availableCapital": None,
                            "accumCapital": None,
                            "multA": None,
                            "multB": None,
                            "unitsA": None,
                            "unitsB": None,
                            "pipsA": None,
                            "pipsB": None,
                            "pnlA": None,
                            "pnlB": None,
                            "entryPriceA": round(float(st["entryPxA"]), 5),
                            "exitPriceA": round(float(pxA), 5),
                            "entryPriceB": round(float(st["entryPxB"]), 5),
                            "exitPriceB": round(float(pxB), 5),
                            "durationBars": i - st.get("entryIndex", i),
                            "returnPct": None,
                            "pnl": None,
                            "exitReason": "INDICADOR_MARGEN_MENOR_200",
                            "isWin": None
                        })

                    # Ordenar registros del ciclo cronológicamente por entryIndex
                    cycleTrades.sort(key=lambda x: x.get("entryIndex", 0))

                    for ct in cycleTrades:
                        finishedTrades.append(ct)

                    operatedCycleTrades = [ct for ct in cycleTrades if not ct.get("isSkipped")]
                    cycleAvgRet = round((cyclePnl / cycleMargin) * 100.0, 2) if cycleMargin > 0 else 0.0

                    if operatedCycleTrades:
                        entrySpan = f"{operatedCycleTrades[0]['entryDate'][:10]} a {operatedCycleTrades[-1]['entryDate'][:10]}" if len(operatedCycleTrades) > 1 else operatedCycleTrades[0]['entryDate'][:10]
                        firstIdx = operatedCycleTrades[0].get("entryIndex", 0)
                    else:
                        entrySpan = cycleTrades[0]['entryDate'][:10] if cycleTrades else d[:10]
                        firstIdx = cycleTrades[0].get("entryIndex", 0) if cycleTrades else i

                    lastIdx = i
                    if has_time:
                        diffSecs = (commonIdx[lastIdx] - commonIdx[firstIdx]).total_seconds()
                        cycleDurationStr = f"{max(1, int(round(diffSecs / 3600.0)))}h"
                    else:
                        diffDays = (commonIdx[lastIdx].date() - commonIdx[firstIdx].date()).days if hasattr(commonIdx[firstIdx], "date") else (lastIdx - firstIdx)
                        cycleDurationStr = f"{max(1, int(diffDays))}d"

                    avgEntryPxA = (sum(ct["entryPriceA"] * ct["unitsA"] for ct in operatedCycleTrades) / cycleUnitsA) if cycleUnitsA > 0 else 0.0
                    avgEntryPxB = (sum(ct["entryPriceB"] * ct["unitsB"] for ct in operatedCycleTrades) / cycleUnitsB) if cycleUnitsB > 0 else 0.0
                    cycleMarginPct = round(float((cycleMargin / cycleStartEquity) * 100.0), 1) if cycleStartEquity > 0 else 0.0
                    cycleMarginIndicator = round(float((cycleStartEquity / cycleMargin) * 100.0), 1) if cycleMargin > 0 else 0.0

                    finishedTrades.append({
                        "tradeNum": None,
                        "isSubtotal": True,
                        "cycleNum": cycleCounter,
                        "signalType": f"SUBTOTAL CIERRE #{cycleCounter}",
                        "direction": f"🔄 Reversal ({len(operatedCycleTrades)} ops cerradas)",
                        "entryDate": cycleDurationStr,
                        "entryDateRange": entrySpan,
                        "exitDate": d,
                        "allocatedCapital": round(float(cycleMargin), 2),
                        "marginA": round(float(cycleMarginA), 2),
                        "marginB": round(float(cycleMarginB), 2),
                        "availableCapital": round(float(equity), 2),
                        "accumCapital": round(float(equity), 2),
                        "multA": None,
                        "multB": None,
                        "unitsA": int(cycleUnitsA),
                        "unitsB": int(cycleUnitsB),
                        "pipsA": round(float(sum(ct["pipsA"] for ct in operatedCycleTrades if ct.get("pipsA") is not None)), 1),
                        "pipsB": round(float(sum(ct["pipsB"] for ct in operatedCycleTrades if ct.get("pipsB") is not None)), 1),
                        "pnlA": round(float(sum(ct["pnlA"] for ct in operatedCycleTrades if ct.get("pnlA") is not None)), 2),
                        "pnlB": round(float(sum(ct["pnlB"] for ct in operatedCycleTrades if ct.get("pnlB") is not None)), 2),
                        "entryPriceA": round(float(avgEntryPxA), 5),
                        "exitPriceA": round(float(pxA), 5),
                        "entryPriceB": round(float(avgEntryPxB), 5),
                        "exitPriceB": round(float(pxB), 5),
                        "durationBars": len(operatedCycleTrades),
                        "returnPct": cycleAvgRet,
                        "pnl": round(float(cyclePnl), 2),
                        "exitReason": f"CAMBIO DE DIRECCIÓN ({direction}) {d}",
                        "isWin": bool(cyclePnl > 0),
                        "isMarginCapped": bool(isCycleMarginCapped or len(cycleSkippedTrades) > 0),
                        "stoppedEntriesCount": int(len(cycleSkippedTrades)),
                        "peakCapitalRequired": round(float(cyclePeakCapitalReq), 2),
                        "peakMargin": round(float(cyclePeakMargin), 2),
                        "maxAdverseFloat": round(float(cycleMaxAdverseFloat), 2),
                        "marginAccountPct": cycleMarginPct,
                        "marginIndicator": cycleMarginIndicator
                    })

                    # REINVERSIÓN AL CIERRE: 100% Margen liberado para el siguiente ciclo
                    cycleStartEquity = equity
                    cycleAvailableEquity = equity
                    isCycleMarginCapped = False
                    cycleStoppedEntries = 0
                    cyclePeakCapitalReq = 0.0
                    cyclePeakMargin = 0.0
                    cycleMaxAdverseFloat = 0.0
                    activeTrades = []
                    cycleSkippedTrades = []

                # 2. ABRIR NUEVA ENTRADA (O DETENER SI EL INDICADOR DE MARGEN BAJARÍA DE 200)
                minMargen1LotA = minLotsA * margenRateA
                minMargen1LotB = minLotsB * margenRateB
                minReqMargen = minMargen1LotA + minMargen1LotB

                currActiveMarginBefore = sum(t["margin"] for t in activeTrades)
                # Regla: el indicador de margen solo debe ser mayor que 200 (> 200%)
                # Si una entrada adicional hace que el indicador baje de 200, ya no se opera
                projectedMinMargin = currActiveMarginBefore + minReqMargen
                projectedMinIndicator = (cycleStartEquity / projectedMinMargin) * 100.0 if (projectedMinMargin > 0 and cycleStartEquity > 0) else 0.0

                if (cycleAvailableEquity < minReqMargen) or (projectedMinIndicator < 200.0):
                    # No alcanza el margen o bajaría de 200%: no operar, pero registrar lo que se estaría perdiendo
                    isCycleMarginCapped = True
                    cycleStoppedEntries += 1
                    cycleSkippedTrades.append({
                        "signalType": sigType,
                        "direction": direction,
                        "entryDate": d,
                        "entryIndex": i,
                        "entryPxA": pxA,
                        "entryPxB": pxB,
                        "isSkipped": True
                    })
                else:
                    totalEntryBudget = max(0.0, cycleAvailableEquity) * allocationRate
                    budgetA = totalEntryBudget / 2.0
                    budgetB = totalEntryBudget / 2.0

                    multA = max(1, int(budgetA // minMargen1LotA)) if (budgetA >= minMargen1LotA) else 1
                    multB = max(1, int(budgetB // minMargen1LotB)) if (budgetB >= minMargen1LotB) else 1

                    realMargenA = multA * minMargen1LotA
                    realMargenB = multB * minMargen1LotB
                    totalMargen = realMargenA + realMargenB

                    projectedMargen = currActiveMarginBefore + totalMargen
                    projectedIndicator = (cycleStartEquity / projectedMargen) * 100.0 if (projectedMargen > 0 and cycleStartEquity > 0) else 0.0

                    if (totalMargen > cycleAvailableEquity) or (projectedIndicator < 200.0):
                        # Intentar reducir a 1 lote mínimo
                        multA = 1
                        multB = 1
                        realMargenA = minMargen1LotA
                        realMargenB = minMargen1LotB
                        totalMargen = minReqMargen
                        projectedMargen = currActiveMarginBefore + totalMargen
                        projectedIndicator = (cycleStartEquity / projectedMargen) * 100.0 if (projectedMargen > 0 and cycleStartEquity > 0) else 0.0

                    if (totalMargen <= cycleAvailableEquity) and (projectedIndicator >= 200.0):
                        unitsA = multA * minLotsA
                        unitsB = multB * minLotsB

                        cycleAvailableEquity = max(0.0, cycleAvailableEquity - totalMargen)

                        currActiveMargin = currActiveMarginBefore + totalMargen
                        if currActiveMargin > cyclePeakMargin:
                            cyclePeakMargin = currActiveMargin
                        if currActiveMargin > cyclePeakCapitalReq:
                            cyclePeakCapitalReq = currActiveMargin

                        tradeMarginPct = round(float((totalMargen / cycleStartEquity) * 100.0), 1) if cycleStartEquity > 0 else 0.0
                        tradeMarginIndicator = round(float((cycleStartEquity / currActiveMargin) * 100.0), 1) if currActiveMargin > 0 else 0.0
                        activeTrades.append({
                            "signalType": sigType,
                            "direction": direction,
                            "entryDate": d,
                            "entryIndex": i,
                            "entryPxA": pxA,
                            "entryPxB": pxB,
                            "margin": totalMargen,
                            "marginA": round(float(realMargenA), 2),
                            "marginB": round(float(realMargenB), 2),
                            "availableCapital": round(float(cycleAvailableEquity), 2),
                            "multA": multA,
                            "multB": multB,
                            "unitsA": unitsA,
                            "unitsB": unitsB,
                            "marginAccountPct": tradeMarginPct,
                            "marginIndicator": tradeMarginIndicator,
                            "isSkipped": False
                        })
                    else:
                        isCycleMarginCapped = True
                        cycleStoppedEntries += 1
                        cycleSkippedTrades.append({
                            "signalType": sigType,
                            "direction": direction,
                            "entryDate": d,
                            "entryIndex": i,
                            "entryPxA": pxA,
                            "entryPxB": pxB,
                            "isSkipped": True
                        })

        # 3. PROCESAR POSICIONES ABIERTAS / EN CURSO AL FINAL DEL HISTORIAL
        openCycleTrades = []
        if activeTrades or cycleSkippedTrades:
            lastPxA = pricesA[-1]
            lastPxB = pricesB[-1]
            openPnl = 0.0
            openMargin = sum(t["margin"] for t in activeTrades)
            openMarginA = sum(t.get("marginA", 0.0) for t in activeTrades)
            openMarginB = sum(t.get("marginB", 0.0) for t in activeTrades)
            openUnitsA = sum(t["unitsA"] for t in activeTrades)
            openUnitsB = sum(t["unitsB"] for t in activeTrades)

            for t in activeTrades:
                rawTradesCount += 1
                if "LONG " + nameA in t["direction"] or "LONG A" in t["direction"]:
                    deltaA = (lastPxA - t["entryPxA"])
                    deltaB = (t["entryPxB"] - lastPxB)
                else:
                    deltaA = (t["entryPxA"] - lastPxA)
                    deltaB = (lastPxB - t["entryPxB"])

                pipsA = deltaA / pipA if pipA > 0 else 0.0
                pipsB = deltaB / pipB if pipB > 0 else 0.0

                pipValA = t["unitsA"] * pipA if quoteA == "USD" else ((t["unitsA"] * pipA) / lastPxA if lastPxA > 0 else t["unitsA"] * pipA)
                pipValB = t["unitsB"] * pipB if quoteB == "USD" else ((t["unitsB"] * pipB) / lastPxB if lastPxB > 0 else t["unitsB"] * pipB)

                pnlA = pipsA * pipValA
                pnlB = pipsB * pipValB

                tradePnl = pnlA + pnlB
                tradeNetRet = (tradePnl / t["margin"]) if t["margin"] > 0 else 0.0
                openPnl += tradePnl

                openCycleTrades.append({
                    "tradeNum": rawTradesCount,
                    "isSubtotal": False,
                    "isSkipped": False,
                    "isOpen": True,
                    "cycleNum": cycleCounter + 1,
                    "signalType": f"{t['signalType']} (EN CURSO)",
                    "direction": t["direction"],
                    "entryDate": t["entryDate"],
                    "exitDate": "EN CURSO",
                    "allocatedCapital": round(float(t["margin"]), 2),
                    "marginA": round(float(t.get("marginA", 0.0)), 2),
                    "marginB": round(float(t.get("marginB", 0.0)), 2),
                    "availableCapital": round(float(t.get("availableCapital", 0.0)), 2),
                    "accumCapital": round(float(equity + tradePnl), 2),
                    "multA": t["multA"],
                    "multB": t["multB"],
                    "unitsA": int(t["unitsA"]),
                    "unitsB": int(t["unitsB"]),
                    "pipsA": round(float(pipsA), 1),
                    "pipsB": round(float(pipsB), 1),
                    "pnlA": round(float(pnlA), 2),
                    "pnlB": round(float(pnlB), 2),
                    "entryPriceA": round(float(t["entryPxA"]), 5),
                    "exitPriceA": round(float(lastPxA), 5),
                    "entryPriceB": round(float(t["entryPxB"]), 5),
                    "exitPriceB": round(float(lastPxB), 5),
                    "durationBars": (totalBars - 1) - t["entryIndex"],
                    "returnPct": round(float(tradeNetRet * 100.0), 2),
                    "pnl": round(float(tradePnl), 2),
                    "exitReason": "POSICION_ABIERTA_EN_CURSO",
                    "isWin": bool(tradePnl > 0),
                    "entryIndex": t.get("entryIndex", 0),
                    "marginAccountPct": t.get("marginAccountPct", 0.0),
                    "marginIndicator": t.get("marginIndicator", 0.0)
                })

            for st in cycleSkippedTrades:
                openCycleTrades.append({
                    "tradeNum": None,
                    "isSubtotal": False,
                    "isSkipped": True,
                    "isOpen": True,
                    "cycleNum": cycleCounter + 1,
                    "signalType": f"{st['signalType']} (NO OPERADA)",
                    "direction": st["direction"],
                    "entryDate": st["entryDate"],
                    "exitDate": "EN CURSO",
                    "allocatedCapital": None,
                    "marginA": None,
                    "marginB": None,
                    "marginAccountPct": None,
                    "marginIndicator": None,
                    "availableCapital": None,
                    "accumCapital": None,
                    "multA": None,
                    "multB": None,
                    "unitsA": None,
                    "unitsB": None,
                    "pipsA": None,
                    "pipsB": None,
                    "pnlA": None,
                    "pnlB": None,
                    "entryPriceA": round(float(st["entryPxA"]), 5),
                    "exitPriceA": round(float(lastPxA), 5),
                    "entryPriceB": round(float(st["entryPxB"]), 5),
                    "exitPriceB": round(float(lastPxB), 5),
                    "durationBars": (totalBars - 1) - st.get("entryIndex", 0),
                    "returnPct": None,
                    "pnl": None,
                    "exitReason": "INDICADOR_MARGEN_MENOR_200",
                    "isWin": None,
                    "entryIndex": st.get("entryIndex", 0)
                })

            openCycleTrades.sort(key=lambda x: x.get("entryIndex", 0))

            for ot in openCycleTrades:
                finishedTrades.append(ot)

            operatedOpenTrades = [ot for ot in openCycleTrades if not ot.get("isSkipped")]

            if operatedOpenTrades:
                openEntrySpan = f"{operatedOpenTrades[0]['entryDate']} a {operatedOpenTrades[-1]['entryDate']}" if len(operatedOpenTrades) > 1 else operatedOpenTrades[0]['entryDate']
                firstIdx = operatedOpenTrades[0].get("entryIndex", 0)
            else:
                openEntrySpan = openCycleTrades[0]['entryDate']
                firstIdx = openCycleTrades[0].get("entryIndex", 0)

            lastIdx = totalBars - 1
            if has_time:
                diffSecs = (commonIdx[lastIdx] - commonIdx[firstIdx]).total_seconds()
                openDurationStr = f"{max(1, int(round(diffSecs / 3600.0)))}h"
            else:
                diffDays = (commonIdx[lastIdx].date() - commonIdx[firstIdx].date()).days if hasattr(commonIdx[firstIdx], "date") else (lastIdx - firstIdx)
                openDurationStr = f"{max(1, int(diffDays))}d"

            openAvgEntryPxA = (sum(ot["entryPriceA"] * ot["unitsA"] for ot in operatedOpenTrades) / openUnitsA) if openUnitsA > 0 else 0.0
            openAvgEntryPxB = (sum(ot["entryPriceB"] * ot["unitsB"] for ot in operatedOpenTrades) / openUnitsB) if openUnitsB > 0 else 0.0
            openMarginPct = round(float((openMargin / cycleStartEquity) * 100.0), 1) if cycleStartEquity > 0 else 0.0
            openMarginIndicator = round(float((cycleStartEquity / openMargin) * 100.0), 1) if openMargin > 0 else 0.0

            finishedTrades.append({
                "tradeNum": None,
                "isSubtotal": True,
                "isOpen": True,
                "cycleNum": cycleCounter + 1,
                "signalType": "SUBTOTAL POSICIONES ABIERTAS",
                "direction": f"● En Curso ({len(operatedOpenTrades)} ops activas)",
                "entryDate": openDurationStr,
                "entryDateRange": openEntrySpan,
                "exitDate": "EN CURSO",
                "allocatedCapital": round(float(openMargin), 2),
                "marginA": round(float(openMarginA), 2),
                "marginB": round(float(openMarginB), 2),
                "availableCapital": round(float(cycleAvailableEquity), 2),
                "accumCapital": round(float(equity + openPnl), 2),
                "multA": None,
                "multB": None,
                "unitsA": int(openUnitsA),
                "unitsB": int(openUnitsB),
                "pipsA": round(float(sum(ot["pipsA"] for ot in operatedOpenTrades if ot.get("pipsA") is not None)), 1),
                "pipsB": round(float(sum(ot["pipsB"] for ot in operatedOpenTrades if ot.get("pipsB") is not None)), 1),
                "pnlA": round(float(sum(ot["pnlA"] for ot in operatedOpenTrades if ot.get("pnlA") is not None)), 2),
                "pnlB": round(float(sum(ot["pnlB"] for ot in operatedOpenTrades if ot.get("pnlB") is not None)), 2),
                "entryPriceA": round(float(openAvgEntryPxA), 5),
                "exitPriceA": round(float(lastPxA), 5),
                "entryPriceB": round(float(openAvgEntryPxB), 5),
                "exitPriceB": round(float(lastPxB), 5),
                "durationBars": len(operatedOpenTrades),
                "returnPct": round((openPnl / openMargin) * 100.0, 2) if openMargin > 0 else 0.0,
                "pnl": round(float(openPnl), 2),
                "exitReason": "POSICIONES_ACTIVAS",
                "isWin": bool(openPnl > 0),
                "isMarginCapped": bool(isCycleMarginCapped or len(cycleSkippedTrades) > 0),
                "stoppedEntriesCount": int(len(cycleSkippedTrades)),
                "peakCapitalRequired": round(float(cyclePeakCapitalReq), 2),
                "peakMargin": round(float(cyclePeakMargin), 2),
                "maxAdverseFloat": round(float(cycleMaxAdverseFloat), 2),
                "marginAccountPct": openMarginPct,
                "marginIndicator": openMarginIndicator
            })

        # Métricas consolidadas
        realTrades = [t for t in finishedTrades if not t.get("isSubtotal", False) and not t.get("isSkipped", False)]
        totalTrades = len(realTrades)
        wins = [t for t in realTrades if t["isWin"]]
        losses = [t for t in realTrades if not t["isWin"]]

        winRate = (len(wins) / totalTrades * 100.0) if totalTrades > 0 else 0.0
        totalGrossWin = sum(t["pnl"] for t in wins) if wins else 0.0
        totalGrossLoss = abs(sum(t["pnl"] for t in losses)) if losses else 0.0

        profitFactor = (totalGrossWin / totalGrossLoss) if totalGrossLoss > 0 else (99.0 if totalGrossWin > 0 else 0.0)
        totalReturnPct = ((equity - initialCapital) / initialCapital) * 100.0 if initialCapital > 0 else 0.0

        returnsList = [t["returnPct"] / 100.0 for t in realTrades]
        if len(returnsList) > 2 and np.std(returnsList) > 0:
            sharpeRatio = (np.mean(returnsList) / np.std(returnsList)) * np.sqrt(252 / max(1, np.mean([t["durationBars"] for t in realTrades])))
        else:
            sharpeRatio = 0.0

        totalDays = (int((commonIdx[-1].date() - commonIdx[0].date()).days) + 1) if len(commonIdx) > 0 and hasattr(commonIdx[0], "date") else (totalBars if totalBars > 0 else 0)

        # Cálculo de ciclos detectados y promedios temporales por ciclo (como en Hechos)
        totalCycles = (cycleCounter + 1) if len(openCycleTrades) > 0 else cycleCounter
        if totalTrades == 0:
            totalCycles = 0

        total_seconds = (commonIdx[-1] - commonIdx[0]).total_seconds() if len(commonIdx) > 1 else 0.0
        total_hours = max(1.0, total_seconds / 3600.0) if total_seconds > 0 else float(totalBars)
        avg_cycle_hours = round(total_hours / totalCycles, 1) if totalCycles > 0 else 0.0
        avg_cycle_days = round(totalDays / totalCycles, 1) if totalCycles > 0 else 0.0

        # Subtotales de ciclos para cálculo de rendimiento promedio y entradas por ciclo
        subtotalTrades = [t for t in finishedTrades if t.get("isSubtotal", False)]
        cycleReturns = [t["returnPct"] for t in subtotalTrades if t.get("returnPct") is not None]
        cyclePnls = [t["pnl"] for t in subtotalTrades if t.get("pnl") is not None]

        # 1. Rendimiento promedio por ciclo (% y $ USD)
        avgReturnPerCycle = round(float(np.mean(cycleReturns)), 2) if cycleReturns else 0.0
        avgPnlPerCycle = round(float(np.mean(cyclePnls)), 2) if cyclePnls else 0.0

        # 2. Promedio de entradas por ciclo
        avgEntriesPerCycle = round(float(totalTrades / totalCycles), 1) if totalCycles > 0 else 0.0

        # 3. Métricas de Capital Requerido, Margen Pico y Separación de Precio (Flotante)
        cycleCapReqs = [t["peakCapitalRequired"] for t in subtotalTrades if t.get("peakCapitalRequired") is not None]
        cyclePeakMargins = [t["peakMargin"] for t in subtotalTrades if t.get("peakMargin") is not None]
        cycleAdverseFloats = [t["maxAdverseFloat"] for t in subtotalTrades if t.get("maxAdverseFloat") is not None]

        avgCapitalRequiredPerCycle = round(float(np.mean(cycleCapReqs)), 2) if cycleCapReqs else 0.0
        avgPeakMarginPerCycle = round(float(np.mean(cyclePeakMargins)), 2) if cyclePeakMargins else 0.0
        avgAdverseFloatPerCycle = round(float(np.mean(cycleAdverseFloats)), 2) if cycleAdverseFloats else 0.0

        # 4. Ciclos que alcanzaron tope de margen y total de entradas detenidas
        marginCappedCycles = sum(1 for t in subtotalTrades if t.get("isMarginCapped", False))
        totalStoppedEntries = sum(t.get("stoppedEntriesCount", 0) for t in subtotalTrades)

        return {
            "mode": "TRIANGLES_AND_BOXES" if includeBoxes else "TRIANGLES_ONLY",
            "modeLabel": "Triángulos + Cuadros (Con Equidad)" if includeBoxes else "Solo Triángulos (Coincidentes)",
            "allocationPct": allocationPct,
            "minLotsA": minLotsA,
            "minLotsB": minLotsB,
            "margenPctA": margenPctA,
            "margenPctB": margenPctB,
            "reqMarginPerMinLot": round(float(sampleReqMarginLot), 2),
            "totalTrades": totalTrades,
            "totalCycles": totalCycles,
            "avgCycleHours": avg_cycle_hours,
            "avgCycleDays": avg_cycle_days,
            "avgReturnPerCycle": avgReturnPerCycle,
            "avgPnlPerCycle": avgPnlPerCycle,
            "avgEntriesPerCycle": avgEntriesPerCycle,
            "avgCapitalRequiredPerCycle": avgCapitalRequiredPerCycle,
            "avgPeakMarginPerCycle": avgPeakMarginPerCycle,
            "avgAdverseFloatPerCycle": avgAdverseFloatPerCycle,
            "marginCappedCycles": marginCappedCycles,
            "totalStoppedEntries": totalStoppedEntries,
            "winningTrades": len(wins),
            "losingTrades": len(losses),
            "winRate": round(float(winRate), 2),
            "profitFactor": round(float(profitFactor), 2),
            "sharpeRatio": round(float(sharpeRatio), 2),
            "maxDrawdown": round(float(maxDrawdown * 100.0), 2),
            "totalReturnPct": round(float(totalReturnPct), 2),
            "initialCapital": float(initialCapital),
            "finalCapital": round(float(equity), 2),
            "netProfit": round(float(equity - initialCapital), 2),
            "startDate": dates[0] if len(dates) > 0 else "-",
            "endDate": dates[-1] if len(dates) > 0 else "-",
            "totalBars": totalBars,
            "totalDays": totalDays,
            "equityCurve": equityCurve,
            "trades": finishedTrades
        }


cruceEmaEngine = CruceEmaEngine()
