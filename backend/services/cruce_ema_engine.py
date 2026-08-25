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

        dates = [str(d)[:10] for d in commonIdx]
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

            isMeanCross = candleSig["isMeanCross"]

            # 1. CIERRE DE TODAS LAS ENTRADAS DEL CICLO EN LA MEDIA (●) -> LIQUIDACIÓN POR PIPS Y REINVERSIÓN
            if isMeanCross:
                if activeTrades:
                    cycleCounter += 1
                    cycleTrades = []
                    cyclePnl = 0.0
                    cycleMargin = sum(t["margin"] for t in activeTrades)
                    cycleMarginA = sum(t.get("marginA", 0.0) for t in activeTrades)
                    cycleMarginB = sum(t.get("marginB", 0.0) for t in activeTrades)
                    cycleUnitsA = sum(t["unitsA"] for t in activeTrades)
                    cycleUnitsB = sum(t["unitsB"] for t in activeTrades)

                    for t in activeTrades:
                        rawTradesCount += 1
                        if "LONG " + nameA in t["direction"] or "LONG A" in t["direction"]:
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

                        nominalA = t["unitsA"] * pxA if quoteA == "USD" else t["unitsA"]
                        nominalB = t["unitsB"] if quoteB != "USD" else t["unitsB"] * pxB
                        costs = (nominalA + nominalB) * (costRate * 2.0)

                        tradePnl = pnlA + pnlB - costs
                        tradeNetRet = (tradePnl / t["margin"]) if t["margin"] > 0 else 0.0

                        equity += tradePnl
                        cyclePnl += tradePnl

                        cycleTrades.append({
                            "tradeNum": rawTradesCount,
                            "isSubtotal": False,
                            "cycleNum": cycleCounter,
                            "signalType": t["signalType"],
                            "direction": t["direction"],
                            "entryDate": t["entryDate"],
                            "exitDate": d,
                            "allocatedCapital": round(float(t["margin"]), 2),
                            "marginA": round(float(t.get("marginA", 0.0)), 2),
                            "marginB": round(float(t.get("marginB", 0.0)), 2),
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
                            "exitReason": "CRUCE_MEDIA_CIRCULO",
                            "isWin": bool(tradePnl > 0)
                        })

                    cycleAvgRet = round((cyclePnl / cycleMargin) * 100.0, 2) if cycleMargin > 0 else 0.0
                    for ct in cycleTrades:
                        finishedTrades.append(ct)

                    entrySpan = f"{cycleTrades[0]['entryDate'][:10]} a {cycleTrades[-1]['entryDate'][:10]}" if len(cycleTrades) > 1 else cycleTrades[0]['entryDate'][:10]
                    finishedTrades.append({
                        "tradeNum": None,
                        "isSubtotal": True,
                        "cycleNum": cycleCounter,
                        "signalType": f"SUBTOTAL CIERRE #{cycleCounter}",
                        "direction": f"● Salida Media ({len(cycleTrades)} ops)",
                        "entryDate": entrySpan,
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
                        "pipsA": round(float(sum(ct["pipsA"] for ct in cycleTrades)), 1),
                        "pipsB": round(float(sum(ct["pipsB"] for ct in cycleTrades)), 1),
                        "pnlA": round(float(sum(ct["pnlA"] for ct in cycleTrades)), 2),
                        "pnlB": round(float(sum(ct["pnlB"] for ct in cycleTrades)), 2),
                        "entryPriceA": None,
                        "exitPriceA": None,
                        "entryPriceB": None,
                        "exitPriceB": None,
                        "durationBars": len(cycleTrades),
                        "returnPct": cycleAvgRet,
                        "pnl": round(float(cyclePnl), 2),
                        "exitReason": f"CIERRE EN MEDIA (●) {d}",
                        "isWin": bool(cyclePnl > 0)
                    })

                    # REINVERSIÓN AL CIERRE
                    cycleStartEquity = equity
                    cycleAvailableEquity = equity
                    activeTrades = []

                # EL CRUCE DE MEDIA PREDOMINA: No se abren órdenes en fecha de cruce de media
                continue

            # 2. EVALUAR ENTRADA CON 3% DE CAPITAL, symbols.margen Y NOMBRES REALES DE PARES
            sigType = candleSig.get("signalType")
            direction = candleSig.get("direction")

            if sigType and direction:
                totalEntryBudget = max(0.0, cycleAvailableEquity) * allocationRate
                budgetA = totalEntryBudget / 2.0
                budgetB = totalEntryBudget / 2.0

                margen1LotA = minLotsA * margenRateA
                margen1LotB = minLotsB * margenRateB
                sampleReqMarginLot = margen1LotA + margen1LotB

                multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA) else 1
                multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB) else 1

                realMargenA = multA * margen1LotA
                realMargenB = multB * margen1LotB
                totalMargen = realMargenA + realMargenB

                unitsA = multA * minLotsA
                unitsB = multB * minLotsB

                cycleAvailableEquity = max(0.0, cycleAvailableEquity - totalMargen)

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
                    "unitsB": unitsB
                })

        # 3. PROCESAR POSICIONES ABIERTAS / EN CURSO AL FINAL DEL HISTORIAL
        if activeTrades:
            lastPxA = pricesA[-1]
            lastPxB = pricesB[-1]
            openCycleTrades = []
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

                nominalA = t["unitsA"] * lastPxA if quoteA == "USD" else t["unitsA"]
                nominalB = t["unitsB"] if quoteB != "USD" else t["unitsB"] * lastPxB
                costs = (nominalA + nominalB) * (costRate * 2.0)

                tradePnl = pnlA + pnlB - costs
                tradeNetRet = (tradePnl / t["margin"]) if t["margin"] > 0 else 0.0
                openPnl += tradePnl

                openCycleTrades.append({
                    "tradeNum": rawTradesCount,
                    "isSubtotal": False,
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
                    "isWin": bool(tradePnl > 0)
                })

            for ot in openCycleTrades:
                finishedTrades.append(ot)

            openEntrySpan = f"{openCycleTrades[0]['entryDate']} a {openCycleTrades[-1]['entryDate']}" if len(openCycleTrades) > 1 else openCycleTrades[0]['entryDate']
            finishedTrades.append({
                "tradeNum": None,
                "isSubtotal": True,
                "isOpen": True,
                "cycleNum": cycleCounter + 1,
                "signalType": "SUBTOTAL POSICIONES ABIERTAS",
                "direction": f"● En Curso ({len(openCycleTrades)} ops activas)",
                "entryDate": openEntrySpan,
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
                "pipsA": round(float(sum(ot["pipsA"] for ot in openCycleTrades)), 1),
                "pipsB": round(float(sum(ot["pipsB"] for ot in openCycleTrades)), 1),
                "pnlA": round(float(sum(ot["pnlA"] for ot in openCycleTrades)), 2),
                "pnlB": round(float(sum(ot["pnlB"] for ot in openCycleTrades)), 2),
                "entryPriceA": None,
                "exitPriceA": None,
                "entryPriceB": None,
                "exitPriceB": None,
                "durationBars": len(openCycleTrades),
                "returnPct": round(float((openPnl / openMargin) * 100.0), 2) if openMargin > 0 else 0.0,
                "pnl": round(float(openPnl), 2),
                "exitReason": "FLOTANTE_ACTUAL",
                "isWin": bool(openPnl > 0)
            })

        # Métricas consolidadas
        realTrades = [t for t in finishedTrades if not t.get("isSubtotal", False)]
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
            "equityCurve": equityCurve,
            "trades": finishedTrades
        }


cruceEmaEngine = CruceEmaEngine()
