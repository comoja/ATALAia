"""
Motor de Análisis de Cruces EMA para Pair Trading y Arbitraje Estadístico de Ratios.
Diseñado para cálculo de Precios Normalizados, EMAs, Cruces en Zonas Extremas (Triángulos y Cuadros),
Cierre en Media (●) y Bitácora de Trades con Dimensionamiento Institucional al 3%.
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class CruceEmaEngine:
    """
    Motor matemático para el Análisis de Cruces EMA.
    """

    def __init__(self, riskFreeRate: float = 0.03):
        self.riskFreeRate = riskFreeRate

    def runSignalBacktest(
        self,
        dfA: pd.DataFrame,
        dfB: pd.DataFrame,
        pairA: str = "",
        pairB: str = "",
        smaPeriod: int = 3,
        sigmaWindow: int = 30,
        includeBoxes: bool = False,
        initialCapital: float = 10000.0,
        allocationPct: float = 3.0,  # 3% del capital de la cuenta por entrada
        minLotsA: float = 1000.0,    # symbols.min_lots de BD para Par A
        minLotsB: float = 1000.0,    # symbols.min_lots de BD para Par B
        margenPctA: float = 0.25,    # symbols.margen (%) de BD para Par A
        margenPctB: float = 1.0,     # symbols.margen (%) de BD para Par B
        pipA: float = 0.00010,       # symbols.pip de BD para Par A
        pipB: float = 0.01000,       # symbols.pip de BD para Par B
        quoteA: str = "USD",         # symbols.quote_currency de BD para Par A
        quoteB: str = "USD",         # symbols.quote_currency de BD para Par B
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
                "finalCapital": float(initialCapital), "netProfit": 0.0, "equityCurve": [], "trades": []
            }

        sA = dfA.loc[commonIdx, 'closePrice'] if 'closePrice' in dfA.columns else dfA.loc[commonIdx].iloc[:, 0]
        sB = dfB.loc[commonIdx, 'closePrice'] if 'closePrice' in dfB.columns else dfB.loc[commonIdx].iloc[:, 0]

        # Normalización Min-Max (0 a 1)
        minA, maxA = sA.min(), sA.max()
        minB, maxB = sB.min(), sB.max()
        rangeA = (maxA - minA) if (maxA - minA) != 0 else 1.0
        rangeB = (maxB - minB) if (maxB - minB) != 0 else 1.0

        normA = (sA - minA) / rangeA
        normB = (sB - minB) / rangeB

        # EMAs rápidas
        emaA = normA.ewm(span=smaPeriod, adjust=False).mean()
        emaB = normB.ewm(span=smaPeriod, adjust=False).mean()

        # Media entre ambos pares
        priceMean = (normA + normB) / 2.0
        avgOfMean = float(priceMean.mean())

        # Desviación estándar de los Precios Normalizados anclada al promedio de la media (idéntica a la gráfica)
        stdNormA = float(normA.std())
        stdNormB = float(normB.std())

        stdAboveA = avgOfMean + stdNormA
        stdBelowA = avgOfMean - stdNormA
        stdAboveB = avgOfMean + stdNormB
        stdBelowB = avgOfMean - stdNormB

        dates = [str(d)[:10] for d in commonIdx]
        pricesA = sA.values
        pricesB = sB.values
        normA_vals = normA.values
        normB_vals = normB.values
        emaA_vals = emaA.values
        emaB_vals = emaB.values
        n = len(dates)

        active_trades: List[Dict[str, Any]] = []
        finished_trades: List[Dict[str, Any]] = []
        raw_trades_count = 0
        cycle_counter = 0
        equity = float(initialCapital) if initialCapital > 0 else 800.0
        cycle_start_equity = equity # Capital base del ciclo para calcular el 3%
        cycle_available_equity = equity # Capital disminuido decrementando margen para entradas sucesivas
        peakEquity = equity
        maxDrawdown = 0.0
        equityCurve = []
        costRate = (commissionBps + slippageBps) / 10000.0
        allocationRate = allocationPct / 100.0

        # Ratios de margen institucional directo desde symbols.margen
        margenRateA = (margenPctA / 100.0) if margenPctA >= 0.05 else margenPctA
        margenRateB = (margenPctB / 100.0) if margenPctB >= 0.05 else margenPctB

        sample_req_margin_lot = 20.0

        for i in range(1, n):
            d = dates[i]
            pxA = pricesA[i]
            pxB = pricesB[i]

            # Calcular PnL no realizado de los trades activos mediante pips
            unrealizedTotalPnl = 0.0
            if active_trades:
                for t in active_trades:
                    if "LONG " + nameA in t["direction"] or "LONG A" in t["direction"]:
                        dA = (pxA - t["entryPxA"])
                        dB = (t["entryPxB"] - pxB)
                    else:
                        dA = (t["entryPxA"] - pxA)
                        dB = (pxB - t["entryPxB"])

                    pipsA_now = dA / pipA if pipA > 0 else 0.0
                    pipsB_now = dB / pipB if pipB > 0 else 0.0

                    pipValA_now = t["unitsA"] * pipA if quoteA == "USD" else ((t["unitsA"] * pipA) / pxA if pxA > 0 else t["unitsA"] * pipA)
                    pipValB_now = t["unitsB"] * pipB if quoteB == "USD" else ((t["unitsB"] * pipB) / pxB if pxB > 0 else t["unitsB"] * pipB)

                    nomA = t["unitsA"] * pxA if quoteA == "USD" else t["unitsA"]
                    nomB = t["unitsB"] if quoteB != "USD" else t["unitsB"] * pxB
                    c_now = (nomA + nomB) * costRate

                    unrealizedTotalPnl += (pipsA_now * pipValA_now) + (pipsB_now * pipValB_now) - c_now

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

            # Detectar señales en la vela i
            currP_A, prevP_A = normA_vals[i], normA_vals[i - 1]
            currE_A, prevE_A = emaA_vals[i], emaA_vals[i - 1]
            currP_B, prevP_B = normB_vals[i], normB_vals[i - 1]
            currE_B, prevE_B = emaB_vals[i], emaB_vals[i - 1]

            isCrossDownHigh_A = (prevP_A >= prevE_A and currP_A < currE_A and (prevP_A >= stdAboveA or currP_A >= stdAboveA))
            isCrossUpLow_A = (prevP_A <= prevE_A and currP_A > currE_A and (prevP_A <= stdBelowA or currP_A <= stdBelowA))
            hasSignal_A = (isCrossDownHigh_A or isCrossUpLow_A)

            isCrossDownHigh_B = (prevP_B >= prevE_B and currP_B < currE_B and (prevP_B >= stdAboveB or currP_B >= stdAboveB))
            isCrossUpLow_B = (prevP_B <= prevE_B and currP_B > currE_B and (prevP_B <= stdBelowB or currP_B <= stdBelowB))
            hasSignal_B = (isCrossDownHigh_B or isCrossUpLow_B)

            # Detectar cruce con la media (CÍRCULO DE SALIDA)
            prevDiff = normA_vals[i - 1] - normB_vals[i - 1]
            currDiff = normA_vals[i - 1] - normB_vals[i] if i == 0 else normA_vals[i] - normB_vals[i]
            prevDiffReal = normA_vals[i - 1] - normB_vals[i - 1]
            isMeanCross = (prevDiffReal > 0 and currDiff <= 0) or (prevDiffReal < 0 and currDiff >= 0)

            # 1. CIERRE DE TODAS LAS ENTRADAS DEL CICLO EN LA MEDIA (●) -> LIQUIDACIÓN POR PIPS Y REINVERSIÓN
            if isMeanCross:
                if active_trades:
                    cycle_counter += 1
                    cycle_trades = []
                    cycle_pnl = 0.0
                    cycle_margin = sum(t["margin"] for t in active_trades)
                    cycle_margin_a = sum(t.get("marginA", 0.0) for t in active_trades)
                    cycle_margin_b = sum(t.get("marginB", 0.0) for t in active_trades)
                    cycle_units_a = sum(t["unitsA"] for t in active_trades)
                    cycle_units_b = sum(t["unitsB"] for t in active_trades)

                    for t in active_trades:
                        raw_trades_count += 1
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
                        cycle_pnl += tradePnl

                        cycle_trades.append({
                            "tradeNum": raw_trades_count,
                            "isSubtotal": False,
                            "cycleNum": cycle_counter,
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

                    cycleAvgRet = round((cycle_pnl / cycle_margin) * 100.0, 2) if cycle_margin > 0 else 0.0
                    for ct in cycle_trades:
                        finished_trades.append(ct)

                    entrySpan = f"{cycle_trades[0]['entryDate'][:10]} a {cycle_trades[-1]['entryDate'][:10]}" if len(cycle_trades) > 1 else cycle_trades[0]['entryDate'][:10]
                    finished_trades.append({
                        "tradeNum": None,
                        "isSubtotal": True,
                        "cycleNum": cycle_counter,
                        "signalType": f"SUBTOTAL CIERRE #{cycle_counter}",
                        "direction": f"● Salida Media ({len(cycle_trades)} ops)",
                        "entryDate": entrySpan,
                        "exitDate": d,
                        "allocatedCapital": round(float(cycle_margin), 2),
                        "marginA": round(float(cycle_margin_a), 2),
                        "marginB": round(float(cycle_margin_b), 2),
                        "availableCapital": round(float(equity), 2),
                        "accumCapital": round(float(equity), 2),
                        "multA": None,
                        "multB": None,
                        "unitsA": int(cycle_units_a),
                        "unitsB": int(cycle_units_b),
                        "pipsA": round(float(sum(ct["pipsA"] for ct in cycle_trades)), 1),
                        "pipsB": round(float(sum(ct["pipsB"] for ct in cycle_trades)), 1),
                        "pnlA": round(float(sum(ct["pnlA"] for ct in cycle_trades)), 2),
                        "pnlB": round(float(sum(ct["pnlB"] for ct in cycle_trades)), 2),
                        "entryPriceA": None,
                        "exitPriceA": None,
                        "entryPriceB": None,
                        "exitPriceB": None,
                        "durationBars": len(cycle_trades),
                        "returnPct": cycleAvgRet,
                        "pnl": round(float(cycle_pnl), 2),
                        "exitReason": f"CIERRE EN MEDIA (●) {d}",
                        "isWin": bool(cycle_pnl > 0)
                    })

                    # REINVERSIÓN AL CIERRE
                    cycle_start_equity = equity
                    cycle_available_equity = equity
                    active_trades = []

                # EL CRUCE DE MEDIA PREDOMINA: No se abren órdenes en fecha de cruce de media
                continue

            # 2. EVALUAR ENTRADA CON 3% DE CAPITAL, symbols.margen Y NOMBRES REALES DE PARES
            nameA = pairA if pairA else "Par A"
            nameB = pairB if pairB else "Par B"
            sigType, direction = None, None
            if hasSignal_A and hasSignal_B:
                if isCrossUpLow_A and isCrossDownHigh_B:
                    sigType = f"TRIANGULO_VERDE ({nameA} + {nameB})"
                    direction = f"LONG {nameA} / SHORT {nameB}"
                elif isCrossDownHigh_A and isCrossUpLow_B:
                    sigType = f"TRIANGULO_ROJO ({nameA} + {nameB})"
                    direction = f"SHORT {nameA} / LONG {nameB}"

            elif includeBoxes and (hasSignal_A or hasSignal_B):
                if hasSignal_A:
                    if isCrossUpLow_A:
                        sigType = f"CUADRO_VERDE {nameA}"
                        direction = f"LONG {nameA} / SHORT {nameB}"
                    elif isCrossDownHigh_A:
                        sigType = f"CUADRO_ROJO {nameA}"
                        direction = f"SHORT {nameA} / LONG {nameB}"
                elif hasSignal_B:
                    if isCrossUpLow_B:
                        sigType = f"CUADRO_VERDE {nameB}"
                        direction = f"SHORT {nameA} / LONG {nameB}"
                    elif isCrossDownHigh_B:
                        sigType = f"CUADRO_ROJO {nameB}"
                        direction = f"LONG {nameA} / SHORT {nameB}"

            if sigType and direction:
                totalEntryBudget = max(0.0, cycle_available_equity) * allocationRate
                budgetA = totalEntryBudget / 2.0
                budgetB = totalEntryBudget / 2.0

                # Margen invertido = symbols.margen * lote
                margen1LotA = minLotsA * margenRateA
                margen1LotB = minLotsB * margenRateB
                sample_req_margin_lot = margen1LotA + margen1LotB

                multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA) else 1
                multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB) else 1

                realMargenA = multA * margen1LotA
                realMargenB = multB * margen1LotB
                totalMargen = realMargenA + realMargenB

                unitsA = multA * minLotsA
                unitsB = multB * minLotsB

                # Decrementar capital disminuido con el margen retenido
                cycle_available_equity = max(0.0, cycle_available_equity - totalMargen)

                active_trades.append({
                    "signalType": sigType,
                    "direction": direction,
                    "entryDate": d,
                    "entryIndex": i,
                    "entryPxA": pxA,
                    "entryPxB": pxB,
                    "margin": totalMargen,
                    "marginA": round(float(realMargenA), 2),
                    "marginB": round(float(realMargenB), 2),
                    "availableCapital": round(float(cycle_available_equity), 2),
                    "multA": multA,
                    "multB": multB,
                    "unitsA": unitsA,
                    "unitsB": unitsB
                })

        # 3. PROCESAR POSICIONES ABIERTAS / EN CURSO AL FINAL DEL HISTORIAL
        if active_trades:
            lastPxA = pricesA[-1]
            lastPxB = pricesB[-1]
            lastDate = dates[-1]
            open_cycle_trades = []
            open_pnl = 0.0
            open_margin = sum(t["margin"] for t in active_trades)
            open_margin_a = sum(t.get("marginA", 0.0) for t in active_trades)
            open_margin_b = sum(t.get("marginB", 0.0) for t in active_trades)
            open_units_a = sum(t["unitsA"] for t in active_trades)
            open_units_b = sum(t["unitsB"] for t in active_trades)

            for t in active_trades:
                raw_trades_count += 1
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
                open_pnl += tradePnl

                open_cycle_trades.append({
                    "tradeNum": raw_trades_count,
                    "isSubtotal": False,
                    "isOpen": True,
                    "cycleNum": cycle_counter + 1,
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
                    "durationBars": (n - 1) - t["entryIndex"],
                    "returnPct": round(float(tradeNetRet * 100.0), 2),
                    "pnl": round(float(tradePnl), 2), # PnL actual flotante
                    "exitReason": "POSICION_ABIERTA_EN_CURSO",
                    "isWin": bool(tradePnl > 0)
                })

            for ot in open_cycle_trades:
                finished_trades.append(ot)

            openEntrySpan = f"{open_cycle_trades[0]['entryDate']} a {open_cycle_trades[-1]['entryDate']}" if len(open_cycle_trades) > 1 else open_cycle_trades[0]['entryDate']
            finished_trades.append({
                "tradeNum": None,
                "isSubtotal": True,
                "isOpen": True,
                "cycleNum": cycle_counter + 1,
                "signalType": "SUBTOTAL POSICIONES ABIERTAS",
                "direction": f"● En Curso ({len(open_cycle_trades)} ops activas)",
                "entryDate": openEntrySpan,
                "exitDate": "EN CURSO",
                "allocatedCapital": round(float(open_margin), 2),
                "marginA": round(float(open_margin_a), 2),
                "marginB": round(float(open_margin_b), 2),
                "availableCapital": round(float(cycle_available_equity), 2),
                "accumCapital": round(float(equity + open_pnl), 2),
                "multA": None,
                "multB": None,
                "unitsA": int(open_units_a),
                "unitsB": int(open_units_b),
                "pipsA": round(float(sum(ot["pipsA"] for ot in open_cycle_trades)), 1),
                "pipsB": round(float(sum(ot["pipsB"] for ot in open_cycle_trades)), 1),
                "pnlA": round(float(sum(ot["pnlA"] for ot in open_cycle_trades)), 2),
                "pnlB": round(float(sum(ot["pnlB"] for ot in open_cycle_trades)), 2),
                "entryPriceA": None,
                "exitPriceA": None,
                "entryPriceB": None,
                "exitPriceB": None,
                "durationBars": len(open_cycle_trades),
                "returnPct": round(float((open_pnl / open_margin) * 100.0), 2) if open_margin > 0 else 0.0,
                "pnl": round(float(open_pnl), 2),
                "exitReason": "FLOTANTE_ACTUAL",
                "isWin": bool(open_pnl > 0)
            })

        # Métricas consolidadas
        real_trades = [t for t in finished_trades if not t.get("isSubtotal", False)]
        totalTrades = len(real_trades)
        wins = [t for t in real_trades if t["isWin"]]
        losses = [t for t in real_trades if not t["isWin"]]

        winRate = (len(wins) / totalTrades * 100.0) if totalTrades > 0 else 0.0
        totalGrossWin = sum(t["pnl"] for t in wins) if wins else 0.0
        totalGrossLoss = abs(sum(t["pnl"] for t in losses)) if losses else 0.0

        profitFactor = (totalGrossWin / totalGrossLoss) if totalGrossLoss > 0 else (99.0 if totalGrossWin > 0 else 0.0)
        totalReturnPct = ((equity - initialCapital) / initialCapital) * 100.0 if initialCapital > 0 else 0.0

        returnsList = [t["returnPct"] / 100.0 for t in real_trades]
        if len(returnsList) > 2 and np.std(returnsList) > 0:
            sharpeRatio = (np.mean(returnsList) / np.std(returnsList)) * np.sqrt(252 / max(1, np.mean([t["durationBars"] for t in real_trades])))
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
            "reqMarginPerMinLot": round(float(sample_req_margin_lot), 2),
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
            "trades": finished_trades
        }


cruceEmaEngine = CruceEmaEngine()
