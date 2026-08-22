quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

new_engine_3pct = '''    # =========================================================================
    # 7. BACKTESTING CON ASIGNACIÓN DEL 3%, MULTIPLOS min_lots Y REINVERSIÓN
    # =========================================================================
    def runSignalBacktest(
        self,
        dfA: pd.DataFrame,
        dfB: pd.DataFrame,
        smaPeriod: int = 3,
        sigmaWindow: int = 30,
        includeBoxes: bool = False,
        initialCapital: float = 10000.0,
        allocationPct: float = 3.0,  # 3% del capital de la cuenta por entrada
        minLotsA: float = 1000.0,    # symbols.min_lots de BD para Par A
        minLotsB: float = 1000.0,    # symbols.min_lots de BD para Par B
        margenPctA: float = 0.25,    # symbols.margen (%) de BD para Par A
        margenPctB: float = 1.0,     # symbols.margen (%) de BD para Par B
        commissionBps: float = 2.0,
        slippageBps: float = 1.0
    ) -> Dict[str, Any]:
        """
        Ejecuta el backtest asignando un 3% del capital de la cuenta por entrada,
        calculando los múltiplos enteros del lote mínimo (symbols.min_lots) y
        reinvirtiendo el capital acumulado al cierre de todas las posiciones (●).
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

        # Desviación estándar móvil anclada al promedio de la media
        stdA = emaA.rolling(window=sigmaWindow, min_periods=max(3, sigmaWindow // 2)).std().fillna(0.0)
        stdB = emaB.rolling(window=sigmaWindow, min_periods=max(3, sigmaWindow // 2)).std().fillna(0.0)

        stdAboveA = avgOfMean + stdA
        stdBelowA = avgOfMean - stdA
        stdAboveB = avgOfMean + stdB
        stdBelowB = avgOfMean - stdB

        dates = [str(d) for d in commonIdx]
        pricesA = sA.values
        pricesB = sB.values
        normA_vals = normA.values
        normB_vals = normB.values
        emaA_vals = emaA.values
        emaB_vals = emaB.values
        stdAboveA_vals = stdAboveA.values
        stdBelowA_vals = stdBelowA.values
        stdAboveB_vals = stdAboveB.values
        stdBelowB_vals = stdBelowB.values
        n = len(dates)

        active_trades: List[Dict[str, Any]] = []
        finished_trades: List[Dict[str, Any]] = []
        raw_trades_count = 0
        cycle_counter = 0
        equity = float(initialCapital) if initialCapital > 0 else 800.0
        cycle_start_equity = equity # Capital base del ciclo para calcular el 3%
        peakEquity = equity
        maxDrawdown = 0.0
        equityCurve = []
        costMultiplier = (commissionBps + slippageBps) / 10000.0
        allocationRate = allocationPct / 100.0

        # Ratios de margen institucional desde symbols.margen
        margenRateA = (margenPctA / 100.0) if margenPctA > 0 else 0.01
        margenRateB = (margenPctB / 100.0) if margenPctB > 0 else 0.01

        sample_req_margin_lot = 20.0

        for i in range(1, n):
            d = dates[i]
            pxA = pricesA[i]
            pxB = pricesB[i]

            # Calcular PnL no realizado de los trades activos
            unrealizedTotalPnl = 0.0
            if active_trades:
                for t in active_trades:
                    if t["direction"] == "LONG A / SHORT B":
                        rA = (pxA - t["entryPxA"]) / t["entryPxA"]
                        rB = (t["entryPxB"] - pxB) / t["entryPxB"]
                    else:
                        rA = (t["entryPxA"] - pxA) / t["entryPxA"]
                        rB = (pxB - t["entryPxB"]) / t["entryPxB"]

                    tradePnlA = t["nominalA"] * rA
                    tradePnlB = t["nominalB"] * rB
                    tradeCosts = (t["nominalA"] + t["nominalB"]) * costMultiplier
                    unrealizedTotalPnl += (tradePnlA + tradePnlB - tradeCosts)

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

            isCrossDownHigh_A = (prevP_A >= prevE_A and currP_A < currE_A and currP_A >= stdAboveA_vals[i])
            isCrossUpLow_A = (prevP_A <= prevE_A and currP_A > currE_A and currP_A <= stdBelowA_vals[i])
            hasSignal_A = (isCrossDownHigh_A or isCrossUpLow_A)

            isCrossDownHigh_B = (prevP_B >= prevE_B and currP_B < currE_B and currP_B >= stdAboveB_vals[i])
            isCrossUpLow_B = (prevP_B <= prevE_B and currP_B > currE_B and currP_B <= stdBelowB_vals[i])
            hasSignal_B = (isCrossDownHigh_B or isCrossUpLow_B)

            # Detectar cruce con la media (CÍRCULO DE SALIDA)
            prevDiff = normA_vals[i - 1] - normB_vals[i - 1]
            currDiff = normA_vals[i] - normB_vals[i]
            isMeanCross = (prevDiff > 0 and currDiff <= 0) or (prevDiff < 0 and currDiff >= 0)

            # 1. CIERRE DE TODAS LAS ENTRADAS DEL CICLO EN LA MEDIA (●) -> SE REINVIERTE
            if isMeanCross and active_trades:
                cycle_counter += 1
                cycle_trades = []
                cycle_pnl = 0.0
                cycle_margin = sum(t["margin"] for t in active_trades)

                for t in active_trades:
                    raw_trades_count += 1
                    if t["direction"] == "LONG A / SHORT B":
                        rA = (pxA - t["entryPxA"]) / t["entryPxA"]
                        rB = (t["entryPxB"] - pxB) / t["entryPxB"]
                    else:
                        rA = (t["entryPxA"] - pxA) / t["entryPxA"]
                        rB = (pxB - t["entryPxB"]) / t["entryPxB"]

                    tradePnlA = t["nominalA"] * rA
                    tradePnlB = t["nominalB"] * rB
                    tradeCosts = (t["nominalA"] + t["nominalB"]) * (costMultiplier * 2.0)
                    tradePnl = tradePnlA + tradePnlB - tradeCosts
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
                        "allocatedCapital": round(float(t["margin"]), 2), # Margen invertido en esta entrada
                        "nominalCapital": round(float(t["nominalA"] + t["nominalB"]), 2),
                        "multA": t["multA"],
                        "multB": t["multB"],
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

                # Fila de Subtotal de Cierre
                entrySpan = f"{cycle_trades[0]['entryDate'][:10]} a {cycle_trades[-1]['entryDate'][:10]}" if len(cycle_trades) > 1 else cycle_trades[0]['entryDate'][:10]
                finished_trades.append({
                    "tradeNum": None,
                    "isSubtotal": True,
                    "cycleNum": cycle_counter,
                    "signalType": f"SUBTOTAL CIERRE #{cycle_counter}",
                    "direction": f"● Salida Media ({len(cycle_trades)} ops)",
                    "entryDate": f"Entradas: {entrySpan}",
                    "exitDate": d,
                    "allocatedCapital": round(float(cycle_margin), 2),
                    "nominalCapital": round(sum(ct['nominalCapital'] for ct in cycle_trades), 2),
                    "multA": None,
                    "multB": None,
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

                # REINVERSIÓN: El capital para el siguiente ciclo se actualiza al nuevo saldo
                cycle_start_equity = equity
                active_trades = []

            # 2. EVALUAR ENTRADA (3% DEL CAPITAL DISPONIBLE DEL CICLO EN MÚLTIPLOS DE min_lots)
            sigType, direction = None, None
            if hasSignal_A and hasSignal_B:
                if isCrossUpLow_A and isCrossDownHigh_B:
                    sigType = "TRIANGULO_VERDE_COINCIDENTE"
                    direction = "LONG A / SHORT B"
                elif isCrossDownHigh_A and isCrossUpLow_B:
                    sigType = "TRIANGULO_ROJO_COINCIDENTE"
                    direction = "SHORT A / LONG B"

            elif includeBoxes and (hasSignal_A or hasSignal_B):
                if hasSignal_A:
                    if isCrossUpLow_A:
                        sigType = "CUADRO_VERDE_PAR_A"
                        direction = "LONG A / SHORT B"
                    elif isCrossDownHigh_A:
                        sigType = "CUADRO_ROJO_PAR_A"
                        direction = "SHORT A / LONG B"
                elif hasSignal_B:
                    if isCrossUpLow_B:
                        sigType = "CUADRO_VERDE_PAR_B"
                        direction = "SHORT A / LONG B"
                    elif isCrossDownHigh_B:
                        sigType = "CUADRO_ROJO_PAR_B"
                        direction = "LONG A / SHORT B"

            if sigType and direction:
                # 3% del capital base del ciclo para la entrada (ej. $800 * 3% = $24.00)
                totalEntryBudget = cycle_start_equity * allocationRate
                budgetA = totalEntryBudget / 2.0
                budgetB = totalEntryBudget / 2.0

                # Margen requerido por 1 lote mínimo de cada pata
                margen1LotA = (minLotsA * pxA) * margenRateA
                margen1LotB = (minLotsB * (1.0 if 'USD' in pairB else pxB)) * margenRateB
                sample_req_margin_lot = margen1LotA + margen1LotB

                # Múltiplos enteros de min_lots según presupuesto del 3%
                multA = max(1, int(budgetA // margen1LotA)) if (budgetA >= margen1LotA) else 1
                multB = max(1, int(budgetB // margen1LotB)) if (budgetB >= margen1LotB) else 1

                realMargenA = multA * margen1LotA
                realMargenB = multB * margen1LotB

                realNominalA = multA * minLotsA * pxA
                realNominalB = multB * minLotsB * (1.0 if 'USD' in pairB else pxB)

                active_trades.append({
                    "signalType": sigType,
                    "direction": direction,
                    "entryDate": d,
                    "entryIndex": i,
                    "entryPxA": pxA,
                    "entryPxB": pxB,
                    "margin": realMargenA + realMargenB,
                    "nominalA": realNominalA,
                    "nominalB": realNominalB,
                    "multA": multA,
                    "multB": multB
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
'''

import re
code = re.sub(
    r'    # =========================================================================\n    # 7\. BACKTESTING BASADO EN symbols.margen.*?(?=quantEngine = QuantPairEngine\(\))',
    new_engine_3pct + '\n\n',
    code,
    flags=re.DOTALL
)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py with 3% allocation and cycle close reinvestment!")
