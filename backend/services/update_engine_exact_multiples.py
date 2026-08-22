quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

new_engine_exact_multiples = '''    # =========================================================================
    # 7. BACKTESTING DE TODAS LAS SEÑALES (MÚLTIPLOS EXACTOS DE symbols.min_lots)
    # =========================================================================
    def runSignalBacktest(
        self,
        dfA: pd.DataFrame,
        dfB: pd.DataFrame,
        smaPeriod: int = 3,
        sigmaWindow: int = 30,
        includeBoxes: bool = False,
        initialCapital: float = 10000.0,
        allocationPct: float = 20.0, # 20% por entrada (10% Par A + 10% Par B)
        minLotsA: float = 1000.0,    # symbols.min_lots de BD para Par A
        minLotsB: float = 1000.0,    # symbols.min_lots de BD para Par B
        commissionBps: float = 2.0,
        slippageBps: float = 1.0
    ) -> Dict[str, Any]:
        """
        Ejecuta el backtest donde cada operación invierte ESTRICTAMENTE múltiplos
        enteros del lote mínimo (symbols.min_lots) consultado de la base de datos.
        """
        commonIdx = dfA.index.intersection(dfB.index)
        if len(commonIdx) < max(20, sigmaWindow + 5):
            return {
                "totalTrades": 0, "winRate": 0.0, "profitFactor": 0.0,
                "sharpeRatio": 0.0, "maxDrawdown": 0.0, "totalReturnPct": 0.0,
                "allocationPct": allocationPct,
                "minLotsA": minLotsA,
                "minLotsB": minLotsB,
                "finalCapital": initialCapital, "netProfit": 0.0, "equityCurve": [], "trades": []
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
        equity = initialCapital
        peakEquity = initialCapital
        maxDrawdown = 0.0
        equityCurve = []
        costMultiplier = (commissionBps + slippageBps) / 10000.0
        allocationRate = allocationPct / 100.0

        for i in range(1, n):
            d = dates[i]
            pxA = pricesA[i]
            pxB = pricesB[i]

            # Calcular PnL no realizado de todos los trades activos
            unrealizedTotalPnl = 0.0
            if active_trades:
                for t in active_trades:
                    if t["direction"] == "LONG A / SHORT B":
                        rA = (pxA - t["entryPxA"]) / t["entryPxA"]
                        rB = (t["entryPxB"] - pxB) / t["entryPxB"]
                    else:
                        rA = (t["entryPxA"] - pxA) / t["entryPxA"]
                        rB = (pxB - t["entryPxB"]) / t["entryPxB"]
                    tradeNetRet = ((rA + rB) / 2.0) - costMultiplier
                    unrealizedTotalPnl += t["allocatedCapital"] * tradeNetRet

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

            # 1. SI HAY CRUCE CON LA MEDIA (CÍRCULO), CERRAMOS EL CICLO DE TRADES ACTIVOS
            if isMeanCross and active_trades:
                cycle_counter += 1
                cycle_trades = []
                cycle_pnl = 0.0
                cycle_invested = sum(t["allocatedCapital"] for t in active_trades)

                for t in active_trades:
                    raw_trades_count += 1
                    if t["direction"] == "LONG A / SHORT B":
                        rA = (pxA - t["entryPxA"]) / t["entryPxA"]
                        rB = (t["entryPxB"] - pxB) / t["entryPxB"]
                    else:
                        rA = (t["entryPxA"] - pxA) / t["entryPxA"]
                        rB = (pxB - t["entryPxB"]) / t["entryPxB"]

                    grossRet = (rA + rB) / 2.0
                    netRet = grossRet - (costMultiplier * 2.0)
                    tradePnl = t["allocatedCapital"] * netRet
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
                        "allocatedCapital": round(float(t["allocatedCapital"]), 2),
                        "entryPriceA": round(float(t["entryPxA"]), 5),
                        "exitPriceA": round(float(pxA), 5),
                        "entryPriceB": round(float(t["entryPxB"]), 5),
                        "exitPriceB": round(float(pxB), 5),
                        "durationBars": i - t["entryIndex"],
                        "returnPct": round(float(netRet * 100.0), 2),
                        "pnl": round(float(tradePnl), 2),
                        "exitReason": "CRUCE_MEDIA_CIRCULO",
                        "isWin": bool(tradePnl > 0)
                    })

                cycleAvgRet = round((cycle_pnl / cycle_invested) * 100.0, 2) if cycle_invested > 0 else 0.0
                for ct in cycle_trades:
                    finished_trades.append(ct)

                # Inyectar fila de Subtotal explícita por fecha de salida
                entrySpan = f"{cycle_trades[0]['entryDate'][:10]} a {cycle_trades[-1]['entryDate'][:10]}" if len(cycle_trades) > 1 else cycle_trades[0]['entryDate'][:10]
                finished_trades.append({
                    "tradeNum": None,
                    "isSubtotal": True,
                    "cycleNum": cycle_counter,
                    "signalType": f"SUBTOTAL CIERRE #{cycle_counter}",
                    "direction": f"● Salida Media ({len(cycle_trades)} ops)",
                    "entryDate": f"Entradas: {entrySpan}",
                    "exitDate": d,
                    "allocatedCapital": round(float(cycle_invested), 2),
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

                active_trades = []

            # 2. EVALUAR ENTRADA (ESTRICTAMENTE EN MÚLTIPLOS DE symbols.min_lots)
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
                targetCapA = (equity * allocationRate) / 2.0
                targetCapB = (equity * allocationRate) / 2.0

                # 1 lote mínimo valor nominal
                valMinLotA = minLotsA * (pxA if minLotsA < 100 else 1.0)
                valMinLotB = minLotsB * (pxB if minLotsB < 100 else 1.0)

                # Múltiplos enteros estrictos (1x, 2x, 3x... min_lots)
                multA = max(1, int(round(targetCapA / valMinLotA))) if valMinLotA > 0 else 1
                multB = max(1, int(round(targetCapB / valMinLotB))) if valMinLotB > 0 else 1

                realCapA = multA * valMinLotA
                realCapB = multB * valMinLotB
                totalTradeCap = realCapA + realCapB

                active_trades.append({
                    "signalType": sigType,
                    "direction": direction,
                    "entryDate": d,
                    "entryIndex": i,
                    "entryPxA": pxA,
                    "entryPxB": pxB,
                    "multA": multA,
                    "multB": multB,
                    "allocatedCapital": totalTradeCap
                })

        # Métricas consolidadas (excluyendo filas de subtotales para los cálculos globales)
        real_trades = [t for t in finished_trades if not t.get("isSubtotal", False)]
        totalTrades = len(real_trades)
        wins = [t for t in real_trades if t["isWin"]]
        losses = [t for t in real_trades if not t["isWin"]]

        winRate = (len(wins) / totalTrades * 100.0) if totalTrades > 0 else 0.0
        totalGrossWin = sum(t["pnl"] for t in wins) if wins else 0.0
        totalGrossLoss = abs(sum(t["pnl"] for t in losses)) if losses else 0.0

        profitFactor = (totalGrossWin / totalGrossLoss) if totalGrossLoss > 0 else (99.0 if totalGrossWin > 0 else 0.0)
        totalReturnPct = ((equity - initialCapital) / initialCapital) * 100.0

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
    r'    # =========================================================================\n    # 7\. BACKTESTING DE TODAS LAS SEÑALES.*?(?=quantEngine = QuantPairEngine\(\))',
    new_engine_exact_multiples + '\n\n',
    code,
    flags=re.DOTALL
)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py with strict integer multiples of symbols.min_lots!")
