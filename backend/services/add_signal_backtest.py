quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

signal_backtest_method = '''
    # =========================================================================
    # 7. BACKTESTING DE SEÑALES GRÁFICAS (TRIÁNGULOS Y CUADROS CON SALIDA EN MEDIA)
    # =========================================================================
    def runSignalBacktest(
        self,
        dfA: pd.DataFrame,
        dfB: pd.DataFrame,
        smaPeriod: int = 3,
        sigmaWindow: int = 30,
        includeBoxes: bool = False,
        initialCapital: float = 10000.0,
        commissionBps: float = 2.0,
        slippageBps: float = 1.0
    ) -> Dict[str, Any]:
        """
        Ejecuta el backtest de señales visuales de la gráfica:
        - Triángulos (Coincidentes): Cruce de EMA en Par A y Par B simultáneo fuera de +/- 1σ.
        - Cuadros (Independientes, si includeBoxes=True): Cruce en un solo par, replicando
          la pierna simétrica en el otro par para mantener la equidad del arbitraje.
        - Salidas (Círculos): Cruce de los precios normalizados con la media.
        """
        # Alinear series
        commonIdx = dfA.index.intersection(dfB.index)
        if len(commonIdx) < max(20, sigmaWindow + 5):
            return {
                "totalTrades": 0, "winRate": 0.0, "profitFactor": 0.0,
                "sharpeRatio": 0.0, "maxDrawdown": 0.0, "totalReturnPct": 0.0,
                "finalCapital": initialCapital, "equityCurve": [], "trades": []
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

        # EMAs rápidas de las series normalizadas
        emaA = normA.ewm(span=smaPeriod, adjust=False).mean()
        emaB = normB.ewm(span=smaPeriod, adjust=False).mean()

        # Media entre ambos pares
        priceMean = (normA + normB) / 2.0
        avgOfMean = float(priceMean.mean())

        # Desviación estándar móvil de las EMAs para anclar las bandas +/- 1σ al promedio de la media
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

        position = 0 # 1 = Long Par A / Short Par B; -1 = Short Par A / Long Par B; 0 = Fuera
        entryDate = None
        entryIndex = 0
        entrySignalType = ""
        entryPxA = 0.0
        entryPxB = 0.0

        trades: List[Dict[str, Any]] = []
        equity = initialCapital
        peakEquity = initialCapital
        maxDrawdown = 0.0
        equityCurve = []
        costMultiplier = (commissionBps + slippageBps) / 10000.0

        for i in range(1, n):
            d = dates[i]
            pxA = pricesA[i]
            pxB = pricesB[i]

            # PnL no realizado de la posición
            unrealizedPct = 0.0
            if position == 1:
                retA = (pxA - entryPxA) / entryPxA
                retB = (entryPxB - pxB) / entryPxB
                unrealizedPct = (retA + retB) / 2.0
            elif position == -1:
                retA = (entryPxA - pxA) / entryPxA
                retB = (pxB - entryPxB) / entryPxB
                unrealizedPct = (retA + retB) / 2.0

            currentEquity = equity + (equity * unrealizedPct)
            if currentEquity > peakEquity:
                peakEquity = currentEquity
            dd = (peakEquity - currentEquity) / peakEquity if peakEquity > 0 else 0.0
            if dd > maxDrawdown:
                maxDrawdown = dd

            equityCurve.append({
                "x": d,
                "y": round(float(currentEquity), 2)
            })

            # Detectar cruces de señales en la vela i
            currP_A, prevP_A = normA_vals[i], normA_vals[i - 1]
            currE_A, prevE_A = emaA_vals[i], emaA_vals[i - 1]
            currP_B, prevP_B = normB_vals[i], normB_vals[i - 1]
            currE_B, prevE_B = emaB_vals[i], emaB_vals[i - 1]

            # Condiciones Par A
            isCrossDownHigh_A = (prevP_A >= prevE_A and currP_A < currE_A and currP_A >= stdAboveA_vals[i])
            isCrossUpLow_A = (prevP_A <= prevE_A and currP_A > currE_A and currP_A <= stdBelowA_vals[i])
            hasSignal_A = (isCrossDownHigh_A or isCrossUpLow_A)

            # Condiciones Par B
            isCrossDownHigh_B = (prevP_B >= prevE_B and currP_B < currE_B and currP_B >= stdAboveB_vals[i])
            isCrossUpLow_B = (prevP_B <= prevE_B and currP_B > currE_B and currP_B <= stdBelowB_vals[i])
            hasSignal_B = (isCrossDownHigh_B or isCrossUpLow_B)

            # Detectar si hay cruce con la media (CÍRCULO DE SALIDA)
            prevDiff = normA_vals[i - 1] - normB_vals[i - 1]
            currDiff = normA_vals[i] - normB_vals[i]
            isMeanCross = (prevDiff > 0 and currDiff <= 0) or (prevDiff < 0 and currDiff >= 0)

            # 1. EVALUAR SALIDA SI ESTAMOS EN POSICIÓN
            if position != 0:
                # Salida por cruce con la media
                if isMeanCross:
                    if position == 1:
                        retA = (pxA - entryPxA) / entryPxA
                        retB = (entryPxB - pxB) / entryPxB
                    else:
                        retA = (entryPxA - pxA) / entryPxA
                        retB = (pxB - entryPxB) / entryPxB

                    grossRetPct = (retA + retB) / 2.0
                    netRetPct = grossRetPct - (costMultiplier * 2.0)
                    tradePnl = equity * netRetPct
                    equity += tradePnl

                    trades.append({
                        "tradeNum": len(trades) + 1,
                        "signalType": entrySignalType,
                        "direction": "LONG A / SHORT B" if position == 1 else "SHORT A / LONG B",
                        "entryDate": entryDate,
                        "exitDate": d,
                        "entryPriceA": round(float(entryPxA), 5),
                        "exitPriceA": round(float(pxA), 5),
                        "entryPriceB": round(float(entryPxB), 5),
                        "exitPriceB": round(float(pxB), 5),
                        "durationBars": i - entryIndex,
                        "returnPct": round(float(netRetPct * 100.0), 2),
                        "pnl": round(float(tradePnl), 2),
                        "exitReason": "CRUCE_MEDIA_CIRCULO",
                        "isWin": bool(tradePnl > 0)
                    })
                    position = 0

            # 2. EVALUAR ENTRADA SI ESTAMOS FUERA DE POSICIÓN
            if position == 0 and i < n - 1:
                # CASO 1: TRIÁNGULOS (Señales coincidentes en ambos pares)
                if hasSignal_A and hasSignal_B:
                    if isCrossUpLow_A and isCrossDownHigh_B:
                        # Triángulo Verde: Par A abajo subiendo, Par B arriba bajando -> Long A / Short B
                        position = 1
                        entryDate = d
                        entryIndex = i
                        entryPxA = pxA
                        entryPxB = pxB
                        entrySignalType = "TRIANGULO_VERDE_COINCIDENTE"
                    elif isCrossDownHigh_A and isCrossUpLow_B:
                        # Triángulo Rojo: Par A arriba bajando, Par B abajo subiendo -> Short A / Long B
                        position = -1
                        entryDate = d
                        entryIndex = i
                        entryPxA = pxA
                        entryPxB = pxB
                        entrySignalType = "TRIANGULO_ROJO_COINCIDENTE"

                # CASO 2: CUADROS (Señales independientes con equidad de piernas)
                elif includeBoxes and (hasSignal_A or hasSignal_B):
                    if hasSignal_A:
                        if isCrossUpLow_A:
                            # Cuadro Verde Par A -> Long A / Short B
                            position = 1
                            entryDate = d
                            entryIndex = i
                            entryPxA = pxA
                            entryPxB = pxB
                            entrySignalType = "CUADRO_VERDE_PAR_A"
                        elif isCrossDownHigh_A:
                            # Cuadro Rojo Par A -> Short A / Long B
                            position = -1
                            entryDate = d
                            entryIndex = i
                            entryPxA = pxA
                            entryPxB = pxB
                            entrySignalType = "CUADRO_ROJO_PAR_A"

                    elif hasSignal_B:
                        if isCrossUpLow_B:
                            # Cuadro Verde Par B -> Long B / Short A
                            position = -1
                            entryDate = d
                            entryIndex = i
                            entryPxA = pxA
                            entryPxB = pxB
                            entrySignalType = "CUADRO_VERDE_PAR_B"
                        elif isCrossDownHigh_B:
                            # Cuadro Rojo Par B -> Short B / Long A
                            position = 1
                            entryDate = d
                            entryIndex = i
                            entryPxA = pxA
                            entryPxB = pxB
                            entrySignalType = "CUADRO_ROJO_PAR_B"

        # Métricas de rendimiento
        totalTrades = len(trades)
        wins = [t for t in trades if t["isWin"]]
        losses = [t for t in trades if not t["isWin"]]

        winRate = (len(wins) / totalTrades * 100.0) if totalTrades > 0 else 0.0
        totalGrossWin = sum(t["pnl"] for t in wins) if wins else 0.0
        totalGrossLoss = abs(sum(t["pnl"] for t in losses)) if losses else 0.0

        profitFactor = (totalGrossWin / totalGrossLoss) if totalGrossLoss > 0 else (99.0 if totalGrossWin > 0 else 0.0)
        totalReturnPct = ((equity - initialCapital) / initialCapital) * 100.0

        returnsList = [t["returnPct"] / 100.0 for t in trades]
        if len(returnsList) > 2 and np.std(returnsList) > 0:
            sharpeRatio = (np.mean(returnsList) / np.std(returnsList)) * np.sqrt(252 / max(1, np.mean([t["durationBars"] for t in trades])))
        else:
            sharpeRatio = 0.0

        return {
            "mode": "TRIANGLES_AND_BOXES" if includeBoxes else "TRIANGLES_ONLY",
            "modeLabel": "Triángulos + Cuadros (Con Equidad)" if includeBoxes else "Solo Triángulos (Coincidentes)",
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
            "trades": trades
        }
'''

if 'def runSignalBacktest' not in code:
    code = code.replace('quantEngine = QuantPairEngine()', signal_backtest_method + '\n\nquantEngine = QuantPairEngine()')
    with open(quant_path, 'w', encoding='utf-8') as f:
        f.write(code)
    print("Added runSignalBacktest method to QuantPairEngine!")
else:
    print("runSignalBacktest already present.")
