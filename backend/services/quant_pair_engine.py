"""
Motor Cuantitativo Avanzado para Pair Trading y Arbitraje Estadístico de Ratios.

Implementa:
1. Identificación y seguimiento de Fair Value Gaps (FVG) en la estructura OHLC del ratio.
2. Cálculo de Bandas Dinámicas Z-Score móvil (+/- 2σ, +/- 3σ).
3. Análisis Espectral con Transformada Rápida de Fourier (FFT) y filtrado de ruido.
4. Vida Media de Reversión basada en el proceso estocástico Ornstein-Uhlenbeck (Half-Life).
5. Módulo de Backtesting vectorizado con costos transaccionales, métricas y Curva de Equidad.
6. Evaluación de señales en tiempo real (modo live) para alertas webhook.
"""

import numpy as np
import pandas as pd
from scipy import stats
from typing import Dict, List, Any, Optional, Tuple
import logging

logger = logging.getLogger("QuantPairEngine")


class QuantPairEngine:
    """
    Motor matemático y cuantitativo para el análisis de cointegración,
    reversión a la media y backtest de pares de trading.
    """

    def __init__(self, riskFreeRate: float = 0.03):
        self.riskFreeRate = riskFreeRate

    # =========================================================================
    # 1. FAIR VALUE GAPS (FVG) EN LA ESTRUCTURA DEL RATIO
    # =========================================================================
    @staticmethod
    def identifyFvg(df: pd.DataFrame) -> List[Dict[str, Any]]:
        """
        Escanea ventanas móviles de 3 velas consecutivas en el ratio OHLC:
        - Bullish FVG (Alcista / BISY): Low[i] > High[i-2] -> Zona [High[i-2], Low[i]]
        - Bearish FVG (Bajista / SIBI): High[i] < Low[i-2] -> Zona [High[i], Low[i-2]]
        Rastrea si la zona ha sido mitigada por velas posteriores.
        """
        if len(df) < 3 or not {'high', 'low', 'close', 'open'}.issubset(df.columns):
            return []

        fvgs: List[Dict[str, Any]] = []
        highs = df['high'].values
        lows = df['low'].values
        closes = df['close'].values
        dates = df['datetime'].astype(str).values if 'datetime' in df.columns else df.index.astype(str).values

        n = len(df)
        for i in range(2, n):
            # Bullish FVG: Vela 3 deja gap por encima del máximo de Vela 1
            if lows[i] > highs[i - 2]:
                bottomGap = float(highs[i - 2])
                topGap = float(lows[i])
                gapSize = topGap - bottomGap

                # Verificar si alguna vela posterior (i+1 en adelante) mitigó la zona
                mitigated = False
                mitigationDate = None
                for j in range(i + 1, n):
                    if lows[j] <= bottomGap:
                        mitigated = True
                        mitigationDate = str(dates[j])
                        break

                fvgs.append({
                    "index": int(i),
                    "datetime": str(dates[i]),
                    "type": "BULLISH_FVG",
                    "top": round(topGap, 6),
                    "bottom": round(bottomGap, 6),
                    "gapSize": round(gapSize, 6),
                    "mitigated": mitigated,
                    "mitigationDate": mitigationDate
                })

            # Bearish FVG: Vela 3 deja gap por debajo del mínimo de Vela 1
            elif highs[i] < lows[i - 2]:
                topGap = float(lows[i - 2])
                bottomGap = float(highs[i])
                gapSize = topGap - bottomGap

                # Verificar si alguna vela posterior mitigó la zona
                mitigated = False
                mitigationDate = None
                for j in range(i + 1, n):
                    if highs[j] >= topGap:
                        mitigated = True
                        mitigationDate = str(dates[j])
                        break

                fvgs.append({
                    "index": int(i),
                    "datetime": str(dates[i]),
                    "type": "BEARISH_FVG",
                    "top": round(topGap, 6),
                    "bottom": round(bottomGap, 6),
                    "gapSize": round(gapSize, 6),
                    "mitigated": mitigated,
                    "mitigationDate": mitigationDate
                })

        return fvgs

    # =========================================================================
    # 2. CÁLCULO DE BANDAS DINÁMICAS (Z-SCORE)
    # =========================================================================
    @staticmethod
    def calculateDynamicZScoreBands(
        series: pd.Series,
        window: int = 20,
        stdThreshold: float = 2.0
    ) -> pd.DataFrame:
        """
        Calcula la media móvil, desviación estándar móvil y el Z-Score del ratio:
        Z_t = (Ratio_t - RollingMean_t) / RollingStd_t
        Genera bandas dinámicas a +/- 2σ y +/- 3σ.
        """
        rollingMean = series.rolling(window=window, min_periods=max(3, window // 2)).mean()
        rollingStd = series.rolling(window=window, min_periods=max(3, window // 2)).std()
        # Evitar división por cero
        rollingStdSafe = rollingStd.replace(0, np.nan)

        zScore = (series - rollingMean) / rollingStdSafe
        zScore = zScore.fillna(0.0)

        dfBands = pd.DataFrame({
            "ratio": series,
            "rollingMean": rollingMean,
            "rollingStd": rollingStd,
            "zScore": zScore,
            "upperBand2Std": rollingMean + (2.0 * rollingStd),
            "lowerBand2Std": rollingMean - (2.0 * rollingStd),
            "upperBand3Std": rollingMean + (3.0 * rollingStd),
            "lowerBand3Std": rollingMean - (3.0 * rollingStd),
            "zUpperThreshold": stdThreshold,
            "zLowerThreshold": -stdThreshold
        })
        return dfBands

    # =========================================================================
    # 3. ANÁLISIS ESPECTRAL (FFT - TRANSFORMADA RÁPIDA DE FOURIER)
    # =========================================================================
    @staticmethod
    def computeFftSpectralAnalysis(
        series: pd.Series,
        numHarmonics: int = 5
    ) -> Dict[str, Any]:
        """
        Descompone la serie temporal en el dominio de la frecuencia mediante FFT:
        1. Elimina la tendencia lineal base para analizar componentes estacionarias.
        2. Aplica Transformada Rápida de Fourier Real (rfft).
        3. Filtra el ruido de alta frecuencia conservando únicamente los armónicos dominantes.
        4. Reconstruye la onda limpia (irfft) y estima los periodos estimados hasta el cruce con la media.
        """
        values = series.dropna().values
        n = len(values)
        if n < 8:
            return {
                "reconstructed": values.tolist(),
                "dominantPeriod": 0.0,
                "periodsToMeanCross": 0.0,
                "spectralPower": []
            }

        # Detrending (remover tendencia lineal)
        xIndices = np.arange(n)
        slope, intercept, _, _, _ = stats.linregress(xIndices, values)
        trend = intercept + (slope * xIndices)
        detrended = values - trend

        # FFT Real
        fftCoeffs = np.fft.rfft(detrended)
        frequencies = np.fft.rfftfreq(n)
        magnitudes = np.abs(fftCoeffs)

        # Identificar las frecuencias dominantes (excluyendo la componente continua f=0)
        magnitudesNoDc = magnitudes.copy()
        magnitudesNoDc[0] = 0.0

        dominantIdx = int(np.argmax(magnitudesNoDc))
        dominantFreq = frequencies[dominantIdx] if dominantIdx < len(frequencies) else 0.0
        dominantPeriod = (1.0 / dominantFreq) if dominantFreq > 0 else float(n)

        # Filtrar conservando los top N armónicos
        topIndices = np.argsort(magnitudesNoDc)[-numHarmonics:]
        filteredCoeffs = np.zeros_like(fftCoeffs)
        filteredCoeffs[0] = fftCoeffs[0] # Mantener nivel medio
        filteredCoeffs[topIndices] = fftCoeffs[topIndices]

        # Reconstrucción inversa + retrending
        reconstructedDetrended = np.fft.irfft(filteredCoeffs, n=n)
        reconstructed = reconstructedDetrended + trend

        # Proyección futura de la onda FFT para estimar el cruce con la media
        meanLevel = np.mean(values)
        lastVal = reconstructed[-1]
        
        # Simular hasta 2 ciclos completos hacia adelante
        futureHorizon = int(min(100, max(10, dominantPeriod * 2)))
        futureIndices = np.arange(n, n + futureHorizon)
        futureTrend = intercept + (slope * futureIndices)
        
        # Evaluación trigonométrica directa de los armónicos dominantes
        futureWave = np.zeros(futureHorizon)
        for idx in topIndices:
            freq = frequencies[idx]
            amp = np.abs(fftCoeffs[idx]) / (n / 2)
            phase = np.angle(fftCoeffs[idx])
            futureWave += amp * np.cos(2 * np.pi * freq * futureIndices + phase)

        futureProjection = futureWave + futureTrend
        
        # Encontrar el primer cruce de la media
        periodsToCross = futureHorizon
        initialSide = (lastVal >= meanLevel)
        for step, val in enumerate(futureProjection, start=1):
            currSide = (val >= meanLevel)
            if currSide != initialSide:
                periodsToCross = step
                break

        return {
            "reconstructed": [round(float(v), 6) for v in reconstructed],
            "dominantPeriod": round(float(dominantPeriod), 2),
            "periodsToMeanCross": int(periodsToCross),
            "dominantFrequency": round(float(dominantFreq), 4)
        }

    # =========================================================================
    # 4. VIDA MEDIA DE REVERSIÓN (ORNSTEIN-UHLENBECK PROCESS)
    # =========================================================================
    @staticmethod
    def calculateOrnsteinUhlenbeckHalfLife(series: pd.Series) -> Dict[str, Any]:
        """
        Modela el ratio bajo una Ecuación Diferencial Estocástica de Ornstein-Uhlenbeck:
        dY_t = λ(μ - Y_t)dt + σ dW_t

        Formulación discreta OLS:
        ΔY_t = Y_t - Y_{t-1} = a + b * Y_{t-1} + ε_t
        donde:
        λ (Velocidad de reversión) = -b
        Half-Life (τ_1/2) = -ln(2) / b
        """
        cleanSeries = series.dropna()
        if len(cleanSeries) < 10:
            return {
                "halfLife": None,
                "reversionSpeed": 0.0,
                "equilibriumMean": 0.0,
                "rSquared": 0.0,
                "isMeanReverting": False,
                "halfLifeDescription": "Datos insuficientes"
            }

        y = cleanSeries.values
        yLag = y[:-1]
        deltaY = y[1:] - yLag

        # Regresión lineal OLS: deltaY = a + b * yLag
        slope, intercept, rValue, pValue, stdErr = stats.linregress(yLag, deltaY)

        # Si slope < 0, el proceso es de reversión a la media
        if slope < 0:
            reversionSpeed = -slope
            halfLife = -np.log(2.0) / slope
            equilibriumMean = -intercept / slope
            isMeanReverting = True
            rSquared = rValue ** 2
            halfLifeDesc = f"{round(halfLife, 1)} periodos"
        else:
            reversionSpeed = 0.0
            halfLife = np.nan
            equilibriumMean = np.mean(y)
            isMeanReverting = False
            rSquared = 0.0
            halfLifeDesc = "No revierte (Paseo aleatorio/Tendencial)"

        return {
            "halfLife": round(float(halfLife), 2) if not np.isnan(halfLife) else None,
            "reversionSpeed": round(float(reversionSpeed), 5),
            "equilibriumMean": round(float(equilibriumMean), 6),
            "rSquared": round(float(rSquared), 4),
            "pValue": round(float(pValue), 5),
            "isMeanReverting": bool(isMeanReverting),
            "halfLifeDescription": halfLifeDesc
        }

    # =========================================================================
    # 5. MÓDULO DE BACKTESTING VECTORIZADO Y EVALUACIÓN DE RENDIMIENTO
    # =========================================================================
    def runVectorizedBacktest(
        self,
        df: pd.DataFrame,
        entryZThreshold: float = 2.0,
        exitZThreshold: float = 0.0,
        stopLossZThreshold: float = 3.5,
        commissionBps: float = 2.0,
        slippageBps: float = 1.0,
        initialCapital: float = 10000.0
    ) -> Dict[str, Any]:
        """
        Ejecuta una simulación vectorizada/iterativa optimizada de Pair Trading:
        - Entrada VENTA Spread (Corto A / Largo B): Z >= entryZThreshold
        - Entrada COMPRA Spread (Largo A / Corto B): Z <= -entryZThreshold
        - Salida Take Profit: Cuando Z cruza exitZThreshold (reversión a la media Z=0)
        - Salida Stop Loss: Cuando |Z| >= stopLossZThreshold (divergencia extrema)
        - Descuenta costos de transacción (comisiones + slippage en bps).
        """
        if len(df) < 20 or "zScore" not in df.columns or "ratio" not in df.columns:
            return {
                "totalTrades": 0,
                "winRate": 0.0,
                "profitFactor": 0.0,
                "sharpeRatio": 0.0,
                "maxDrawdown": 0.0,
                "totalReturnPct": 0.0,
                "finalCapital": initialCapital,
                "equityCurve": [],
                "trades": []
            }

        dates = df["datetime"].astype(str).values if "datetime" in df.columns else df.index.astype(str).values
        ratios = df["ratio"].values
        zScores = df["zScore"].values
        n = len(df)

        position = 0 # 1 = Largo Ratio, -1 = Corto Ratio, 0 = Fuera
        entryPrice = 0.0
        entryDate = None
        entryIndex = 0

        trades: List[Dict[str, Any]] = []
        equity = initialCapital
        equityCurve = []
        peakEquity = initialCapital
        maxDrawdown = 0.0

        costMultiplier = (commissionBps + slippageBps) / 10000.0

        for i in range(n):
            z = zScores[i]
            price = ratios[i]
            d = str(dates[i])[:10]

            # Registro de equidad
            currentPnl = 0.0
            if position == 1:
                # PnL no realizado
                unrealizedPct = (price - entryPrice) / entryPrice
                currentPnl = equity * unrealizedPct
            elif position == -1:
                unrealizedPct = (entryPrice - price) / entryPrice
                currentPnl = equity * unrealizedPct

            currentEquity = equity + currentPnl
            if currentEquity > peakEquity:
                peakEquity = currentEquity
            dd = (peakEquity - currentEquity) / peakEquity if peakEquity > 0 else 0.0
            if dd > maxDrawdown:
                maxDrawdown = dd

            equityCurve.append({
                "x": str(d),
                "y": round(float(currentEquity), 2),
                "zScore": round(float(z), 3)
            })

            # 1. EVALUAR SALIDA SI ESTAMOS EN POSICIÓN
            if position == 1:
                # Salida por TP (Z cruzó hacia arriba de exitZThreshold) o SL (Z cayó más allá de stopLoss)
                isTakeProfit = z >= exitZThreshold
                isStopLoss = z <= -stopLossZThreshold

                if isTakeProfit or isStopLoss:
                    grossReturnPct = (price - entryPrice) / entryPrice
                    netReturnPct = grossReturnPct - (costMultiplier * 2.0)
                    tradePnl = equity * netReturnPct
                    equity += tradePnl

                    trades.append({
                        "tradeNum": len(trades) + 1,
                        "type": "LARGO_RATIO",
                        "entryDate": str(entryDate),
                        "exitDate": str(d),
                        "allocatedCapital": round(float(entryCapital), 2),
                        "entryPrice": round(float(entryPrice), 6),
                        "exitPrice": round(float(price), 6),
                        "durationBars": i - entryIndex,
                        "returnPct": round(float(netReturnPct * 100.0), 2),
                        "pnl": round(float(tradePnl), 2),
                        "exitReason": "TAKE_PROFIT_MEAN" if isTakeProfit else "STOP_LOSS_DIVERGENCE",
                        "isWin": bool(tradePnl > 0)
                    })
                    position = 0

            elif position == -1:
                # Salida por TP (Z cruzó hacia abajo de exitZThreshold) o SL (Z subió más allá de stopLoss)
                isTakeProfit = z <= exitZThreshold
                isStopLoss = z >= stopLossZThreshold

                if isTakeProfit or isStopLoss:
                    grossReturnPct = (entryPrice - price) / entryPrice
                    netReturnPct = grossReturnPct - (costMultiplier * 2.0)
                    tradePnl = equity * netReturnPct
                    equity += tradePnl

                    trades.append({
                        "tradeNum": len(trades) + 1,
                        "type": "CORTO_RATIO",
                        "entryDate": str(entryDate),
                        "exitDate": str(d),
                        "allocatedCapital": round(float(entryCapital), 2),
                        "entryPrice": round(float(entryPrice), 6),
                        "exitPrice": round(float(price), 6),
                        "durationBars": i - entryIndex,
                        "returnPct": round(float(netReturnPct * 100.0), 2),
                        "pnl": round(float(tradePnl), 2),
                        "exitReason": "TAKE_PROFIT_MEAN" if isTakeProfit else "STOP_LOSS_DIVERGENCE",
                        "isWin": bool(tradePnl > 0)
                    })
                    position = 0

            # 2. EVALUAR ENTRADA SI ESTAMOS FUERA
            if position == 0 and i < n - 1:
                if z <= -entryZThreshold:
                    # Sobreventa extrema -> Entrada LARGO
                    position = 1
                    entryPrice = price
                    entryDate = d
                    entryIndex = i
                    entryCapital = equity
                elif z >= entryZThreshold:
                    # Sobrecompra extrema -> Entrada CORTO
                    position = -1
                    entryPrice = price
                    entryDate = d
                    entryIndex = i
                    entryCapital = equity

        # Métricas de rendimiento
        totalTrades = len(trades)
        wins = [t for t in trades if t["isWin"]]
        losses = [t for t in trades if not t["isWin"]]

        winRate = (len(wins) / totalTrades * 100.0) if totalTrades > 0 else 0.0
        totalGrossWin = sum(t["pnl"] for t in wins) if wins else 0.0
        totalGrossLoss = abs(sum(t["pnl"] for t in losses)) if losses else 0.0

        profitFactor = (totalGrossWin / totalGrossLoss) if totalGrossLoss > 0 else (99.0 if totalGrossWin > 0 else 0.0)
        totalReturnPct = ((equity - initialCapital) / initialCapital) * 100.0

        # Sharpe Ratio anualizado (asumiendo rendimientos por trade)
        returnsList = [t["returnPct"] / 100.0 for t in trades]
        if len(returnsList) > 2 and np.std(returnsList) > 0:
            sharpeRatio = (np.mean(returnsList) / np.std(returnsList)) * np.sqrt(252 / max(1, np.mean([t["durationBars"] for t in trades])))
        else:
            sharpeRatio = 0.0

        return {
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
            "equityCurve": equityCurve,
            "trades": trades
        }

    # =========================================================================
    # 6. EVALUACIÓN DE SEÑAL EN TIEMPO REAL (MODO LIVE / WEBHOOK)
    # =========================================================================
    def evaluateLiveSignal(
        self,
        df: pd.DataFrame,
        pairA: str,
        pairB: str,
        entryZThreshold: float = 2.0
    ) -> Dict[str, Any]:
        """
        Evalúa la última vela disponible para verificar si se cumple condición
        de entrada para el webhook/alerta institucional.
        """
        if df.empty or "zScore" not in df.columns:
            return {"hasSignal": False, "signalType": "NONE", "message": "Datos insuficientes"}

        lastRow = df.iloc[-1]
        z = float(lastRow["zScore"])
        ratio = float(lastRow["ratio"])
        dt = str(lastRow["datetime"]) if "datetime" in lastRow else str(df.index[-1])

        signalType = "NONE"
        hasSignal = False
        actionA = "HOLD"
        actionB = "HOLD"

        if z <= -entryZThreshold:
            hasSignal = True
            signalType = "BUY_SPREAD_LONG_RATIO"
            actionA = f"COMPRA {pairA}"
            actionB = f"VENTA {pairB}"
        elif z >= entryZThreshold:
            hasSignal = True
            signalType = "SELL_SPREAD_SHORT_RATIO"
            actionA = f"VENTA {pairA}"
            actionB = f"COMPRA {pairB}"

        return {
            "hasSignal": hasSignal,
            "signalType": signalType,
            "datetime": dt,
            "zScore": round(z, 3),
            "ratio": round(ratio, 6),
            "actionPairA": actionA,
            "actionPairB": actionB,
            "threshold": entryZThreshold
        }


# Instancia singleton accesible

    # =========================================================================
    # 7. BACKTESTING BASADO EN PIPS REALES, symbols.margen Y MULTIPLOS DE LOTES
    # =========================================================================
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

        # Desviación estándar móvil anclada al promedio de la media
        stdA = emaA.rolling(window=sigmaWindow, min_periods=max(3, sigmaWindow // 2)).std().fillna(0.0)
        stdB = emaB.rolling(window=sigmaWindow, min_periods=max(3, sigmaWindow // 2)).std().fillna(0.0)

        stdAboveA = avgOfMean + stdA
        stdBelowA = avgOfMean - stdA
        stdAboveB = avgOfMean + stdB
        stdBelowB = avgOfMean - stdB

        dates = [str(d)[:10] for d in commonIdx]
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

            isCrossDownHigh_A = (prevP_A >= prevE_A and currP_A < currE_A and currP_A >= stdAboveA_vals[i])
            isCrossUpLow_A = (prevP_A <= prevE_A and currP_A > currE_A and currP_A <= stdBelowA_vals[i])
            hasSignal_A = (isCrossDownHigh_A or isCrossUpLow_A)

            isCrossDownHigh_B = (prevP_B >= prevE_B and currP_B < currE_B and currP_B >= stdAboveB_vals[i])
            isCrossUpLow_B = (prevP_B <= prevE_B and currP_B > currE_B and currP_B <= stdBelowB_vals[i])
            hasSignal_B = (isCrossDownHigh_B or isCrossUpLow_B)

            # Detectar cruce con la media (CÍRCULO DE SALIDA)
            prevDiff = normA_vals[i - 1] - normB_vals[i - 1]
            currDiff = normA_vals[i - 1] - normB_vals[i] if i == 0 else normA_vals[i] - normB_vals[i]
            prevDiffReal = normA_vals[i - 1] - normB_vals[i - 1]
            isMeanCross = (prevDiffReal > 0 and currDiff <= 0) or (prevDiffReal < 0 and currDiff >= 0)

            # 1. CIERRE DE TODAS LAS ENTRADAS DEL CICLO EN LA MEDIA (●) -> LIQUIDACIÓN POR PIPS Y REINVERSIÓN
            if isMeanCross and active_trades:
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
                        "allocatedCapital": round(float(t["margin"]), 2), # Margen invertido de la cuenta
                        "marginA": round(float(t.get("marginA", 0.0)), 2),
                        "marginB": round(float(t.get("marginB", 0.0)), 2),
                        "availableCapital": round(float(t.get("availableCapital", 0.0)), 2), # Capital disminuido
                        "accumCapital": round(float(equity), 2), # Capital acumulado tras la operación
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
                        "pnl": round(float(tradePnl), 2), # PnL Real Total en USD
                        "exitReason": "CRUCE_MEDIA_CIRCULO",
                        "isWin": bool(tradePnl > 0)
                    })

                cycleAvgRet = round((cycle_pnl / cycle_margin) * 100.0, 2) if cycle_margin > 0 else 0.0
                for ct in cycle_trades:
                    finished_trades.append(ct)

                # Fila de Subtotal de Cierre con PnL separado por par
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


quantEngine = QuantPairEngine()
