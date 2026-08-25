"""
Motor de Cálculo Matemático y Detección de Señales para Cruces EMA y Arbitraje Estadístico.
Módulo único y centralizado para que el Frontend (Dashboard/Gráficas) y el Backend (microRatio.py / CruceEmaEngine)
compartan exactamente la misma lógica de normalización, EMAs, bandas de desviación estándar y detección de cruces.

Todas las funciones y parámetros utilizan nomenclatura camelCase.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
import numpy as np
import pandas as pd

logger = logging.getLogger("signalEngine")


class SignalEngine:
    """
    Motor centralizado de cálculo matemático para ratios y detección de señales EMA.
    """

    @staticmethod
    def calculateNormalizedSeries(
        seriesA: pd.Series,
        seriesB: pd.Series
    ) -> Dict[str, Any]:
        """
        Calcula la normalización Min-Max (0 a 1), la serie de media aritmética entre ambos pares,
        el promedio global de la media (avgOfMean) y las desviaciones estándar ancladas al avgOfMean.
        """
        minA, maxA = float(seriesA.min()), float(seriesA.max())
        minB, maxB = float(seriesB.min()), float(seriesB.max())

        rangeA = (maxA - minA) if (maxA - minA) != 0 else 1.0
        rangeB = (maxB - minB) if (maxB - minB) != 0 else 1.0

        normA = (seriesA - minA) / rangeA
        normB = (seriesB - minB) / rangeB

        priceMean = (normA + normB) / 2.0
        avgOfMean = float(priceMean.mean())

        stdNormA = float(normA.std())
        stdNormB = float(normB.std())

        stdAboveA = avgOfMean + stdNormA
        stdBelowA = avgOfMean - stdNormA
        stdAboveB = avgOfMean + stdNormB
        stdBelowB = avgOfMean - stdNormB

        rawStdAboveA = minA + (stdAboveA * rangeA)
        rawStdBelowA = minA + (stdBelowA * rangeA)
        rawStdAboveB = minB + (stdAboveB * rangeB)
        rawStdBelowB = minB + (stdBelowB * rangeB)

        return {
            "minA": minA,
            "maxA": maxA,
            "rangeA": rangeA,
            "minB": minB,
            "maxB": maxB,
            "rangeB": rangeB,
            "normA": normA,
            "normB": normB,
            "priceMean": priceMean,
            "avgOfMean": avgOfMean,
            "stdNormA": stdNormA,
            "stdNormB": stdNormB,
            "stdAboveA": stdAboveA,
            "stdBelowA": stdBelowA,
            "stdAboveB": stdAboveB,
            "stdBelowB": stdBelowB,
            "rawStdAboveA": rawStdAboveA,
            "rawStdBelowA": rawStdBelowA,
            "rawStdAboveB": rawStdAboveB,
            "rawStdBelowB": rawStdBelowB
        }

    @staticmethod
    def calculateEmaSeries(series: pd.Series, period: int) -> pd.Series:
        """
        Calcula la media móvil exponencial (EMA) con la fórmula ewm idéntica a Chart.js y Pandas.
        """
        return series.ewm(span=period, adjust=False).mean()

    @staticmethod
    def detectCandleSignals(
        prevPriceA: float,
        currPriceA: float,
        prevEmaA: float,
        currEmaA: float,
        stdAboveA: float,
        stdBelowA: float,
        prevPriceB: float,
        currPriceB: float,
        prevEmaB: float,
        currEmaB: float,
        stdAboveB: float,
        stdBelowB: float,
        prevNormA: float,
        currNormA: float,
        prevNormB: float,
        currNormB: float,
        pairA: str = "Par A",
        pairB: str = "Par B",
        includeBoxes: bool = False
    ) -> Dict[str, Any]:
        """
        Evalúa la vela actual frente a la anterior para detectar:
        1. Cruce entre los precios de ambos pares (Convergencia / Cierre de Posiciones ●).
           Se produce cuando el precio normalizado de Par A cruza el precio normalizado de Par B.
        2. Señales de entrada en Zonas Extremas:
           - Par A: Venta si precio > +1σ y cruza hacia abajo la EMA rápida.
           - Par A: Compra si precio < -1σ y cruza hacia arriba la EMA rápida.
           - Par B: Venta si precio > +1σ y cruza hacia abajo la EMA rápida.
           - Par B: Compra si precio < -1σ y cruza hacia arriba la EMA rápida.
        3. Caso Coincidente (ambos pares cumplen simultáneamente -> Triángulos Verde/Rojo).
        4. Caso No Coincidente (un solo par cumple -> Cuadros Verde/Rojo si includeBoxes=True).
        """
        # 1. Detección de Cruce entre los dos precios (Convergencia / Cierre de Posiciones ●)
        prevDiff = prevNormA - prevNormB
        currDiff = currNormA - currNormB
        isPriceCross = (prevDiff > 0 and currDiff <= 0) or (prevDiff < 0 and currDiff >= 0)

        if isPriceCross:
            return {
                "isPriceCross": True,
                "isMeanCross": True,
                "hasSignalA": False,
                "hasSignalB": False,
                "hasSignalBoth": False,
                "isCrossDownHighA": False,
                "isCrossUpLowA": False,
                "isCrossDownHighB": False,
                "isCrossUpLowB": False,
                "signalType": "CRUCE_PRECIOS_CONVERGENCIA",
                "direction": "CLOSE_ALL",
                "symbolAAction": None,
                "symbolBAction": None
            }

        # 2. Condiciones individuales por par en zonas extremas (+1σ o -1σ)
        isCrossDownHighA = bool(
            prevPriceA >= prevEmaA and currPriceA < currEmaA and (prevPriceA >= stdAboveA or currPriceA >= stdAboveA)
        )
        isCrossUpLowA = bool(
            prevPriceA <= prevEmaA and currPriceA > currEmaA and (prevPriceA <= stdBelowA or currPriceA <= stdBelowA)
        )
        hasSignalA = isCrossDownHighA or isCrossUpLowA

        isCrossDownHighB = bool(
            prevPriceB >= prevEmaB and currPriceB < currEmaB and (prevPriceB >= stdAboveB or currPriceB >= stdAboveB)
        )
        isCrossUpLowB = bool(
            prevPriceB <= prevEmaB and currPriceB > currEmaB and (prevPriceB <= stdBelowB or currPriceB <= stdBelowB)
        )
        hasSignalB = isCrossDownHighB or isCrossUpLowB

        hasSignalBoth = hasSignalA and hasSignalB
        sigType = None
        direction = None
        symbolAAction = None
        symbolBAction = None

        # 3. Caso Coincidente (Ambos pares disparan en la misma vela -> Triángulos)
        if hasSignalBoth:
            if isCrossUpLowA and isCrossDownHighB:
                sigType = f"TRIANGULO_VERDE ({pairA} + {pairB})"
                direction = f"LONG {pairA} / SHORT {pairB}"
                symbolAAction = "BUY"
                symbolBAction = "SELL"
            elif isCrossDownHighA and isCrossUpLowB:
                sigType = f"TRIANGULO_ROJO ({pairA} + {pairB})"
                direction = f"SHORT {pairA} / LONG {pairB}"
                symbolAAction = "SELL"
                symbolBAction = "BUY"
            elif isCrossUpLowA and isCrossUpLowB:
                sigType = f"TRIANGULO_MIXTO_ALTO ({pairA} + {pairB})"
                direction = f"LONG {pairA} / SHORT {pairB}"
                symbolAAction = "BUY"
                symbolBAction = "SELL"
            elif isCrossDownHighA and isCrossDownHighB:
                sigType = f"TRIANGULO_MIXTO_BAJO ({pairA} + {pairB})"
                direction = f"SHORT {pairA} / LONG {pairB}"
                symbolAAction = "SELL"
                symbolBAction = "BUY"

        # 4. Caso No Coincidente (Un solo par dispara -> Cuadros)
        elif includeBoxes and (hasSignalA or hasSignalB):
            if hasSignalA:
                if isCrossUpLowA:
                    sigType = f"CUADRO_VERDE {pairA}"
                    direction = f"LONG {pairA} / SHORT {pairB}"
                    symbolAAction = "BUY"
                    symbolBAction = "SELL"
                elif isCrossDownHighA:
                    sigType = f"CUADRO_ROJO {pairA}"
                    direction = f"SHORT {pairA} / LONG {pairB}"
                    symbolAAction = "SELL"
                    symbolBAction = "BUY"
            elif hasSignalB:
                if isCrossUpLowB:
                    sigType = f"CUADRO_VERDE {pairB}"
                    direction = f"SHORT {pairA} / LONG {pairB}"
                    symbolAAction = "SELL"
                    symbolBAction = "BUY"
                elif isCrossDownHighB:
                    sigType = f"CUADRO_ROJO {pairB}"
                    direction = f"LONG {pairA} / SHORT {pairB}"
                    symbolAAction = "BUY"
                    symbolBAction = "SELL"

        return {
            "isPriceCross": False,
            "isMeanCross": False,
            "hasSignalA": hasSignalA,
            "hasSignalB": hasSignalB,
            "hasSignalBoth": hasSignalBoth,
            "isCrossDownHighA": isCrossDownHighA,
            "isCrossUpLowA": isCrossUpLowA,
            "isCrossDownHighB": isCrossDownHighB,
            "isCrossUpLowB": isCrossUpLowB,
            "signalType": sigType,
            "direction": direction,
            "symbolAAction": symbolAAction,
            "symbolBAction": symbolBAction
        }

    def evaluateRatioSignals(
        self,
        dfA: pd.DataFrame,
        dfB: pd.DataFrame,
        pairA: str = "Par A",
        pairB: str = "Par B",
        smaPeriod: int = 2,
        sigmaWindow: int = 20,
        includeBoxes: bool = False
    ) -> Dict[str, Any]:
        """
        Ejecuta el análisis completo vela por vela sobre los DataFrames de precios.
        Retorna las series normalizadas, las EMAs, las bandas de desviación y la lista completa
        de señales detectadas en cada fecha.
        """
        commonIdx = dfA.index.intersection(dfB.index)
        if len(commonIdx) < 5:
            return {
                "status": "error",
                "message": "Datos históricos insuficientes",
                "signals": [],
                "latestSignal": None
            }

        sA = dfA.loc[commonIdx, 'closePrice'] if 'closePrice' in dfA.columns else dfA.loc[commonIdx].iloc[:, 0]
        sB = dfB.loc[commonIdx, 'closePrice'] if 'closePrice' in dfB.columns else dfB.loc[commonIdx].iloc[:, 0]

        normData = self.calculateNormalizedSeries(sA, sB)
        normA = normData["normA"]
        normB = normData["normB"]

        emaA = self.calculateEmaSeries(normA, smaPeriod)
        emaB = self.calculateEmaSeries(normB, smaPeriod)

        dates = [str(d)[:10] for d in commonIdx]
        pricesA = sA.values
        pricesB = sB.values
        normAVals = normA.values
        normBVals = normB.values
        emaAVals = emaA.values
        emaBVals = emaB.values
        n = len(dates)

        signalsList = []
        for i in range(1, n):
            candleSig = self.detectCandleSignals(
                prevPriceA=normAVals[i - 1],
                currPriceA=normAVals[i],
                prevEmaA=emaAVals[i - 1],
                currEmaA=emaAVals[i],
                stdAboveA=normData["stdAboveA"],
                stdBelowA=normData["stdBelowA"],
                prevPriceB=normBVals[i - 1],
                currPriceB=normBVals[i],
                prevEmaB=emaBVals[i - 1],
                currEmaB=emaBVals[i],
                stdAboveB=normData["stdAboveB"],
                stdBelowB=normData["stdBelowB"],
                prevNormA=normAVals[i - 1],
                currNormA=normAVals[i],
                prevNormB=normBVals[i - 1],
                currNormB=normBVals[i],
                pairA=pairA,
                pairB=pairB,
                includeBoxes=includeBoxes
            )
            candleSig["date"] = dates[i]
            candleSig["priceA"] = float(pricesA[i])
            candleSig["priceB"] = float(pricesB[i])
            candleSig["normA"] = float(normAVals[i])
            candleSig["normB"] = float(normBVals[i])
            candleSig["emaA"] = float(emaAVals[i])
            candleSig["emaB"] = float(emaBVals[i])
            signalsList.append(candleSig)

        latestSignal = signalsList[-1] if signalsList else None

        return {
            "status": "success",
            "pairA": pairA,
            "pairB": pairB,
            "dates": dates,
            "normData": normData,
            "signals": signalsList,
            "latestSignal": latestSignal
        }


signalEngine = SignalEngine()
