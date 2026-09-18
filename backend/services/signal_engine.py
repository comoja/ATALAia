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
        includeBoxes: bool = False,
        avgOfMean: Optional[float] = None
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
        # 1. Detección de Cruce entre los dos precios / Media (Hito informativo ●)
        prevDiff = prevNormA - prevNormB
        currDiff = currNormA - currNormB
        isPriceCross = bool((prevDiff > 0 and currDiff <= 0) or (prevDiff < 0 and currDiff >= 0))

        # 2. Condiciones individuales por par: cruce que ocurre en zonas extremas
        # Cruce arriba de +1σ: precio cruza de arriba hacia abajo a la EMA estando arriba de la desviación estándar superior
        isCrossDownHighA = bool(
            prevPriceA >= prevEmaA and currPriceA < currEmaA and prevPriceA >= stdAboveA and (currPriceA >= stdAboveA or currEmaA >= stdAboveA)
        )
        # Cruce abajo de -1σ: precio cruza de abajo hacia arriba a la EMA estando abajo de la desviación estándar inferior
        isCrossUpLowA = bool(
            prevPriceA <= prevEmaA and currPriceA > currEmaA and prevPriceA <= stdBelowA and (currPriceA <= stdBelowA or currEmaA <= stdBelowA)
        )
        hasSignalA = isCrossDownHighA or isCrossUpLowA

        isCrossDownHighB = bool(
            prevPriceB >= prevEmaB and currPriceB < currEmaB and prevPriceB >= stdAboveB and (currPriceB >= stdAboveB or currEmaB >= stdAboveB)
        )
        isCrossUpLowB = bool(
            prevPriceB <= prevEmaB and currPriceB > currEmaB and prevPriceB <= stdBelowB and (currPriceB <= stdBelowB or currEmaB <= stdBelowB)
        )
        hasSignalB = isCrossDownHighB or isCrossUpLowB

        # RESTRICCIÓN DE ENTRADA: Si ambos precios normalizados se encuentran en el mismo lado
        # de la desviación estándar (ambos arriba de +1σ o ambos abajo de -1σ), NO se opera
        # (no debe generar cuadros ni triángulos).
        bothAbove = (
            (currNormA >= stdAboveA or prevNormA >= stdAboveA)
            and (currNormB >= stdAboveB or prevNormB >= stdAboveB)
            and not (currNormA <= stdBelowA or currNormB <= stdBelowB)
        )
        bothBelow = (
            (currNormA <= stdBelowA or prevNormA <= stdBelowA)
            and (currNormB <= stdBelowB or prevNormB <= stdBelowB)
            and not (currNormA >= stdAboveA or currNormB >= stdAboveB)
        )
        sameSideDeviation = bool(bothAbove or bothBelow)

        # RESTRICCIÓN DE ENTRADA 2: El par contrario NO debe estar del mismo lado del
        # Promedio de la Media (la línea recta negra avgOfMean).
        # Si un par está arriba de +1σ, el par contrario debe estar ABAJO de la línea media (< avgOfMean).
        # Si un par está abajo de -1σ, el par contrario debe estar ARRIBA de la línea media (> avgOfMean).
        meanRef = avgOfMean if avgOfMean is not None else ((stdAboveA + stdBelowA) / 2.0)
        sameSideMean = False

        if isCrossDownHighA and (currNormB >= meanRef or (prevNormB is not None and prevNormB >= meanRef)):
            hasSignalA = False
            sameSideMean = True

        if isCrossUpLowA and (currNormB <= meanRef or (prevNormB is not None and prevNormB <= meanRef)):
            hasSignalA = False
            sameSideMean = True

        if isCrossDownHighB and (currNormA >= meanRef or (prevNormA is not None and prevNormA >= meanRef)):
            hasSignalB = False
            sameSideMean = True

        if isCrossUpLowB and (currNormA <= meanRef or (prevNormA is not None and prevNormA <= meanRef)):
            hasSignalB = False
            sameSideMean = True

        if sameSideDeviation or (not hasSignalA and not hasSignalB):
            hasSignalA = False
            hasSignalB = False
            hasSignalBoth = False
        else:
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

        triggerPair = None
        isContraryOutsideStd = False
        if hasSignalBoth:
            triggerPair = "BOTH"
            isContraryOutsideStd = True
        elif hasSignalA:
            triggerPair = "A"
            if isCrossDownHighA:
                isContraryOutsideStd = bool(currNormB <= stdBelowB)
            elif isCrossUpLowA:
                isContraryOutsideStd = bool(currNormB >= stdAboveB)
        elif hasSignalB:
            triggerPair = "B"
            if isCrossDownHighB:
                isContraryOutsideStd = bool(currNormA <= stdBelowA)
            elif isCrossUpLowB:
                isContraryOutsideStd = bool(currNormA >= stdAboveA)

        return {
            "isPriceCross": isPriceCross,
            "isMeanCross": isPriceCross,
            "hasSignalA": hasSignalA,
            "hasSignalB": hasSignalB,
            "hasSignalBoth": hasSignalBoth,
            "triggerPair": triggerPair,
            "isContraryOutsideStd": isContraryOutsideStd,
            "isCrossDownHighA": isCrossDownHighA,
            "isCrossUpLowA": isCrossUpLowA,
            "isCrossDownHighB": isCrossDownHighB,
            "isCrossUpLowB": isCrossUpLowB,
            "signalType": sigType,
            "direction": direction,
            "symbolAAction": symbolAAction,
            "symbolBAction": symbolBAction,
            "sameSideDeviation": sameSideDeviation,
            "sameSideMean": sameSideMean
        }

    @staticmethod
    def isSelectiveEntryValid(
        sig: Dict[str, Any],
        currNormA: float,
        currNormB: float,
        lastEntryA: Optional[float],
        lastEntryB: Optional[float],
        stdAboveA: float,
        stdBelowA: float,
        stdAboveB: float,
        stdBelowB: float
    ) -> Tuple[bool, bool, bool, str]:
        """
        Valida si una entrada sucesiva (acumulación en el mismo ciclo) cumple con la regla selectiva:
        - Los precios de las señales solo se comparan contra los de su mismo color.
        - El par contrario (réplica) debe estar fuera de su desviación estándar correspondiente
          (definida como fuera del área que está entre las dos desviaciones estándar del mismo color).
        
        1. Para Cuadros en Par A (dicta Azul):
           - Par B (Naranja) debe estar fuera de su desviación estándar: (currNormB <= stdBelowB or currNormB >= stdAboveB).
           - Par A (Azul) debe estar más alejado hacia afuera que la última entrada de Par A (del mismo color):
             - Si A está en zona alta (venta): currNormA > lastEntryA
             - Si A está en zona baja (compra): currNormA < lastEntryA
        2. Para Cuadros en Par B (dicta Naranja):
           - Par A (Azul) debe estar fuera de su desviación estándar: (currNormA <= stdBelowA or currNormA >= stdAboveA).
           - Par B (Naranja) debe estar más alejado hacia afuera que la última entrada de Par B (del mismo color):
             - Si B está en zona alta (venta): currNormB > lastEntryB
             - Si B está en zona baja (compra): currNormB < lastEntryB
        3. Para Triángulos (disparan ambos pares):
           - Ambos pares ya están fuera de sus desviaciones estándar.
           - Si al menos una pata está más alejada hacia afuera que su última entrada del mismo color, es válida como entrada.
           
        Retorna (isValid: bool, updateA: bool, updateB: bool, reason: str).
        """
        hasBoth = sig.get("hasSignalBoth", False)
        hasA = sig.get("hasSignalA", False)
        hasB = sig.get("hasSignalB", False)

        isCrossDownHighA = sig.get("isCrossDownHighA", False)
        isCrossUpLowA = sig.get("isCrossUpLowA", False)
        isCrossDownHighB = sig.get("isCrossDownHighB", False)
        isCrossUpLowB = sig.get("isCrossUpLowB", False)

        isOutA = bool(currNormA <= stdBelowA or currNormA >= stdAboveA)
        isOutB = bool(currNormB <= stdBelowB or currNormB >= stdAboveB)

        moreExtremeA = False
        if isCrossDownHighA:
            moreExtremeA = bool(lastEntryA is None or currNormA > lastEntryA)
        elif isCrossUpLowA:
            moreExtremeA = bool(lastEntryA is None or currNormA < lastEntryA)

        moreExtremeB = False
        if isCrossDownHighB:
            moreExtremeB = bool(lastEntryB is None or currNormB > lastEntryB)
        elif isCrossUpLowB:
            moreExtremeB = bool(lastEntryB is None or currNormB < lastEntryB)

        if hasBoth:
            if moreExtremeA or moreExtremeB:
                return True, moreExtremeA, moreExtremeB, f"OK Triángulo (ExtA: {moreExtremeA}, ExtB: {moreExtremeB})"
            else:
                return False, False, False, f"TRIANGULO: ningún par más alejado que su entrada anterior (A: {currNormA:.4f} vs {lastEntryA}, B: {currNormB:.4f} vs {lastEntryB})"

        if hasA:
            if not isOutB:
                return False, False, False, f"CUADRO Par A: Par B dentro de bandas de desviación ({currNormB:.4f})"
            if moreExtremeA:
                return True, True, False, f"OK Cuadro Par A: Par A más alejado ({currNormA:.4f} vs {lastEntryA}) y Par B fuera ({currNormB:.4f})"
            else:
                return False, False, False, f"CUADRO Par A: Par A no más alejado que su entrada anterior ({currNormA:.4f} vs {lastEntryA})"

        if hasB:
            if not isOutA:
                return False, False, False, f"CUADRO Par B: Par A dentro de bandas de desviación ({currNormA:.4f})"
            if moreExtremeB:
                return True, False, True, f"OK Cuadro Par B: Par B más alejado ({currNormB:.4f} vs {lastEntryB}) y Par A fuera ({currNormA:.4f})"
            else:
                return False, False, False, f"CUADRO Par B: Par B no más alejado que su entrada anterior ({currNormB:.4f} vs {lastEntryB})"

        return False, False, False, "Sin señal detonante válida"

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

        dates = [str(d) for d in commonIdx]
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
                includeBoxes=includeBoxes,
                avgOfMean=normData.get("avgOfMean")
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
