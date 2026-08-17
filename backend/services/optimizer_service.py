import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

class OptimizerService:
    """
    Servicio cuantitativo de optimización de parámetros de calibración.
    Utiliza una búsqueda de rejilla (Grid Search) vectorizada para maximizar
    el beneficio neto de la estrategia de Pairs Trading (Arbitraje).
    """

    def __init__(self):
        # Constante de días del año para anualizar
        self.tradingDaysYear = 252

    def simulateTrades(self, priceArray: np.ndarray, cicloArray: np.ndarray, offsetVal: float) -> float:
        """
        Simula las operaciones de Pairs Trading basadas en los cruces del ciclo y el offset.
        Retorna el beneficio neto acumulado.
        """
        # Generar señal: 1 para SHORT (ciclo > offset), -1 para LONG (ciclo <= offset)
        signalArray = np.where(cicloArray > offsetVal, 1, -1)

        # Crossovers son los puntos donde la señal cambia
        diffArray = np.diff(signalArray)
        crossoverIndices = np.where(diffArray != 0)[0] + 1

        if len(crossoverIndices) < 2:
            return -99999.0  # Penalizar si hay menos de 2 cruces (evitar ciclos inactivos)

        tradesProfit = 0.0
        openPosition = None  # Puede ser 'SHORT' o 'LONG'
        entryPrice = 0.0

        for idx in crossoverIndices:
            pCurr = priceArray[idx]
            sigChange = diffArray[idx - 1]  # +2 (LONG -> SHORT) o -2 (SHORT -> LONG)

            if sigChange == 2:  # LONG a SHORT: Vender Ratio
                if openPosition == 'LONG':
                    # Cerrar posición de compra
                    tradesProfit += (pCurr - entryPrice)
                # Abrir posición de venta
                openPosition = 'SHORT'
                entryPrice = pCurr
            elif sigChange == -2:  # SHORT a LONG: Comprar Ratio
                if openPosition == 'SHORT':
                    # Cerrar posición de venta
                    tradesProfit += (entryPrice - pCurr)
                # Abrir posición de compra
                openPosition = 'LONG'
                entryPrice = pCurr

        # Cerrar posición abierta al último precio disponible (Mark-to-Market)
        if openPosition == 'LONG':
            tradesProfit += (priceArray[-1] - entryPrice)
        elif openPosition == 'SHORT':
            tradesProfit += (entryPrice - priceArray[-1])

        return tradesProfit

    def optimizeCycle(self, priceList: list, sma20List: list) -> dict:
        """
        Ejecuta la optimización barriendo el espacio de parámetros para maximizar el PnL.
        """
        priceArray = np.array(priceList, dtype=float)
        sma20Array = np.array(sma20List, dtype=float)
        deviation = priceArray - sma20Array

        # Cálculo analítico de la amplitud óptima como la desviación estándar por raíz de 2
        optimalAmplitude = float(np.std(deviation) * 1.414)
        if optimalAmplitude <= 0:
            optimalAmplitude = 1.0

        # Rango de barrido (Grid Search acotado)
        freqRange = np.arange(0.01, 0.21, 0.01)    # 20 valores
        phaseRange = np.arange(0.0, 6.28, 0.20)     # 31 valores
        offsetRange = np.arange(-0.3, 0.31, 0.05)   # 13 valores

        bestProfit = float('-inf')
        bestFreq = 0.05
        bestPhase = 1.2
        bestOffset = 0.0

        tSeq = np.arange(1, len(priceArray) + 1)

        # Barrido de parámetros
        for f in freqRange:
            # Pre-calcular el seno para esta frecuencia y todas las fases
            for p in phaseRange:
                sinPart = np.sin(f * tSeq + p)
                for o in offsetRange:
                    ciclo = optimalAmplitude * sinPart + o
                    profit = self.simulateTrades(priceArray, ciclo, o)

                    if profit > bestProfit:
                        bestProfit = profit
                        bestFreq = float(f)
                        bestPhase = float(p)
                        bestOffset = float(o)

        logger.info(f"Optimización completada. Mejor PnL: {bestProfit:.5f} | Amp: {optimalAmplitude:.4f} | Freq: {bestFreq:.3f} | Phase: {bestPhase:.2f} | Offset: {bestOffset:.2f}")

        return {
            "amplitude": optimalAmplitude,
            "freq": bestFreq,
            "phase": bestPhase,
            "offset": bestOffset,
            "projectedProfit": bestProfit
        }

optimizer = OptimizerService()
