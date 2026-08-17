import logging
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import talib as ta

from middleware.database import dbManager
from Sentinel.analysis import technical
from Sentinel.analysis.technical import check_signal_health
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateBEPrice
from Sentinel.analysis import risk as riskAnalysis
from middleware.config import constants as config
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class ReversionMediaBot:
    def __init__(self):
        self._signals_sent = {}
        logger.info("Bot Reversión a la Media (LRC + RSI) iniciado")

    def rsi(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        return pd.Series(ta.RSI(df['close'].values, timeperiod=period), index=df.index)

    def atr(self, df: pd.DataFrame, period: int = 14) -> pd.Series:
        return pd.Series(ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=period), index=df.index)

    def calculateLrc(self, closePrices: np.ndarray, period: int = 100, dev: float = 2.0):
        """
        Calcula de forma exacta, robusta y no repintada el Canal de Regresión Lineal (LRC),
        las desviaciones estándar y la pendiente móvil (slope) utilizando datos de velas cerradas.
        """
        n = len(closePrices)
        center = np.full(n, np.nan)
        upper = np.full(n, np.nan)
        lower = np.full(n, np.nan)
        slope = np.full(n, np.nan)
        
        if n < period:
            return center, upper, lower, slope
            
        x = np.arange(period)
        
        # Bucle móvil para calcular la regresión en cada vela final i
        for i in range(period - 1, n):
            y = closePrices[i - period + 1 : i + 1]
            
            # Ajuste de regresión lineal por mínimos cuadrados ordinarios OLS
            m, c = np.polyfit(x, y, 1)
            
            # Valor de la línea de regresión en el punto actual (último índice de la ventana móvil)
            predVal = m * (period - 1) + c
            center[i] = predVal
            slope[i] = m # Coeficiente de la pendiente
            
            # Calcular desviación estándar de los residuos para el canal de desviación
            yFit = m * x + c
            residuals = y - yFit
            stdDev = np.std(residuals)
            
            upper[i] = predVal + (dev * stdDev)
            lower[i] = predVal - (dev * stdDev)
            
        return center, upper, lower, slope

    def checkDivergence(self, df: pd.DataFrame, rsiSeries: pd.Series, lookback: int = 5) -> Dict[str, bool]:
        """
        Detecta divergencias alcistas (Bullish) y bajistas (Bearish) entre el precio y el RSI
        en las últimas N velas de forma limpia.
        """
        divergences = {"bullish": False, "bearish": False}
        if len(df) < lookback + 1:
            return divergences

        # Obtener rebanadas de datos del lookback
        pricesClose = df['close'].tail(lookback)
        pricesLow = df['low'].tail(lookback)
        pricesHigh = df['high'].tail(lookback)
        rsiVals = rsiSeries.tail(lookback)

        # 1. Divergencia Alcista (Bullish Divergence)
        # El precio hace un mínimo más bajo, pero el RSI hace un mínimo más alto
        if pricesLow.iloc[-1] <= pricesLow.iloc[:-1].min():
            minPriceIdx = pricesLow.iloc[:-1].idxmin()
            if minPriceIdx in rsiVals.index:
                if rsiVals.iloc[-1] > rsiVals.loc[minPriceIdx]:
                    divergences["bullish"] = True

        # 2. Divergencia Bajista (Bearish Divergence)
        # El precio hace un máximo más alto, pero el RSI hace un máximo más bajo
        if pricesHigh.iloc[-1] >= pricesHigh.iloc[:-1].max():
            maxPriceIdx = pricesHigh.iloc[:-1].idxmax()
            if maxPriceIdx in rsiVals.index:
                if rsiVals.iloc[-1] < rsiVals.loc[maxPriceIdx]:
                    divergences["bearish"] = True

        return divergences

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo['symbol']
        timeframe = symbolInfo.get('timeframe', '1h')
        
        # Validar restricción de temporalidad (timeframe < 15m no permitido)
        if timeframe in ['1m', '3m', '5m'] or (timeframe.endswith('m') and int(timeframe[:-1]) < 15):
            logger.warning(f"[{symbol}] Regresivol: Temporalidad '{timeframe}' bloqueada por alto ruido de mercado.")
            return None

        logger.info(f"[{symbol}] Analizando estrategia RegressiVol Mean Reversion Avanzada...")
        
        master = preloadedData.get(symbol) if preloadedData else None
        if isinstance(master, dict):
            df = master.get(timeframe)
        else:
            df = master

        if df is None or len(df) < 100:
            logger.warning(f"[{symbol}] Regresivol: Historial insuficiente ({len(df) if df is not None else 0} de 100 velas).")
            return None

        # Copiar DataFrame para cálculos
        df = df.copy()
        
        # Cargar parámetros de configuración de la BD (con soporte para parámetros dinámicos por símbolo)
        stratConfig = dbManager.getSymbolStrategyConfig("ReversionMedia", symbol) or {}
        
        # Parámetros técnicos dinámicos
        lrcPeriod = int(stratConfig.get('lrcPeriod') or stratConfig.get('lrc_period') or 100)
        lrcDev = float(stratConfig.get('lrcDev') or stratConfig.get('lrc_dev') or 2.0)
        minRrVal = float(stratConfig.get('minRr') or stratConfig.get('min_rr') or 2.5)
        rsiPeriodVal = int(stratConfig.get('rsiPeriod') or stratConfig.get('rsi_period') or 14)
        atrPeriodVal = int(stratConfig.get('atrPeriod') or stratConfig.get('atr_period') or 14)
        
        minConfVal = float(stratConfig.get('minConfidence') or stratConfig.get('min_confidence', 70)) / 100.0
        minUsdProfit = float(stratConfig.get('minUsdProfit') or stratConfig.get('min_usd_profit', 10.0))
        
        # 1. Indicadores técnicos
        df["rsi"] = self.rsi(df, rsiPeriodVal)
        df["atr"] = self.atr(df, atrPeriodVal)
        
        closePrices = df['close'].values
        centerChannel, upperChannel, lowerChannel, slopeChannel = self.calculateLrc(closePrices, period=lrcPeriod, dev=lrcDev)
        
        df["lrcCenter"] = centerChannel
        df["lrcUpper"] = upperChannel
        df["lrcLower"] = lowerChannel
        df["lrcSlope"] = slopeChannel

        # Validar nulos al inicio
        if pd.isna(df["lrcCenter"].iloc[-1]) or pd.isna(df["rsi"].iloc[-1]) or pd.isna(df["atr"].iloc[-1]) or pd.isna(df["lrcSlope"].iloc[-1]):
            return None

        # --- MEJORA 2: Volume Breakout Protection ---
        # Si la vela rompe la banda del LRC con volumen institucional (volumen > 1.5x promedio 20 periodos), descartamos reversión
        avgVolume = df['volume'].rolling(window=20).mean().iloc[-1] if 'volume' in df.columns else 0
        currentVolume = df['volume'].iloc[-1] if 'volume' in df.columns else 0
        
        if currentVolume > 1.5 * avgVolume and avgVolume > 0:
            logger.info(f"[{symbol}] Regresivol: Volumen actual ({currentVolume:.0f}) supera 1.5x el promedio ({avgVolume:.0f}). Breakout real en curso - descartando reversión a la media.")
            return None

        # 2. Obtener valores de la vela cerrada actual (no intra-vela, lookahead_off)
        currentClose = df['close'].iloc[-1]
        currentHigh = df['high'].iloc[-1]
        currentLow = df['low'].iloc[-1]
        currentRsi = df['rsi'].iloc[-1]
        currentAtr = df['atr'].iloc[-1]
        
        currentLrcUpper = df['lrcUpper'].iloc[-1]
        currentLrcLower = df['lrcLower'].iloc[-1]
        currentLrcCenter = df['lrcCenter'].iloc[-1]
        currentLrcSlope = df['lrcSlope'].iloc[-1]

        # Obtener divergencias
        divergences = self.checkDivergence(df, df["rsi"], lookback=5)

        direction = None
        setup = ""

        # --- MEJORA 1: LRC Slope Filter (Filtro Seguidor de Tendencia Macro) ---
        # Si la pendiente es positiva (alcista), bloqueamos Shorts y solo permitimos Largos.
        # Si la pendiente es negativa (bajista), bloqueamos Longs y solo permitimos Shorts.
        isTrendBullish = (currentLrcSlope > 0)

        # Impulse MACD (Filtro de giro de momentum para evitar entrar contra un impulso fuerte)
        currentImpulse = df["impulseMacd"].iloc[-1] if "impulseMacd" in df.columns else 0.0
        prevImpulse = df["impulseMacd"].iloc[-2] if "impulseMacd" in df.columns else 0.0
        currentSignal = df["impulseSignal"].iloc[-1] if "impulseSignal" in df.columns else 0.0
        prevSignal = df["impulseSignal"].iloc[-2] if "impulseSignal" in df.columns else 0.0

        # Detectar giros/cambios en el Impulse MACD (cruce estricto para evitar entradas prematuras)
        impulseGiroLong = (currentImpulse > currentSignal) and (prevImpulse <= prevSignal)
        impulseGiroShort = (currentImpulse < currentSignal) and (prevImpulse >= prevSignal)

        # 3. Evaluar condiciones de entrada con confluencia del filtro de tendencia e Impulse MACD
        # Configuración para COMPRA (Long Trigger)
        if currentClose < currentLrcLower and isTrendBullish:
            if (currentRsi < 30 or divergences["bullish"]) and impulseGiroLong:
                direction = "LARGO"
                setup = "LRC Oversold Buy (Impulse Confirmed)"

        # Configuración para VENTA (Short Trigger)
        elif currentClose > currentLrcUpper and not isTrendBullish:
            if (currentRsi > 70 or divergences["bearish"]) and impulseGiroShort:
                direction = "CORTO"
                setup = "LRC Overbought Sell (Impulse Confirmed)"

        if not direction:
            return None

        # --- MEJORA 3: Stop Loss Estructural Adaptativo (Híbrido) ---
        # Se calcula la distancia mínima 1.5x ATR y se contrasta contra el Swing Low/High de las últimas 15 velas.
        swingLow = df['low'].tail(15).min()
        swingHigh = df['high'].tail(15).max()

        if direction == "LARGO":
            # Colocar por debajo del mínimo de la vela y del swing low anterior para mayor seguridad
            slPrice = min(currentLow - (1.5 * currentAtr), swingLow - (0.2 * currentAtr))
        else:
            # Colocar por encima del máximo de la vela y del swing high anterior para mayor seguridad
            slPrice = max(currentHigh + (1.5 * currentAtr), swingHigh + (0.2 * currentAtr))

        slDist = abs(currentClose - slPrice)
        if slDist <= 0:
            return None

        # El take profit final se calcula a partir del min R:R dinámico
        tpPrice = adjustTPForMinRR(currentClose, slPrice, (currentClose + (slDist * minRrVal) if direction == "LARGO" else currentClose - (slDist * minRrVal)), direction, minRR=minRrVal)
        
        # La confianza se estima a partir de la confluencia (RSI y divergencia aumentan confianza)
        baseConfidence = 0.70
        if currentRsi < 20 or currentRsi > 80:
            baseConfidence += 0.10
        if (direction == "LARGO" and divergences["bullish"]) or (direction == "CORTO" and divergences["bearish"]):
            baseConfidence += 0.15
            
        if baseConfidence < minConfVal:
            logger.info(f"[{symbol}] ReversionMedia: Confianza {baseConfidence:.2f} < {minConfVal:.2f} - saltando")
            return None

        # Obtener marcas de tiempo y vela cerrada
        lastV = get_last_closed_candle(datetime.now(ZoneInfo(TIMEZONE)), 60, df=df)
        candleTime = (lastV.name if hasattr(lastV, 'name') else lastV).strftime("%Y-%m-%d %H:%M:%S")

        # Deduplicación por vela para evitar re-entradas rápidas en caso de tocar SL
        sigKey = f"{symbol}_{candleTime}"
        if sigKey in self._signals_sent:
            logger.info(f"[{symbol}] ReversionMedia: Señal ya emitida para la vela {candleTime} — omitiendo duplicado.")
            return None

        # Chequeo de salud de la señal
        isHealth, _, healthMsg = check_signal_health(currentClose, tpPrice, slPrice, direction, currentClose, threshold=0.65, candleTime=candleTime)
        if not isHealth:
            logger.info(f"[{symbol}] ReversionMedia: Señal descartada por salud - {healthMsg}")
            return None

        multiplier = getPipMultiplier(symbol)
        
        # --- Cálculo de Tamaño de Posición y Gestión de Riesgo ---
        refCapital = symbolInfo.get('refCapital', 10000.0)
        refRiskPct = symbolInfo.get('refRiskPct', 1.0) # Parámetro "Risk_Percent", sugerido 1%
        
        size, riskUsdActual, marginUsed = riskAnalysis.calculatePositionSize(
            refCapital, refRiskPct, slDist, symbolInfo, entryPrice=currentClose
        )

        if size is None or size <= 0:
            logger.info(f"[{symbol}] ReversionMedia: Tamaño de posición inválido o margen insuficiente - saltando")
            return None

        rrVal = round(abs(tpPrice - currentClose) / slDist, 2)
        expectedProfit = riskUsdActual * rrVal

        # Criterio mínimo de USD Profit
        if expectedProfit < minUsdProfit:
            logger.info(f"[{symbol}] ReversionMedia: Beneficio Est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - descartando por devaluación de ganancia")
            return None

        # --- MEJORA 4: Trailing Stop dinámico sobre la línea central del LRC ---
        # Se calcula la línea media central del LRC como Break Even para proteger capital
        beTrigger = currentLrcCenter

        self._signals_sent[sigKey] = True
        logger.info(f"[{symbol}] ¡Señal de Reversión a la Media Optimizada EN ZONA! {direction} - Entrada: {currentClose}, SL: {slPrice}, TP: {tpPrice}")

        return Signal(
            strategy="ReversionMedia",
            symbol=symbol,
            direction=direction,
            entry_price=currentClose,
            stop_loss=slPrice,
            take_profit=tpPrice,
            sl_distance=slDist,
            confidence=int(baseConfidence * 100),
            setup=setup,
            status="EN ZONA ✅",
            candleTime=candleTime,
            intervalo=timeframe,
            riesgo_pips=round(slDist * multiplier, 1),
            rr_ratio=rrVal,
            break_even=beTrigger,
            size=size,
            metadata={
                "rsi": round(currentRsi, 2),
                "lrc_center": round(currentLrcCenter, 5),
                "lrc_slope": round(currentLrcSlope, 6),
                "risk_usd": round(riskUsdActual, 2),
                "expected_profit": round(expectedProfit, 2),
                "margin_used": round(marginUsed, 2),
                "volume_ratio": round(currentVolume / avgVolume, 2) if avgVolume > 0 else 0,
                "slippage_ticks": 15,
                "comision_pct": 0.05
            }
        )
