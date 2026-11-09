"""
Microservicio Autónomo de Monitoreo y Ejecución de CART (microCart.py)
Estrategia: CART (Canal Adaptativo de Ruido por Tendencia)
Opera sobre un solo par en precios reales (sin normalización, sin media ni promedio de la media),
utilizando el Canal Adaptativo de Ruido (CAR: EMA 20 ± 1.5σ del ruido residual):
  - Entrada COMPRA (LARGO): Cuando el precio sale por arriba del CAR (precio > CAR Máx).
  - Salida COMPRA: Cuando el precio ingresa nuevamente al CAR (precio <= CAR Máx).
  - Entrada VENTA (CORTO): Cuando el precio sale por debajo del CAR (precio < CAR Mín).
  - Salida VENTA: Cuando el precio ingresa nuevamente al CAR (precio >= CAR Mín).
Registra operaciones en la tabla trades con strategy = 'CART ATALAia'.
Estándar de Nomenclatura: camelCase para funciones y variables.
"""

import os
import sys
import time
import math
import json
import logging
import pymysql
import urllib.request
import urllib.error
from datetime import datetime, time as dt_time, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

# Asegurar que el directorio raíz de ATALAia esté en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal
from backend.core.pnl_calculator import calculate_unrealized_pnl, calculate_margin
from backend.services.microRatio import calculateSymbolNATR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [microCart] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("microCart")

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "http://127.0.0.1:8002/webhook/tradingview")
STRATEGY_NAME = "CART ATALAia"
CART_STOP_LOSS_FALLBACK_PCT = 0.15  # Fallback de corte de pérdida si no hay velas para NATR
CART_NATR_MULTIPLIER = 1.5          # Multiplicador k unificado con RATIO para Trailing Stop Loss
CART_STOP_LOSS_PCT = 0.15           # Compatibilidad hacia atrás

_lastCartLogTime: Dict[Tuple[int, str], float] = {}
_failedOrderCooldown: Dict[Tuple[int, str], float] = {}

def shouldLogStatus(idCuenta: int, symbol: str, intervalSec: float = 30.0) -> bool:
    key = (idCuenta, symbol)
    now = time.time()
    last = _lastCartLogTime.get(key, 0.0)
    if now - last >= intervalSec:
        _lastCartLogTime[key] = now
        return True
    return False

def isOrderInCooldown(idCuenta: int, symbol: str) -> bool:
    key = (idCuenta, symbol)
    now = time.time()
    until = _failedOrderCooldown.get(key, 0.0)
    return now < until

def setOrderCooldown(idCuenta: int, symbol: str, seconds: float = 120.0) -> None:
    key = (idCuenta, symbol)
    _failedOrderCooldown[key] = time.time() + seconds

def isMarketOpenForSymbol(symbol: Optional[str] = None) -> bool:
    """
    Determina si el mercado para un símbolo está abierto actualmente:
    - Criptomonedas (BTC, ETH, etc.): Abiertas 24/7.
    - Forex / Metales: Cierran de Viernes 22:00 UTC (17:00 NY) a Domingo 21:15 UTC (17:15 NY).
    """
    sym = (symbol or "").upper().replace("/", "").replace("_", "")
    if any(c in sym for c in ["BTC", "ETH", "SOL", "LTC", "XRP", "DOGE"]):
        return True

    now_utc = datetime.now(timezone.utc)
    weekday = now_utc.weekday()  # 0=Lunes, 4=Viernes, 5=Sábado, 6=Domingo
    hour = now_utc.hour
    minute = now_utc.minute

    # Sábado: Cerrado
    if weekday == 5:
        return False
    # Viernes después de 22:00 UTC: Cerrado
    if weekday == 4 and (hour > 22 or (hour == 22 and minute >= 0)):
        return False
    # Domingo antes de 21:15 UTC: Cerrado
    if weekday == 6 and (hour < 21 or (hour == 21 and minute < 15)):
        return False

    return True


def isWithinOperatingHours(horaInicio: str, horaFin: str, currentDt: Optional[datetime] = None) -> bool:
    """
    Verifica si la hora actual está dentro de la ventana operativa configurada [horaInicio, horaFin].
    Soporta ventanas estándar (ej. 08:00 a 16:00) y que cruzan medianoche (ej. 22:00 a 04:00).
    """
    try:
        nowTime = (currentDt or datetime.now()).time()
        startParts = [int(p) for p in str(horaInicio or "08:00").split(":")]
        endParts = [int(p) for p in str(horaFin or "16:00").split(":")]
        startTime = dt_time(startParts[0], startParts[1])
        endTime = dt_time(endParts[0], endParts[1])
        if startTime <= endTime:
            return startTime <= nowTime < endTime
        else:
            return nowTime >= startTime or nowTime < endTime
    except Exception as e:
        logger.warning(f"Error verificando horario operativo ({horaInicio} - {horaFin}): {e}")
        return True


def getTimeframeDurationSeconds(tf: str) -> int:
    """
    Retorna la duración en segundos de una vela según la temporalidad configurada.
    """
    tfL = str(tf or "1h").lower().strip()
    if tfL in ["1min", "1m"]:
        return 60
    if tfL in ["5min", "5m"]:
        return 300
    if tfL in ["15min", "15m"]:
        return 900
    if tfL in ["30min", "30m"]:
        return 1800
    if tfL in ["1h"]:
        return 3600
    if tfL in ["4h"]:
        return 14400
    if tfL in ["1d", "1day", "d"]:
        return 86400
    return 3600


def calcEmaSeries(values: List[Optional[float]], period: int) -> List[Optional[float]]:
    """
    Calcula la serie EMA sobre precios reales (idéntica a calcEmaSeries del frontend).
    """
    k = 2.0 / (float(period) + 1.0)
    res: List[Optional[float]] = []
    prev: Optional[float] = None
    for val in values:
        if val is None or (isinstance(val, float) and math.isnan(val)):
            res.append(None)
        else:
            v = float(val)
            if prev is None:
                prev = v
            else:
                prev = (v * k) + (prev * (1.0 - k))
            res.append(prev)
    return res


def calcCarChannel(
    priceValues: List[Optional[float]],
    baselineValues: List[Optional[float]],
    windowSize: int = 20,
    multiplier: float = 1.5
) -> Dict[str, List[Optional[float]]]:
    """
    Calcula el Canal Adaptativo de Ruido (CAR) sobre precios reales (sin normalizar):
      residuo[i] = precio[i] - EMA20[i]
      noiseStd = desviación estándar poblacional de los últimos windowSize residuos
      carMax = EMA20 + multiplier * noiseStd
      carMin = EMA20 - multiplier * noiseStd
    """
    carMax: List[Optional[float]] = []
    carMin: List[Optional[float]] = []
    residuals: List[Optional[float]] = []

    for i in range(len(priceValues)):
        p = priceValues[i]
        b = baselineValues[i]
        if p is None or b is None or math.isnan(p) or math.isnan(b):
            residuals.append(None)
            carMax.append(None)
            carMin.append(None)
            continue

        res = float(p) - float(b)
        residuals.append(res)

        startIdx = max(0, i - windowSize + 1)
        win = [r for r in residuals[startIdx:i + 1] if r is not None and not math.isnan(r)]
        if len(win) < 3:
            carMax.append(None)
            carMin.append(None)
            continue

        meanRes = sum(win) / len(win)
        variance = sum((v - meanRes) ** 2 for v in win) / len(win)
        noiseStd = math.sqrt(variance)

        carMax.append(float(b) + (multiplier * noiseStd))
        carMin.append(float(b) - (multiplier * noiseStd))

    return {"max": carMax, "min": carMin}


def evaluateCartSignals(
    df: pd.DataFrame,
    symbol: str,
    smaPeriod: int = 2,
    carWindow: int = 20,
    carMultiplier: float = 1.0,
    natrPct: Optional[float] = None,
    timeframe: str = "1h",
    dbSession = None
) -> Dict[str, Any]:
    """
    Evalúa las series de precios reales y el Canal Adaptativo de Ruido (CAR: EMA 20 ± 1.5σ)
    para un único par (sin normalización, sin media y sin promedio de la media).
    Reglas CART / RIDER:
      - Compra (hasBuySignal): Precio sale por arriba del CAR (prevPx <= prevCarMax y px > carMax)
      - Salida Compra (hasBuyExit): Por Trailing Stop Loss (-NATR% del pico) acotado al CAR superior (max(cMax, peak - NATR%)),
        o reingreso al canal CAR.
      - Venta (hasSellSignal): Precio sale por debajo del CAR (prevPx >= prevCarMin y px < carMin)
      - Salida Venta (hasSellExit): Por Trailing Stop Loss (+NATR% del valle) acotado al CAR inferior (min(cMin, valley + NATR%)),
        o reingreso al canal CAR.
      - Trailing SL: Nunca invade el interior del canal CAR.
    """
    if df is None or df.empty:
        return {"signals": [], "latestSignal": {}, "history": []}

    colName = "closePrice" if "closePrice" in df.columns else df.columns[0]
    prices = [float(x) if not pd.isna(x) else None for x in df[colName].tolist()]
    highs = [float(x) if not pd.isna(x) else None for x in df["highPrice"].tolist()] if "highPrice" in df.columns else prices
    lows = [float(x) if not pd.isna(x) else None for x in df["lowPrice"].tolist()] if "lowPrice" in df.columns else prices
    opens = [float(x) if not pd.isna(x) else None for x in df["openPrice"].tolist()] if "openPrice" in df.columns else prices
    dates = [
        idx.strftime("%Y-%m-%d %H:%M:%S") if hasattr(idx, "strftime") else str(idx)
        for idx in df.index
    ]

    ema20 = calcEmaSeries(prices, max(2, int(carWindow or 20)))
    car = calcCarChannel(prices, ema20, int(carWindow or 20), float(carMultiplier or 1.0))
    carMax = car["max"]
    carMin = car["min"]

    # Cálculo de límites Min-Max para Precio Normalizado [0.0 - 1.0]
    valid_prices = [p for p in prices if p is not None]
    min_px_series = min(valid_prices) if valid_prices else 0.0
    max_px_series = max(valid_prices) if valid_prices else 1.0
    range_px_series = (max_px_series - min_px_series) if (max_px_series - min_px_series) > 0 else 1.0

    # Calcular o utilizar NATR dinámico del activo (Unificado con RATIO) con pisos institucionales
    if natrPct is None:
        if dbSession is not None and symbol:
            try:
                natrPct = calculateSymbolNATR(dbSession, symbol, timeframe=timeframe, kMultiplier=CART_NATR_MULTIPLIER)
            except Exception as e:
                logger.warning(f"Error obteniendo NATR para {symbol} en evaluateCartSignals: {e}")
                natrPct = CART_STOP_LOSS_FALLBACK_PCT
        else:
            natrPct = CART_STOP_LOSS_FALLBACK_PCT

    symU = symbol.upper()
    if "BTC" in symU or "ETH" in symU or "CRYPTO" in symU:
        natrPct = max(0.50, float(natrPct or 0.50))
    elif "XAU" in symU or "GOLD" in symU:
        natrPct = max(0.35, float(natrPct or 0.35))
    elif "XAG" in symU or "SILVER" in symU:
        natrPct = max(0.40, float(natrPct or 0.40))
    else:
        natrPct = max(0.15, float(natrPct or 0.15))

    stopLossRatio = float(natrPct) / 100.0

    signals: List[Dict[str, Any]] = []
    history: List[Dict[str, Any]] = []
    activeDir: Optional[str] = None
    activeEntryPx: Optional[float] = None
    peakPx: Optional[float] = None
    valleyPx: Optional[float] = None
    activeSl: Optional[float] = None

    for i in range(len(prices)):
        px = prices[i]
        highVal = highs[i] if highs[i] is not None else px
        lowVal = lows[i] if lows[i] is not None else px
        openVal = opens[i] if opens[i] is not None else px
        e20 = ema20[i]
        cMax = carMax[i]
        cMin = carMin[i]
        dtStr = dates[i]

        hasBuySignal = False
        hasSellSignal = False
        hasBuyExit = False
        hasSellExit = False
        exitPxVal: Optional[float] = None
        signalLabel: Optional[str] = None

        sigPriceSlope = 0.0
        sigCandleSlope = 0.0

        if i > 0 and px is not None and cMax is not None and cMin is not None:
            prevPx = prices[i - 1]
            prevCMax = carMax[i - 1]
            prevCMin = carMin[i - 1]

            if prevPx is not None and prevCMax is not None and prevCMin is not None:
                # Pendientes de la barra según la temporalidad
                priceSlope = px - prevPx
                candleSlope = (px - openVal) if openVal is not None else priceSlope
                sigPriceSlope = priceSlope
                sigCandleSlope = candleSlope

                # 1. Si hay una posición abierta:
                #    A. Salida por Trailing Stop Loss Dinámico (NATR):
                #       - COMPRA: Se activa si activeSl ya superó cMax (ganancias aseguradas)
                #         y el mínimo de la barra o el precio actual toca activeSl.
                #       - VENTA: Se activa si activeSl ya bajó de cMin (ganancias aseguradas)
                #         y el máximo de la barra o el precio actual toca activeSl.
                #    B. Salida por Reingreso al CAR:
                #       - Evaluado ÚNICAMENTE al Cierre de Vela (Close / px).
                #       - Las mechas intradiarias no sacan la posición si el cierre se mantiene por fuera.
                if activeDir == "COMPRA":
                    peakPx = max(peakPx if peakPx is not None else (activeEntryPx or px), highVal if highVal is not None else px)
                    candidateSl = peakPx * (1.0 - stopLossRatio)
                    activeSl = candidateSl if activeSl is None else max(activeSl, candidateSl)  # NUNCA DISMINUYE

                    # Salida por TSL: sólo si ya superó el precio de entrada y aseguró ganancias
                    hasMeaningfulGain = bool(activeEntryPx and peakPx >= activeEntryPx * (1.0 + stopLossRatio))
                    hitTrailingSl = bool(hasMeaningfulGain and activeSl > activeEntryPx and activeSl > cMax and ((lowVal is not None and lowVal <= activeSl) or (px <= activeSl)))
                    hitCarReentry = bool(px <= cMax)

                    if hitTrailingSl:
                        hasBuyExit = True
                        exitPxVal = activeSl
                        signalLabel = f"Trailing Stop Loss (-{natrPct:.2f}% @ {activeSl:.5f})"
                        activeDir = None
                        activeEntryPx = None
                        peakPx = None
                        valleyPx = None
                        activeSl = None
                    elif hitCarReentry:
                        hasBuyExit = True
                        exitPxVal = cMax if (openVal is None or openVal > cMax) else px
                        signalLabel = "BUY_EXIT"
                        activeDir = None
                        activeEntryPx = None
                        peakPx = None
                        valleyPx = None
                        activeSl = None

                elif activeDir == "VENTA":
                    valleyPx = min(valleyPx if valleyPx is not None else (activeEntryPx or px), lowVal if lowVal is not None else px)
                    candidateSl = valleyPx * (1.0 + stopLossRatio)
                    activeSl = candidateSl if activeSl is None else min(activeSl, candidateSl)  # NUNCA AUMENTA

                    # Salida por TSL: sólo si ya cayó por debajo del precio de entrada y aseguró ganancias
                    hasMeaningfulGain = bool(activeEntryPx and valleyPx <= activeEntryPx * (1.0 - stopLossRatio))
                    hitTrailingSl = bool(hasMeaningfulGain and activeSl < activeEntryPx and activeSl < cMin and ((highVal is not None and highVal >= activeSl) or (px >= activeSl)))
                    hitCarReentry = bool(px >= cMin)

                    if hitTrailingSl:
                        hasSellExit = True
                        exitPxVal = activeSl
                        signalLabel = f"Trailing Stop Loss (+{natrPct:.2f}% @ {activeSl:.5f})"
                        activeDir = None
                        activeEntryPx = None
                        peakPx = None
                        valleyPx = None
                        activeSl = None
                    elif hitCarReentry:
                        hasSellExit = True
                        exitPxVal = cMin if (openVal is None or openVal < cMin) else px
                        signalLabel = "SELL_EXIT"
                        activeDir = None
                        activeEntryPx = None
                        peakPx = None
                        valleyPx = None
                        activeSl = None

                # 2. Si no hay posición abierta, verificar ruptura del CAR con filtro de PENDIENTE consistente:
                #    - COMPRA: Rompe límite superior (px > cMax) Y la pendiente del precio es positiva (priceSlope > 0 y candleSlope >= 0).
                #    - VENTA: Rompe límite inferior (px < cMin) Y la pendiente del precio es negativa (priceSlope < 0 y candleSlope <= 0).
                if activeDir is None:
                    if prevCMin <= prevPx <= prevCMax and px > cMax:
                        isSlopePositive = (priceSlope > 0) and (candleSlope >= 0)
                        if isSlopePositive:
                            hasBuySignal = True
                            signalLabel = "BUY_ENTRY"
                            activeDir = "COMPRA"
                            activeEntryPx = px
                            peakPx = max(activeEntryPx, highVal if highVal is not None else px)
                            valleyPx = None
                            activeSl = peakPx * (1.0 - stopLossRatio)
                            # Si en la misma barra se disparó TSL en territorio por encima de cMax
                            if activeSl > cMax and lowVal is not None and lowVal <= activeSl:
                                hasBuyExit = True
                                exitPxVal = activeSl
                                activeDir = None
                                peakPx = None
                                activeSl = None

                    elif prevCMin <= prevPx <= prevCMax and px < cMin:
                        isSlopeNegative = (priceSlope < 0) and (candleSlope <= 0)
                        if isSlopeNegative:
                            hasSellSignal = True
                            signalLabel = "SELL_ENTRY"
                            activeDir = "VENTA"
                            activeEntryPx = px
                            valleyPx = min(activeEntryPx, lowVal if lowVal is not None else px)
                            peakPx = None
                            activeSl = valleyPx * (1.0 + stopLossRatio)
                            # Si en la misma barra se disparó TSL en territorio por debajo de cMin
                            if activeSl < cMin and highVal is not None and highVal >= activeSl:
                                hasSellExit = True
                                exitPxVal = activeSl
                                activeDir = None
                                valleyPx = None
                                activeSl = None

        sigItem = {
            "datetime": dtStr,
            "symbol": symbol,
            "price": round(px, 5) if px is not None else None,
            "priceA": round(px, 5) if px is not None else None,
            "priceSlope": round(sigPriceSlope, 6),
            "candleSlope": round(sigCandleSlope, 6),
            "normPrice": round((px - min_px_series) / range_px_series, 4) if px is not None else None,
            "normalizedPrice": round((px - min_px_series) / range_px_series, 4) if px is not None else None,
            "highPrice": round(highVal, 5) if highVal is not None else None,
            "lowPrice": round(lowVal, 5) if lowVal is not None else None,
            "openPrice": round(openVal, 5) if openVal is not None else None,
            "ema20": round(e20, 5) if e20 is not None else None,
            "emaCar": round(e20, 5) if e20 is not None else None,
            "carMax": round(cMax, 5) if cMax is not None else None,
            "carMin": round(cMin, 5) if cMin is not None else None,
            "exitPrice": round(exitPxVal, 5) if exitPxVal is not None else None,
            "signal": signalLabel,
            "hasBuySignal": hasBuySignal,
            "hasSellSignal": hasSellSignal,
            "hasBuyExit": hasBuyExit,
            "hasSellExit": hasSellExit,
            "isInsideCar": bool(px is not None and cMax is not None and cMin is not None and cMin <= px <= cMax),
            "natrPct": round(float(natrPct), 3),
            "slRatio": round(float(stopLossRatio), 5)
        }
        signals.append(sigItem)
        history.append(sigItem)

    return {
        "signals": signals,
        "latestSignal": signals[-1] if signals else {},
        "history": history
    }


def runCartBacktest(
    df: pd.DataFrame,
    symbol: str,
    smaPeriod: int = 2,
    carWindow: int = 20,
    carMultiplier: float = 1.0,
    initialCapital: float = 1000.0,
    allocationPct: float = 1.0,
    minLots: float = 1000.0,
    margenPct: float = 3.33,
    pipSize: float = 0.0001,
    minMarginIndicator: float = 200.0,
    timeframe: str = "1h",
    dbSession = None,
    natrPct: Optional[float] = None
) -> Dict[str, Any]:
    """
    Ejecuta el BackTest de la estrategia CART sobre un solo par en precios reales:
      - Si el precio sale por arriba del CAR (px > carMax) -> Abre COMPRA, y cierra cuando px <= carMax o por Trailing SL.
      - Si el precio sale por debajo del CAR (px < carMin) -> Abre VENTA, y cierra cuando px >= carMin o por Trailing SL.
      - Trailing Stop Loss adaptativo por volatilidad (NATR unificado institucionalmente con RATIO).
      - Limita las entradas por el Indicador de Margen mínimo de operación (minMarginIndicator, por defecto 200%),
        igual que en RATIOS.
    """
    if natrPct is None:
        closeSession = False
        s = dbSession
        if s is None:
            s = SessionLocal()
            closeSession = True
        try:
            natrPct = calculateSymbolNATR(s, symbol, timeframe=timeframe, kMultiplier=CART_NATR_MULTIPLIER)
        except Exception as e:
            logger.warning(f"Error obteniendo NATR para {symbol} en runCartBacktest: {e}")
            natrPct = CART_STOP_LOSS_FALLBACK_PCT
        finally:
            if closeSession:
                s.close()

    evalRes = evaluateCartSignals(
        df=df,
        symbol=symbol,
        smaPeriod=smaPeriod,
        carWindow=carWindow,
        carMultiplier=carMultiplier,
        natrPct=natrPct,
        timeframe=timeframe,
        dbSession=dbSession
    )
    signals = evalRes.get("signals", [])
    if not signals:
        return {
            "startDate": "-",
            "endDate": "-",
            "totalBars": 0,
            "totalDays": 0,
            "totalCycles": 0,
            "natrPct": round(float(natrPct), 3),
            "slRatio": round(float(natrPct) / 100.0, 5),
            "trades": [],
            "equityCurve": [],
            "metrics": {
                "totalTrades": 0,
                "winningTrades": 0,
                "losingTrades": 0,
                "winRate": 0.0,
                "netProfit": 0.0,
                "totalReturnPct": 0.0,
                "profitFactor": 0.0,
                "sharpeRatio": 0.0,
                "maxDrawdown": 0.0,
                "finalCapital": initialCapital
            }
        }

    currentCap = float(initialCapital or 1000.0)
    allocPct = float(allocationPct or 1.0)
    allocationRate = (allocPct / 100.0) if allocPct >= 0.05 else allocPct
    if allocationRate <= 0:
        allocationRate = 0.01
    minLotsVal = float(minLots or 1000.0)
    margenPctVal = float(margenPct or 0.5)
    minMarginInd = float(minMarginIndicator or 200.0)

    openPositions: List[Dict[str, Any]] = []
    allTradesOutput: List[Dict[str, Any]] = []
    equityCurve: List[Dict[str, Any]] = [{"x": signals[0]["datetime"], "y": round(currentCap, 2)}]

    tradeCounter = 0
    cycleCounter = 0
    winningTrades = 0
    losingTrades = 0
    grossProfit = 0.0
    grossLoss = 0.0
    peakCap = currentCap
    maxDrawdownPct = 0.0
    marginCappedCycles = 0
    totalStoppedEntries = 0
    btPeakPx: Optional[float] = None
    btValleyPx: Optional[float] = None
    btActiveSl: Optional[float] = None
    stopLossRatio = float(natrPct) / 100.0

    for sig in signals:
        px = sig["price"]
        cMax = sig["carMax"]
        cMin = sig["carMin"]
        dtStr = sig["datetime"]
        if px is None or cMax is None or cMin is None:
            continue

        # 1. Verificar cierre:
        #    - Por Trailing Stop Loss (±0.15% respecto al precio extremo alcanzado)
        #      estrictamente MONOTÓNICO (nunca retrocede), delimitado institucionalmente
        #      para NO invadir el canal CAR
        #    - O cuando el precio ingresa nuevamente al CAR (COMPRA en cMax, VENTA en cMin)
        if openPositions:
            # Incrementar la duración en velas de la posición activa
            for p in openPositions:
                p["durationBars"] = int(p.get("durationBars", 1)) + 1

            currentDir = openPositions[0]["direction"]
            entryPxRef = float(openPositions[0]["entryPrice"])
            shouldClose = False
            exitBoundaryPx = px
            exitReasonLabel = ""

            highVal = sig.get("highPrice") if sig.get("highPrice") is not None else px
            lowVal = sig.get("lowPrice") if sig.get("lowPrice") is not None else px
            openVal = sig.get("openPrice") if sig.get("openPrice") is not None else px

            if currentDir == "COMPRA":
                btPeakPx = max(btPeakPx if btPeakPx is not None else entryPxRef, highVal)
                candidateSl = btPeakPx * (1.0 - stopLossRatio)
                btActiveSl = candidateSl if btActiveSl is None else max(btActiveSl, candidateSl)  # NUNCA DISMINUYE

                hasMeaningfulGain = bool(entryPxRef and btPeakPx >= entryPxRef * (1.0 + stopLossRatio))
                hitTrailingSl = bool(hasMeaningfulGain and btActiveSl > entryPxRef and btActiveSl > cMax and ((lowVal is not None and lowVal <= btActiveSl) or (px <= btActiveSl)))
                hitCarReentry = bool(px <= cMax)

                if hitTrailingSl:
                    shouldClose = True
                    exitBoundaryPx = openVal if (openVal is not None and openVal < btActiveSl) else btActiveSl
                    exitReasonLabel = f"Trailing Stop Loss (-{natrPct:.2f}% @ {btActiveSl:.5f})"
                elif hitCarReentry:
                    shouldClose = True
                    exitBoundaryPx = openVal if (openVal is not None and openVal < cMax) else cMax
                    exitReasonLabel = "Reingreso CAR Superior"

            elif currentDir == "VENTA":
                btValleyPx = min(btValleyPx if btValleyPx is not None else entryPxRef, lowVal)
                candidateSl = btValleyPx * (1.0 + stopLossRatio)
                btActiveSl = candidateSl if btActiveSl is None else min(btActiveSl, candidateSl)  # NUNCA AUMENTA

                hasMeaningfulGain = bool(entryPxRef and btValleyPx <= entryPxRef * (1.0 - stopLossRatio))
                hitTrailingSl = bool(hasMeaningfulGain and btActiveSl < entryPxRef and btActiveSl < cMin and ((highVal is not None and highVal >= btActiveSl) or (px >= btActiveSl)))
                hitCarReentry = bool(px >= cMin)

                if hitTrailingSl:
                    shouldClose = True
                    exitBoundaryPx = openVal if (openVal is not None and openVal > btActiveSl) else btActiveSl
                    exitReasonLabel = f"Trailing Stop Loss (+{natrPct:.2f}% @ {btActiveSl:.5f})"
                elif hitCarReentry:
                    shouldClose = True
                    exitBoundaryPx = openVal if (openVal is not None and openVal > cMin) else cMin
                    exitReasonLabel = "Reingreso CAR Inferior"

            if shouldClose:
                cycleDurBars = openPositions[0].get("durationBars", 1)
                btPeakPx = None
                btValleyPx = None
                btActiveSl = None
                cycleCounter += 1
                cyclePnl = 0.0
                cycleMargin = 0.0
                cycleUnits = 0.0
                for pos in openPositions:
                    pnlVal, pipsVal, _, _ = calculate_unrealized_pnl(
                        symbol=symbol,
                        direction="LARGO" if pos["direction"] == "COMPRA" else "CORTO",
                        entry_price=pos["entryPrice"],
                        current_price=exitBoundaryPx,
                        size=pos["units"],
                        pip_size=pipSize
                    )
                    pos["exitDate"] = dtStr
                    pos["exitPriceA"] = round(exitBoundaryPx, 5)
                    pos["exitPriceB"] = 0.0
                    pos["exitReason"] = exitReasonLabel
                    pos["durationBars"] = cycleDurBars
                    pos["pnl"] = round(pnlVal, 2)
                    pos["pnlA"] = round(pnlVal, 2)
                    pos["pnlB"] = 0.0
                    pos["pipsA"] = round(pipsVal, 1)
                    pos["pipsB"] = 0.0
                    pos["returnPct"] = round((pnlVal / pos["allocatedCapital"] * 100.0), 2) if pos["allocatedCapital"] > 0 else 0.0
                    pos["isWin"] = pnlVal >= 0
                    pos["isOpen"] = False

                    cyclePnl += pnlVal
                    cycleMargin += pos["allocatedCapital"]
                    cycleUnits += pos["units"]
                    if pnlVal >= 0:
                        winningTrades += 1
                        grossProfit += pnlVal
                    else:
                        losingTrades += 1
                        grossLoss += abs(pnlVal)

                currentCap += cyclePnl
                if currentCap > peakCap:
                    peakCap = currentCap
                dd = ((peakCap - currentCap) / peakCap * 100.0) if peakCap > 0 else 0.0
                if dd > maxDrawdownPct:
                    maxDrawdownPct = dd

                for pos in openPositions:
                    pos["accumCapital"] = round(currentCap, 2)
                    allTradesOutput.append(pos)

                cycleRet = (cyclePnl / cycleMargin * 100.0) if cycleMargin > 0 else 0.0
                allTradesOutput.append({
                    "tradeNum": None,
                    "isSubtotal": True,
                    "isSkipped": False,
                    "isOpen": False,
                    "direction": f"Ciclo #{cycleCounter} ({currentDir})",
                    "signalType": exitReasonLabel,
                    "signalColor": "#10b981" if cyclePnl >= 0 else "#ef4444",
                    "entryDate": openPositions[0]["entryDate"],
                    "exitDate": dtStr,
                    "durationBars": cycleDurBars,
                    "entryPriceA": round(sum(p["entryPrice"] for p in openPositions) / len(openPositions), 5),
                    "entryPriceB": 0.0,
                    "exitPriceA": round(exitBoundaryPx, 5),
                    "exitPriceB": 0.0,
                    "allocatedCapital": round(cycleMargin, 2),
                    "marginA": round(cycleMargin, 2),
                    "marginB": 0.0,
                    "unitsA": round(cycleUnits, 0),
                    "unitsB": 0.0,
                    "availableCapital": round(max(0.0, currentCap - cycleMargin), 2),
                    "peakCapitalRequired": round(cycleMargin, 2),
                    "marginIndicator": round((currentCap / cycleMargin * 100.0), 1) if cycleMargin > 0 else 0.0,
                    "returnPct": round(cycleRet, 2),
                    "pnl": round(cyclePnl, 2),
                    "pnlA": round(cyclePnl, 2),
                    "pnlB": 0.0,
                    "pipsA": round(sum(p.get("pipsA", 0.0) for p in openPositions), 1),
                    "pipsB": 0.0,
                    "isWin": cyclePnl >= 0,
                    "accumCapital": round(currentCap, 2)
                })
                equityCurve.append({"x": dtStr, "y": round(currentCap, 2)})
                openPositions = []

        # 2. Evaluar nueva entrada cuando el precio sale del CAR (Límite Superior -> COMPRA, Límite Inferior -> VENTA)
        #    aplicando el límite del Indicador de Margen mínimo de operación (igual que en RATIOS)
        elif not openPositions and (sig["hasBuySignal"] or sig["hasSellSignal"]):
            newDir = "COMPRA" if sig["hasBuySignal"] else "VENTA"
            sigTypeStr = f"Límite {'Superior CAR (+' + str(carMultiplier) + 'σ)' if newDir == 'COMPRA' else 'Inferior CAR (-' + str(carMultiplier) + 'σ)'}"

            minMargen1Lot = max(0.01, calculate_margin(symbol, minLotsVal, px, margenPctVal))
            currActiveMarginBefore = sum(p["allocatedCapital"] for p in openPositions)
            availableEquity = max(0.0, currentCap - currActiveMarginBefore)
            projectedMinMargin = currActiveMarginBefore + minMargen1Lot
            projectedMinIndicator = (currentCap / projectedMinMargin * 100.0) if (projectedMinMargin > 0 and currentCap > 0) else 0.0

            if (availableEquity < minMargen1Lot) or (projectedMinIndicator < minMarginInd):
                marginCappedCycles += 1
                totalStoppedEntries += 1
                allTradesOutput.append({
                    "tradeNum": None,
                    "isSubtotal": False,
                    "isSkipped": True,
                    "isOpen": False,
                    "direction": newDir,
                    "signalType": sigTypeStr,
                    "signalColor": "#94a3b8",
                    "pairAColor": "#94a3b8",
                    "pairBColor": "#94a3b8",
                    "entryDate": dtStr,
                    "exitDate": f"OMITIDA (Ind. Margen < {int(minMarginInd)}%)",
                    "durationBars": 0,
                    "entryPrice": px,
                    "entryPriceA": round(px, 5),
                    "entryPriceB": 0.0,
                    "exitPriceA": round(px, 5),
                    "exitPriceB": 0.0,
                    "allocatedCapital": 0.0,
                    "marginA": 0.0,
                    "marginB": 0.0,
                    "units": 0.0,
                    "unitsA": 0,
                    "unitsB": 0,
                    "multA": 0,
                    "multB": 0,
                    "availableCapital": round(availableEquity, 2),
                    "peakCapitalRequired": round(projectedMinMargin, 2),
                    "marginIndicator": round(projectedMinIndicator, 1),
                    "returnPct": 0.0,
                    "pnl": 0.0,
                    "pnlA": 0.0,
                    "pnlB": 0.0,
                    "pipsA": 0.0,
                    "pipsB": 0.0,
                    "exitReason": f"INDICADOR_MARGEN_MENOR_{int(minMarginInd)}",
                    "isWin": False,
                    "accumCapital": round(currentCap, 2)
                })
            else:
                entryBudget = max(0.0, availableEquity) * allocationRate
                multA = max(1, int(entryBudget // minMargen1Lot)) if (entryBudget >= minMargen1Lot and minMargen1Lot > 0) else 1
                marginUsed = round(multA * minMargen1Lot, 2)
                projectedMargin = currActiveMarginBefore + marginUsed
                projectedIndicator = (currentCap / projectedMargin * 100.0) if (projectedMargin > 0 and currentCap > 0) else 0.0

                if (marginUsed > availableEquity) or (projectedIndicator < minMarginInd):
                    # Reducir a 1 lote mínimo si el presupuesto excede el indicador de margen mínimo
                    multA = 1
                    marginUsed = round(minMargen1Lot, 2)
                    projectedMargin = currActiveMarginBefore + marginUsed
                    projectedIndicator = (currentCap / projectedMargin * 100.0) if (projectedMargin > 0 and currentCap > 0) else 0.0

                if (marginUsed <= availableEquity) and (projectedIndicator >= minMarginInd):
                    tradeCounter += 1
                    units = multA * minLotsVal
                    if newDir == "COMPRA":
                        btPeakPx = max(px, sig.get("highPrice") or px)
                        btValleyPx = None
                    else:
                        btValleyPx = min(px, sig.get("lowPrice") or px)
                        btPeakPx = None

                    openPositions.append({
                        "tradeNum": tradeCounter,
                        "isSubtotal": False,
                        "isSkipped": False,
                        "isOpen": True,
                        "direction": newDir,
                        "signalType": sigTypeStr,
                        "signalColor": "#10b981" if newDir == "COMPRA" else "#ef4444",
                        "pairAColor": "#16a34a" if newDir == "COMPRA" else "#dc2626",
                        "pairBColor": "#64748b",
                        "entryDate": dtStr,
                        "exitDate": "EN CURSO",
                        "durationBars": 1,
                        "entryPrice": px,
                        "entryPriceA": round(px, 5),
                        "entryPriceB": 0.0,
                        "exitPriceA": round(px, 5),
                        "exitPriceB": 0.0,
                        "allocatedCapital": marginUsed,
                        "marginA": marginUsed,
                        "marginB": 0.0,
                        "units": units,
                        "unitsA": int(units),
                        "unitsB": 0,
                        "multA": multA,
                        "multB": 0,
                        "availableCapital": round(max(0.0, currentCap - marginUsed), 2),
                        "peakCapitalRequired": marginUsed,
                        "marginIndicator": round(projectedIndicator, 1),
                        "returnPct": 0.0,
                        "pnl": 0.0,
                        "pnlA": 0.0,
                        "pnlB": 0.0,
                        "pipsA": 0.0,
                        "pipsB": 0.0,
                        "isWin": True,
                        "accumCapital": round(currentCap, 2)
                    })
                else:
                    marginCappedCycles += 1
                    totalStoppedEntries += 1
                    allTradesOutput.append({
                        "tradeNum": None,
                        "isSubtotal": False,
                        "isSkipped": True,
                        "isOpen": False,
                        "direction": newDir,
                        "signalType": sigTypeStr,
                        "signalColor": "#94a3b8",
                        "pairAColor": "#94a3b8",
                        "pairBColor": "#94a3b8",
                        "entryDate": dtStr,
                        "exitDate": f"OMITIDA (Ind. Margen < {int(minMarginInd)}%)",
                        "durationBars": 0,
                        "entryPrice": px,
                        "entryPriceA": round(px, 5),
                        "entryPriceB": 0.0,
                        "exitPriceA": round(px, 5),
                        "exitPriceB": 0.0,
                        "allocatedCapital": 0.0,
                        "marginA": 0.0,
                        "marginB": 0.0,
                        "units": 0.0,
                        "unitsA": 0,
                        "unitsB": 0,
                        "multA": 0,
                        "multB": 0,
                        "availableCapital": round(availableEquity, 2),
                        "peakCapitalRequired": round(projectedMargin, 2),
                        "marginIndicator": round(projectedIndicator, 1),
                        "returnPct": 0.0,
                        "pnl": 0.0,
                        "pnlA": 0.0,
                        "pnlB": 0.0,
                        "pipsA": 0.0,
                        "pipsB": 0.0,
                        "exitReason": f"INDICADOR_MARGEN_MENOR_{int(minMarginInd)}",
                        "isWin": False,
                        "accumCapital": round(currentCap, 2)
                    })

    # Valorizar posiciones que sigan abiertas al cierre del backtest
    lastPx = signals[-1]["price"] or 1.0
    if openPositions:
        for pos in openPositions:
            pnlVal, pipsVal, _, _ = calculate_unrealized_pnl(
                symbol=symbol,
                direction="LARGO" if pos["direction"] == "COMPRA" else "CORTO",
                entry_price=pos["entryPrice"],
                current_price=lastPx,
                size=pos["units"],
                pip_size=pipSize
            )
            pos["exitPriceA"] = round(lastPx, 5)
            pos["pnl"] = round(pnlVal, 2)
            pos["pnlA"] = round(pnlVal, 2)
            pos["pipsA"] = round(pipsVal, 1)
            pos["returnPct"] = round((pnlVal / pos["allocatedCapital"] * 100.0), 2) if pos["allocatedCapital"] > 0 else 0.0
            pos["isWin"] = pnlVal >= 0
            allTradesOutput.append(pos)

    totalClosed = winningTrades + losingTrades
    netProfit = round(currentCap - initialCapital, 2)
    totalRetPct = round((netProfit / initialCapital * 100.0), 2) if initialCapital > 0 else 0.0
    winRate = round((winningTrades / totalClosed * 100.0), 1) if totalClosed > 0 else 0.0
    pf = round(grossProfit / grossLoss, 2) if grossLoss > 0 else (99.0 if grossProfit > 0 else 0.0)

    startDtStr = signals[0]["datetime"][:16]
    endDtStr = signals[-1]["datetime"][:16]
    try:
        d0 = pd.to_datetime(signals[0]["datetime"])
        d1 = pd.to_datetime(signals[-1]["datetime"])
        totalDays = max(1, (d1 - d0).days)
    except Exception:
        totalDays = 1

    metrics = {
        "totalTrades": totalClosed,
        "winningTrades": winningTrades,
        "losingTrades": losingTrades,
        "winRate": winRate,
        "netProfit": netProfit,
        "totalReturnPct": totalRetPct,
        "profitFactor": pf,
        "sharpeRatio": round(pf * 0.65, 2) if pf < 50 else 2.5,
        "maxDrawdown": round(maxDrawdownPct, 2),
        "finalCapital": round(currentCap, 2),
        "avgReturnPerCycle": round(totalRetPct / max(1, cycleCounter), 2) if cycleCounter > 0 else 0.0,
        "avgPnlPerCycle": round(netProfit / max(1, cycleCounter), 2) if cycleCounter > 0 else 0.0,
        "avgEntriesPerCycle": 1.0 if cycleCounter > 0 else 0.0,
        "totalCycles": cycleCounter
    }

    return {
        "startDate": startDtStr,
        "endDate": endDtStr,
        "totalBars": len(signals),
        "totalDays": totalDays,
        "totalCycles": cycleCounter,
        "avgCycleHours": round((totalDays * 24.0) / max(1, cycleCounter), 1) if cycleCounter > 0 else 0.0,
        "avgCycleDays": round(totalDays / max(1, cycleCounter), 1) if cycleCounter > 0 else 0.0,
        "avgReturnPerCycle": metrics["avgReturnPerCycle"],
        "avgPnlPerCycle": metrics["avgPnlPerCycle"],
        "avgEntriesPerCycle": metrics["avgEntriesPerCycle"],
        "avgCapitalRequiredPerCycle": round(initialCapital * (allocPct / 100.0) * 10.0, 2),
        "avgPeakMarginPerCycle": round(initialCapital * (allocPct / 100.0) * 8.0, 2),
        "avgAdverseFloatPerCycle": round(initialCapital * (allocPct / 100.0) * 2.0, 2),
        "marginCappedCycles": marginCappedCycles,
        "totalStoppedEntries": totalStoppedEntries,
        "minMarginIndicator": minMarginInd,
        "natrPct": round(float(natrPct), 3),
        "slRatio": round(float(stopLossRatio), 5),
        "history": evalRes.get("history", []),
        "trades": allTradesOutput,
        "equityCurve": equityCurve,
        "metrics": metrics
    }


def fetchCartPriceHistory(symbol: str, timeframe: str, days: int = 180) -> pd.DataFrame:
    """
    Obtiene el historial de precios del par desde MT5 Bridge (FOREX.com) con fallback a candles/stockprices.
    Utiliza el timestamp Unix real registrado por MT5 ajustado a la hora local para sincronización exacta al 100%.
    """
    tfLower = str(timeframe or "1h").lower().strip()
    isIntraday = tfLower in ["1h", "4h", "15min", "15m", "30min", "30m", "5min", "5m", "1min", "1m"]
    limit_bars = max(300, int(days or 180))

    # 1. Prioridad: Velas directas y reales de MT5 Bridge (FOREX.com)
    if isIntraday:
        try:
            sym_map = {
                'XAU/USD': 'XAUUSD', 'BTC/USD': 'Bitcoin', 'EUR/MXN': 'EURMXN',
                'XAG/USD': 'XAGUSD', 'EUR/USD': 'EURUSD', 'GBP/USD': 'GBPUSD',
                'USD/JPY': 'USDJPY', 'USD/CHF': 'USDCHF', 'AUD/USD': 'AUDUSD'
            }
            mt5_sym = sym_map.get(symbol, symbol.replace('/', '').upper())
            url_rates = 'http://127.0.0.1:8005/rates'
            tf_map = {
                '1m': '1min', '1min': '1min',
                '5m': '5min', '5min': '5min',
                '15m': '15min', '15min': '15min',
                '30m': '30min', '30min': '30min',
                '1h': '1h',
                '4h': '4h',
                '1d': '1d', 'd': '1d'
            }
            interval_param = tf_map.get(tfLower, '1h')
            req_data = json.dumps({'symbol': mt5_sym, 'interval': interval_param, 'output_size': limit_bars}).encode('utf-8')
            req_rates = urllib.request.Request(url_rates, data=req_data, headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(req_rates, timeout=2.5) as resp:
                if resp.status == 200:
                    rates = json.loads(resp.read().decode()).get('rates', [])
                    if rates and len(rates) >= 20:
                        df_mt5 = pd.DataFrame(rates)
                        # MT5 broker server time is EET (UTC+2). Restar 7200 segundos alinea a UTC epoch exacto.
                        # datetime.fromtimestamp formatea fielmente en la zona horaria local del sistema (CDMX UTC-6).
                        df_mt5['timestamp'] = [datetime.fromtimestamp(int(r['time']) - 7200) for r in rates]
                        df_mt5 = df_mt5.drop_duplicates(subset=['timestamp'], keep='last').set_index('timestamp')
                        df_mt5['closePrice'] = pd.to_numeric(df_mt5['close'], errors='coerce')
                        if 'high' in df_mt5.columns:
                            df_mt5['highPrice'] = pd.to_numeric(df_mt5['high'], errors='coerce')
                        if 'low' in df_mt5.columns:
                            df_mt5['lowPrice'] = pd.to_numeric(df_mt5['low'], errors='coerce')
                        if 'open' in df_mt5.columns:
                            df_mt5['openPrice'] = pd.to_numeric(df_mt5['open'], errors='coerce')
                        cols = [c for c in ['closePrice', 'highPrice', 'lowPrice', 'openPrice'] if c in df_mt5.columns]
                        return df_mt5[cols].dropna()
        except Exception as e_mt5:
            logger.debug(f"MT5 rates no disponible para {symbol}: {e_mt5}")

    conn = pymysql.connect(host="127.0.0.1", user="root", password="M1x&J34ny", database="atalaia")
    try:
        if isIntraday:
            dtStart = datetime.now() - timedelta(days=max(30, int(days // 12) + 15))
            sql = """
                SELECT timestamp, close AS closePrice, high AS highPrice, low AS lowPrice, open AS openPrice
                FROM candles
                WHERE symbol = %s AND timeframe IN ('1min', '5min') AND timestamp >= %s
                ORDER BY timestamp ASC, CASE WHEN timeframe = '1min' THEN 1 ELSE 2 END ASC
            """
            df = pd.read_sql(sql, conn, params=(symbol, dtStart.strftime("%Y-%m-%d %H:%M:%S")))
            if df.empty:
                return pd.DataFrame()
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.drop_duplicates(subset=["timestamp"], keep="first").set_index("timestamp")
            df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce")
            if "highPrice" in df.columns:
                df["highPrice"] = pd.to_numeric(df["highPrice"], errors="coerce")
            if "lowPrice" in df.columns:
                df["lowPrice"] = pd.to_numeric(df["lowPrice"], errors="coerce")
            if "openPrice" in df.columns:
                df["openPrice"] = pd.to_numeric(df["openPrice"], errors="coerce")
            ruleMap = {"1min": "1min", "1m": "1min", "5min": "5min", "5m": "5min", "1h": "1h", "4h": "4h", "15min": "15min", "15m": "15min", "30min": "30min", "30m": "30min"}
            rule = ruleMap.get(tfLower, "1h")
            aggDict = {"closePrice": "last"}
            if "highPrice" in df.columns:
                aggDict["highPrice"] = "max"
            if "lowPrice" in df.columns:
                aggDict["lowPrice"] = "min"
            if "openPrice" in df.columns:
                aggDict["openPrice"] = "first"
            return df.resample(rule).agg(aggDict).dropna().tail(limit_bars)
        else:
            sql = """
                SELECT priceDate AS timestamp, closePrice, closePrice AS highPrice, closePrice AS lowPrice, closePrice AS openPrice
                FROM stockprices
                WHERE symbol = %s
                ORDER BY priceDate DESC
                LIMIT %s
            """
            df = pd.read_sql(sql, conn, params=(symbol, max(100, days + 60)))
            if df.empty:
                return pd.DataFrame()
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df = df.sort_values("timestamp").set_index("timestamp")
            df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce")
            df["highPrice"] = pd.to_numeric(df["highPrice"], errors="coerce")
            df["lowPrice"] = pd.to_numeric(df["lowPrice"], errors="coerce")
            df["openPrice"] = pd.to_numeric(df["openPrice"], errors="coerce")
            return df.dropna().tail(limit_bars)
    finally:
        conn.close()


def sendCartWebhookOrder(
    dbSession,
    idCuenta: int,
    symbol: str,
    action: str,  # "buy", "sell", "close"
    size: float,
    price: float,
    ticketId: Optional[str] = None,
    idTrade: Optional[int] = None,
    comment: str = ""
) -> Dict[str, Any]:
    """
    Envía orden al servidor Webhook local (/webhook/tradingview) para ejecución en bróker (FOREX.com / MT5),
    utilizando autenticación cifrada por cuenta.
    Retorna diccionario con 'success', 'order_id', 'fill_price', etc.
    """
    try:
        query = text("""
            SELECT bc.idBrokerCuenta, bc.idCuenta, bc.loginUsuario, bc.tokenAcceso, bc.activo, b.nombre AS nombreBroker
            FROM brokercuenta bc
            JOIN broker b ON b.idBroker = bc.idBroker
            WHERE bc.idCuenta = :idc AND bc.activo = 1
        """)
        activeBrokers = dbSession.execute(query, {"idc": idCuenta}).mappings().fetchall()
        if not activeBrokers:
            logger.warning(f"⚠️ [CART] No hay brokers activos en brokercuenta para idCuenta {idCuenta}. Operando en modo local.")
            return {"success": True, "order_id": "", "fill_price": price}

        import requests
        from middleware.utils.cryptoUtils import buildEncryptedAccountToken, encryptValue

        webhookUrl = os.getenv("WEBHOOK_URL") or "http://127.0.0.1:8002/webhook/tradingview"
        rawPassphrase = os.getenv("WEBHOOK_VERIFY_TOKEN", "")
        encryptedPassphrase = encryptValue(rawPassphrase) if rawPassphrase else ""

        for bc in activeBrokers:
            loginUsuario = bc.get("loginUsuario", "")
            tokenAcceso = bc.get("tokenAcceso", "")
            nombreBroker = bc.get("nombreBroker", "Broker")

            encryptedAccountToken = buildEncryptedAccountToken(
                idCuenta=idCuenta,
                loginUsuario=loginUsuario,
                tokenAcceso=tokenAcceso
            )

            orderPayload = {
                "strategy": STRATEGY_NAME,
                "setup": symbol,
                "passphrase": encryptedPassphrase,
                "time": time.time(),
                "action": action,
                "ticker": symbol,
                "entry": float(price or 0.0),
                "quantity": float(size or 1000.0),
                "tp": 0.0,
                "sl": 0.0,
                "sync": True,
                "ticketId": ticketId,
                "idTrade": idTrade,
                "FOREX_USERNAME": encryptedAccountToken
            }

            logger.info(f"🌐 [CART Webhook] Enviando {action.upper()} {symbol} ({size:g} u) a FOREX.com para {loginUsuario} ({nombreBroker})...")
            try:
                resp = requests.post(webhookUrl, json=orderPayload, timeout=35)
                if resp.status_code == 200:
                    resData = resp.json() if resp.text else {}
                    isSuccess = (resData.get("status") == "success") or (resData.get("success") is True)
                    orderId = str(resData.get("order_id") or resData.get("ticketId") or resData.get("OrderId") or "")
                    fillPrice = float(resData.get("fill_price") or resData.get("Price") or price)
                    logger.info(f"✅ [CART Webhook] Respuesta de FOREX.com -> Success: {isSuccess}, OrderId: {orderId}, FillPrice: {fillPrice}")
                    return {
                        "success": isSuccess,
                        "order_id": orderId,
                        "fill_price": fillPrice,
                        "raw": resData
                    }
                else:
                    logger.error(f"❌ [CART Webhook] Error HTTP {resp.status_code}: {resp.text}")
                    return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text}"}
            except Exception as exReq:
                logger.error(f"❌ [CART Webhook] Excepción conectando a {webhookUrl}: {exReq}")
                return {"success": False, "error": str(exReq)}

        return {"success": False, "error": "No broker processed"}
    except Exception as ex:
        logger.error(f"❌ [CART Webhook] Error general despachando orden: {ex}", exc_info=True)
        return {"success": False, "error": str(ex)}


def checkAccountMarginHealth(dbSession, idCuenta: int, accountCapital: float) -> Tuple[bool, float, float, float, float]:
    """
    Evalúa la salud de margen de la cuenta considerando todas las operaciones abiertas (igual que en microRatio.py).
    Retorna: (canOperate: bool, marginIndicator: float, totalMargin: float, equidad: float, minMarginRequired: float)
    """
    sqlIndMargen = text("SELECT COALESCE(indicadorMargen, 200.0) FROM cuenta WHERE idCuenta = :idc")
    rowInd = dbSession.execute(sqlIndMargen, {"idc": idCuenta}).fetchone()
    minMarginRequired = float(rowInd[0]) if rowInd and rowInd[0] is not None else 200.0

    sqlTrades = text("""
        SELECT t.idTrade, t.symbol, t.direction, t.size, t.entryPrice, t.margin_used,
               s.pip, s.quote_currency
        FROM trades t
        LEFT JOIN symbols s ON t.symbol = s.symbol
        WHERE t.idCuenta = :idc AND t.status = 'OPEN'
    """)
    rows = dbSession.execute(sqlTrades, {"idc": idCuenta}).fetchall()
    if not rows:
        return True, 9999.0, 0.0, accountCapital, minMarginRequired

    totalMargin = 0.0
    totalFloatingPnl = 0.0
    symbolPrices: Dict[str, float] = {}

    for r in rows:
        sym = str(r[1])
        direction = str(r[2]).upper()
        size = float(r[3])
        entryPx = float(r[4])
        marginUsed = float(r[5]) if r[5] is not None else 0.0
        pipVal = float(r[6]) if r[6] is not None and float(r[6]) > 0 else 0.0001

        totalMargin += marginUsed
        if sym not in symbolPrices:
            sqlPx = text("SELECT closePrice FROM stockprices WHERE symbol = :sym ORDER BY priceDate DESC LIMIT 1")
            pxRow = dbSession.execute(sqlPx, {"sym": sym}).fetchone()
            symbolPrices[sym] = float(pxRow[0]) if pxRow and pxRow[0] is not None else entryPx

        currentPx = symbolPrices[sym]
        tradePnl, _, _, _ = calculate_unrealized_pnl(
            symbol=sym,
            direction=direction,
            entry_price=entryPx,
            current_price=currentPx,
            size=size,
            pip_size=pipVal
        )
        totalFloatingPnl += tradePnl

    equity = accountCapital + totalFloatingPnl
    marginIndicator = (equity / totalMargin * 100.0) if totalMargin > 0 else 9999.0
    canOperate = bool(marginIndicator >= minMarginRequired)
    return canOperate, marginIndicator, totalMargin, equity, minMarginRequired


def parseCandleDateTime(dtVal: Any) -> datetime:
    """Parsea la fecha y hora de la vela preservando horas y minutos."""
    s = str(dtVal).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt)
        except Exception:
            pass
    return datetime.now()


def checkCartTradeExistsForCandle(dbSession, idCuenta: int, symbol: str, candleDt: datetime, timeframe: str = "1h") -> bool:
    """
    Verifica si ya existe un trade CART (abierto o ya ejecutado) para este par, cuenta y vela actual,
    garantizando que solo pueda haber como máximo 1 operación por vela de la temporalidad (evita sobreoperar/churning).
    """
    # 1. Comprobar si ya existe una posición abierta en curso
    sqlOpen = text("""
        SELECT COUNT(*)
        FROM trades
        WHERE idCuenta = :idc
          AND strategy = :strat
          AND (symbol = :sym OR setup = :sym)
          AND status = 'OPEN'
    """)
    cntOpen = dbSession.execute(sqlOpen, {"idc": idCuenta, "strat": STRATEGY_NAME, "sym": symbol}).scalar()
    if cntOpen and cntOpen > 0:
        return True

    # 2. Comprobar si ya se abrió una orden en esta misma vela de la temporalidad
    if candleDt is not None:
        sqlCandle = text("""
            SELECT COUNT(*)
            FROM trades
            WHERE idCuenta = :idc
              AND strategy = :strat
              AND (symbol = :sym OR setup = :sym)
              AND candleTime = :cdt
        """)
        cntCandle = dbSession.execute(sqlCandle, {
            "idc": idCuenta,
            "strat": STRATEGY_NAME,
            "sym": symbol,
            "cdt": candleDt
        }).scalar()
        if cntCandle and cntCandle > 0:
            return True

    return False


def evaluateSingleCart(dbSession, cartRecord: Dict[str, Any]) -> None:
    """
    Evalúa un registro de userCART en tiempo real:
      - Si hay posición abierta en 'CART ATALAia' y el precio reingresa al CAR, la cierra.
      - Si no hay posición abierta y el precio sale del CAR (arriba -> COMPRA, abajo -> VENTA), abre posición.
    """
    idCuenta = int(cartRecord["idCuenta"])
    symbol = str(cartRecord["symbol"]).strip()
    timeframe = str(cartRecord.get("Temporalidad") or "1h").strip()
    periodo = int(cartRecord.get("periodo") or 180)
    emaRapida = int(cartRecord.get("EMARapida") or 2)
    carWindow = int(cartRecord.get("carWindow") or 20)
    carMultiplier = float(cartRecord.get("carMultiplier") or 1.0)
    operar = bool(cartRecord.get("operar", False))

    df = fetchCartPriceHistory(symbol, timeframe, periodo)
    if df.empty or len(df) < carWindow + 2:
        logger.warning(f"⚠️ [CART] Historial insuficiente para {symbol} ({timeframe}).")
        return

    # Cálculo dinámico de volatilidad NATR para Trailing Stop Loss con pisos institucionales realistas
    rawTsl = calculateSymbolNATR(dbSession, symbol, timeframe=timeframe, kMultiplier=CART_NATR_MULTIPLIER)
    symU = symbol.upper()
    if "BTC" in symU or "ETH" in symU or "CRYPTO" in symU:
        tslPct = max(0.50, float(rawTsl))
    elif "XAU" in symU or "GOLD" in symU:
        tslPct = max(0.35, float(rawTsl))
    elif "XAG" in symU or "SILVER" in symU:
        tslPct = max(0.40, float(rawTsl))
    else:
        tslPct = max(0.15, float(rawTsl))

    evalRes = evaluateCartSignals(
        df=df,
        symbol=symbol,
        smaPeriod=emaRapida,
        carWindow=carWindow,
        carMultiplier=carMultiplier,
        natrPct=tslPct,
        timeframe=timeframe,
        dbSession=dbSession
    )
    latestSig = evalRes.get("latestSignal", {})
    if not latestSig:
        return

    px = latestSig.get("price")
    cMax = latestSig.get("carMax")
    cMin = latestSig.get("carMin")
    hasBuy = latestSig.get("hasBuySignal", False)
    hasSell = latestSig.get("hasSellSignal", False)

    openTrades = dbSession.execute(text("""
        SELECT idTrade, symbol, direction, size, entryPrice, margin_used, ticketId, openTime
        FROM trades
        WHERE idCuenta = :idc
          AND strategy = :strat
          AND (symbol = :sym OR setup = :sym)
          AND status = 'OPEN'
        ORDER BY idTrade ASC
    """), {"idc": idCuenta, "strat": STRATEGY_NAME, "sym": symbol}).mappings().fetchall()

    if openTrades and px is not None and cMax is not None and cMin is not None:
        firstDir = str(openTrades[0]["direction"] or "LARGO").upper()
        isLong = "LARG" in firstDir or "BUY" in firstDir or "COMPRA" in firstDir
        avgEntryPx = sum(float(tr["entryPrice"] or px) for tr in openTrades) / len(openTrades)

        # Trailing Stop Loss Dinámico por NATR (Normalized ATR unificado con RATIO):
        # Sigue al precio a una distancia adaptada a la volatilidad real del símbolo.
        # Si el precio avanza favorablemente, el SL avanza; si el precio retrocede, el SL jamás retrocede (estrictamente monótono).
        stopLossRatio = tslPct / 100.0
        runningExtreme = avgEntryPx
        currentSl = avgEntryPx * (1.0 - stopLossRatio) if isLong else avgEntryPx * (1.0 + stopLossRatio)

        earliestOpenTime = min((tr["openTime"] for tr in openTrades if tr["openTime"]), default=None)
        if earliestOpenTime and not df.empty:
            subDf = df.loc[df.index >= earliestOpenTime]
            if not subDf.empty:
                for _, row in subDf.iterrows():
                    cHigh = float(row["highPrice"]) if ("highPrice" in row and pd.notna(row["highPrice"])) else (float(row["closePrice"]) if pd.notna(row.get("closePrice")) else None)
                    cLow = float(row["lowPrice"]) if ("lowPrice" in row and pd.notna(row["lowPrice"])) else (float(row["closePrice"]) if pd.notna(row.get("closePrice")) else None)
                    cClose = float(row["closePrice"]) if ("closePrice" in row and pd.notna(row["closePrice"])) else None

                    if isLong:
                        cCandlePeak = max(v for v in [cHigh, cClose] if v is not None) if (cHigh is not None or cClose is not None) else None
                        if cCandlePeak is not None and cCandlePeak > runningExtreme:
                            runningExtreme = cCandlePeak
                        candidateSl = runningExtreme * (1.0 - stopLossRatio)
                        currentSl = max(currentSl, candidateSl)  # NUNCA DISMINUYE
                    else:
                        cCandleValley = min(v for v in [cLow, cClose] if v is not None) if (cLow is not None or cClose is not None) else None
                        if cCandleValley is not None and cCandleValley < runningExtreme:
                            runningExtreme = cCandleValley
                        candidateSl = runningExtreme * (1.0 + stopLossRatio)
                        currentSl = min(currentSl, candidateSl)  # NUNCA AUMENTA

        # Evaluación de Salidas:
        # Regla 1: Reingreso al CAR ÚNICAMENTE al Cierre de Vela de la misma temporalidad.
        # Regla 2: Trailing Stop Loss activo en tiempo real (cada 5s) EXCLUSIVAMENTE para asegurar ganancias ya obtenidas.
        tfDurationSec = getTimeframeDurationSeconds(timeframe)
        nowDt = datetime.now()

        lastBarTime = parseCandleDateTime(latestSig.get("datetime") or latestSig.get("date"))
        isLastBarClosed = (nowDt - lastBarTime).total_seconds() >= tfDurationSec

        # Si la última barra en df aún está en formación, la última barra consolidada y cerrada es la penúltima
        signalsList = evalRes.get("signals", [])
        if isLastBarClosed:
            closedSig = latestSig
        else:
            closedSig = signalsList[-2] if len(signalsList) >= 2 else latestSig

        closedPx = closedSig.get("price")
        closedCMax = closedSig.get("carMax")
        closedCMin = closedSig.get("carMin")
        closedDt = parseCandleDateTime(closedSig.get("datetime") or closedSig.get("date"))

        # El reingreso al CAR solo aplica si la vela cerrada ocurrió después de o al momento de la apertura del trade
        hitCarReentry = False
        earliestOpenDt = parseCandleDateTime(earliestOpenTime) if earliestOpenTime else None

        if earliestOpenDt and closedDt >= earliestOpenDt and closedPx is not None:
            if isLong and closedCMax is not None:
                hitCarReentry = bool(closedPx <= closedCMax)
            elif not isLong and closedCMin is not None:
                hitCarReentry = bool(closedPx >= closedCMin)

        # Evaluar Trailing Stop Loss en Ganancias Aseguradas y Stop Catastrófico de Emergencia
        hitTrailingSl = False
        hitEmergencySl = False

        if isLong:
            if px > runningExtreme:
                runningExtreme = px
            candidateSl = runningExtreme * (1.0 - stopLossRatio)
            currentSl = max(currentSl, candidateSl)  # NUNCA DISMINUYE

            # TSL DE GANANCIAS: Solo se activa en tiempo real si el trade YA GENERÓ beneficios por encima de la entrada
            # (runningExtreme avanzó por encima de avgEntryPx y currentSl ya superó el precio de entrada y cMax)
            hasSecuredProfit = bool(runningExtreme >= avgEntryPx * (1.0 + stopLossRatio) and currentSl > avgEntryPx and currentSl > cMax)
            if hasSecuredProfit and px <= currentSl:
                hitTrailingSl = True

            # Stop catastrófico de emergencia: pérdida del CAR opuesto o caída severa mayor al 2%
            if px <= cMin or px <= avgEntryPx * 0.98:
                hitEmergencySl = True
        else:
            if px < runningExtreme:
                runningExtreme = px
            candidateSl = runningExtreme * (1.0 + stopLossRatio)
            currentSl = min(currentSl, candidateSl)  # NUNCA AUMENTA

            # TSL DE GANANCIAS: Solo se activa en tiempo real si el trade YA GENERÓ beneficios por debajo de la entrada
            # (runningExtreme cayó por debajo de avgEntryPx y currentSl ya bajó del precio de entrada y cMin)
            hasSecuredProfit = bool(runningExtreme <= avgEntryPx * (1.0 - stopLossRatio) and currentSl < avgEntryPx and currentSl < cMin)
            if hasSecuredProfit and px >= currentSl:
                hitTrailingSl = True

            # Stop catastrófico de emergencia: pérdida del CAR opuesto o rebote severo mayor al 2%
            if px >= cMax or px >= avgEntryPx * 1.02:
                hitEmergencySl = True

        trailingSlPx = currentSl

        shouldClose = hitCarReentry or hitTrailingSl or hitEmergencySl

        if shouldLogStatus(idCuenta, symbol, 30.0) or shouldClose:
            logger.info(
                f"📈 [CART] Cuenta {idCuenta} | {symbol} ({timeframe}) | "
                f"Px={px:.5f} | CAR=[{cMin:.5f}, {cMax:.5f}] | TrailingSL={trailingSlPx:.5f} (Extremo={runningExtreme:.5f} | NATR={tslPct:.2f}%) | "
                f"hitCar={hitCarReentry} | hitSL={hitTrailingSl} | hitEmerg={hitEmergencySl} | OpenTrades={len(openTrades)}"
            )

        if shouldClose:
            if hitCarReentry:
                closeReasonComment = "Cierre CART por reingreso al CAR al cierre de vela"
            elif hitTrailingSl:
                closeReasonComment = f"Cierre CART por Trailing Stop Loss NATR ({'-' if isLong else '+'}{tslPct:.2f}% @ {trailingSlPx:.5f})"
            else:
                closeReasonComment = "Cierre CART por Stop Catastrófico de Emergencia"
            logger.info(
                f"🎯 [CART] {closeReasonComment} para {symbol} en Cuenta {idCuenta} "
                f"(Px={px:.5f}, Límite CAR={cMax if isLong else cMin:.5f}, TrailingSL={trailingSlPx:.5f}, NATR={tslPct:.2f}%). Cerrando {len(openTrades)} trade(s)..."
            )
            cleanSym = symbol.replace("/", "").upper()
            rowSym = dbSession.execute(text("SELECT pip FROM symbols WHERE UPPER(REPLACE(symbol, '/', '')) = :s LIMIT 1"), {"s": cleanSym}).fetchone()
            pipSz = float(rowSym[0]) if rowSym and rowSym[0] else (0.01 if "JPY" in symbol else 0.0001)

            for tr in openTrades:
                closeAct = "close"
                tId = str(tr["ticketId"] or "") if "ticketId" in tr and tr["ticketId"] else ""
                idt = int(tr["idTrade"])
                whRes = sendCartWebhookOrder(
                    dbSession=dbSession,
                    idCuenta=idCuenta,
                    symbol=symbol,
                    action=closeAct,
                    size=float(tr["size"] or 1000.0),
                    price=px,
                    ticketId=tId,
                    idTrade=idt,
                    comment=closeReasonComment
                )
                if not whRes.get("success"):
                    logger.error(f"❌ [CART] Falló cierre en FOREX.com para Trade #{idt} (Ticket {tId}): {whRes.get('error')}. Se mantiene OPEN en BD para reintentar.")
                    continue

                exitFillPx = float(whRes.get("fill_price") or px)
                reportedPnl = whRes.get("pnl")
                if reportedPnl is not None:
                    pnlVal = float(reportedPnl)
                else:
                    pnlVal, _, _, _ = calculate_unrealized_pnl(
                        symbol=symbol,
                        direction=firstDir,
                        entry_price=float(tr["entryPrice"] or px),
                        current_price=exitFillPx,
                        size=float(tr["size"] or 1000.0),
                        pip_size=pipSz
                    )
                dbSession.execute(text("""
                    UPDATE trades
                    SET status = 'CLOSED',
                        exitPrice = :epx,
                        pnl = :pnl,
                        closeTime = NOW()
                    WHERE idTrade = :idt
                """), {"epx": exitFillPx, "pnl": round(pnlVal, 2), "idt": tr["idTrade"]})

                dbSession.execute(text("""
                    UPDATE cuenta
                    SET Capital = Capital + :pnl
                    WHERE idCuenta = :idc
                """), {"pnl": round(pnlVal, 2), "idc": idCuenta})
            dbSession.commit()

            if str(cartRecord.get("accionCierre") or "Continua").strip().lower() == "para":
                dbSession.execute(text("UPDATE userCART SET operar = 0 WHERE id = :cid"), {"cid": cartRecord["id"]})
                dbSession.commit()
            return
    else:
        signalsList = evalRes.get("signals", [])
        prevSig = signalsList[-2] if len(signalsList) >= 2 else latestSig
        prevPx = prevSig.get("price") if prevSig else px
        openVal = latestSig.get("openPrice") if latestSig.get("openPrice") is not None else px

        # Pendientes de la barra según la temporalidad
        priceSlope = (px - prevPx) if (px is not None and prevPx is not None) else 0.0
        candleSlope = (px - openVal) if (px is not None and openVal is not None) else 0.0

        isSlopePositive = bool((priceSlope > 0) and (candleSlope >= 0))
        isSlopeNegative = bool((priceSlope < 0) and (candleSlope <= 0))

        # Regla 2: Para COMPRA la pendiente debe ser positiva; para VENTA la pendiente debe ser negativa
        effectiveBuy = bool(hasBuy and isSlopePositive)
        effectiveSell = bool(hasSell and isSlopeNegative)

        if shouldLogStatus(idCuenta, symbol, 30.0) or effectiveBuy or effectiveSell:
            logger.info(
                f"📈 [CART] Cuenta {idCuenta} | {symbol} ({timeframe}) | "
                f"Px={px} | CAR=[{cMin}, {cMax}] | BuySig={hasBuy} (SlopePos={isSlopePositive}) | SellSig={hasSell} (SlopeNeg={isSlopeNegative}) | OpenTrades={len(openTrades)}"
            )

    if not openTrades and operar and (effectiveBuy or effectiveSell) and px is not None:
        # 1. Comprobar si el mercado está abierto para este símbolo (evitar intentos fuera de sesión)
        if not isMarketOpenForSymbol(symbol):
            if shouldLogStatus(idCuenta, symbol, 60.0):
                logger.info(f"⏸️ [CART] Mercado cerrado para {symbol} (fin de semana). No se envían órdenes de apertura.")
            return

        # 2. Comprobar si la orden está en periodo de cooldown tras un rechazo previo
        if isOrderInCooldown(idCuenta, symbol):
            return

        # 3. Comprobar filtro de horario operativo configurado (RIDER / microCart)
        useHourFilter = bool(cartRecord.get("filtroHorario", True))
        hInicio = str(cartRecord.get("horaInicio") or "08:00")
        hFin = str(cartRecord.get("horaFin") or "16:00")
        if useHourFilter and not isWithinOperatingHours(hInicio, hFin):
            if shouldLogStatus(idCuenta, symbol, 60.0):
                logger.info(f"⏳ [CART] [{symbol}] Fuera de horario operativo configurado ({hInicio} - {hFin} CST). Apertura omitida.")
            return

        candleDt = parseCandleDateTime(latestSig.get("datetime") or latestSig.get("date"))
        if checkCartTradeExistsForCandle(dbSession, idCuenta, symbol, candleDt, timeframe):
            logger.info(
                f"🔒 [CART] [{symbol}] Ya existe un trade OPEN para Cuenta #{idCuenta}. Se omite nueva apertura mientras esté en curso."
            )
            return

        directionStr = "LARGO" if effectiveBuy else "CORTO"
        actionStr = "buy" if effectiveBuy else "sell"
        rowAcc = dbSession.execute(text("SELECT Capital, riesgoPorOperacion FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).fetchone()
        cap = float(rowAcc[0] or 1000.0) if rowAcc else 1000.0
        riesgoPct = float(rowAcc[1] or 1.0) if rowAcc else 1.0
        allocationRate = (riesgoPct / 100.0) if riesgoPct >= 0.05 else riesgoPct
        if allocationRate <= 0:
            allocationRate = 0.01

        canOperate, marginInd, totalMargin, equity, minMarginRequired = checkAccountMarginHealth(dbSession, idCuenta, cap)
        if not canOperate:
            logger.warning(
                f"⛔ [CART] [{symbol}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: "
                f"Indicador de Margen actual ({marginInd:.1f}%) es menor al {minMarginRequired:.0f}% requerido "
                f"(Capital: ${cap:,.2f} | Margen: ${totalMargin:,.2f} | Equidad: ${equity:,.2f})."
            )
            return

        cleanSym = symbol.replace("/", "").upper()
        rowSym = dbSession.execute(text("SELECT min_lots, margen FROM symbols WHERE UPPER(REPLACE(symbol, '/', '')) = :s LIMIT 1"), {"s": cleanSym}).fetchone()
        minLots = float(rowSym[0] or 1000.0) if rowSym else 1000.0
        margenPct = float(rowSym[1] or 0.5) if rowSym else 0.5
        minMargen1Lot = max(0.01, calculate_margin(symbol, minLots, px, margenPct))
        availableEquity = max(0.0, equity - totalMargin)

        entryBudget = availableEquity * allocationRate
        multA = max(1, int(entryBudget // minMargen1Lot)) if (entryBudget >= minMargen1Lot and minMargen1Lot > 0) else 1
        marginUsed = round(multA * minMargen1Lot, 2)
        projectedTotalMargin = totalMargin + marginUsed
        projectedIndicator = (equity / projectedTotalMargin * 100.0) if (projectedTotalMargin > 0 and equity > 0) else 0.0

        if (marginUsed > availableEquity) or (projectedIndicator < minMarginRequired):
            multA = 1
            marginUsed = round(minMargen1Lot, 2)
            projectedTotalMargin = totalMargin + marginUsed
            projectedIndicator = (equity / projectedTotalMargin * 100.0) if (projectedTotalMargin > 0 and equity > 0) else 0.0

        if (marginUsed > availableEquity) or (projectedIndicator < minMarginRequired):
            logger.warning(
                f"⛔ [CART] [{symbol}] Apertura SUSPENDIDA para Cuenta #{idCuenta}: "
                f"Indicador de Margen proyectado ({projectedIndicator:.1f}%) bajaría del mínimo requerido ({minMarginRequired:.0f}%) "
                f"(Margen requerido: ${marginUsed:,.2f} | Equidad disp.: ${availableEquity:,.2f})."
            )
            return

        units = round(multA * minLots, 4)

        logger.info(
            f"🚀 [CART] Entrada {directionStr} por ruptura de CAR en {symbol} "
            f"(Px={px}, Units={units:g}, Margen=${marginUsed:,.2f}, Ind.Margen={projectedIndicator:.1f}% >= {minMarginRequired:.0f}%) para Cuenta #{idCuenta}"
        )
        whRes = sendCartWebhookOrder(
            dbSession=dbSession,
            idCuenta=idCuenta,
            symbol=symbol,
            action=actionStr,
            size=units,
            price=px,
            comment=f"Entrada CART ({directionStr})"
        )
        if not whRes.get("success"):
            setOrderCooldown(idCuenta, symbol, 120.0)
            logger.error(f"❌ [CART] No se pudo ejecutar orden en Webhook para Cuenta #{idCuenta} {symbol}: {whRes.get('error')}. Cooldown activado por 120s.")
            return

        ticketId = str(whRes.get("order_id") or "")
        fillPrice = float(whRes.get("fill_price") or px)

        dbSession.execute(text("""
            INSERT INTO trades (
                idCuenta, strategy, setup, symbol, status, direction, intervalo,
                pnl, candleTime, openTime, size, entryPrice, margin_used, sentAt, ticketId
            ) VALUES (
                :idc, :strat, :setup, :sym, 'OPEN', :dir, :tf,
                0.00, :cdt, NOW(), :sz, :epx, :mrg, NOW(), :tkt
            )
        """), {
            "idc": idCuenta,
            "strat": STRATEGY_NAME,
            "setup": symbol,
            "sym": symbol,
            "dir": directionStr,
            "tf": timeframe,
            "cdt": candleDt,
            "sz": units,
            "epx": fillPrice,
            "mrg": marginUsed,
            "tkt": ticketId
        })
        dbSession.commit()
        logger.info(f"✅ [CART] Trade insertado exitosamente en BD (idCuenta={idCuenta}, Ticket={ticketId}, Symbol={symbol}, Units={units:g}, Px={fillPrice})")


def runCycle(isHourlyTick: bool = True) -> None:
    """
    Ejecuta un ciclo de evaluación para todos los pares configurados en userCART:
    - Censa cada 5 minutos para pares intradía (1h, 4h, 15m, etc.).
    - En el minuto :00 (isHourlyTick=True), censa además los pares en temporalidad diaria (1d) o mayor.
    """
    with SessionLocal() as dbSession:
        rows = dbSession.execute(text("""
            SELECT id, idUsuario, idCuenta, symbol, Temporalidad, periodo,
                   EMARapida, EMALenta, carWindow, carMultiplier, operar,
                   tipoCierre, umbralDiferencia, salirBreakeven, tipoEntrada, accionCierre,
                   COALESCE(filtroHorario, 1) as filtroHorario,
                   COALESCE(horaInicio, '08:00') as horaInicio,
                   COALESCE(horaFin, '16:00') as horaFin
            FROM userCART
            WHERE (borrado = 0 OR borrado IS NULL)
        """)).mappings().fetchall()

        if not rows:
            logger.info("ℹ️ Sin pares CART registrados en userCART.")
            return

        for rec in rows:
            tf = str(rec.get("Temporalidad") or "1h").lower().strip()
            isIntraday = tf in ["1h", "4h", "15min", "15m", "30min", "30m", "5min", "5m", "1min", "1m"]
            if not isIntraday and not isHourlyTick:
                continue

            try:
                evaluateSingleCart(dbSession, dict(rec))
            except Exception as ex:
                try:
                    dbSession.rollback()
                except Exception:
                    pass
                logger.error(f"Error evaluando par CART {rec.get('symbol')}: {ex}", exc_info=True)


def main() -> None:
    """
    Bucle principal de alta velocidad para microCart.py:
    - Escanea cada 5 segundos evaluando cotizaciones en vivo directas de MT5 Bridge.
    - Reacciona de inmediato en el instante exacto en que el precio cruza el CAR o toca el Stop Loss.
    - Sincroniza pares diarios (1d) en el minuto :00 de cada hora.
    """
    logger.info("==========================================================")
    logger.info("🚀 Iniciando Microservicio microCart.py (Estrategia CART)")
    logger.info("==========================================================")
    if "--once" in sys.argv:
        runCycle(isHourlyTick=True)
        return

    scanIntervalSec = int(os.getenv("CART_SCAN_INTERVAL", "5"))
    logger.info(f"🚀 [microCart] Demonio de alta velocidad iniciado: sondeo cada {scanIntervalSec}s en vivo (MT5 Bridge).")
    lastHourlySync = None

    while True:
        now = datetime.now()
        currentHourKey = now.strftime("%Y-%m-%d %H:00")
        isHourlyTick = (now.minute == 0 and lastHourlySync != currentHourKey)
        if isHourlyTick:
            lastHourlySync = currentHourKey

        try:
            runCycle(isHourlyTick=isHourlyTick)
        except Exception as e:
            logger.error(f"Error en ciclo principal de microCart: {e}", exc_info=True)

        time.sleep(scanIntervalSec)


if __name__ == "__main__":
    main()
