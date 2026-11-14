"""
Microservicio Autónomo de Optimización y Calibración Sabatina de RIDER / CART (microCartOptimizer.py)
Evalúa periódicamente los activos configurados en userCART para sugerir la ventana óptima de RIDE (EMA)
según el régimen reciente de volatilidad e inercia de cada activo.

Reglas institucionales:
1. Solo evalúa combinaciones únicas de (symbol, Temporalidad, carMultiplier) para no desperdiciar recursos.
2. Ejecución programada los sábados a las 03:00 AM (mercados cerrados y precios estáticos).
3. Prueba candidatos de ventana RIDE [12, 14, 16, 18, 20, 22, 24, 26, 28, 30].
4. Filtro de Robustez (Meseta Paramétrica + Umbral Hysteresis del 15%):
   Solo sugiere un valor distinto a 20 si supera con claridad y estabilidad el rendimiento base.
5. Actualiza userCART.emaSugerida para alimentar el semáforo y sugerencias del frontend.
"""

import os
import sys
import time
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import pandas as pd
from sqlalchemy import text

# Asegurar raíz de ATALAia en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal
from backend.services.microCart import (
    fetchCartPriceHistory,
    runCartBacktest,
    CART_NATR_MULTIPLIER,
    CART_STOP_LOSS_FALLBACK_PCT
)
from backend.services.microRatio import calculateSymbolNATR

LOG_DIR = os.path.join(rootDir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "microCartOptimizer.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [microCartOptimizer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("microCartOptimizer")

# Ventanas candidatas de RIDE a evaluar
CANDIDATE_WINDOWS = [12, 14, 16, 18, 20, 22, 24, 26, 28, 30]
DEFAULT_WINDOW = 20
IMPROVEMENT_THRESHOLD = 0.15  # 15% de mejora requerida para desplazar al valor 20


def getLookbackDays(timeframe: str) -> int:
    """Devuelve los días recomendados de histórico según la temporalidad."""
    tf = str(timeframe or "1h").lower().strip()
    if tf in ["1d", "daily"]:
        return 365
    elif tf in ["1h", "4h"]:
        return 90
    elif tf in ["15min", "30min"]:
        return 45
    else:  # 1min, 5min
        return 15


def calculateCandidateScore(btRes: Dict[str, Any]) -> float:
    """
    Calcula una puntuación balanceada del backtest:
    Combina Net Profit, Profit Factor y Win Rate con penalización por bajo número de ciclos.
    """
    if not btRes or "metrics" not in btRes:
        return 0.0

    metrics = btRes.get("metrics", {})
    netProfit = float(metrics.get("netProfit", 0.0) or 0.0)
    pf = float(metrics.get("profitFactor", 0.0) or 0.0)
    winRate = float(metrics.get("winRate", 0.0) or 0.0)
    totalCycles = int(metrics.get("totalCycles", 0) or 0)
    maxDd = float(metrics.get("maxDrawdownPct", 0.0) or 0.0)

    if totalCycles < 2 or netProfit <= 0:
        return 0.0

    # Acotar PF para evitar distorsiones por muestras con 0 pérdidas
    cappedPf = min(pf, 5.0) if pf > 0 else 0.5
    # Penalizar drawdowns excesivos (> 25%)
    ddPenalty = max(0.2, 1.0 - (maxDd / 100.0))

    # Puntuación compuesta
    score = (netProfit ** 0.6) * cappedPf * (winRate / 100.0) * ddPenalty
    return max(0.0, float(score))


def optimizeSingleCartSetup(dbSession, symbol: str, timeframe: str, carMultiplier: float) -> int:
    """
    Ejecuta el barrido de ventanas para una combinación única de (symbol, timeframe, carMultiplier).
    Devuelve la ventana sugerida más robusta.
    """
    lookbackDays = getLookbackDays(timeframe)
    logger.info(f"🔍 Evaluando histórico de {symbol} ({timeframe}) para {lookbackDays} días...")

    try:
        df = fetchCartPriceHistory(symbol=symbol, timeframe=timeframe, days=lookbackDays)
    except Exception as e:
        logger.error(f"❌ Error obteniendo histórico para {symbol} ({timeframe}): {e}")
        return DEFAULT_WINDOW

    if df is None or df.empty or len(df) < 50:
        logger.warning(f"⚠️ Datos insuficientes ({len(df) if df is not None else 0} velas) para {symbol} ({timeframe}). Se mantiene {DEFAULT_WINDOW}.")
        return DEFAULT_WINDOW

    # Obtener NATR una sola vez para el activo
    try:
        natrPct = calculateSymbolNATR(dbSession, symbol, timeframe=timeframe, kMultiplier=CART_NATR_MULTIPLIER)
    except Exception:
        natrPct = CART_STOP_LOSS_FALLBACK_PCT

    results: Dict[int, Dict[str, Any]] = {}
    scores: Dict[int, float] = {}

    for win in CANDIDATE_WINDOWS:
        try:
            btRes = runCartBacktest(
                df=df,
                symbol=symbol,
                smaPeriod=2,
                carWindow=win,
                carMultiplier=carMultiplier,
                initialCapital=1000.0,
                allocationPct=1.0,
                timeframe=timeframe,
                dbSession=dbSession,
                natrPct=natrPct
            )
            score = calculateCandidateScore(btRes)
            results[win] = btRes
            scores[win] = score
            m = btRes.get("metrics", {})
            logger.debug(
                f"   [Candidato RIDE={win}] NetProfit=${m.get('netProfit', 0):.2f} | "
                f"PF={m.get('profitFactor', 0):.2f} | WinRate={m.get('winRate', 0):.1f}% | "
                f"Ciclos={m.get('totalCycles', 0)} | Score={score:.2f}"
            )
        except Exception as e:
            logger.warning(f"Error evaluando ventana {win} para {symbol}: {e}")
            scores[win] = 0.0

    # Puntuación suavizada por meseta (promedio con vecinos adyacentes)
    plateauScores: Dict[int, float] = {}
    for idx, win in enumerate(CANDIDATE_WINDOWS):
        prevWin = CANDIDATE_WINDOWS[idx - 1] if idx > 0 else win
        nextWin = CANDIDATE_WINDOWS[idx + 1] if idx < len(CANDIDATE_WINDOWS) - 1 else win
        smoothed = (scores[prevWin] * 0.25) + (scores[win] * 0.50) + (scores[nextWin] * 0.25)
        plateauScores[win] = smoothed

    # Identificar el mejor candidato
    bestWin = max(plateauScores, key=lambda w: plateauScores[w])
    bestScore = plateauScores[bestWin]
    scoreDefault = plateauScores.get(DEFAULT_WINDOW, 0.0)

    # Regla de Hysteresis: Si la mejora respecto a 20 no supera el 15%, mantener 20
    if bestWin != DEFAULT_WINDOW:
        if scoreDefault > 0:
            improvement = (bestScore - scoreDefault) / scoreDefault
            if improvement < IMPROVEMENT_THRESHOLD:
                logger.info(
                    f"ℹ️ {symbol} ({timeframe}): Mejor candidato RIDE {bestWin} (Mejora: {improvement*100:.1f}%) "
                    f"no supera el umbral del {IMPROVEMENT_THRESHOLD*100:.0f}%. Se conserva RIDE {DEFAULT_WINDOW}."
                )
                return DEFAULT_WINDOW
        elif bestScore <= 0:
            return DEFAULT_WINDOW

    bestM = results.get(bestWin, {}).get("metrics", {})
    defM = results.get(DEFAULT_WINDOW, {}).get("metrics", {})
    logger.info(
        f"🎯 {symbol} ({timeframe}): RIDE Seleccionado = {bestWin} "
        f"(NetProfit=${bestM.get('netProfit', 0):.2f}, PF={bestM.get('profitFactor', 0):.2f}, WinRate={bestM.get('winRate', 0):.1f}%) "
        f"vs RIDE {DEFAULT_WINDOW} (NetProfit=${defM.get('netProfit', 0):.2f}, PF={defM.get('profitFactor', 0):.2f}, WinRate={defM.get('winRate', 0):.1f}%)"
    )
    return bestWin


def runOptimizationCycle() -> None:
    """
    Ejecuta un ciclo completo de optimización sobre todos los RIDERs únicos registrados en userCART.
    """
    logger.info("==========================================================================")
    logger.info("🚀 [microCartOptimizer] Iniciando Ciclo de Calibración Sabatina de RIDER")
    logger.info("==========================================================================")

    startTs = time.time()
    with SessionLocal() as dbSession:
        # Obtener RIDERs únicos
        sqlUnique = text("""
            SELECT DISTINCT symbol, Temporalidad, carMultiplier
            FROM userCART
            WHERE (borrado = 0 OR borrado IS NULL)
            ORDER BY symbol, Temporalidad
        """)
        rows = dbSession.execute(sqlUnique).fetchall()
        if not rows:
            logger.info("ℹ️ Sin configuraciones activas en userCART.")
            return

        logger.info(f"📋 Se encontraron {len(rows)} configuración(es) única(s) a evaluar.")

        updatesCount = 0
        for r in rows:
            sym = str(r[0])
            tf = str(r[1] or "1h")
            cm = float(r[2] if r[2] is not None else 1.0)

            logger.info(f"⚙️ Optimizando: {sym} | TF={tf} | Mult={cm}σ...")
            sugEma = optimizeSingleCartSetup(
                dbSession=dbSession,
                symbol=sym,
                timeframe=tf,
                carMultiplier=cm
            )

            # Actualización AUTOMÁTICA de RIDE: Aplica la ventana óptima directamente a carWindow, EMALenta y emaSugerida
            sqlUpd = text("""
                UPDATE userCART
                SET carWindow = :ema,
                    EMALenta = :ema,
                    emaSugerida = :ema
                WHERE symbol = :sym
                  AND Temporalidad = :tf
                  AND (borrado = 0 OR borrado IS NULL)
            """)
            res = dbSession.execute(sqlUpd, {"ema": sugEma, "sym": sym, "tf": tf})
            dbSession.commit()
            affected = res.rowcount
            updatesCount += affected
            logger.info(f"✅ RIDE Óptimo {sugEma} aplicado AUTOMÁTICAMENTE a {affected} registro(s) de userCART para {sym} ({tf}).")

    elapsed = time.time() - startTs
    logger.info("==========================================================================")
    logger.info(f"🏁 [microCartOptimizer] Ciclo completado en {elapsed:.1f}s. Registros actualizados: {updatesCount}")
    logger.info("==========================================================================")


def runDaemonLoop() -> None:
    """
    Demonio de segundo plano que vigila el calendario y se dispara automáticamente
    los sábados a las 03:00 AM (mercados cerrados).
    """
    logger.info("🚀 [microCartOptimizer] Demonio iniciado. En espera de la ventana sabatina (Sábado 03:00 AM)...")
    lastRunYmd = ""

    while True:
        try:
            now = datetime.now()
            todayYmd = now.strftime("%Y-%m-%d")

            # weekday() 5 == Sábado
            # Ejecutar a las 03:00 AM (ventana entre 03:00 y 03:05)
            if now.weekday() == 5 and now.hour == 3 and now.minute <= 5:
                if lastRunYmd != todayYmd:
                    logger.info(f"⏰ Horario sabatino detectado ({now.strftime('%Y-%m-%d %H:%M:%S')}). Iniciando optimización...")
                    runOptimizationCycle()
                    lastRunYmd = todayYmd
                    logger.info("🛌 Ciclo sabatino concluido. Demonio en reposo hasta la próxima semana.")

            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("🛑 [microCartOptimizer] Detenido por el usuario.")
            break
        except Exception as e:
            logger.error(f"❌ Error en demonio microCartOptimizer: {e}", exc_info=True)
            time.sleep(60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Microservicio de Optimización Sabatina para RIDER (microCartOptimizer)")
    parser.add_argument("--run-now", action="store_true", help="Ejecuta un ciclo de optimización de inmediato y sale.")
    parser.add_argument("--daemon", action="store_true", help="Ejecuta en modo demonio permanente los sábados a las 03:00 AM.")

    args = parser.parse_args()

    if args.run_now:
        runOptimizationCycle()
    elif args.daemon:
        runDaemonLoop()
    else:
        # Por defecto, si se invoca sin argumentos, ejecuta una vez inmediatamente y luego queda en modo demonio
        logger.info("🚀 Inicialización por defecto: Ejecutando primer ciclo inmediato y activando demonio sabatino.")
        runOptimizationCycle()
        runDaemonLoop()


if __name__ == "__main__":
    main()

