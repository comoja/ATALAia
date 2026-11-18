"""
Microservicio Autónomo de Optimización y Parametrización Adaptativa de SNIPER (microSniperOptimizer.py)
Estrategia: SNIPER (Smart Order Block, Momentum & Machine Learning Precision)

Objetivo:
  Automatizar la parametrización de las emisoras en la estrategia SNIPER para encontrar
  el equilibrio óptimo entre:
    1. Menos pérdida (mínimo Drawdown, alta tasa de acierto / Win Rate, reducción de rachas adversas).
    2. Más ganancia (mayor Profit Factor, PnL neto positivo, mayor Sharpe Ratio).
    3. Mejor temporalidad para cada emisora (evalúa 5min, 15min, 30min, 1h).
    4. Mejor horario operativo adaptativo (ventana horaria intradía de mayor volumen y menor spread).

Persistencia:
  - Actualiza en userSNIPER: Temporalidad, minConfidence, minRr, usarBreakEven, usarTSL, filtroHorario, horaInicio, horaFin.
  - Actualiza en symbolstrategyconfig: parametersJson con la calibración institucional y enabled = 1.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

# Asegurar raíz de ATALAia en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal
from backend.services.microSniper import (
    fetchSniperCandlesHistory,
    runSniperBacktest,
    getAdaptiveConfigForSymbol
)
from Sentinel.ml import model as mlModel
from Sentinel.analysis import technical

LOG_DIR = os.path.join(rootDir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "microSniperOptimizer.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [microSniperOptimizer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("microSniperOptimizer")

# Rangos y combinaciones candidatas para búsqueda en cuadrícula (Grid Search)
CANDIDATE_TIMEFRAMES = ["5min", "15min", "30min", "1h"]
CANDIDATE_CONFIDENCES = [55.0, 60.0, 65.0, 70.0]
CANDIDATE_RRS = [1.2, 1.5, 2.0, 2.5]
CANDIDATE_BREAK_EVEN = [True, False]
CANDIDATE_TSL = [True, False]

DEFAULT_TIMEFRAME = "15min"
DEFAULT_CONFIDENCE = 65.0
DEFAULT_RR = 1.5


def calculateSniperScore(metrics: Dict[str, Any]) -> float:
    """
    Función de aptitud multiobjetivo para SNIPER:
    Equilibra:
      - Ganancia neta (netProfit)
      - Calidad de la relación ganancia/pérdida (Profit Factor acotado)
      - Consistencia (Win Rate)
      - Control estricto de drawdown (ddPenalty penaliza drásticamente caídas > 15%)
      - Muestra representativa de trades (sampleWeight penaliza sobreajuste con pocos trades)
      - Estabilidad (Sharpe Ratio)
    """
    if not metrics:
        return 0.0

    totalTrades = int(metrics.get("totalTrades", 0) or 0)
    netProfit = float(metrics.get("netProfit", 0.0) or 0.0)
    winRate = float(metrics.get("winRate", 0.0) or 0.0)
    pf = float(metrics.get("profitFactor", 0.0) or 0.0)
    maxDd = float(metrics.get("maxDrawdown", 0.0) or 0.0)
    sharpe = float(metrics.get("sharpeRatio", 0.0) or 0.0)

    if totalTrades < 5 or netProfit <= 0 or pf < 1.05:
        return 0.0

    cappedPf = min(pf, 5.0)
    ddPenalty = max(0.1, 1.0 - (maxDd / 25.0))
    sampleWeight = min(1.5, max(0.4, np.log10(max(totalTrades, 1))))
    sharpeBonus = max(1.0, 1.0 + min(sharpe * 0.04, 0.5))

    score = (netProfit ** 0.5) * cappedPf * (winRate / 100.0) * ddPenalty * sampleWeight * sharpeBonus
    return float(max(0.0, score))


def computeOptimalSniperHours(trades: List[Dict[str, Any]], symbol: str) -> Dict[str, Any]:
    """
    Deduce estadísticamente la ventana horaria intradía más favorable para el símbolo.
    Evalúa ventanas continuas de 6 a 10 horas de duración y selecciona la que
    maximiza el Win Rate y minimiza operaciones perdedoras en periodos de ruido/baja liquidez.
    """
    valid_trades = [
        t for t in (trades or [])
        if t.get("entryDate") and t.get("pnl") is not None
    ]

    optimal_start = "08:00"
    optimal_end = "16:00"
    optimal_win_rate = 65.0
    optimal_pnl = 0.0
    optimal_label = "08:00 - 16:00 CST"

    if len(valid_trades) >= 8:
        best_score = -1e9
        best_window = (8, 16)
        best_wr = 50.0
        best_pnl_val = 0.0

        for win_len in [6, 7, 8, 9, 10]:
            for start_h in range(0, 24):
                end_h = (start_h + win_len) % 24
                in_trades = []
                for tr in valid_trades:
                    try:
                        ed = str(tr.get("entryDate", "")).strip()
                        parts = ed.split(" ")
                        if len(parts) >= 2 and ":" in parts[1]:
                            h = int(parts[1].split(":")[0])
                        else:
                            continue

                        if start_h < (start_h + win_len):
                            if start_h <= h < (start_h + win_len):
                                in_trades.append(tr)
                        else:
                            if h >= start_h or h < end_h:
                                in_trades.append(tr)
                    except Exception:
                        continue

                if len(in_trades) >= max(4, int(len(valid_trades) * 0.20)):
                    wins = sum(1 for tr in in_trades if tr.get("isWin") or float(tr.get("pnl", 0)) > 0)
                    wr = (wins / len(in_trades)) * 100.0
                    tot_pnl = sum(float(tr.get("pnl", 0)) for tr in in_trades)
                    score = tot_pnl * (wr / 100.0)
                    if score > best_score and wr >= 50.0:
                        best_score = score
                        best_window = (start_h, (start_h + win_len) % 24)
                        best_wr = wr
                        best_pnl_val = tot_pnl

        if best_score > -1e8:
            optimal_start = f"{best_window[0]:02d}:00"
            optimal_end = f"{best_window[1]:02d}:00"
            optimal_win_rate = round(best_wr, 1)
            optimal_pnl = round(best_pnl_val, 2)
            optimal_label = f"{optimal_start} - {optimal_end} CST ({optimal_win_rate}% Win Rate)"
            return {
                "optimalStartHour": optimal_start,
                "optimalEndHour": optimal_end,
                "optimalHoursLabel": optimal_label,
                "optimalWinRate": optimal_win_rate,
                "optimalPnl": optimal_pnl
            }

    # Asignaciones sectoriales institucionales cuando hay pocas operaciones
    sym_upper = symbol.upper()
    if "SPX" in sym_upper or "NAS" in sym_upper or "DJI" in sym_upper or "US" in sym_upper:
        optimal_start = "08:30"
        optimal_end = "15:30"
        optimal_label = "08:30 - 15:30 CST (Sesión regular Wall Street)"
    elif "JPY" in sym_upper or "AUD" in sym_upper or "NZD" in sym_upper:
        optimal_start = "07:00"
        optimal_end = "15:00"
        optimal_label = "07:00 - 15:00 CST (Horario óptimo con liquidez y menor spread)"
    else:
        optimal_start = "08:00"
        optimal_end = "16:00"
        optimal_label = "08:00 - 16:00 CST (Solapamiento Londres / Nueva York)"

    return {
        "optimalStartHour": optimal_start,
        "optimalEndHour": optimal_end,
        "optimalHoursLabel": optimal_label,
        "optimalWinRate": optimal_win_rate,
        "optimalPnl": optimal_pnl
    }


def optimizeSingleSniperSymbol(
    dbSession,
    symbol: str,
    idCuenta: Optional[int] = None,
    candidateTimeframes: Optional[List[str]] = None,
    applyToDb: bool = True
) -> Dict[str, Any]:
    """
    Optimiza de forma integral una emisora para la estrategia SNIPER:
      1. Evalúa el histórico en las temporalidades candidatas (5min, 15min, 30min, 1h).
      2. Barre el espacio de parámetros (Confianza, R:R, BreakEven, TSL).
      3. Selecciona la combinación con mayor puntuación de equilibrio (Score Multiobjetivo).
      4. Determina la ventana horaria óptima de operación intradía.
      5. Aplica los cambios a userSNIPER y symbolstrategyconfig si applyToDb es True.
    """
    tfs = candidateTimeframes or CANDIDATE_TIMEFRAMES
    logger.info(f"🎯 Iniciando auto-calibración SNIPER para {symbol} en temporalidades: {tfs}...")

    cleanSym = symbol.replace("/", "").upper()
    rowSym = dbSession.execute(text("SELECT pip, tipo FROM symbols WHERE UPPER(REPLACE(symbol, '/', '')) = :s LIMIT 1"), {"s": cleanSym}).fetchone()
    pipSz = float(rowSym[0]) if rowSym and rowSym[0] else (0.01 if "JPY" in symbol else 0.0001)

    adaptiveCfg = getAdaptiveConfigForSymbol(dbSession, symbol)

    # Capital de referencia para backtest
    accountCap = 1000.0
    allocPct = 1.0
    if idCuenta:
        accRow = dbSession.execute(text("SELECT Capital, riesgoPorOperacion FROM cuenta WHERE idCuenta = :idc LIMIT 1"), {"idc": idCuenta}).fetchone()
        baseRiesgo = 1.0
        if accRow:
            if accRow[0]:
                accountCap = float(accRow[0])
            if accRow[1] is not None and float(accRow[1]) > 0:
                baseRiesgo = float(accRow[1])
        rowUsr = dbSession.execute(text("SELECT multiplicadorRiesgo FROM userSNIPER WHERE idCuenta = :idc AND symbol = :sym AND (borrado = 0 OR borrado IS NULL) LIMIT 1"), {"idc": idCuenta, "sym": symbol}).fetchone()
        multR = float(rowUsr[0]) if rowUsr and rowUsr[0] is not None else 1.0
        allocPct = round(baseRiesgo * multR, 2)

    bestResult = None
    bestScore = -1.0
    allCombinationsEvaluated = 0

    # Guardar línea base con configuración por defecto (15min, conf=65, rr=1.5)
    baselineResult = None

    for tf in tfs:
        daysBack = 45 if tf in ["5min", "5m"] else (60 if tf in ["15min", "15m"] else 90)
        try:
            df = fetchSniperCandlesHistory(symbol=symbol, timeframe=tf, days=daysBack, limitBars=1500)
        except Exception as e:
            logger.warning(f"Error cargando velas para {symbol} ({tf}): {e}")
            continue

        if df is None or df.empty or len(df) < 50:
            logger.warning(f"Datos insuficientes para {symbol} en {tf} ({len(df) if df is not None else 0} velas). Saltando...")
            continue

        # Precalcular ML probas una sola vez para toda la temporalidad
        probas_tf = None
        df_op_tf = df
        try:
            df_feats = technical.calculateFeatures(df.copy())
            if df_feats is not None and not df_feats.empty:
                df_ml = mlModel.defineMlTarget(df_feats)
                X, _ = mlModel.cleanDataForModel(df_ml)
                model = mlModel.loadModel()
                if model is not None and X is not None and not X.empty:
                    feats = mlModel.MODEL_FEATURES
                    X_clean = X.dropna(subset=feats)
                    if not X_clean.empty and len(X_clean) >= 20:
                        probas_tf = model.predict_proba(X_clean[feats])[:, 1]
                        df_op_tf = df_feats.loc[X_clean.index]
        except Exception as eProbas:
            logger.debug(f"Pre-cálculo ML para {symbol} ({tf}): {eProbas}")

        for conf in CANDIDATE_CONFIDENCES:
            for rr in CANDIDATE_RRS:
                for be in CANDIDATE_BREAK_EVEN:
                    for tsl in CANDIDATE_TSL:
                        allCombinationsEvaluated += 1
                        try:
                            bt = runSniperBacktest(
                                df=df,
                                symbol=symbol,
                                minConfidence=conf,
                                minRr=rr,
                                maxRr=rr * 1.5,
                                initialCapital=accountCap,
                                allocationPct=allocPct,
                                usarBreakEven=be,
                                usarTSL=tsl,
                                pipSize=pipSz,
                                timeframe=tf,
                                adaptiveCfg=adaptiveCfg,
                                precomputedProbas=probas_tf,
                                precomputedDfOp=df_op_tf
                            )
                            if not bt or bt.get("status") != "success":
                                continue

                            m = bt.get("metrics", {})
                            score = calculateSniperScore(m)

                            # Registrar línea base
                            if tf == DEFAULT_TIMEFRAME and conf == DEFAULT_CONFIDENCE and rr == DEFAULT_RR and be is True and tsl is False:
                                baselineResult = {
                                    "timeframe": tf, "minConfidence": conf, "minRr": rr,
                                    "usarBreakEven": be, "usarTSL": tsl, "metrics": m, "score": score
                                }

                            if score > bestScore:
                                bestScore = score
                                bestResult = {
                                    "timeframe": tf,
                                    "minConfidence": conf,
                                    "minRr": rr,
                                    "maxRr": round(rr * 1.5, 2),
                                    "usarBreakEven": be,
                                    "usarTSL": tsl,
                                    "metrics": m,
                                    "score": round(score, 3),
                                    "tradesSample": bt.get("trades", [])
                                }
                        except Exception as exCombo:
                            logger.debug(f"Error evaluando combo {symbol} ({tf}, conf={conf}, rr={rr}): {exCombo}")

    # Si ninguna combinación superó el umbral mínimo de ganancia, usar la línea base
    if not bestResult:
        logger.warning(f"⚠️ No se encontró combinación rentable superior para {symbol}. Aplicando valores robustos por defecto.")
        bestResult = baselineResult or {
            "timeframe": DEFAULT_TIMEFRAME,
            "minConfidence": DEFAULT_CONFIDENCE,
            "minRr": DEFAULT_RR,
            "maxRr": 2.25,
            "usarBreakEven": True,
            "usarTSL": False,
            "metrics": {
                "totalTrades": 0, "winningTrades": 0, "losingTrades": 0,
                "winRate": 50.0, "profitFactor": 1.0, "netProfit": 0.0,
                "totalReturnPct": 0.0, "maxDrawdown": 0.0, "sharpeRatio": 0.0,
                "finalCapital": accountCap
            },
            "score": 0.0,
            "tradesSample": []
        }

    # Calcular horario operativo óptimo para la mejor configuración
    hoursInfo = computeOptimalSniperHours(bestResult.get("tradesSample", []), symbol)
    bestResult.update(hoursInfo)
    # Remover tradesSample del payload final para mantener respuesta ligera
    bestResult.pop("tradesSample", None)

    logger.info(
        f"✨ [SNIPER OPT] {symbol} => Mejor TF: {bestResult['timeframe']} | "
        f"Conf: {bestResult['minConfidence']}% | R:R: {bestResult['minRr']}:1 | "
        f"BE: {bestResult['usarBreakEven']} | TSL: {bestResult['usarTSL']} | "
        f"Horario: {bestResult['optimalStartHour']}-{bestResult['optimalEndHour']} | "
        f"WR: {bestResult['metrics']['winRate']}% | PF: {bestResult['metrics']['profitFactor']} | "
        f"Net PnL: ${bestResult['metrics']['netProfit']} USD"
    )

    if applyToDb:
        persistOptimalSniperConfig(dbSession, symbol, bestResult, idCuenta)

    return {
        "status": "success",
        "symbol": symbol,
        "combinationsEvaluated": allCombinationsEvaluated,
        "optimal": bestResult,
        "baseline": baselineResult
    }


def persistOptimalSniperConfig(dbSession, symbol: str, optimal: Dict[str, Any], idCuenta: Optional[int] = None) -> None:
    """
    Persiste la parametrización optimizada en userSNIPER y symbolstrategyconfig.
    """
    try:
        tfVal = optimal.get("timeframe", "15min")
        confVal = float(optimal.get("minConfidence", 65.0))
        rrVal = float(optimal.get("minRr", 1.5))
        maxRrVal = float(optimal.get("maxRr", 2.5))
        beVal = int(1 if optimal.get("usarBreakEven") else 0)
        tslVal = int(1 if optimal.get("usarTSL") else 0)
        hInicio = str(optimal.get("optimalStartHour", "08:00"))
        hFin = str(optimal.get("optimalEndHour", "16:00"))

        # 1. Actualizar userSNIPER para la cuenta o todas las cuentas registradas
        whereAccount = "AND idCuenta = :idc" if idCuenta else ""
        sqlUser = f"""
            UPDATE userSNIPER
            SET Temporalidad = :tf,
                minConfidence = :conf,
                minRr = :rr,
                maxRr = :maxrr,
                usarBreakEven = :be,
                usarTSL = :tsl,
                filtroHorario = 1,
                horaInicio = :hi,
                horaFin = :hf
            WHERE symbol = :sym {whereAccount}
        """
        paramsUser = {
            "tf": tfVal, "conf": confVal, "rr": rrVal, "maxrr": maxRrVal,
            "be": beVal, "tsl": tslVal, "hi": hInicio, "hf": hFin, "sym": symbol
        }
        if idCuenta:
            paramsUser["idc"] = idCuenta

        resUser = dbSession.execute(text(sqlUser), paramsUser)
        logger.info(f"💾 userSNIPER actualizado para {symbol}: {resUser.rowcount} filas.")

        # 2. Actualizar symbolstrategyconfig
        paramsJson = json.dumps({
            "timeframe": tfVal,
            "minConfidence": confVal,
            "minRr": rrVal,
            "maxRr": maxRrVal,
            "usarBreakEven": bool(beVal),
            "usarTSL": bool(tslVal),
            "horaInicio": hInicio,
            "horaFin": hFin,
            "score": optimal.get("score", 0.0),
            "winRate": optimal.get("metrics", {}).get("winRate", 0.0),
            "profitFactor": optimal.get("metrics", {}).get("profitFactor", 0.0)
        })

        sqlStrat = """
            INSERT INTO symbolstrategyconfig (strategy, symbol, enabled, parametersJson, updatedAt)
            VALUES ('Sniper', :sym, 1, :pjson, NOW())
            ON DUPLICATE KEY UPDATE
                parametersJson = :pjson,
                enabled = 1,
                updatedAt = NOW()
        """
        dbSession.execute(text(sqlStrat), {"sym": symbol, "pjson": paramsJson})
        dbSession.commit()
        logger.info(f"💾 symbolstrategyconfig actualizado exitosamente para {symbol} ('Sniper').")
    except Exception as e:
        dbSession.rollback()
        logger.error(f"❌ Error persistiendo configuración óptima para {symbol}: {e}", exc_info=True)


def optimizeAllSniperSymbols(dbSession=None) -> Dict[str, Any]:
    """
    Barrido masivo de optimización para todas las emisoras únicas registradas en userSNIPER.
    """
    closeSession = False
    if dbSession is None:
        dbSession = SessionLocal()
        closeSession = True

    try:
        logger.info("=================================================================")
        logger.info(" INICIANDO OPTIMIZACIÓN GLOBAL DE EMISORAS SNIPER")
        logger.info("=================================================================")

        symbolsRows = dbSession.execute(text("""
            SELECT DISTINCT symbol
            FROM userSNIPER
            WHERE borrado = 0
            ORDER BY symbol ASC
        """)).fetchall()

        uniqueSymbols = [r[0] for r in symbolsRows if r and r[0]]
        logger.info(f"📋 Total de emisoras a optimizar: {len(uniqueSymbols)} -> {uniqueSymbols}")

        resultsSummary = []
        for sym in uniqueSymbols:
            try:
                res = optimizeSingleSniperSymbol(dbSession, sym, applyToDb=True)
                opt = res.get("optimal", {})
                m = opt.get("metrics", {})
                resultsSummary.append({
                    "symbol": sym,
                    "timeframe": opt.get("timeframe"),
                    "confidence": opt.get("minConfidence"),
                    "rr": opt.get("minRr"),
                    "hours": f"{opt.get('optimalStartHour')}-{opt.get('optimalEndHour')}",
                    "winRate": m.get("winRate"),
                    "profitFactor": m.get("profitFactor"),
                    "netProfit": m.get("netProfit")
                })
            except Exception as exSym:
                logger.error(f"Error optimizando {sym}: {exSym}")

        logger.info(f"✅ Optimización masiva finalizada exitosamente para {len(resultsSummary)}/{len(uniqueSymbols)} pares.")
        return {
            "status": "success",
            "totalSymbols": len(uniqueSymbols),
            "optimizedCount": len(resultsSummary),
            "summary": resultsSummary
        }
    finally:
        if closeSession:
            dbSession.close()


def runDaemonScheduler(intervalHours: float = 24.0) -> None:
    """
    Ejecución continua del microservicio: ejecuta una optimización periódica
    (por defecto cada 24 horas, o programada para fines de semana).
    """
    logger.info(f"🚀 Microservicio microSniperOptimizer iniciado en modo DAEMON (intervalo: {intervalHours}h).")
    while True:
        try:
            logger.info("⏰ Ejecutando ciclo de optimización programado...")
            optimizeAllSniperSymbols()
            sleepSeconds = int(intervalHours * 3600)
            logger.info(f"💤 Ciclo finalizado. Durmiendo {intervalHours} horas hasta el próximo análisis ({sleepSeconds}s)...")
            time.sleep(sleepSeconds)
        except KeyboardInterrupt:
            logger.info("🛑 Deteniendo microSniperOptimizer por señal de interrupción.")
            break
        except Exception as e:
            logger.error(f"⚠️ Error no controlado en ciclo daemon: {e}", exc_info=True)
            time.sleep(300)


def main() -> None:
    parser = argparse.ArgumentParser(description="Microservicio de Optimización Adaptativa de SNIPER")
    parser.add_argument("--symbol", type=str, help="Símbolo a optimizar (ej. EUR/USD, SPX500)")
    parser.add_argument("--account", type=int, help="ID de cuenta específica a actualizar")
    parser.add_argument("--all", action="store_true", help="Optimizar todos los símbolos de userSNIPER")
    parser.add_argument("--daemon", action="store_true", help="Ejecutar en modo daemon continuo (cada 24h)")
    parser.add_argument("--interval", type=float, default=24.0, help="Intervalo en horas para modo daemon")

    args = parser.parse_args()

    if args.daemon:
        runDaemonScheduler(intervalHours=args.interval)
    elif args.all:
        optimizeAllSniperSymbols()
    elif args.symbol:
        db = SessionLocal()
        try:
            res = optimizeSingleSniperSymbol(db, args.symbol.strip(), idCuenta=args.account, applyToDb=True)
            print(json.dumps(res, indent=2))
        finally:
            db.close()
    else:
        # Por defecto si se invoca sin argumentos: optimiza todas las emisoras
        optimizeAllSniperSymbols()


if __name__ == "__main__":
    main()
