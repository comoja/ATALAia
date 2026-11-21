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
    getAdaptiveConfigForSymbol,
    isWithinOperatingHours
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
    Evalúa tanto ventanas continuas como esquemas institucionales de Doble Horario (Split Sessions),
    seleccionando la configuración que maximice el PnL neto y el Win Rate eliminando las horas de ruido.
    """
    valid_trades = [
        t for t in (trades or [])
        if t.get("entryDate") and t.get("pnl") is not None
    ]

    optimal_start = "08:00"
    optimal_end = "16:00"
    optimal_start2 = None
    optimal_end2 = None
    optimal_win_rate = 65.0
    optimal_pnl = 0.0
    optimal_label = "08:00 - 16:00 CST"

    if len(valid_trades) >= 8:
        best_score = -1e9
        best_window = (8, 16, None, None)
        best_wr = 50.0
        best_pnl_val = 0.0

        # Helper para extraer hora de entrada de un trade
        trade_hours = []
        for tr in valid_trades:
            try:
                ed = str(tr.get("entryDate", "")).strip()
                parts = ed.split(" ")
                if len(parts) >= 2 and ":" in parts[1]:
                    trade_hours.append((int(parts[1].split(":")[0]), tr))
            except Exception:
                continue

        def eval_candidate(s1, e1, s2=None, e2=None):
            matched = []
            for h, tr in trade_hours:
                in_w1 = (s1 <= h < e1) if s1 <= e1 else (h >= s1 or h < e1)
                in_w2 = False
                if s2 is not None and e2 is not None:
                    in_w2 = (s2 <= h < e2) if s2 <= e2 else (h >= s2 or h < e2)
                if in_w1 or in_w2:
                    matched.append(tr)

            if len(matched) >= max(4, int(len(valid_trades) * 0.18)):
                wins = sum(1 for tr in matched if tr.get("isWin") or float(tr.get("pnl", 0)) > 0)
                wr = (wins / len(matched)) * 100.0
                tot_pnl = sum(float(tr.get("pnl", 0)) for tr in matched)
                # Ponderación score: PnL positivo con premio a mayor tasa de acierto y consistencia
                score = tot_pnl * (wr / 100.0)
                return score, wr, tot_pnl, len(matched)
            return -1e9, 0.0, 0.0, 0

        # 1. Evaluar ventanas individuales (6 a 11 horas de duración)
        for win_len in [6, 7, 8, 9, 10, 11]:
            for start_h in range(0, 24):
                end_h = (start_h + win_len) % 24
                sc, wr, pnl, cnt = eval_candidate(start_h, end_h)
                if sc > best_score and wr >= 38.0:
                    best_score = sc
                    best_window = (start_h, end_h, None, None)
                    best_wr = wr
                    best_pnl_val = pnl

        # 2. Búsqueda y Evaluación Exhaustiva de Doble Horario (Sesión 1: Madrugada/Londres + Sesión 2: Tarde/Asia)
        is_metal = any(m in symbol.upper() for m in ["XAG", "XAU", "SILVER", "GOLD"])

        # Identificar las mejores sub-sesiones en madrugada/mañana y tarde/noche
        best_morn_candidates = []
        for s in [23, 0, 1, 2, 3, 4, 5, 6, 7, 8]:
            for wlen in [3, 4, 5, 6, 7]:
                e = (s + wlen) % 24
                sc, wr, pnl, cnt = eval_candidate(s, e)
                if sc > 0 and wr >= 40.0 and pnl > 0:
                    best_morn_candidates.append((sc, s, e, wr, pnl, cnt))
        best_morn_candidates.sort(key=lambda x: x[0], reverse=True)

        best_aft_candidates = []
        for s in [13, 14, 15, 16, 17, 18, 19]:
            for wlen in [3, 4, 5, 6, 7]:
                e = (s + wlen) % 24
                sc, wr, pnl, cnt = eval_candidate(s, e)
                if sc > 0 and wr >= 40.0 and pnl > 0:
                    best_aft_candidates.append((sc, s, e, wr, pnl, cnt))
        best_aft_candidates.sort(key=lambda x: x[0], reverse=True)

        # Evaluar cruces sistemáticos de las mejores sub-sesiones matutinas y vespertinas
        top_morn = best_morn_candidates[:5] if best_morn_candidates else []
        top_aft = best_aft_candidates[:5] if best_aft_candidates else []

        dual_candidates_set = set()
        for _, s1, e1, _, _, _ in top_morn:
            for _, s2, e2, _, _, _ in top_aft:
                dual_candidates_set.add((s1, e1, s2, e2))

        # Añadir candidatos institucionales fijos probados
        fixed_dual = [
            (2, 9, 14, 21),    # Londres/NY Open + CME Close/Asia Reopen
            (2, 8, 14, 20),
            (3, 8, 14, 20),
            (7, 10, 14, 20),
            (7, 10, 16, 19),   # Apertura NY + Apertura Asia
            (7, 11, 16, 20),
            (23, 10, 16, 19),  # Tokio-Londres-NY + Apertura Asia
            (23, 8, 14, 20),
        ]
        for fd in fixed_dual:
            dual_candidates_set.add(fd)

        for s1, e1, s2, e2 in dual_candidates_set:
            sc, wr, pnl, cnt = eval_candidate(s1, e1, s2, e2)
            # Ponderación a favor de Doble Horario: evita la exposición a ruido de mediodía
            dual_boost = 1.40 if is_metal else 1.20
            effective_score = sc * dual_boost if sc > 0 else sc
            if effective_score > best_score and wr >= 40.0 and pnl > 0:
                best_score = effective_score
                best_window = (s1, e1, s2, e2)
                best_wr = wr
                best_pnl_val = pnl

        # Si tras evaluar todo, la mejor ventana siguió siendo solo una individual (s2 es None),
        # pero existe una sub-sesión complementaria positiva, acoplar la segunda para garantizar el Doble Horario
        if best_window[2] is None and best_window[3] is None:
            s_single, e_single = best_window[0], best_window[1]
            # Si la ventana única es vespertina (ej. 14:00-20:00), acoplar la mejor matutina
            if s_single >= 12 and top_morn:
                m_s, m_e = top_morn[0][1], top_morn[0][2]
                sc_c, wr_c, pnl_c, cnt_c = eval_candidate(m_s, m_e, s_single, e_single)
                if pnl_c > 0 and wr_c >= 40.0:
                    best_window = (m_s, m_e, s_single, e_single)
                    best_wr = wr_c
                    best_pnl_val = pnl_c
            # Si la ventana única es matutina (ej. 02:00-09:00), acoplar la mejor vespertina
            elif s_single < 12 and top_aft:
                a_s, a_e = top_aft[0][1], top_aft[0][2]
                sc_c, wr_c, pnl_c, cnt_c = eval_candidate(s_single, e_single, a_s, a_e)
                if pnl_c > 0 and wr_c >= 40.0:
                    best_window = (s_single, e_single, a_s, a_e)
                    best_wr = wr_c
                    best_pnl_val = pnl_c

        if best_score > -1e8:
            optimal_start = f"{best_window[0]:02d}:00"
            optimal_end = f"{best_window[1]:02d}:00"
            if best_window[2] is not None and best_window[3] is not None:
                optimal_start2 = f"{best_window[2]:02d}:00"
                optimal_end2 = f"{best_window[3]:02d}:00"
                optimal_label = f"{optimal_start}-{optimal_end} y {optimal_start2}-{optimal_end2} CST ({round(best_wr, 1)}% Win Rate)"
            else:
                optimal_label = f"{optimal_start} - {optimal_end} CST ({round(best_wr, 1)}% Win Rate)"

            optimal_win_rate = round(best_wr, 1)
            optimal_pnl = round(best_pnl_val, 2)
            return {
                "optimalStartHour": optimal_start,
                "optimalEndHour": optimal_end,
                "optimalStartHour2": optimal_start2,
                "optimalEndHour2": optimal_end2,
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
    elif "XAG" in sym_upper or "SILVER" in sym_upper or "XAU" in sym_upper or "GOLD" in sym_upper:
        optimal_start = "23:00"
        optimal_end = "10:00"
        optimal_start2 = "16:00"
        optimal_end2 = "19:00"
        optimal_label = "23:00-10:00 y 16:00-19:00 CST (Doble Horario Metales)"
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
        "optimalStartHour2": optimal_start2,
        "optimalEndHour2": optimal_end2,
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

    # Recalcular métricas de bestResult exclusivamente para la ventana horaria óptima (simple o doble)
    opt_start = hoursInfo.get("optimalStartHour", "08:00")
    opt_end = hoursInfo.get("optimalEndHour", "16:00")
    opt_start2 = hoursInfo.get("optimalStartHour2")
    opt_end2 = hoursInfo.get("optimalEndHour2")
    filtered_trades = [
        t for t in bestResult.get("tradesSample", [])
        if isWithinOperatingHours(opt_start, opt_end, pd.to_datetime(t.get("entryDate")), opt_start2, opt_end2)
    ]
    if filtered_trades:
        w_tr = [t for t in filtered_trades if t.get("isWin")]
        l_tr = [t for t in filtered_trades if not t.get("isWin")]
        net_pnl = sum(float(t.get("pnl", 0.0)) for t in filtered_trades)
        w_pnl = sum(float(t.get("pnl", 0.0)) for t in w_tr)
        l_pnl = abs(sum(float(t.get("pnl", 0.0)) for t in l_tr))
        bestResult["metrics"].update({
            "totalTrades": len(filtered_trades),
            "winningTrades": len(w_tr),
            "losingTrades": len(l_tr),
            "winRate": round(len(w_tr) / len(filtered_trades) * 100.0, 1),
            "netProfit": round(net_pnl, 2),
            "profitFactor": round(w_pnl / l_pnl, 2) if l_pnl > 0 else (99.9 if w_pnl > 0 else 0.0)
        })

    # Remover tradesSample del payload final para mantener respuesta ligera
    bestResult.pop("tradesSample", None)

    horarioLog = f"{bestResult['optimalStartHour']}-{bestResult['optimalEndHour']}"
    if bestResult.get("optimalStartHour2") and bestResult.get("optimalEndHour2"):
        horarioLog += f" y {bestResult['optimalStartHour2']}-{bestResult['optimalEndHour2']}"

    logger.info(
        f"✨ [SNIPER OPT] {symbol} => Mejor TF: {bestResult['timeframe']} | "
        f"Conf: {bestResult['minConfidence']}% | R:R: {bestResult['minRr']}:1 | "
        f"BE: {bestResult['usarBreakEven']} | TSL: {bestResult['usarTSL']} | "
        f"Horario: {horarioLog} | "
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
        hInicio2 = optimal.get("optimalStartHour2")
        hFin2 = optimal.get("optimalEndHour2")

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
                horaFin = :hf,
                horaInicio2 = :hi2,
                horaFin2 = :hf2
            WHERE symbol = :sym {whereAccount}
        """
        paramsUser = {
            "tf": tfVal, "conf": confVal, "rr": rrVal, "maxrr": maxRrVal,
            "be": beVal, "tsl": tslVal, "hi": hInicio, "hf": hFin,
            "hi2": hInicio2, "hf2": hFin2, "sym": symbol
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
            "horaInicio2": hInicio2,
            "horaFin2": hFin2,
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


def runDaemonScheduler(targetWeekday: int = 5, targetHour: int = 3, targetMinute: int = 0) -> None:
    """
    Ejecución continua del microservicio: optimización automática programada
    exclusivamente los SÁBADOS EN LA MADRUGADA (por defecto 03:00 AM local).
    targetWeekday: 5 = Sábado (0=Lunes, 6=Domingo).
    """
    logger.info("🚀 Microservicio microSniperOptimizer iniciado en modo DAEMON (Sábados de madrugada).")
    logger.info(f"📅 Horario automático programado: Sábados a las {targetHour:02d}:{targetMinute:02d} AM.")
    lastRunDate = None
    while True:
        try:
            now = datetime.now()
            # Verificar si es sábado (weekday 5) y la hora coincide con la madrugada
            if now.weekday() == targetWeekday and now.hour == targetHour and now.minute >= targetMinute:
                todayDate = now.date()
                if lastRunDate != todayDate:
                    logger.info("⏰ [AUTO-CALIBRACIÓN SABATINA] Iniciando ciclo de optimización profunda semanal para todos los símbolos...")
                    optimizeAllSniperSymbols()
                    lastRunDate = todayDate
                    logger.info(f"✅ [AUTO-CALIBRACIÓN SABATINA] Calibración completada. Próxima ejecución: Sábado de la próxima semana a las {targetHour:02d}:{targetMinute:02d} AM.")
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("🛑 Deteniendo microSniperOptimizer por señal de interrupción.")
            break
        except Exception as e:
            logger.error(f"⚠️ Error no controlado en ciclo daemon sabatino: {e}", exc_info=True)
            time.sleep(60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Microservicio de Optimización Adaptativa de SNIPER")
    parser.add_argument("--symbol", type=str, help="Símbolo a optimizar bajo demanda (ej. EUR/USD, SPX500)")
    parser.add_argument("--account", type=int, help="ID de cuenta específica a actualizar")
    parser.add_argument("--all", action="store_true", help="Optimizar todos los símbolos explícitamente")
    parser.add_argument("--daemon", action="store_true", help="Modo daemon (Auto-calibración los sábados en la madrugada)")
    parser.add_argument("--interval", type=float, default=24.0, help="Intervalo en horas")

    args = parser.parse_args()

    if args.daemon:
        logger.info("🚀 Iniciando auto-calibración en modo daemon programada para los sábados en la madrugada (03:00 AM)...")
        print("INICIANDO: Auto-calibración en segundo plano programada para ejecutarse todos los sábados en la madrugada (03:00 AM).")
        runDaemonScheduler(targetWeekday=5, targetHour=3, targetMinute=0)
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
        parser.print_help()


if __name__ == "__main__":
    main()
