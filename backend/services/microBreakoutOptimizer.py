"""
Microservicio Autónomo de Optimización y Recomendación de BREAKOUT (microBreakoutOptimizer.py)
Estrategia: BREAKOUT (Ruptura de Rango Apertura NY / Londres)

Objetivo:
  Analizar el comportamiento histórico de las emisoras bajo la estrategia BREAKOUT y determinar:
    1. En qué emisoras funciona con mayor consistencia (Recomendación: RECOMENDADA, NEUTRAL, NO RECOMENDADA).
    2. La mejor sesión para cada par (NY vs. LONDRES).
    3. La mejor duración de formación de rango (15 min vs. 30 min).
    4. El horario límite óptimo después del cual ya no conviene entrar (horaFin / cutoffTime).
    5. La relación Riesgo:Beneficio óptima (minRr: 1.0, 1.2, 1.5, 2.0).

Persistencia:
  - Guarda en symbolstrategyconfig la calibración óptima con strategy = 'BreakoutNY'.
  - Si se proporciona idCuenta / idUsuario, actualiza o propone la configuración en userBREAKOUT.
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text

rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal
from backend.services.microBreakout import (
    fetchBreakoutCandlesHistory,
    calculateBreakoutRangeAndSignals,
    getAdaptiveConfigForBreakout
)

logger = logging.getLogger("microBreakoutOptimizer")

# Universo de prueba predeterminado para el optimizador
CANDIDATE_UNIVERSE = [
    "EUR/USD", "GBP/USD", "USD/JPY", "AUD/USD", "USD/CAD",
    "USD/CHF", "NZD/USD", "EUR/GBP", "EUR/JPY", "GBP/JPY",
    "XAU/USD", "BTC/USD", "NAS100", "US30", "GER40"
]


def calculateBreakoutScore(metrics: Dict[str, Any]) -> float:
    """
    Función de aptitud multiobjetivo para BREAKOUT:
    Premia:
      - Tasa de acierto (Win Rate)
      - Profit Factor
      - Ganancia neta (Net Profit en pips / dólares)
    Penaliza:
      - Drawdown elevado (> 15%)
      - Muestras muy pequeñas de trades (< 5)
    """
    trades = metrics.get("totalTrades", 0)
    if trades < 3:
        return -100.0

    wr = metrics.get("winRate", 0.0) / 100.0
    pf = min(metrics.get("profitFactor", 0.0), 10.0)
    netPnl = metrics.get("netProfit", 0.0)
    dd = metrics.get("maxDrawdownPct", 0.0)

    # Penalización por drawdown excesivo
    ddPenalty = 0.0
    if dd > 25.0:
        ddPenalty = (dd - 25.0) * 8.0
    elif dd > 15.0:
        ddPenalty = (dd - 15.0) * 3.0

    score = (wr * 40.0) + (pf * 20.0) + (min(netPnl, 500.0) * 0.1) - ddPenalty

    # Bonificación por consistencia si tiene más de 8 operaciones
    if trades >= 8:
        score += 10.0

    return round(score, 2)


def optimizeBreakoutForSymbol(
    symbol: str,
    days: int = 60,
    idCuenta: Optional[int] = None
) -> Dict[str, Any]:
    """
    Realiza una búsqueda en cuadrícula (Grid Search) sobre el par evaluando
    sesiones, duraciones de rango, horarios límites y multiplicadores R:R.
    """
    df = fetchBreakoutCandlesHistory(symbol, timeframe="5min", days=days, limit=3000)
    if df.empty or len(df) < 50:
        return {
            "symbol": symbol,
            "status": "ERROR",
            "message": f"Datos insuficientes para optimizar {symbol}",
            "optimal": None,
            "recommendation": "NO RECOMENDADA"
        }

    # Espacio de búsqueda optimizado
    candidateSessions = ["NY", "LONDRES"]
    candidateDurations = [15, 30]
    candidateRrs = [1.0, 1.2, 1.5, 2.0]
    candidateEma = [True, False]

    # Horarios límite según sesión (en horarios nativos de cada sesión)
    # NY: apertura nativa 09:30 -> límites nativos 11:30, 12:00, 13:00
    # Londres: apertura nativa 08:00 -> límites nativos 10:30, 11:30, 12:30
    cutoffMap = {
        "NY": {"horaInicio": "09:30", "cutoffs": ["11:30", "12:00", "13:00"]},
        "LONDRES": {"horaInicio": "08:00", "cutoffs": ["10:30", "11:30", "12:30"]}
    }

    bestScore = -999.0
    bestConfig: Optional[Dict[str, Any]] = None
    bestMetrics: Optional[Dict[str, Any]] = None

    for ses in candidateSessions:
        hIni = cutoffMap[ses]["horaInicio"]
        for hFin in cutoffMap[ses]["cutoffs"]:
            for dur in candidateDurations:
                for rr in candidateRrs:
                    for ema in candidateEma:
                        res = calculateBreakoutRangeAndSignals(
                            df=df,
                            symbol=symbol,
                            sesion=ses,
                            horaInicio=hIni,
                            duracionRangoMin=dur,
                            horaFin=hFin,
                            minRr=rr,
                            usarEma200=ema,
                            usarImpulseMacd=True,
                            cuerpoVelaMin=0.60
                        )
                        metrics = res.get("metrics", {})
                        score = calculateBreakoutScore(metrics)

                        if score > bestScore:
                            bestScore = score
                            bestMetrics = metrics
                            bestConfig = {
                                "sesion": ses,
                                "horaInicio": hIni,
                                "duracionRangoMin": dur,
                                "horaFin": hFin,
                                "minRr": rr,
                                "usarEma200": ema,
                                "usarImpulseMacd": True,
                                "cuerpoVelaMin": 0.60,
                                "score": score
                            }

    if not bestConfig or not bestMetrics:
        bestConfig = {
            "sesion": "NY",
            "horaInicio": "08:30",
            "duracionRangoMin": 15,
            "horaFin": "11:00",
            "minRr": 1.0,
            "usarEma200": True,
            "usarImpulseMacd": True,
            "cuerpoVelaMin": 0.60,
            "score": 0.0
        }
        bestMetrics = {
            "winRate": 0.0,
            "profitFactor": 0.0,
            "totalTrades": 0,
            "netProfit": 0.0,
            "maxDrawdownPct": 0.0
        }

    # Determinar recomendación institucional
    wr = bestMetrics.get("winRate", 0.0)
    pf = bestMetrics.get("profitFactor", 0.0)
    tt = bestMetrics.get("totalTrades", 0)
    netPnl = bestMetrics.get("netProfit", 0.0)

    if wr >= 58.0 and pf >= 1.25 and tt >= 5 and netPnl > 0:
        recommendation = "RECOMENDADA"
        recBadge = "badge-success"
    elif wr >= 48.0 and pf >= 1.05 and netPnl >= 0:
        recommendation = "NEUTRAL"
        recBadge = "badge-warning"
    else:
        recommendation = "NO RECOMENDADA"
        recBadge = "badge-danger"

    # Persistir en symbolstrategyconfig
    db = SessionLocal()
    try:
        cleanSym = symbol.replace("/", "").strip().upper()
        twMin = 150
        try:
            hI, mI = map(int, bestConfig["horaInicio"].split(":"))
            hF, mF = map(int, bestConfig["horaFin"].split(":"))
            dur = bestConfig["duracionRangoMin"]
            twMin = max(60, (hF * 60 + mF) - (hI * 60 + mI + dur))
        except Exception:
            pass

        paramJson = json.dumps({
            "sesion": bestConfig["sesion"],
            "start_hour": int(bestConfig["horaInicio"].split(":")[0]),
            "start_minute": int(bestConfig["horaInicio"].split(":")[1]),
            "range_duration": bestConfig["duracionRangoMin"],
            "trading_window": twMin,
            "min_rr": bestConfig["minRr"],
            "use_ema200": 1 if bestConfig["usarEma200"] else 0,
            "useImpulseMacdFilter": 1 if bestConfig["usarImpulseMacd"] else 0,
            "cutoff_hour": bestConfig["horaFin"]
        })

        sqlUpd = text("""
            INSERT INTO symbolstrategyconfig (symbol, strategy, parametersJson, enabled, updatedAt)
            VALUES (:sym, 'BreakoutNY', :pj, 1, NOW())
            ON DUPLICATE KEY UPDATE parametersJson = :pj, enabled = 1, updatedAt = NOW()
        """)
        db.execute(sqlUpd, {"sym": symbol, "pj": paramJson})

        # Si se especificó idCuenta, actualizar en userBREAKOUT
        if idCuenta:
            sqlUser = text("""
                UPDATE userBREAKOUT
                SET sesion = :ses,
                    horaInicio = :hini,
                    duracionRangoMin = :dur,
                    horaFin = :hfin,
                    minRr = :rr,
                    usarEma200 = :ema,
                    estadoRecomendacion = :rec,
                    fechaRecomendacion = NOW()
                WHERE idCuenta = :idc AND (symbol = :sym OR REPLACE(symbol, '/', '') = :clean)
            """)
            db.execute(sqlUser, {
                "ses": bestConfig["sesion"],
                "hini": bestConfig["horaInicio"],
                "dur": bestConfig["duracionRangoMin"],
                "hfin": bestConfig["horaFin"],
                "rr": bestConfig["minRr"],
                "ema": 1 if bestConfig["usarEma200"] else 0,
                "rec": "REVISADA" if recommendation == "RECOMENDADA" else "MANUAL",
                "idc": idCuenta,
                "sym": symbol,
                "clean": cleanSym
            })

        db.commit()
    except Exception as e:
        logger.error(f"Error guardando calibración óptima de {symbol}: {e}")
        db.rollback()
    finally:
        db.close()

    return {
        "symbol": symbol,
        "status": "OK",
        "recommendation": recommendation,
        "recBadge": recBadge,
        "optimal": bestConfig,
        "metrics": bestMetrics
    }


def optimizeAllBreakout(days: int = 60, idCuenta: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Optimiza todo el universo de emisoras y devuelve una lista clasificada
    por recomendación y puntaje de rendimiento.
    """
    db = SessionLocal()
    symbolsToTest = list(CANDIDATE_UNIVERSE)
    try:
        # Obtener símbolos activos de la base de datos
        sql = text("SELECT DISTINCT symbol FROM symbols WHERE active = 1 LIMIT 30")
        rows = db.execute(sql).fetchall()
        for r in rows:
            sym = r[0]
            if sym and sym not in symbolsToTest and len(sym) <= 10:
                symbolsToTest.append(sym)
    except Exception as e:
        logger.warn(f"Error obteniendo símbolos para optimizar: {e}")
    finally:
        db.close()

    results: List[Dict[str, Any]] = []
    for sym in symbolsToTest:
        try:
            res = optimizeBreakoutForSymbol(sym, days=days, idCuenta=idCuenta)
            if res.get("status") == "OK" and res.get("optimal"):
                results.append(res)
        except Exception as e:
            logger.error(f"Error optimizando {sym}: {e}")

    # Ordenar por recomendación (RECOMENDADA primero, luego NEUTRAL) y por Win Rate
    orderRank = {"RECOMENDADA": 3, "NEUTRAL": 2, "NO RECOMENDADA": 1}
    results.sort(
        key=lambda x: (
            orderRank.get(x.get("recommendation", "NO RECOMENDADA"), 0),
            x.get("metrics", {}).get("profitFactor", 0.0),
            x.get("metrics", {}).get("winRate", 0.0)
        ),
        reverse=True
    )

    return results
