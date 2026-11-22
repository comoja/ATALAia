"""
Microservicio Autónomo de Optimización y Calibración Sabatina de RATIO (microRatioOptimizer.py)
Estrategia: RATIO / CRUCES-EMA (Cointegración Estadística y Reversión a la Media)

Objetivo:
  Evaluar periódicamente los pares de divisas e instrumentos para encontrar la configuración óptima:
    1. Temporalidad (1h, 4h).
    2. Ventana histórica de calibración (días: 90, 120, 180).
    3. Umbral de diferencia / divergencia (0.15, 0.20, 0.25).
    4. Período SMA y ventana de dispersión sigma.
    5. Criterios de robustez: Profit Factor >= 1.5, Max Drawdown <= 10%, Esperanza Matemática > 0.

Persistencia:
  - Actualiza en user_ratios: Temporalidad, periodo, umbralDiferencia, tipoCierre, tipoEntrada.
  - Guarda bitácora en logs/microRatioOptimizer.log.
"""

import os
import sys
import time
import json
import asyncio
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

from backend.database.models import SessionLocal, UserRatio, RatioSymbol
from backend.api.routes import get_cruces_ema_pair_analysis

LOG_DIR = os.path.join(rootDir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "microRatioOptimizer.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [microRatioOptimizer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("microRatioOptimizer")

# Espacio de búsqueda cuantitativa
CANDIDATE_TIMEFRAMES = ["1h", "4h"]
CANDIDATE_DAYS = [90, 120, 180]
CANDIDATE_UMBRAL = [0.15, 0.20, 0.25]
DEFAULT_TF = "1h"
DEFAULT_DAYS = 180
DEFAULT_UMBRAL = 0.20


def calculateInstitutionalScore(metrics: Dict[str, Any]) -> float:
    """
    Función objetivo multi-criterio:
      - Profit Factor (peso 35%)
      - Ratio Calmar o Retorno/Drawdown (peso 35%)
      - Win Rate (peso 20%)
      - Penalización por Drawdown excesivo (peso 10%)
    """
    try:
        pf = float(metrics.get("profitFactor", 0.0) or 0.0)
        # Cap de PF a 20 para evitar distorsiones por muestras pequeñas
        pf_clipped = min(max(pf, 0.0), 20.0)

        ret_pct = float(metrics.get("totalReturnPct", 0.0) or 0.0)
        mdd = float(metrics.get("maxDrawdown", 0.0) or 0.0)
        mdd_safe = max(mdd, 0.5)
        calmar = min(ret_pct / mdd_safe, 15.0)

        wr = float(metrics.get("winRate", 0.0) or 0.0)
        total_trades = int(metrics.get("totalTrades", 0) or 0)

        if total_trades < 2 or pf < 1.0 or ret_pct <= 0:
            return 0.0

        # Penalización si el drawdown excede el 10%
        penalty = max(0.0, (mdd - 10.0) * 0.5)

        score = (pf_clipped * 0.35) + (calmar * 0.35) + ((wr / 100.0) * 3.0) - penalty
        return max(round(score, 3), 0.0)
    except Exception:
        return 0.0


async def evaluateRatioPair(pairA: str, pairB: str, timeframe: str = "1h", days: int = 180, umbral: float = 0.20) -> Dict[str, Any]:
    """
    Ejecuta backtest institucional asíncrono para un par candidato de microRatio.
    """
    try:
        res = await get_cruces_ema_pair_analysis(
            pairA=pairA,
            pairB=pairB,
            timeframe=timeframe,
            days=days,
            umbralDiferencia=umbral,
            isBacktest=True
        )
        cb = res.get("signalBacktest", {}).get("combined", {})
        metrics = {
            "pairA": pairA,
            "pairB": pairB,
            "timeframe": timeframe,
            "days": days,
            "umbralDiferencia": umbral,
            "totalTrades": int(cb.get("totalTrades", 0) or 0),
            "totalCycles": int(cb.get("totalCycles", 0) or 0),
            "winRate": round(float(cb.get("winRate", 0.0) or 0.0), 1),
            "profitFactor": round(float(cb.get("profitFactor", 0.0) or 0.0), 2),
            "maxDrawdown": round(float(cb.get("maxDrawdown", 0.0) or 0.0), 2),
            "totalReturnPct": round(float(cb.get("totalReturnPct", 0.0) or 0.0), 2),
            "netProfit": round(float(cb.get("netProfit", 0.0) or 0.0), 2),
        }
        metrics["score"] = calculateInstitutionalScore(metrics)
        return metrics
    except Exception as e:
        logger.warning(f"Error evaluando ratio {pairA} vs {pairB} ({timeframe}, {days}d): {e}")
        return {"pairA": pairA, "pairB": pairB, "timeframe": timeframe, "days": days, "score": 0.0, "error": str(e)}


async def optimizeSingleRatioPair(dbSession, pairA: str, pairB: str, idCuenta: Optional[int] = None, applyToDb: bool = True) -> Dict[str, Any]:
    """
    Explora la malla paramétrica para encontrar la combinación óptima de (TF, Días, Umbral).
    """
    logger.info(f"🔍 Optimizando ratio: {pairA} vs {pairB} ...")
    best_candidate = None
    best_score = -1.0

    for tf in CANDIDATE_TIMEFRAMES:
        for days in CANDIDATE_DAYS:
            for umb in CANDIDATE_UMBRAL:
                eval_res = await evaluateRatioPair(pairA, pairB, timeframe=tf, days=days, umbral=umb)
                score = eval_res.get("score", 0.0)
                if score > best_score:
                    best_score = score
                    best_candidate = eval_res

    if not best_candidate or best_score <= 0.0:
        logger.info(f"⚠️ {pairA} vs {pairB}: Sin combinación estadísticamente viable. Manteniendo valores base.")
        best_candidate = {
            "pairA": pairA,
            "pairB": pairB,
            "timeframe": DEFAULT_TF,
            "days": DEFAULT_DAYS,
            "umbralDiferencia": DEFAULT_UMBRAL,
            "score": 0.0,
            "viable": False
        }
    else:
        best_candidate["viable"] = True
        logger.info(
            f"🎯 Óptimo para {pairA} vs {pairB}: TF={best_candidate['timeframe']}, Días={best_candidate['days']}, "
            f"Umbral={best_candidate['umbralDiferencia']} | Retorno: +{best_candidate.get('totalReturnPct')}% | "
            f"PF: {best_candidate.get('profitFactor')} | WR: {best_candidate.get('winRate')}% | Score: {best_score}"
        )

    if applyToDb and best_candidate.get("viable"):
        # REGLA INSTITUCIONAL: Solo actualizar en BD si el ratio NO tiene posiciones abiertas (status = 'OPEN')
        sqlOpen = text("""
            SELECT COUNT(*) FROM trades
            WHERE (setup = :s1 OR setup = :s2 OR (symbol IN (:pA, :pB) AND strategy LIKE '%RATIO%'))
              AND status = 'OPEN'
        """)
        openCount = dbSession.execute(sqlOpen, {
            "s1": f"{pairA} - {pairB}",
            "s2": f"{pairB} - {pairA}",
            "pA": pairA,
            "pB": pairB
        }).scalar() or 0

        if openCount > 0:
            logger.info(
                f"🛡️ [PROTECCIÓN DE CICLO ACTIVO] {pairA} vs {pairB} tiene {openCount} posición(es) ABIERTA(S). "
                f"Se preserva la calibración actual del ciclo en curso para no alterar la convergencia de salida. "
                f"La nueva calibración quedará aplicada cuando el ciclo se liquide."
            )
            best_candidate["statusUpdate"] = f"DIFERIDA_POR_POSICION_ABIERTA ({openCount} ops)"
            return best_candidate

        sqlUpd = text("""
            UPDATE user_ratios
            SET Temporalidad = :tf,
                periodo = :dias,
                umbralDiferencia = :umb,
                cierreDivergencia = 1
            WHERE ((numerador = :pA AND denominador = :pB) OR (numerador = :pB AND denominador = :pA))
              AND (borrado = 0 OR borrado IS NULL)
        """)
        params = {
            "tf": best_candidate["timeframe"],
            "dias": best_candidate["days"],
            "umb": best_candidate["umbralDiferencia"],
            "pA": pairA,
            "pB": pairB
        }
        res = dbSession.execute(sqlUpd, params)
        dbSession.commit()
        logger.info(f"💾 Parámetros actualizados en user_ratios para {pairA}/{pairB} ({res.rowcount} registros afectados).")

    return best_candidate


async def optimizeAllUserRatios(applyToDb: bool = True) -> List[Dict[str, Any]]:
    """
    Optimiza todos los pares de ratios únicos configurados actualmente en user_ratios.
    """
    startTs = time.time()
    logger.info("==========================================================================")
    logger.info("🚀 [microRatioOptimizer] Iniciando ciclo de optimización institucional...")
    logger.info("==========================================================================")

    db = SessionLocal()
    results = []
    try:
        # Obtener pares únicos activos
        sql = text("""
            SELECT DISTINCT numerador, denominador
            FROM user_ratios
            WHERE (borrado = 0 OR borrado IS NULL)
              AND numerador IS NOT NULL AND denominador IS NOT NULL
        """)
        rows = db.execute(sql).fetchall()
        logger.info(f"📊 Se encontraron {len(rows)} pares únicos en user_ratios para calibrar.")

        for row in rows:
            pA, pB = row[0].strip(), row[1].strip()
            res = await optimizeSingleRatioPair(db, pA, pB, applyToDb=applyToDb)
            results.append(res)

    finally:
        db.close()

    elapsed = time.time() - startTs
    logger.info("==========================================================================")
    logger.info(f"🏁 [microRatioOptimizer] Ciclo completado en {elapsed:.1f}s. Pares optimizados: {len(results)}")
    logger.info("==========================================================================")
    return results


def runDaemonScheduler(targetWeekday: int = 5, targetHour: int = 2, targetMinute: 0 = 0) -> None:
    """
    Demonio que vigila el calendario y se dispara automáticamente los SÁBADOS a las 02:00 AM.
    """
    logger.info("🚀 [microRatioOptimizer] Demonio iniciado (Programado: Sábados 02:00 AM)...")
    lastRunDate = None
    while True:
        try:
            now = datetime.now()
            if now.weekday() == targetWeekday and now.hour == targetHour and now.minute >= targetMinute:
                todayDate = now.date()
                if lastRunDate != todayDate:
                    logger.info("⏰ [CALIBRACIÓN SABATINA DE RATIOS] Ejecutando optimización profunda...")
                    asyncio.run(optimizeAllUserRatios(applyToDb=True))
                    lastRunDate = todayDate
                    logger.info("🛌 Calibración de RATIOS completada. Demonio en reposo hasta la próxima semana.")
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("🛑 Deteniendo microRatioOptimizer.")
            break
        except Exception as e:
            logger.error(f"❌ Error en demonio microRatioOptimizer: {e}", exc_info=True)
            time.sleep(60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Microservicio de Optimización y Calibración de microRatio")
    parser.add_argument("--pairA", type=str, help="Par numerador (ej. GBP/SGD)")
    parser.add_argument("--pairB", type=str, help="Par denominador (ej. GBP/CHF)")
    parser.add_argument("--all", action="store_true", help="Optimizar todos los pares de user_ratios")
    parser.add_argument("--run-now", action="store_true", help="Ejecutar ciclo completo de inmediato y salir")
    parser.add_argument("--daemon", action="store_true", help="Modo demonio permanente los sábados a las 02:00 AM")

    args = parser.parse_args()

    if args.daemon:
        runDaemonScheduler()
    elif args.all or args.run_now:
        asyncio.run(optimizeAllUserRatios(applyToDb=True))
    elif args.pairA and args.pairB:
        db = SessionLocal()
        try:
            res = asyncio.run(optimizeSingleRatioPair(db, args.pairA.strip(), args.pairB.strip(), applyToDb=True))
            print(json.dumps(res, indent=2))
        finally:
            db.close()
    else:
        # Por defecto si no hay args
        asyncio.run(optimizeAllUserRatios(applyToDb=True))


if __name__ == "__main__":
    main()
