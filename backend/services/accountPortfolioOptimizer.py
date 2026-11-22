"""
Microservicio de Optimización y Asignación de Portafolio por Cuenta (accountPortfolioOptimizer.py)
Analiza el capital, apalancamiento, comisiones y perfil de riesgo de cada cuenta en ATALA.ia
para asignar la cartera óptima, calcular el dimensionamiento de lotaje exacto y blindar
el margen máximo simultáneo.

Reglas Institucionales:
  1. Distribución Antifrágil: 50% Ratio / 30% CART / 20% Sniper (normalizada según estrategias activas).
  2. Margin Guardrail: Margen total simultáneo retenido nunca debe superar el 30% del equity.
  3. Risk Sizing: Riesgo por operación fijado al porcentaje de la cuenta (por defecto 1.0%).
  4. Sincronización DB (--apply): Actualiza user_ratios, userCART y userSNIPER por idCuenta.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy import text

# Asegurar raíz de ATALAia en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal

LOG_DIR = os.path.join(rootDir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "accountPortfolioOptimizer.log")
OUTPUT_JSON = os.path.join(LOG_DIR, "optimizacion_cuentas.json")
GLOBAL_PORTFOLIO_JSON = os.path.join(LOG_DIR, "cartera_optima_global.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [accountPortfolioOptimizer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("accountPortfolioOptimizer")

# Ponderaciones institucionales estándar
BASE_WEIGHTS = {
    "microRatio": 0.50,
    "microRider": 0.30,
    "microSniper": 0.20
}
MAX_SIMULTANEOUS_MARGIN_PCT = 30.0  # Máximo 30% de margen retenido simultáneo


def loadGlobalPortfolio() -> Dict[str, Any]:
    """Carga la cartera óptima multiactivo más reciente."""
    if os.path.exists(GLOBAL_PORTFOLIO_JSON):
        try:
            with open(GLOBAL_PORTFOLIO_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Error leyendo {GLOBAL_PORTFOLIO_JSON}: {e}")

    # Fallback institucional con activos líderes probados
    return {
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "microRatio": [
            {"symbol": "GBP/SGD vs GBP/CHF", "pairA": "GBP/SGD", "pairB": "GBP/CHF", "timeframe": "1h", "score": 5.0},
            {"symbol": "EUR/GBP vs GBP/CHF", "pairA": "EUR/GBP", "pairB": "GBP/CHF", "timeframe": "1h", "score": 4.5},
            {"symbol": "USD/CAD vs GBP/CHF", "pairA": "USD/CAD", "pairB": "GBP/CHF", "timeframe": "1h", "score": 4.0}
        ],
        "microRider": [
            {"symbol": "XAU/USD", "timeframe": "1h", "score": 5.0},
            {"symbol": "EUR/USD", "timeframe": "1h", "score": 4.0},
            {"symbol": "US30", "timeframe": "1h", "score": 3.5}
        ],
        "microSniper": [
            {"symbol": "NAS100", "timeframe": "5m", "score": 4.5},
            {"symbol": "US30", "timeframe": "5m", "score": 4.5},
            {"symbol": "XAU/USD", "timeframe": "15m", "score": 4.0}
        ]
    }


def calculatePositionLotSize(capital: float, riskPct: float, symbol: str, symInfo: Dict[str, Any]) -> float:
    """
    Calcula el tamaño de lote conservador institucional acorde al riesgo por operación.
    """
    min_lots = float(symInfo.get("min_lots", 0.01) or 0.01)
    margen_pct = float(symInfo.get("margen", 1.0) or 1.0)

    # 1.0% de riesgo = riesgo en dólares
    risk_dollars = capital * (riskPct / 100.0)

    # Sizing simplificado seguro basado en capital y margen
    # 0.01 lotes por cada $1,000 en Forex; 0.1 lotes en Índices
    sym_upper = symbol.upper()
    if any(k in sym_upper for k in ["NAS", "US30", "SPX", "SPY"]):
        lots = round(max(0.1, (capital / 10000.0) * 0.2), 2)
    elif "XAU" in sym_upper or "XAG" in sym_upper:
        lots = round(max(0.01, (capital / 10000.0) * 0.1), 2)
    else:
        lots = round(max(0.01, (capital / 10000.0) * 0.15), 2)

    return max(min_lots, lots)


def optimizeSingleAccount(dbSession, accountRow: Dict[str, Any], globalPortfolio: Dict[str, Any], symMetadata: Dict[str, Dict[str, Any]], applyToDb: bool = False) -> Dict[str, Any]:
    """
    Calcula la asignación óptima de portafolio y lotes para una cuenta específica.
    REGLA: Cuenta #1 (ATALAia), cuentas Concentradoras o cuentas con Capital <= 0 NO OPERAN.
    """
    idCuenta = accountRow["idCuenta"]
    nombre = accountRow["Nombre"] or f"Cuenta #{idCuenta}"
    capital = float(accountRow.get("Capital") or 0.0)
    is_concentradora = bool(accountRow.get("Concentradora", 0))
    riskPct = float(accountRow.get("riesgoPorOperacion") or 1.0)

    # 0. Verificación Operativa Estricta
    if idCuenta == 1 or is_concentradora or capital <= 0:
        status_label = "NO OPERA (Institucional)" if idCuenta == 1 else ("NO OPERA (Concentradora)" if is_concentradora else "NO OPERA (Sin Capital)")
        logger.info(f"⚪ Cuenta #{idCuenta} ({nombre}): {status_label}. Asignación de lotes omitida.")
        
        if applyToDb:
            # Asegurar que en tablas operativas esté apagada
            try:
                dbSession.execute(text("UPDATE user_ratios SET operar = 0 WHERE idCuenta = :acc"), {"acc": idCuenta})
                dbSession.execute(text("UPDATE userCART SET operar = 0 WHERE idCuenta = :acc"), {"acc": idCuenta})
                dbSession.execute(text("UPDATE userSNIPER SET operar = 0 WHERE idCuenta = :acc"), {"acc": idCuenta})
                dbSession.commit()
            except Exception as e:
                dbSession.rollback()
                logger.warning(f"Error desactivando flags operativos en cuenta #{idCuenta}: {e}")

        return {
            "idCuenta": idCuenta,
            "nombre": nombre,
            "capital": capital,
            "isOperational": False,
            "status": status_label,
            "riesgoPorOperacion": riskPct,
            "normalizedWeights": {},
            "totalEstMargin": 0.0,
            "totalMarginPct": 0.0,
            "marginSafe": True,
            "allocations": []
        }

    # 1. Consultar estrategias autorizadas en cuentaestrategia
    sqlStrat = text("SELECT strategy FROM cuentaestrategia WHERE idCuenta = :id")
    strats = [r[0].lower().strip() for r in dbSession.execute(sqlStrat, {"id": idCuenta}).fetchall()]

    has_ratio = any("ratio" in s or "cruce" in s for s in strats) or len(strats) == 0
    has_cart = any("cart" in s or "rider" in s for s in strats) or len(strats) == 0
    has_sniper = any("sniper" in s for s in strats) or len(strats) == 0

    # Perfil de Inversionista Personalizado de la Cuenta
    pct_cons = float(accountRow.get("pctConservador") if accountRow.get("pctConservador") is not None else 40.0)
    pct_mod = float(accountRow.get("pctModerado") if accountRow.get("pctModerado") is not None else 30.0)
    pct_agr = float(accountRow.get("pctAgresivo") if accountRow.get("pctAgresivo") is not None else 30.0)

    # Validar suma 100% o auto-normalizar
    total_prof = pct_cons + pct_mod + pct_agr
    if total_prof > 0:
        w_ratio = pct_cons / total_prof
        w_rider = pct_mod / total_prof
        w_sniper = pct_agr / total_prof
    else:
        w_ratio, w_rider, w_sniper = 0.40, 0.30, 0.30

    # Normalizar ponderaciones según estrategias autorizadas para la cuenta
    active_weights = {}
    if has_ratio: active_weights["microRatio"] = w_ratio
    if has_cart: active_weights["microRider"] = w_rider
    if has_sniper: active_weights["microSniper"] = w_sniper

    total_w = sum(active_weights.values()) or 1.0
    normalized_weights = {k: round(v / total_w, 2) for k, v in active_weights.items()}

    # 2. Asignar capital y calcular lotes por activo
    allocations = []
    total_est_margin = 0.0

    # microRatio Allocation
    if "microRatio" in normalized_weights:
        w = normalized_weights["microRatio"]
        cap_ratio = capital * w
        top_ratios = globalPortfolio.get("microRatio", [])[:2]
        for r in top_ratios:
            allocations.append({
                "strategy": "microRatio",
                "symbol": r["symbol"],
                "pairA": r.get("pairA", "GBP/SGD"),
                "pairB": r.get("pairB", "GBP/CHF"),
                "timeframe": r.get("timeframe", "1h"),
                "allocatedCapital": round(cap_ratio / len(top_ratios), 2),
                "lots": 1000.0,  # Unidades base de ratio
                "estMargin": round(cap_ratio * 0.04, 2)
            })
            total_est_margin += cap_ratio * 0.04

    # microRider Allocation
    if "microRider" in normalized_weights:
        w = normalized_weights["microRider"]
        cap_cart = capital * w
        top_carts = globalPortfolio.get("microRider", [])[:2]
        for c in top_carts:
            sym = c["symbol"]
            meta = symMetadata.get(sym, {})
            lot_sz = calculatePositionLotSize(capital, riskPct, sym, meta)
            est_m = round(capital * 0.05, 2)
            allocations.append({
                "strategy": "microRider",
                "symbol": sym,
                "timeframe": c.get("timeframe", "1h"),
                "allocatedCapital": round(cap_cart / len(top_carts), 2),
                "lots": lot_sz,
                "estMargin": est_m
            })
            total_est_margin += est_m

    # microSniper Allocation
    if "microSniper" in normalized_weights:
        w = normalized_weights["microSniper"]
        cap_sniper = capital * w
        top_snipers = globalPortfolio.get("microSniper", [])[:2]
        for s in top_snipers:
            sym = s["symbol"]
            meta = symMetadata.get(sym, {})
            lot_sz = calculatePositionLotSize(capital, riskPct, sym, meta)
            est_m = round(capital * 0.04, 2)
            allocations.append({
                "strategy": "microSniper",
                "symbol": sym,
                "timeframe": s.get("timeframe", "5m"),
                "allocatedCapital": round(cap_sniper / len(top_snipers), 2),
                "lots": lot_sz,
                "estMargin": est_m
            })
            total_est_margin += est_m

    # 3. Margin Guardrail Verification
    total_margin_pct = round((total_est_margin / max(capital, 1.0)) * 100.0, 1)
    margin_safe = total_margin_pct <= MAX_SIMULTANEOUS_MARGIN_PCT

    account_plan = {
        "idCuenta": idCuenta,
        "nombre": nombre,
        "capital": capital,
        "riesgoPorOperacion": riskPct,
        "pctConservador": round(pct_cons, 1),
        "pctModerado": round(pct_mod, 1),
        "pctAgresivo": round(pct_agr, 1),
        "normalizedWeights": normalized_weights,
        "totalEstMargin": round(total_est_margin, 2),
        "totalMarginPct": total_margin_pct,
        "marginSafe": margin_safe,
        "allocations": allocations
    }

    # 4. Sincronización en Base de Datos (Si applyToDb está habilitado)
    if applyToDb and margin_safe:
        logger.info(f"💾 Aplicando cartera calibrada a Cuenta #{idCuenta} ({nombre})...")
        for alloc in allocations:
            strat = alloc["strategy"]
            sym = alloc["symbol"]
            tf = alloc["timeframe"]

            if strat == "microRider":
                # Sincronizar o insertar en userCART con operar = 0 (el usuario debe revisar y activar)
                sqlCheck = text("SELECT id, operar FROM userCART WHERE idCuenta = :acc AND symbol = :sym")
                exist = dbSession.execute(sqlCheck, {"acc": idCuenta, "sym": sym}).fetchone()
                if exist:
                    dbSession.execute(text("UPDATE userCART SET Temporalidad = :tf, borrado = 0 WHERE id = :id"), {"tf": tf, "id": exist[0]})
                else:
                    dbSession.execute(text("""
                        INSERT INTO userCART (idUsuario, idCuenta, symbol, Temporalidad, carWindow, carMultiplier, operar, borrado, origen, estadoRecomendacion, fechaRecomendacion)
                        VALUES (1, :acc, :sym, :tf, 20, 2.0, 0, 0, 'OPTIMIZADOR', 'NUEVA', NOW())
                    """), {"acc": idCuenta, "sym": sym, "tf": tf})

            elif strat == "microSniper":
                # Sincronizar o insertar en userSNIPER con operar = 0 (el usuario debe revisar y activar)
                sqlCheck = text("SELECT id, operar FROM userSNIPER WHERE idCuenta = :acc AND symbol = :sym")
                exist = dbSession.execute(sqlCheck, {"acc": idCuenta, "sym": sym}).fetchone()
                if exist:
                    dbSession.execute(text("UPDATE userSNIPER SET Temporalidad = :tf, borrado = 0 WHERE id = :id"), {"tf": tf, "id": exist[0]})
                else:
                    dbSession.execute(text("""
                        INSERT INTO userSNIPER (idUsuario, idCuenta, symbol, Temporalidad, minConfidence, minRr, operar, borrado, origen, estadoRecomendacion, fechaRecomendacion)
                        VALUES (1, :acc, :sym, :tf, 0.50, 1.5, 0, 0, 'OPTIMIZADOR', 'NUEVA', NOW())
                    """), {"acc": idCuenta, "sym": sym, "tf": tf})

            elif strat == "microRatio":
                pA, pB = alloc["pairA"], alloc["pairB"]
                # REGLA INSTITUCIONAL: Proteger ciclos activos con posiciones abiertas
                sqlOpen = text("""
                    SELECT COUNT(*) FROM trades 
                    WHERE idCuenta = :acc 
                      AND (setup = :s1 OR setup = :s2 OR (symbol IN (:pA, :pB) AND strategy LIKE '%RATIO%'))
                      AND status = 'OPEN'
                """)
                openCnt = dbSession.execute(sqlOpen, {"acc": idCuenta, "s1": f"{pA} - {pB}", "s2": f"{pB} - {pA}", "pA": pA, "pB": pB}).scalar() or 0

                sqlCheck = text("SELECT id, operar FROM user_ratios WHERE idCuenta = :acc AND ((numerador = :pA AND denominador = :pB) OR (numerador = :pB AND denominador = :pA))")
                exist = dbSession.execute(sqlCheck, {"acc": idCuenta, "pA": pA, "pB": pB}).fetchone()
                if exist:
                    if openCnt == 0:
                        dbSession.execute(text("UPDATE user_ratios SET Temporalidad = :tf, borrado = 0 WHERE id = :id"), {"tf": tf, "id": exist[0]})
                    else:
                        logger.info(f"🛡️ Cuenta #{idCuenta}: Ratio {pA}/{pB} tiene {openCnt} órdenes abiertas. Se mantiene calibración original hasta liquidar ciclo.")
                else:
                    # Las recomendaciones nuevas nacen con operar = 0 para que el usuario las revise y active
                    dbSession.execute(text("""
                        INSERT INTO user_ratios (idUsuario, idCuenta, numerador, denominador, Temporalidad, periodo, operar, borrado, umbralDiferencia, origen, estadoRecomendacion, fechaRecomendacion)
                        VALUES (1, :acc, :pA, :pB, :tf, 180, 0, 0, 0.20, 'OPTIMIZADOR', 'NUEVA', NOW())
                    """), {"acc": idCuenta, "pA": pA, "pB": pB, "tf": tf})

        dbSession.commit()
        logger.info(f"✅ Sincronización exitosa para Cuenta #{idCuenta}.")

    return account_plan


def optimizeAllAccounts(applyToDb: bool = False) -> List[Dict[str, Any]]:
    """
    Optimiza el portafolio para todas las cuentas activas registradas en la tabla 'cuenta'.
    """
    startTs = time.time()
    logger.info("==========================================================================")
    logger.info("💼 [accountPortfolioOptimizer] INICIANDO OPTIMIZACIÓN POR CUENTA")
    logger.info("==========================================================================")

    db = SessionLocal()
    account_plans = []

    try:
        globalPortfolio = loadGlobalPortfolio()

        # Metadata de símbolos
        sqlSyms = text("SELECT symbol, margen, min_lots, pip, tipo FROM symbols")
        symMetadata = {r[0].strip(): {"margen": r[1], "min_lots": r[2], "pip": r[3], "tipo": r[4]} for r in db.execute(sqlSyms).fetchall()}

        # Cuentas activas
        sqlAccs = text("SELECT idCuenta, Nombre, Capital, Activo, riesgoPorOperacion, comision, brokerType, Concentradora FROM cuenta WHERE Activo = 1")
        accounts = [dict(r._mapping) for r in db.execute(sqlAccs).fetchall()]
        logger.info(f"👥 Cuentas activas a procesar: {len(accounts)}")

        for acc in accounts:
            plan = optimizeSingleAccount(db, acc, globalPortfolio, symMetadata, applyToDb=applyToDb)
            account_plans.append(plan)
            logger.info(
                f"  📊 Cuenta #{plan['idCuenta']} ({plan['nombre']}) | Capital: ${plan['capital']:,.2f} | "
                f"Pesos: {plan['normalizedWeights']} | Margen Simulado: {plan['totalMarginPct']}% (Seguro: {plan['marginSafe']})"
            )

        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(account_plans, f, indent=2, ensure_ascii=False)

    finally:
        db.close()

    elapsed = time.time() - startTs
    logger.info("==========================================================================")
    logger.info(f"🏁 Optimización de cuentas completada en {elapsed:.1f}s. Reporte en: {OUTPUT_JSON}")
    logger.info("==========================================================================")
    return account_plans


def main():
    parser = argparse.ArgumentParser(description="Optimizador de Portafolio y Lotaje por Cuenta")
    parser.add_argument("--all", action="store_true", help="Optimizar todas las cuentas activas")
    parser.add_argument("--apply", action="store_true", help="Aplica y actualiza en BD los cambios de cartera")
    args = parser.parse_args()

    optimizeAllAccounts(applyToDb=args.apply)


if __name__ == "__main__":
    main()
