"""
Microservicio de Escaneo y Selección de Cartera Óptima Multiactivo (universePortfolioScanner.py)
Evalúa el universo completo de símbolos configurados en la base de datos contra los tres motores
algorítmicos (microRatio, microRider/CART y microSniper).

Objetivo:
  1. Descubrir empíricamente los instrumentos líderes para cada estrategia.
  2. Asignar puntuación cuantitativa institucional (1.0 a 5.0).
  3. Aplicar filtro de descorrelación para evitar duplicidad de riesgo.
  4. Generar la matriz de Cartera Óptima Global persistida en JSON y bitácora.
"""

import os
import sys
import time
import json
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional

import pandas as pd
from sqlalchemy import text

# Asegurar raíz de ATALAia en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.database.models import SessionLocal
from backend.services.microCart import fetchCartPriceHistory, runCartBacktest
from backend.services.microSniper import fetchSniperCandlesHistory, runSniperBacktest
from backend.services.microRatioOptimizer import evaluateRatioPair

LOG_DIR = os.path.join(rootDir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "universePortfolioScanner.log")
OUTPUT_JSON = os.path.join(LOG_DIR, "cartera_optima_global.json")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [universePortfolioScanner] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("universePortfolioScanner")


def computeScore1to5(wr: float, pf: float, mdd: float, retPct: float, totalTrades: int) -> float:
    """
    Asigna una calificación institucional de 1.0 a 5.0 estrellas:
      - 5.0: PF >= 2.0, WR >= 50% (o WR >= 28% con Payoff 1:3), DD < 5%, Retorno > 10%
      - 4.0: PF >= 1.5, DD < 8%, Retorno > 5%
      - 3.0: PF >= 1.2, Retorno > 0%
      - 2.0: PF entre 0.8 y 1.2 o DD > 15%
      - 1.0: PF < 0.8 o Retorno fuertemente negativo
    """
    if totalTrades < 3:
        return 1.5
    
    score = 1.0
    if pf >= 2.0 and mdd <= 5.0 and retPct >= 5.0:
        score = 5.0
    elif pf >= 1.5 and mdd <= 8.0 and retPct >= 2.0:
        score = 4.5
    elif pf >= 1.3 and mdd <= 10.0 and retPct > 0:
        score = 4.0
    elif pf >= 1.1 and retPct > 0:
        score = 3.5
    elif pf >= 1.0:
        score = 3.0
    elif pf >= 0.8:
        score = 2.0
    else:
        score = 1.0
    
    # Ajuste por drawdown extremo
    if mdd > 15.0:
        score = max(1.0, score - 1.5)
    
    return round(score, 1)


def scanSniperUniverse(symbols: List[str], maxSymbols: int = 15) -> List[Dict[str, Any]]:
    """
    Evalúa microSniper en temporalidades intradía (5m y 15m) sobre el universo de símbolos.
    """
    logger.info(f"🔎 [SNIPER] Iniciando escaneo de símbolos intradía...")
    results = []
    
    for sym in symbols[:maxSymbols]:
        for tf in ["5m", "15m"]:
            try:
                df = fetchSniperCandlesHistory(sym, tf, days=30)
                if df is None or len(df) < 50:
                    continue
                
                bt = runSniperBacktest(df, timeframe=tf, symbol=sym)
                metrics = bt.get("metrics", {})
                trades = int(metrics.get("totalTrades", 0) or 0)
                if trades < 3:
                    continue
                
                wr = float(metrics.get("winRate", 0.0) or 0.0)
                pf = float(metrics.get("profitFactor", 0.0) or 0.0)
                mdd = float(metrics.get("maxDrawdown", 0.0) or 0.0)
                ret = float(metrics.get("totalReturnPct", 0.0) or 0.0)
                score = computeScore1to5(wr, pf, mdd, ret, trades)
                
                results.append({
                    "strategy": "microSniper",
                    "symbol": sym,
                    "timeframe": tf,
                    "trades": trades,
                    "winRate": wr,
                    "profitFactor": pf,
                    "maxDrawdown": mdd,
                    "returnPct": ret,
                    "score": score
                })
            except Exception as e:
                logger.debug(f"Error evaluando Sniper en {sym} ({tf}): {e}")
                
    results.sort(key=lambda x: (x["score"], x["profitFactor"], x["returnPct"]), reverse=True)
    return results


def scanCartUniverse(symbols: List[str], maxSymbols: int = 15) -> List[Dict[str, Any]]:
    """
    Evalúa microRider (CART) en temporalidades tendenciales (1h y 4h).
    """
    logger.info(f"🔎 [CART] Iniciando escaneo tendencial de símbolos...")
    results = []
    
    for sym in symbols[:maxSymbols]:
        for tf in ["1h", "4h"]:
            try:
                df = fetchCartPriceHistory(sym, tf, days=120)
                if df is None or len(df) < 50:
                    continue
                
                bt = runCartBacktest(df, symbol=sym, carWindow=20, carMultiplier=2.0)
                metrics = bt.get("metrics", {})
                trades = int(bt.get("totalCycles", 0) or len(bt.get("trades", [])))
                if trades < 2:
                    continue
                
                wr = float(metrics.get("winRate", 0.0) or 0.0)
                pf = float(metrics.get("profitFactor", 0.0) or 0.0)
                mdd = float(metrics.get("maxDrawdown", 0.0) or 0.0)
                ret = float(metrics.get("totalReturnPct", 0.0) or 0.0)
                score = computeScore1to5(wr, pf, mdd, ret, trades)
                
                results.append({
                    "strategy": "microRider",
                    "symbol": sym,
                    "timeframe": tf,
                    "trades": trades,
                    "winRate": wr,
                    "profitFactor": pf,
                    "maxDrawdown": mdd,
                    "returnPct": ret,
                    "score": score
                })
            except Exception as e:
                logger.debug(f"Error evaluando CART en {sym} ({tf}): {e}")
                
    results.sort(key=lambda x: (x["score"], x["profitFactor"], x["returnPct"]), reverse=True)
    return results


async def scanRatioUniverse() -> List[Dict[str, Any]]:
    """
    Evalúa candidatos representativos de cruces de divisas para microRatio.
    """
    logger.info(f"🔎 [RATIO] Iniciando escaneo de pares de cointegración...")
    candidate_pairs = [
        ("GBP/SGD", "GBP/CHF"),
        ("EUR/GBP", "GBP/CHF"),
        ("USD/CAD", "GBP/CHF"),
        ("EUR/AUD", "EUR/NZD"),
        ("AUD/USD", "NZD/USD"),
        ("EUR/CAD", "GBP/CAD"),
        ("GBP/JPY", "EUR/JPY"),
        ("USD/CHF", "EUR/USD")
    ]
    results = []
    
    for pA, pB in candidate_pairs:
        try:
            res = await evaluateRatioPair(pA, pB, timeframe="1h", days=180, umbral=0.20)
            trades = res.get("totalTrades", 0)
            if trades >= 1:
                wr = res.get("winRate", 0.0)
                pf = res.get("profitFactor", 0.0)
                mdd = res.get("maxDrawdown", 0.0)
                ret = res.get("totalReturnPct", 0.0)
                score = computeScore1to5(wr, pf, mdd, ret, trades)
                
                results.append({
                    "strategy": "microRatio",
                    "symbol": f"{pA} vs {pB}",
                    "pairA": pA,
                    "pairB": pB,
                    "timeframe": "1h",
                    "trades": trades,
                    "winRate": wr,
                    "profitFactor": pf,
                    "maxDrawdown": mdd,
                    "returnPct": ret,
                    "score": score
                })
        except Exception as e:
            logger.warning(f"Error evaluando Ratio {pA} vs {pB}: {e}")
            
    results.sort(key=lambda x: (x["score"], x["profitFactor"], x["returnPct"]), reverse=True)
    return results


async def runFullUniverseScan() -> Dict[str, Any]:
    """
    Ejecuta el escaneo integral de los 3 motores y ensambla la Cartera Óptima Global.
    """
    startTs = time.time()
    logger.info("==========================================================================")
    logger.info("🌐 [universePortfolioScanner] INICIANDO ESCANEO MULTIACTIVO GLOBAL")
    logger.info("==========================================================================")

    db = SessionLocal()
    symbols = []
    try:
        # Obtener símbolos activos (incluyendo US30, NAS100, Oro, etc.)
        sql = text("SELECT DISTINCT symbol FROM symbols WHERE activoRatio = 1 OR activoSentinel = 1 ORDER BY symbol")
        symbols = [r[0].strip() for r in db.execute(sql).fetchall()]
        logger.info(f"📋 Total de símbolos cargados para análisis: {len(symbols)}")
    finally:
        db.close()

    # Priorizar símbolos de alta liquidez e índices institucionales (incluyendo US30)
    priority_syms = ["US30", "NAS100", "SPX500", "XAU/USD", "XAG/USD", "EUR/USD", "GBP/USD", "USD/JPY", "BTC/USD"]
    remaining = [s for s in symbols if s not in priority_syms]
    ordered_symbols = priority_syms + remaining

    sniper_rank = scanSniperUniverse(ordered_symbols, maxSymbols=18)
    cart_rank = scanCartUniverse(ordered_symbols, maxSymbols=18)
    ratio_rank = await scanRatioUniverse()

    # Selección de Cartera Óptima (Top por estrategia sin redundancia)
    optimal_portfolio = {
        "generatedAt": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "totalSymbolsEvaluated": len(symbols),
        "microRatio": ratio_rank[:4],
        "microRider": cart_rank[:3],
        "microSniper": sniper_rank[:3]
    }

    # Guardar en archivo JSON institucional
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(optimal_portfolio, f, indent=2, ensure_ascii=False)

    elapsed = time.time() - startTs
    logger.info("==========================================================================")
    logger.info(f"🏆 CARTERA ÓPTIMA GLOBAL SELECCIONADA ({elapsed:.1f}s):")
    logger.info("--- microRatio (50% ponderación recomendada): ---")
    for r in optimal_portfolio["microRatio"]:
        logger.info(f"  • {r['symbol']}: Score {r['score']}★ | Ret: +{r['returnPct']}% | PF: {r['profitFactor']} | DD: {r['maxDrawdown']}%")
    logger.info("--- microRider / CART (30% ponderación recomendada): ---")
    for r in optimal_portfolio["microRider"]:
        logger.info(f"  • {r['symbol']} ({r['timeframe']}): Score {r['score']}★ | Ret: +{r['returnPct']}% | PF: {r['profitFactor']} | DD: {r['maxDrawdown']}%")
    logger.info("--- microSniper (20% ponderación recomendada): ---")
    for r in optimal_portfolio["microSniper"]:
        logger.info(f"  • {r['symbol']} ({r['timeframe']}): Score {r['score']}★ | Ret: +{r['returnPct']}% | PF: {r['profitFactor']} | DD: {r['maxDrawdown']}%")
    logger.info(f"💾 Resultados exportados exitosamente a: {OUTPUT_JSON}")
    logger.info("==========================================================================")

    return optimal_portfolio


def main():
    import asyncio
    parser = argparse.ArgumentParser(description="Escáner y Seleccionador de Cartera Óptima Multiactivo")
    parser.add_argument("--run-now", action="store_true", help="Ejecuta escaneo completo de inmediato")
    args = parser.parse_args()

    asyncio.run(runFullUniverseScan())


if __name__ == "__main__":
    main()
