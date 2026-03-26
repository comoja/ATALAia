import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.utils.loggerConfig import setupLogging
setupLogging(logPara="generateLongIntervals", projectDir=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from middleware.database import dbManager
from core.databaseManager import DatabaseManager
import logging

logger = logging.getLogger(__name__)


def generateLongIntervals(symbol: str = None):
    db = DatabaseManager()
    
    if symbol:
        symbols = [{'symbol': symbol}]
    else:
        symbols = dbManager.getSymbols()
    
    if not symbols:
        logger.warning("No se encontraron símbolos")
        return
    
    for symbolData in symbols:
        sym = symbolData['symbol']
        logger.info(f"\n--- Generando intervalos largos para {sym} ---")
        results = db.resampleLongIntervals(sym)
        for interval, count in results.items():
            logger.info(f"[{sym}] {interval}: {count} velas")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Genera intervalos largos (1day, 1week, 1month)')
    parser.add_argument('--symbol', '-s', type=str, help='Símbolo específico (opcional)')
    args = parser.parse_args()
    
    generateLongIntervals(args.symbol)
