import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from middleware.utils.loggerConfig import setupLogging
setupLogging(logPara="cleanupWeekendData", projectDir=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataSymbol.core.databaseManager import DatabaseManager

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Limpia velas de fin de semana (Excluyendo Criptos)')
    parser.add_argument('--symbol', '-s', type=str, help='Símbolo específico (opcional)')
    args = parser.parse_args()
    
    db = DatabaseManager()
    db.cleanupWeekendData(args.symbol)
