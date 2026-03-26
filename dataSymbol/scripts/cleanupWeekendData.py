import logging
import os
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from middleware.utils.loggerConfig import setupLogging
setupLogging(logPara="cleanupWeekendData", projectDir=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

logger = logging.getLogger(__name__)

dbConfig = {
    "host": "localhost",
    "user": "root",
    "password": "M1x&J34ny",
    "database": "ATALAia"
}

engine = create_engine(
    f"mysql+mysqlconnector://{dbConfig['user']}:{dbConfig['password']}@{dbConfig['host']}/{dbConfig['database']}"
)


def cleanupWeekendData(symbol: str = None):
    tzNY = ZoneInfo("America/Mexico_City")
    deleted = 0
    
    try:
        with engine.begin() as conn:
            if symbol:
                result = conn.execute(
                    text("SELECT symbol, timeframe, timestamp FROM candles WHERE symbol = :symbol"),
                    {"symbol": symbol}
                )
            else:
                result = conn.execute(text("SELECT symbol, timeframe, timestamp FROM candles"))
            
            rows = result.fetchall()
            
            candlesToDelete = []
            for row in rows:
                sym, tf, ts = row
                if tf not in ["5min", "15min", "1h"]:
                    continue
                
                tsNY = ts.astimezone(tzNY)
                weekday = tsNY.weekday()
                
                if weekday == 5:
                    candlesToDelete.append((sym, tf, ts))
                elif weekday == 6 and tsNY.hour < 17:
                    candlesToDelete.append((sym, tf, ts))
            
            for sym, tf, ts in candlesToDelete:
                conn.execute(
                    text("DELETE FROM candles WHERE symbol = :symbol AND timeframe = :timeframe AND timestamp = :ts"),
                    {"symbol": sym, "timeframe": tf, "ts": ts}
                )
                deleted += 1
        
        logger.info(f"Velas eliminadas: {deleted}")
        
    except Exception as e:
        logger.error(f"Error: {e}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Limpia velas de fin de semana')
    parser.add_argument('--symbol', '-s', type=str, help='Símbolo específico')
    args = parser.parse_args()
    
    cleanupWeekendData(args.symbol)
