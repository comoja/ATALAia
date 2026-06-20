import sys
import pytz
from datetime import datetime, timedelta

sys.path.append('/Volumes/TimeMachine/ATALAia')
from dataSymbol.core.databaseManager import DatabaseManager
from middleware.database import dbManager

def simulate():
    db = DatabaseManager()
    symbol = "XAG/USD"
    
    # 1. Get last timestamp
    lastDb = db.getLastTimestamp(symbol, "5min")
    print("lastDb from DB:", lastDb, type(lastDb))
    
    # 2. Get symbol config
    symbols = dbManager.getSymbols()
    symbolData = next(s for s in symbols if s['symbol'] == symbol)
    
    if lastDb:
        startDate = lastDb + timedelta(minutes=5)
    else:
        startDateRaw = symbolData.get('startDate')
        if isinstance(startDateRaw, str):
            startDate = datetime.strptime(startDateRaw, '%Y-%m-%d')
        elif startDateRaw:
            startDate = datetime.combine(startDateRaw, datetime.min.time())
        else:
            startDate = datetime(2020, 1, 1)
            
    print("startDate initial:", startDate)
    
    TIMEZONE = "America/Mexico_City"
    TZ = pytz.timezone(TIMEZONE)
    
    def normalize_datetime(dt, tz):
        import pandas as pd
        if isinstance(dt, pd.Timestamp):
            dt = dt.to_pydatetime()
        if dt.tzinfo is None:
            return tz.localize(dt)
        else:
            return dt.astimezone(tz)
            
    startDate = normalize_datetime(startDate, TZ)
    print("startDate normalized:", startDate)
    
    # Simulated now_local
    now_local = datetime.now(TZ)
    print("now_local:", now_local)
    
    def get_safe_last_candle(now, interval=5):
        safe_now = now - timedelta(minutes=interval) - timedelta(seconds=30)
        minute = (safe_now.minute // interval) * interval
        return safe_now.replace(minute=minute, second=0, microsecond=0)
        
    lastClosed = get_safe_last_candle(now_local)
    print("lastClosed:", lastClosed)
    
    MAX_CANDLES_PER_CALL = 5000
    CANDLE_INTERVAL_MINUTES = 5
    MAX_MINUTES_PER_CALL = MAX_CANDLES_PER_CALL * CANDLE_INTERVAL_MINUTES
    
    def round5min(timestamp):
        if isinstance(timestamp, str):
            dt = datetime.strptime(timestamp, "%Y/%m/%d %H:%M:%S")
        else:
            dt = timestamp
        minute = (dt.minute // 5) * 5
        return dt.replace(minute=minute, second=0, microsecond=0)
        
    if startDate.date() == now_local.date():
        endDate = lastClosed
        print("Path: Same date")
    else:
        endDate = round5min(startDate + timedelta(minutes=MAX_MINUTES_PER_CALL))
        if endDate > lastClosed:
            endDate = lastClosed
        print("Path: Different date")
        
    print("endDate calculated:", endDate)
    print("Log format: %H:%M to %H:%M ->", startDate.strftime('%H:%M'), "to", endDate.strftime('%H:%M'))

if __name__ == "__main__":
    simulate()
