import pytz
from datetime import datetime, timedelta

TIMEZONE = "America/Mexico_City"
TZ = pytz.timezone(TIMEZONE)

def get_safe_last_candle(now, interval=5):
    safe_now = now - timedelta(minutes=interval) - timedelta(seconds=30)
    minute = (safe_now.minute // interval) * interval
    return safe_now.replace(minute=minute, second=0, microsecond=0)

now_local = datetime.now(TZ)
print("now_local:", now_local)
lastClosed = get_safe_last_candle(now_local)
print("lastClosed:", lastClosed)
print("lastClosed formatting:", lastClosed.strftime('%H:%M'))
