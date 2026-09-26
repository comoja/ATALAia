import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/microRatio.py"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

target = """    import pandas as pd
    from datetime import datetime
    createdAtStr = ratioRecord.get("createdAt")
    createdAtDt = None
    if createdAtStr:
        try:
            if isinstance(createdAtStr, datetime):
                createdAtDt = createdAtStr
            else:
                createdAtDt = pd.to_datetime(createdAtStr).tz_localize(None)
        except Exception:
            pass"""

replacement = """    import pandas as pd
    from datetime import datetime, timedelta
    createdAtStr = ratioRecord.get("createdAt")
    createdAtDt = None
    if createdAtStr:
        try:
            if isinstance(createdAtStr, datetime):
                base_dt = createdAtStr
            else:
                base_dt = pd.to_datetime(createdAtStr).tz_localize(None)
            
            tf_clean = str(periodo).lower().strip()
            num_periods = dias
            
            if "mo" in tf_clean or "month" in tf_clean:
                createdAtDt = base_dt - pd.DateOffset(months=num_periods)
            elif "w" in tf_clean:
                createdAtDt = base_dt - timedelta(weeks=num_periods)
            elif "d" in tf_clean:
                createdAtDt = base_dt - timedelta(days=num_periods)
            elif "15m" in tf_clean:
                createdAtDt = base_dt - timedelta(minutes=15 * num_periods)
            elif "30m" in tf_clean:
                createdAtDt = base_dt - timedelta(minutes=30 * num_periods)
            elif "5m" in tf_clean:
                createdAtDt = base_dt - timedelta(minutes=5 * num_periods)
            elif "4h" in tf_clean:
                createdAtDt = base_dt - timedelta(hours=4 * num_periods)
            else: # fallback 1h
                createdAtDt = base_dt - timedelta(hours=num_periods)
                
            logger.info(f"⏳ Fecha efectiva calculada: base={base_dt}, tf={tf_clean}, periodos={num_periods}, efectiva={createdAtDt}")
            
        except Exception as e:
            logger.error(f"Error parseando o calculando createdAt: {e}")
            pass"""

if target in content:
    content = content.replace(target, replacement)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    print("microRatio.py patched successfully")
else:
    print("target not found")

