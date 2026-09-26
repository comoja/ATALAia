import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/microRatio.py"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

target1 = """    dias = ratioRecord["dias"] or 180
    emaRapida = ratioRecord["EMARapida"] or 2
    emaLenta = ratioRecord["EMALenta"] or 20

    # 2. Datos de símbolos y cuenta
    symDataA = fetchSymbolData(dbSession, numerador)"""

replacement1 = """    dias = ratioRecord["dias"] or 180
    emaRapida = ratioRecord["EMARapida"] or 2
    emaLenta = ratioRecord["EMALenta"] or 20

    import pandas as pd
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
            pass

    # 2. Datos de símbolos y cuenta
    symDataA = fetchSymbolData(dbSession, numerador)"""

if target1 in content:
    content = content.replace(target1, replacement1)
else:
    print("target1 not found")


target2 = """    if not activeBrokers:
        openTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)
        if not openTrades:"""

replacement2 = """    if not activeBrokers:
        openTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)
        if createdAtDt:
            openTrades = [t for t in openTrades if (t.get("openTime") or t.get("candleTime")) and parseCandleDateTime(t.get("openTime") or t.get("candleTime")) >= createdAtDt]
        if not openTrades:"""

if target2 in content:
    content = content.replace(target2, replacement2)
else:
    print("target2 not found")


target3 = """    # Evaluar ÚNICAMENTE la vela actual (sin retroceder a velas del pasado)
    targetSignal = latestSig
    candleDtStr = targetSignal.get("datetime")
    candleDt = parseCandleDateTime(candleDtStr)"""

replacement3 = """    # Evaluar ÚNICAMENTE la vela actual (sin retroceder a velas del pasado)
    targetSignal = latestSig
    candleDtStr = targetSignal.get("datetime")
    candleDt = parseCandleDateTime(candleDtStr)

    if createdAtDt and candleDt < createdAtDt:
        logger.info(f"⏳ [{accHeader}] Vela actual ({candleDtStr}) es anterior a la creación del ratio ({createdAtDt}). Se ignora evaluación.")
        return"""

if target3 in content:
    content = content.replace(target3, replacement3)
else:
    print("target3 not found")


target4 = """    dbOpenTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)"""

replacement4 = """    dbOpenTrades = checkActiveOpenTrades(dbSession, idCuenta, setupName)
    if createdAtDt:
        dbOpenTrades = [t for t in dbOpenTrades if (t.get("openTime") or t.get("candleTime")) and parseCandleDateTime(t.get("openTime") or t.get("candleTime")) >= createdAtDt]"""

if target4 in content:
    content = content.replace(target4, replacement4)
else:
    print("target4 not found")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("microRatio.py patched successfully")
