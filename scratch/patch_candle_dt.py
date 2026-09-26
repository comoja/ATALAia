import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/microRatio.py"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

target = """        # Validación estricta por temporalidad: solo insertar si el cruce sucedió en esa hora / día
        if not isCandleSignalFresh(candleDt, periodo):"""

replacement = """        # Filtrar si la vela es anterior a la fecha de creación
        if createdAtDt and candleDt < createdAtDt:
            logger.info(f"⏳ [{accHeader}] Vela actual ({entryDateStr}) es anterior a la creación del ratio ({createdAtDt}). Se ignora la evaluación.")
            return

        # Validación estricta por temporalidad: solo insertar si el cruce sucedió en esa hora / día
        if not isCandleSignalFresh(candleDt, periodo):"""

if target in content:
    content = content.replace(target, replacement)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    print("candleDt patched successfully")
else:
    print("target not found")
