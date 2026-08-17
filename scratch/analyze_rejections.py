import re
from collections import Counter

logPath = "./Sentinel/logs/sentinel.log"

rejections = []
try:
    with open(logPath, "r") as f:
        for line in f:
            if "Rechazada:" in line or "descartada" in line or "descartando" in line or "vetada" in line or "Filtrado:" in line or "CONTRADICCIÓN:" in line or "omitido" in line:
                # Extraer la razón
                match = re.search(r'(Rechazada:|descartada:|descartando|vetada\.|Filtrado:|CONTRADICCIÓN:|omitido)(.*)', line)
                if match:
                    reason = match.group(2).strip()
                    # Limpiar razón de símbolos o números específicos para agrupar mejor
                    reason = re.sub(r'\[.*?\]', '', reason)
                    reason = re.sub(r'EUR/USD|XAU/USD|GBP/USD|USD/JPY|AUD/USD|USD/CHF|USD/MXN|BTC/USD|GBP/JPY|EUR/GBP|NZD/USD|GBP/CAD|USD/CAD', 'SYMBOL', reason)
                    reason = re.sub(r'\d+\.\d+', 'NUM', reason)
                    reason = re.sub(r'=\d+', '=NUM', reason)
                    reason = re.sub(r'Fvg \d+', 'Fvg ID', reason, flags=re.IGNORECASE)
                    reason = re.sub(r'FVG \d+', 'FVG ID', reason, flags=re.IGNORECASE)
                    rejections.append(reason)

    counter = Counter(rejections)
    print("=== ANÁLISIS DE DESCARTES DE SEÑALES HOY ===")
    for reason, count in counter.most_common(20):
        print(f"{count:>5} veces: {reason}")
except Exception as e:
    print("Error leyendo log:", e)
