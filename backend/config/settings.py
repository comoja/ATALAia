import numpy as np

import time
import sys
import os
import warnings
warnings.filterwarnings("ignore")

# ===== GENERAL =====

SYMBOLS = np.array(["XAU/USD","USD/MXN", "AUD/USD","EUR/USD", "GBP/USD","CAD/USD","JPY/USD"])
timeframes = [22,23,6,7,8,9,10,11,12,13,14]
# --- DÍAS FESTIVOS (Añadir YYYY-MM-DD) ---
FESTIVOS = ["2026-01-01", "2026-12-25", "2026-05-01"]
# ===== TWELVE DATA =====
API_KEYS = ["98c13fd2d0714dc984ca2791e9e3d521", # Jaime
            "99cc3d9bead5422c99f5614131c9ba4c", # Raul
            "6ac737658dde42fc9874bec8200b01ca" # Sebastian
]
URLTDSERIES = "https://api.twelvedata.com/time_series"
MaxXminuto = 8
MaxXdia = 800
minutosXdia = 24 * 60 

# ===== STRATEGY DE SENTINEL=====
INTERVAL = "15min"
INTERVALmax = "1h"
CAPITAL_ACTUAL = 500 # Esto debería leerse de tu balance real
Riesgo_Por_Operacion = 0.02 # 2% de riesgo por trade ($10 USD)
RISK_REWARD = 1.9
VELAS_HISTORIAL = 1000
tiempoEspera = 5 #expresado en minutos


# ===== RISK =====
riskPerTrade = 0.01
maxPortfolioRisk = 0.05
correlationLimit = 0.75
minProfitFactor = 1.2

# ===== DB =====
dbConfig = {
    "host": "localhost",
    "user": "root",
    "password": "M1x&J34ny",
    "database": "ATALAia"
}

# ===== TIMEZONE =====
timeZone = "America/Mexico_City"

# ===== TELEGRAM =====
TOKEN = '8709556193:AAGbqWrLlbr6WVp3fPjYmctDS09dvc9QvA8'
IDGRUPO = -1003763086164



