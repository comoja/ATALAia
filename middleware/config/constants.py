"""
Centralized configuration for ATALAia middleware.
This file contains all shared constants and settings used across subprojects.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent.parent.parent.resolve()
load_dotenv(dotenv_path=BASE_DIR / ".env")

mt5Login = int(os.getenv("MT5_LOGIN", "0"))
mt5Password = os.getenv("MT5_PASSWORD", "")
mt5Server = os.getenv("MT5_SERVER", "")

import numpy as np

SYMBOLS = np.array(["USD/MXN", "XAU/USD"])
timeframes = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
FESTIVOS = ["2026-01-01", "2026-12-25"]

API_KEYS = [
    "98c13fd2d0714dc984ca2791e9e3d521",
    "99cc3d9bead5422c99f5614131c9ba4c",
    "6ac737658dde42fc9874bec8200b01ca",
    "a227afb20ee24294a55afe11e526bf6f"
]

TWELVE_DATA_API_URL = "https://api.twelvedata.com"
URLTDSERIES = "https://api.twelvedata.com/timeSeries"
MaxXminuto = 7
MaxXdia = 750
minutosXdia = 24 * 60

TWELVE_DATA_CREDIT_LIMIT = 750
TWELVE_DATA_CREDIT_EMERGENCY_THRESHOLD = 50

FINNHUB_API_KEY = "d63po69r01ql6dj11hbgd63po69r01ql6dj11hc0"
TRADERMADE_API_KEY = ""
ALPHA_VANTAGE_API_KEY = "YGKNWXFES0X0UL5J"

FOREXCOM_USERNAME = "comoja66@gmail.com"
FOREXCOM_PASSWORD = "Trade123@"
FOREXCOM_APP_KEY = "Ja.Morales"

TIMEZONE = "America/Mexico_City"
DEFAULT_INTERVAL = "15min"
MAX_INTERVAL = "1h"
INTERVAL = "15min"
DATA_SOURCE = "db"
RISK_REWARD = 1.9
VELAS_HISTORIAL = 1000
tiempoEspera = 5
INTERVALmax = "15min"
timeZone = TIMEZONE

dbConfig = {
    "host": "localhost",
    "user": "root",
    "password": "M1x&J34ny",
    "database": "ATALAia",
    "connect_timeout": 10
}

MODEL_PARAMS = {
    "n_estimators": 300,
    "max_depth": 15,
    "random_state": 42,
    "n_jobs": -1
}

MODEL_FEATURES = [
    "rsi", "atr", "emaDist", "emaTrend", "slopeEma50", "volRatio", "cci",
    "lag1", "lag2", "lag3", "volRegime", "macdHist", "macdNorm", "pendienteRsi",
    "sarTrend", "sarDist"
]

ML_TARGET_HORIZON_LOW_VOL = 12
ML_TARGET_HORIZON_HIGH_VOL = 5
ML_TARGET_HORIZON_NORMAL_VOL = 8

PROBA_THRESHOLD_LONG = 0.55
PROBA_THRESHOLD_SHORT = 0.45

RSI_OVERBOUGHT_THRESHOLD = 70
RSI_SOLD_THRESHOLD = 30

MIN_VOLATILITY_PERCENT = 0.10

CONTRARIAN_CONFIDENCE_THRESHOLD = 65
MIN_CONFIDENCE_THRESHOLD = 60

ATR_MULTIPLIER_DEFAULT = 1.5
ATR_MULTIPLIER_HIGH_CONFIDENCE = 1.15

BASE_RISK_REWARD_RATIO = 2.0
HIGH_CONFIDENCE_RISK_REWARD_RATIO = 2.2

MODEL_FILE_PATH = str(BASE_DIR / "Sentinel/ml/trainedModel.joblib")
MODEL_REG_FILE_PATH = str(BASE_DIR / "Sentinel/ml/trainedRegModel.joblib")

# --- Production Settings ---
PRODUCTION_MODE = True # Cambiar a True para ejecución real en Broker

# --- Risk & Safety ---
MAX_SIGNAL_AGE_MINUTES = 45 # Tiempo máximo permitido desde la vela origen hasta la ejecución
MAX_RISK_PER_TRADE = 10.0 # Riesgo máximo permitido por operación (% del capital)
