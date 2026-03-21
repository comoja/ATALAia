"""
Centralized configuration for ATALAia middleware.
This file contains all shared constants and settings used across subprojects.
"""
import numpy as np

SYMBOLS = np.array(["USD/MXN", "XAU/USD"])
timeframes = [6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]
FESTIVOS = ["2026-01-01", "2026-12-25", "2026-05-01"]

API_KEYS = [
    "98c13fd2d0714dc984ca2791e9e3d521",
    "99cc3d9bead5422c99f5614131c9ba4c",
    "6ac737658dde42fc9874bec8200b01ca"
]

TWELVE_DATA_API_URL = "https://api.twelvedata.com"
URLTDSERIES = "https://api.twelvedata.com/timeSeries"
MaxXminuto = 8
MaxXdia = 800
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
INTERVALmax = "15min"
timeZone = TIMEZONE

dbConfig = {
    "host": "localhost",
    "user": "root",
    "password": "M1x&J34ny",
    "database": "ATALAia"
}

MODEL_PARAMS = {
    "n_estimators": 150,
    "max_depth": 7,
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

PROBA_THRESHOLD_LONG = 0.65
PROBA_THRESHOLD_SHORT = 0.35

RSI_OVERBOUGHT_THRESHOLD = 60
RSI_SOLD_THRESHOLD = 30

MIN_VOLATILITY_PERCENT = 0.10

CONTRARIAN_CONFIDENCE_THRESHOLD = 85
MIN_CONFIDENCE_THRESHOLD = 65

ATR_MULTIPLIER_DEFAULT = 1.5
ATR_MULTIPLIER_HIGH_CONFIDENCE = 1.15

BASE_RISK_REWARD_RATIO = 2.0
HIGH_CONFIDENCE_RISK_REWARD_RATIO = 2.2

MODEL_FILE_PATH = "Sentinel/ml/trainedModel.joblib"
