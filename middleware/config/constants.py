"""
Centralized configuration constants for ATALAia middleware.
This file contains all shared constants used across subprojects.
"""

TWELVE_DATA_API_URL = "https://api.twelvedata.com"
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

MODEL_FILE_PATH = "middlend/ml/trainedModel.joblib"
