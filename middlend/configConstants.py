"""
Centralized configuration for the middlend trading bot.
Values are extracted and consolidated from the original sentinel.py script.
"""

# --- API Configuration ---
TWELVE_DATA_API_URL = "https://api.twelvedata.com"
# Total credits available per API key before sending an emergency alert.
TWELVE_DATA_CREDIT_LIMIT = 750
# Credit threshold to trigger an emergency alert.
TWELVE_DATA_CREDIT_EMERGENCY_THRESHOLD = 50

# --- Time and Scheduling ---
TIMEZONE = "America/Mexico_City"
# Default interval for analysis. Can be overridden by dynamic parameters.
DEFAULT_INTERVAL = "15min"
MAX_INTERVAL = "1h"
# List of holidays or dates to avoid trading. (Example, needs to be populated)
FESTIVOS = []


# --- Machine Learning Model Configuration ---
MODEL_PARAMS = {
    "n_estimators": 150,
    "max_depth": 7,
    "random_state": 42,
    "n_jobs": -1
}

# Features used for the Random Forest model.
MODEL_FEATURES = [
    "rsi", "atr", "emaDist", "emaTrend", "slopeEma50", "volRatio", "cci",
    "lag1", "lag2", "lag3", "volRegime", "macdHist", "macdNorm", "pendienteRsi",
    "sarTrend", "sarDist"
]

# The prediction horizon for the ML target variable.
# (12 if vol < 0.8, 5 if vol > 1.2, else 8)
ML_TARGET_HORIZON_LOW_VOL = 12
ML_TARGET_HORIZON_HIGH_VOL = 5
ML_TARGET_HORIZON_NORMAL_VOL = 8


# --- Trading Logic Thresholds & Parameters ---

# Probability thresholds from the ML model to consider a signal.
PROBA_THRESHOLD_LONG = 0.65
PROBA_THRESHOLD_SHORT = 0.35

# RSI levels to avoid entering trades.
RSI_OVERBOUGHT_THRESHOLD = 60
RSI_SOLD_THRESHOLD = 30

# Minimum volatility (ATR as % of close price) to consider a signal.
MIN_VOLATILITY_PERCENT = 0.10

# Confidence level below which trading against the EMA50 is forbidden.
CONTRARIAN_CONFIDENCE_THRESHOLD = 85

# Minimum confidence required to accept a signal (below this, signal is discarded).
MIN_CONFIDENCE_THRESHOLD = 65

# --- Risk Management ---

# Base ATR multiplier for setting the Stop Loss distance.
# This is the default value, can be adjusted dynamically.
ATR_MULTIPLIER_DEFAULT = 1.5
# ATR multiplier for high-confidence signals.
ATR_MULTIPLIER_HIGH_CONFIDENCE = 1.15

# Base Risk-Reward Ratio. Can be adjusted dynamically.
BASE_RISK_REWARD_RATIO = 2.0
# Higher R/R for very high confidence signals.
HIGH_CONFIDENCE_RISK_REWARD_RATIO = 2.2

# --- File Paths ---
# Path to save the trained machine learning model.
MODEL_FILE_PATH = "middlend/ml/trainedModel.joblib"
