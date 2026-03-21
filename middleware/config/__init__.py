"""
Configuration settings and constants for ATALAia middleware.
"""
from middleware.config import constants

settings = constants

dbConfig = constants.dbConfig
timeZone = constants.timeZone
FESTIVOS = constants.FESTIVOS
INTERVAL = constants.INTERVAL
INTERVALmax = constants.INTERVALmax
TIMEZONE = constants.TIMEZONE
DEFAULT_INTERVAL = constants.DEFAULT_INTERVAL
MAX_INTERVAL = constants.MAX_INTERVAL
API_KEYS = constants.API_KEYS
timeframes = constants.timeframes
SYMBOLS = constants.SYMBOLS
TWELVE_DATA_API_URL = constants.TWELVE_DATA_API_URL
URLTDSERIES = constants.URLTDSERIES
MaxXminuto = constants.MaxXminuto
MaxXdia = constants.MaxXdia
minutosXdia = constants.minutosXdia

__all__ = [
    "constants",
    "dbConfig",
    "timeZone",
    "FESTIVOS",
    "INTERVAL",
    "INTERVALmax",
    "TIMEZONE",
    "DEFAULT_INTERVAL",
    "MAX_INTERVAL",
    "API_KEYS",
    "timeframes",
    "SYMBOLS",
    "TWELVE_DATA_API_URL",
    "URLTDSERIES",
    "MaxXminuto",
    "MaxXdia",
    "minutosXdia",
]
