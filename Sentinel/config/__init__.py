"""
Configuration for Sentinel - re-exports from middleware.
"""
from middleware.config import constants

dbConfig = constants.dbConfig
timeZone = constants.timeZone
FESTIVOS = constants.FESTIVOS
INTERVAL = constants.INTERVAL
INTERVALmax = constants.INTERVALmax
TIMEZONE = constants.TIMEZONE
DEFAULT_INTERVAL = constants.DEFAULT_INTERVAL
MAX_INTERVAL = constants.MAX_INTERVAL

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
]
