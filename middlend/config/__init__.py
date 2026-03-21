"""
Configuration for middlend - re-exports from middleware.
"""
from middleware.config import settings
from middleware.config import constants

dbConfig = settings.dbConfig
timeZone = settings.timeZone
FESTIVOS = settings.FESTIVOS
INTERVAL = settings.INTERVAL
INTERVALmax = settings.INTERVALmax

__all__ = [
    "settings",
    "constants",
    "dbConfig",
    "timeZone",
    "FESTIVOS",
    "INTERVAL",
    "INTERVALmax",
]
