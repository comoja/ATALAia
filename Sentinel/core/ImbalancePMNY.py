"""
ImbalancePMNY — NY Afternoon Session Bot (2:00 PM – 3:00 PM New York Time)

Implementa el análisis de imbalances para la sesión de tarde de New York,
la tercera killzone de ICT (NY PM Silver Bullet window).
El rango de referencia es la apertura 14:00–15:00 NY; el análisis de FVGs
se ejecuta sobre las velas post-apertura hasta el cierre de sesión (17:00 NY).
"""
from Sentinel.core.BaseImbalanceBot import BaseImbalanceBot


class ImbalancePMNYBot(BaseImbalanceBot):
    def __init__(self):
        super().__init__(strategy_name="ImbalancePMNY")
