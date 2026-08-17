"""
ImbalanceNY Trading Strategy Bot
Strategy based on liquidity sweeps with FVG confirmation in 5min.
Only for XAU/USD symbol.
"""
from Sentinel.core.BaseImbalanceBot import BaseImbalanceBot

class ImbalanceNYBot(BaseImbalanceBot):
    def __init__(self):
        super().__init__(strategy_name="ImbalanceNY")
