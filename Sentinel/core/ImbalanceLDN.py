"""
ImbalanceLDN Trading Strategy Bot
Strategy based on liquidity sweeps with FVG confirmation in 5min for London session.
Only for XAU/USD symbol.
"""
from Sentinel.core.BaseImbalanceBot import BaseImbalanceBot

class ImbalanceLDNBot(BaseImbalanceBot):
    def __init__(self):
        super().__init__(strategy_name="ImbalanceLDN")
