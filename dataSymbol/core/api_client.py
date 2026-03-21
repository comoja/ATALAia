# Módulo: api_client.py
# Proyecto: Sentinel (ATALA.ia)

import time
from itertools import cycle

class ApiClient:
    def __init__(self, apiKeys):
        self.keyPool = cycle(apiKeys)
        self.callsPerMinute = 7
        self.secondsBetweenCalls = 60 / self.callsPerMinute

    def getNextKey(self):
        return next(self.keyPool)

    def fetchCandles(self, symbol, startDate):
        currentKey = self.getNextKey()
        # Lógica de requests aquí...
        time.sleep(self.secondsBetweenCalls)
        return f"Fetching {symbol} with key {currentKey}"