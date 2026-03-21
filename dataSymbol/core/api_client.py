# Módulo: api_client.py
# Proyecto: Sentinel (ATALA.ia)

import time
import requests
import pandas as pd
from itertools import cycle

class ApiClient:
    def __init__(self, apiKeys):
        self.keyPool = cycle(apiKeys)
        self.callsPerMinute = 7
        self.secondsBetweenCalls = 60 / self.callsPerMinute

    def fetchFiveMinuteCandles(self, symbol, startDate):
        currentKey = next(self.keyPool)
        
        # Ejemplo con TwelveData (ajusta según tu proveedor real)
        url = "https://api.twelvedata.com/time_series"
        params = {
            "symbol": symbol,
            "interval": "5min",
            "start_date": startDate.strftime('%Y-%m-%d %H:%M:%S'),
            "apikey": currentKey,
            "outputsize": 5000,
            "order": "ASC"
        }
        
        response = requests.get(url, params=params)
        time.sleep(self.secondsBetweenCalls) # Respetar los 7 por minuto
        
        data = response.json()
        if "values" in data:
            df = pd.DataFrame(data["values"])
            df['timestamp'] = pd.to_datetime(df['datetime'])
            df = df.set_index('timestamp').drop('datetime', axis=1)
            # Convertir columnas a numérico
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = pd.to_numeric(df[col])
            return df
        return pd.DataFrame()