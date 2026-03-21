# Módulo: data_refiner.py
# Proyecto: Sentinel (ATALA.ia)
class DataRefiner:
    def __init__(self, dbManager):
        self.dbManager = dbManager

    def getResampledCandles(self, symbol, targetInterval, startDate):
        # 1. Traer datos de 5min de la DB
        # (Necesitarías implementar un método readFromDb en DatabaseManager)
        df5min = self.dbManager.readCandles(symbol, startDate)
        
        if df5min.empty: return df5min

        # 2. Mapeo para Pandas (1M = ME, 1H = H, etc.)
        map = {'15min': '15T', '1H': 'H', '1month': 'ME'}
        pandasInterval = map.get(targetInterval, targetInterval)

        return df5min.resample(pandasInterval).agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()