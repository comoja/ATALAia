# Módulo: data_refiner.py
# Proyecto: Sentinel (ATALA.ia)

import pandas as pd
import mysql.connector
from datetime import datetime

class DataRefiner:
    def __init__(self, db_config):
        self.db_config = db_config

    def get_candles(self, symbol, interval='1H', start_date='2005-01-01'):
        """
        Solicita velas de cualquier intervalo. 
        Calcula todo a partir de la tabla de 5min.
        """
        # 1. Traer datos base de 5min desde MySQL
        df_5min = self._fetch_from_db(symbol, start_date)
        
        if df_5min.empty:
            print(f"⚠️ No hay datos de 5min para {symbol} desde {start_date}")
            return pd.DataFrame()

        # 2. Si el usuario pide 5min, entregarlos directo
        if interval == '5min':
            return df_5min

        # 3. Transformar a la temporalidad deseada (Resampling)
        # Mapeo de intervalos comunes a formato Pandas (1H, 1D, 1W, 1ME para mes)
        interval_map = {
            '15min': '15T',
            '30min': '30T',
            '1H': 'H',
            '4H': '4H',
            '1D': 'D',
            '1W': 'W',
            '1M': 'ME'
        }
        
        pandas_interval = interval_map.get(interval, interval)
        
        return self._resample_data(df_5min, pandas_interval)

    def _fetch_from_db(self, symbol, start_date):
        try:
            conn = mysql.connector.connect(**self.db_config)
            query = """
                SELECT timestamp, open, high, low, close, volume 
                FROM historical_candles_5min 
                WHERE symbol = %s AND timestamp >= %s
                ORDER BY timestamp ASC
            """
            df = pd.read_sql(query, conn, params=(symbol, start_date), index_col='timestamp')
            return df
        except Exception as e:
            print(f"❌ Error leyendo MySQL: {e}")
            return pd.DataFrame()
        finally:
            if conn.is_connected():
                conn.close()

    def _resample_data(self, df, interval):
        # Lógica OHLCV para agrupar velas
        resampled = df.resample(interval).agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        return resampled

# --- EJEMPLO DE USO EN TU ESTRATEGIA ---
# db_conf = {'host': 'localhost', ...}
# refiner = DataRefiner(db_conf)
# df_monthly = refiner.get_candles("BTC/USD", interval="1M")