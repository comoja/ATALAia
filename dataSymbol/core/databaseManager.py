import mysql.connector
import pandas as pd

class DatabaseManager:
    def __init__(self, dbConfig):
        self.dbConfig = dbConfig

    def getConnection(self):
        return mysql.connector.connect(**self.dbConfig)

    def getLastTimestamp(self, symbol):
        conn = self.getConnection()
        cursor = conn.cursor()
        query = "SELECT MAX(timestamp) FROM candles WHERE symbol = %s"
        cursor.execute(query, (symbol,))
        result = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return result if result else None

    def saveCandlesBulk(self, df, symbol):
        conn = self.getConnection()
        cursor = conn.cursor()
        
        # Preparar datos para INSERT IGNORE
        dfReset = df.reset_index()
        dataToInsert = [
            (symbol, row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'), 
             row['open'], row['high'], row['low'], row['close'], row['volume'])
            for _, row in dfReset.iterrows()
        ]

        sql = """
            INSERT IGNORE INTO candles 
            (symbol, timestamp, open, high, low, close, volume) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        
        try:
            cursor.executemany(sql, dataToInsert)
            conn.commit()
            return cursor.rowcount
        finally:
            cursor.close()
            conn.close()