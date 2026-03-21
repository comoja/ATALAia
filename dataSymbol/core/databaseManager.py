import mysql.connector

class DatabaseManager:
    def __init__(self, dbConfig):
        self.dbConfig = dbConfig

    def saveBulkData(self, dataFrame, symbol):
        # Implementación de executemany con INSERT IGNORE
        print(f"Saving data for {symbol}...")

    def getLastTimestamp(self, symbol):
        # Consulta SELECT MAX(timestamp)
        pass