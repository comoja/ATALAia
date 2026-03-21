from config.dbConfig import DB_CONFIG
from core.databaseManager import DatabaseManager
from core.apiClient import ApiClient
from datetime import datetime, timedelta
import json

def runOrchestrator():
    db = DatabaseManager(DB_CONFIG)
    api = ApiClient(['KEY1', 'KEY2', 'KEY3'])
    
    # Cargar símbolos del JSON
    with open('config/symbols.json', 'r') as f:
        symbolsData = json.load(f)['symbols']

    for item in symbolsData:
        symbol = item['name']
        # Si no hay fecha en DB, usar 2005-01-01
        lastDate = db.getLastTimestamp(symbol)
        if not lastDate:
            lastDate = datetime.strptime(item['start_date'], '%Y-%m-%d')
        
        while lastDate < datetime.now() - timedelta(minutes=5):
            df = api.fetchFiveMinuteCandles(symbol, lastDate)
            
            if df.empty:
                print(f"⚠️ No más datos para {symbol}")
                break
                
            inserted = db.saveCandlesBulk(df, symbol)
            lastDate = df.index.max()
            
            # Calcular progreso (aprox)
            totalDays = (datetime.now() - datetime(2005, 1, 1)).days
            currentDays = (lastDate - datetime(2005, 1, 1)).days
            progress = (currentDays / totalDays) * 100
            
            print(f"[{symbol}] {progress:.2f}% completado. Última: {lastDate}")

if __name__ == "__main__":
    runOrchestrator()