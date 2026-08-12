import sys
import os

project_root = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, project_root)

from middleware.database import dbConnection

# Mapeo de equivalencias para FOREX, MT5 y TradingView
SYMBOL_PROVIDER_MAPPING = {
    'AUD/USD': {'FOREX': 'AUD/USD', 'MT5': 'AUDUSD', 'TradingView': 'AUDUSD'},
    'BTC/USD': {'FOREX': 'BTC/USD', 'MT5': 'BTCUSD', 'TradingView': 'BTCUSD'},
    'EUR/GBP': {'FOREX': 'EUR/GBP', 'MT5': 'EURGBP', 'TradingView': 'EURGBP'},
    'EUR/USD': {'FOREX': 'EUR/USD', 'MT5': 'EURUSD', 'TradingView': 'EURUSD'},
    'GBP/CAD': {'FOREX': 'GBP/CAD', 'MT5': 'GBPCAD', 'TradingView': 'GBPCAD'},
    'GBP/JPY': {'FOREX': 'GBP/JPY', 'MT5': 'GBPJPY', 'TradingView': 'GBPJPY'},
    'GBP/USD': {'FOREX': 'GBP/USD', 'MT5': 'GBPUSD', 'TradingView': 'GBPUSD'},
    'NAS100':  {'FOREX': 'US 100',  'MT5': 'NAS100', 'TradingView': 'NAS100'},
    'NZD/JPY': {'FOREX': 'NZD/JPY', 'MT5': 'NZDJPY', 'TradingView': 'NZDJPY'},
    'NZD/USD': {'FOREX': 'NZD/USD', 'MT5': 'NZDUSD', 'TradingView': 'NZDUSD'},
    'SPX500':  {'FOREX': 'US 500',  'MT5': 'SPX500', 'TradingView': 'SPX500'},
    'USD/CAD': {'FOREX': 'USD/CAD', 'MT5': 'USDCAD', 'TradingView': 'USDCAD'},
    'USD/CHF': {'FOREX': 'USD/CHF', 'MT5': 'USDCHF', 'TradingView': 'USDCHF'},
    'USD/HKD': {'FOREX': 'USD/HKD', 'MT5': 'USDHKD', 'TradingView': 'USDHKD'},
    'USD/JPY': {'FOREX': 'USD/JPY', 'MT5': 'USDJPY', 'TradingView': 'USDJPY'},
    'USD/MXN': {'FOREX': 'USD/MXN', 'MT5': 'USDMXN', 'TradingView': 'USDMXN'},
    'XAG/USD': {'FOREX': 'Silver',  'MT5': 'XAGUSD', 'TradingView': 'XAGUSD'},
    'XAU/USD': {'FOREX': 'Gold',    'MT5': 'XAUUSD', 'TradingView': 'XAUUSD'},
    'XBR/USD': {'FOREX': 'UK Crude Oil', 'MT5': 'XBRUSD', 'TradingView': 'UKOIL'},
    'XTI/USD': {'FOREX': 'US Crude Oil', 'MT5': 'XTIUSD', 'TradingView': 'USOIL'},
}

def populate_provider_symbols():
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    
    print("1. Poblando las columnas FOREX, MT5 y TradingView en `symbols`...")
    updated_count = 0
    for sym, mapping in SYMBOL_PROVIDER_MAPPING.items():
        cursor.execute(
            "UPDATE symbols SET FOREX = %s, MT5 = %s, TradingView = %s WHERE symbol = %s;",
            (mapping['FOREX'], mapping['MT5'], mapping['TradingView'], sym)
        )
        updated_count += cursor.rowcount
        
    conn.commit()
    print(f"✅ Se actualizaron {updated_count} registros en la tabla `symbols`.")

    print("\n2. Estado resultante de los símbolos:")
    cursor.close()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT symbol, FOREX, MT5, TradingView FROM symbols ORDER BY symbol;")
    for row in cursor.fetchall():
        print(f"  {row['symbol']:<10} | FOREX: {row['FOREX']:<15} | MT5: {row['MT5']:<10} | TradingView: {row['TradingView']:<10}")

    cursor.close()
    conn.close()

if __name__ == '__main__':
    populate_provider_symbols()
