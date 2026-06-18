from middleware.database.dbConnection import getConnection

def block_losing_symbols():
    symbols_to_block = [
        'GBP/USD', 'AUD/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 
        'EUR/GBP', 'GBP/CAD', 'GBP/JPY', 'USD/JPY', 'USD/MXN', 
        'XAU/USD', 'BTC/USD'
    ]
    strategy = 'BreakoutProbability'
    reason = 'Bloqueado por optimizador intensivo: Profit Factor < 1.0 (Lateralidad extrema)'
    
    try:
        conn = getConnection()
        cursor = conn.cursor()
        
        for sym in symbols_to_block:
            # Check if it's already there to avoid duplicates if there's no IGNORE or UPSERT
            cursor.execute("SELECT 1 FROM symbolNotStrategia WHERE symbol = %s AND strategy = %s", (sym, strategy))
            if not cursor.fetchone():
                cursor.execute(
                    "INSERT INTO symbolNotStrategia (symbol, strategy, reason) VALUES (%s, %s, %s)",
                    (sym, strategy, reason)
                )
                print(f"Bloqueado: {sym}")
            else:
                print(f"Ya estaba bloqueado: {sym}")
                
        conn.commit()
        cursor.close()
        conn.close()
        print("Bloqueos completados.")
    except Exception as e:
        print(f"Error: {e}")

block_losing_symbols()
