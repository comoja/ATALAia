import os, sys, json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection

conn = dbConnection.getConnection()
cur = conn.cursor()

symbols = [
    "BTC/USD", "EUR/USD", "GBP/USD", "AUD/USD", "USD/JPY", 
    "XAU/USD", "XAG/USD", "US30", "NAS100", "SPX500", 
    "ETH/USD", "SOL/USD", "ADA/USD"
]

for sym in symbols:
    cur.execute("SELECT parametersJson FROM SymbolStrategyConfig WHERE strategy = 'GenericFVG' AND symbol = %s", (sym,))
    res = cur.fetchone()
    if res:
        config = json.loads(res[0]) if res[0] else {}
        config['min_rr'] = 2.0
        config['fvg_min_pct'] = 0.0001
        config['require_htf_sweep'] = False
        
        cur.execute("UPDATE SymbolStrategyConfig SET parametersJson = %s WHERE strategy = 'GenericFVG' AND symbol = %s", (json.dumps(config), sym))

conn.commit()
print("Configuración de la Base de Datos Actualizada correctamente!")
conn.close()
