import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from middleware.database import dbConnection

conn = dbConnection.getConnection()
cur = conn.cursor()
cur.execute("DESCRIBE strategyConfig")
for r in cur.fetchall(): print(r)
cur.execute("DESCRIBE SymbolStrategyConfig")
for r in cur.fetchall(): print(r)
conn.close()
