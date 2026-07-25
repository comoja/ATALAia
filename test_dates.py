import sys
import os
from datetime import datetime

# Add the project root to sys.path
sys.path.append('/Volumes/TimeMachine/ATALAia')

from middleware.database.dbConnection import DBConnectionPool

pool = DBConnectionPool()
conn = pool.get_connection()
cursor = conn.cursor()

cursor.execute("SELECT symbol, MAX(priceDate), COUNT(*) FROM atalaia.stockprices GROUP BY symbol;")
for row in cursor.fetchall():
    print(row)

cursor.close()
conn.close()
