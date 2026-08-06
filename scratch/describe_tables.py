import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

conn = dbConnection.getConnection()
cursor = conn.cursor()
cursor.execute("DESCRIBE strategyConfig")
print("strategyConfig columns:", [row[0] for row in cursor.fetchall()])
cursor.execute("DESCRIBE symbolStrategyConfig")
print("symbolStrategyConfig columns:", [row[0] for row in cursor.fetchall()])
cursor.close()
conn.close()
