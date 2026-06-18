from middleware.database.dbConnection import getConnection
conn = getConnection()
cursor = conn.cursor()
cursor.execute("DESCRIBE symbolNotStrategia")
for row in cursor.fetchall():
    print(row)
cursor.close()
conn.close()
