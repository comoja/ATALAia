import asyncio
from middleware.database import dbConnection

async def main():
    try:
        pool = dbConnection.DBConnectionPool()
        conn = pool.get_connection()
        print("Got connection!")
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        print(cursor.fetchall())
        conn.close()
    except Exception as e:
        print(f"Error: {e}")

asyncio.run(main())
