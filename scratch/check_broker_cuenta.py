import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_broker_cuenta():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    
    print("📋 Registros en la tabla BrokerCuenta:")
    cursor.execute("SELECT * FROM BrokerCuenta")
    rows = cursor.fetchall()
    for r in rows:
        print(r)
        
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_broker_cuenta()
