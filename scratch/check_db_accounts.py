import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check_accounts():
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT idCuenta, Nombre, Capital, riesgoPorOperacion FROM cuenta")
    rows = cursor.fetchall()
    print("📋 Cuentas registradas:")
    for r in rows:
        print(f"  - ID: {r['idCuenta']}, Nombre: {r['Nombre']}, Capital: {r['Capital']}, Riesgo: {r['riesgoPorOperacion']}%")
    cursor.close()
    conn.close()

if __name__ == "__main__":
    check_accounts()
