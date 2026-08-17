from middleware.database import dbConnection

def list_users():
    conn = dbConnection.getConnection()
    if not conn:
        print("No se pudo conectar a la base de datos")
        return
    cur = conn.cursor(dictionary=True)
    try:
        cur.execute("SELECT idUsuario, username, passwordHash, idRole, status FROM Usuario")
        users = cur.fetchall()
        print("\n--- Usuarios ---")
        for u in users:
            print(f"ID: {u['idUsuario']} | Username: {u['username']} | PasswordHash: {u['passwordHash']} | Role: {u['idRole']} | Status: {u['status']}")
            
        cur.execute("SELECT idRole, nameRole FROM Role")
        roles = cur.fetchall()
        print("\n--- Roles ---")
        for r in roles:
            print(f"ID: {r['idRole']} | Name: {r['nameRole']}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    list_users()
