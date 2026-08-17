from middleware.database import dbConnection

def add_menu():
    conn = dbConnection.getConnection()
    if not conn:
        print("No se pudo conectar a la base de datos")
        return
    cur = conn.cursor(dictionary=True)
    try:
        # 1. Verificar si el menú ya existe
        cur.execute("SELECT idMenu FROM Menu WHERE url = 'configuracion.xhtml'")
        menu = cur.fetchone()
        
        if not menu:
            # 2. Insertar el menú de Configuración
            cur.execute(
                "INSERT INTO Menu (nameMenu, url, icon, parentId) VALUES ('Configuración', 'configuracion.xhtml', 'pi pi-sliders-h', NULL)"
            )
            id_menu = cur.lastrowid
            print(f"Menú 'Configuración' insertado con ID: {id_menu}")
        else:
            id_menu = menu['idMenu']
            print(f"El menú 'Configuración' ya existe con ID: {id_menu}")
            
        # 3. Vincular al rol de Administrador (idRole = 1)
        cur.execute(
            "INSERT IGNORE INTO RoleMenu (idRole, idMenu) VALUES (1, %s)",
            (id_menu,)
        )
        print("Vínculo de RoleMenu establecido para el Administrador")
        
        conn.commit()
        print("Transacción de menú confirmada con éxito!")
    except Exception as e:
        conn.rollback()
        print(f"Error al agregar menú: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    add_menu()
