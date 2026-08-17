import os

# Nombre del proyecto
BASE_DIR = "dataSymbol"

# Definición de la estructura: {directorio: [archivos]}
structure = {
    
    "core": ["__init__.py", "database.py", "api_client.py", "data_refiner.py"],
    "logs": ["dataSymbol.log", "errors.log"],
    "scripts": ["validator.py", "migrate_db.sql"],
    "strategies": ["__init__.py", "base_strategy.py", "monthly_angle.py"],
}

root_files = ["main_orchestrator.py", "requirements.txt", "README.md"]

# Estructura: {directorio: [archivos]}
structure = {
    "config": ["dbConfig.py", "telegramConfig.py", "symbols.json"],
    "core": ["databaseManager.py", "apiClient.py", "dataRefiner.py"],
    "logs": ["sentinel.log", "errors.log"],
    "scripts": ["dataValidator.py", "migrateDb.sql"],
    "strategies": ["baseStrategy.py", "monthlyAngle.py"],
}

rootFiles = ["mainOrchestrator.py", "requirements.txt", "README.md"]

def createProjectStructure():
    # Crear carpeta raíz
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
        print(f"📁 Proyecto: {BASE_DIR}")

    # Crear subdirectorios y archivos
    for folder, files in structure.items():
        folderPath = os.path.join(BASE_DIR, folder)
        os.makedirs(folderPath, exist_ok=True)
        
        # Crear un __init__.py en cada paquete para que Python lo reconozca
        with open(os.path.join(folderPath, "__init__.py"), 'w') as f:
            pass

        for file in files:
            filePath = os.path.join(folderPath, file)
            with open(filePath, 'w') as f:
                if file.endswith('.py'):
                    f.write(f"# Componente: {file}\n# Proyecto: AtalaIA\n\n")
                elif file == 'symbols.json':
                    f.write('{"symbols": []}')
            print(f"    📄 {folder}/{file}")

    # Crear archivos en la raíz
    for file in rootFiles:
        filePath = os.path.join(BASE_DIR, file)
        with open(filePath, 'w') as f:
            if file == 'requirements.txt':
                f.write("pandas\nmysql-connector-python\nrequests\npython-dotenv\n")
        print(f"  📄 {file}")

    print("\n✅ Estructura CamelCase generada con éxito.")



def create_structure():
    # Crear carpeta raíz
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
        print(f"📁 Creada carpeta raíz: {BASE_DIR}")

    # Crear subdirectorios y archivos
    for folder, files in structure.items():
        folder_path = os.path.join(BASE_DIR, folder)
        os.makedirs(folder_path, exist_ok=True)
        print(f"  📂 Carpeta: {folder}")
        
        for file in files:
            file_path = os.path.join(folder_path, file)
            with open(file_path, 'w') as f:
                if file.endswith('.py') and file != '__init__.py':
                    f.write(f"# Módulo: {file}\n# Proyecto: Sentinel (ATALA.ia)\n\n")
                elif file == 'symbols.json':
                    f.write('{"symbols": []}')
            print(f"    📄 Archivo: {file}")

    # Crear archivos en la raíz
    for file in root_files:
        file_path = os.path.join(BASE_DIR, file)
        with open(file_path, 'w') as f:
            if file == 'requirements.txt':
                f.write("pandas\nmysql-connector-python\nrequests\npython-dotenv\n")
        print(f"  📄 Archivo raíz: {file}")

    print("\n✅ Estructura de Sentinel generada con éxito.")
    



if __name__ == "__main__":
    create_structure()