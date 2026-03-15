# Crear directorio principal y subpaquetes
mkdir -p app/core app/database app/api app/utils

# Crear los archivos __init__.py (necesarios para que Python los reconozca como paquetes)
touch app/__init__.py app/core/__init__.py app/database/__init__.py app/api/__init__.py app/utils/__init__.py

# Crear archivos de lógica
touch app/core/engine.py app/core/strategy.py
touch app/database/mysql_client.py
touch app/api/twelvedata.py app/api/telegram_bot.py
touch main.py .env requirements.txt

echo "Estructura de paquetes Python creada con éxito."
echo "Ubicación: app"