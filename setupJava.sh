# Definir la ruta base del paquete
PACKAGE_PATH="src/main/java/com/tradingbot"

# 1. Crear estructura de directorios de paquetes
mkdir -p $PACKAGE_PATH/config
mkdir -p $PACKAGE_PATH/controller
mkdir -p $PACKAGE_PATH/entity
mkdir -p $PACKAGE_PATH/repository
mkdir -p $PACKAGE_PATH/service
mkdir -p $PACKAGE_PATH/util

# 2. Crear directorios de recursos y web
mkdir -p src/main/resources/META-INF
mkdir -p src/main/webapp/WEB-INF
mkdir -p src/main/webapp/templates
mkdir -p src/main/webapp/resources/css

# 3. Crear archivos base (esqueletos)
touch $PACKAGE_PATH/entity/Bitacora.java
touch $PACKAGE_PATH/entity/Cuenta.java
touch $PACKAGE_PATH/entity/OperacionCerrada.java
touch $PACKAGE_PATH/controller/BotController.java
touch $PACKAGE_PATH/controller/ReporteController.java
touch $PACKAGE_PATH/service/TradingService.java
touch $PACKAGE_PATH/util/JpaUtil.java
touch src/main/resources/META-INF/persistence.xml
touch src/main/webapp/dashboard.xhtml
touch src/main/webapp/reportes.xhtml

echo "✅ Estructura de Java creada exitosamente."
echo "📍 Ubicación: $PACKAGE_PATH"