#!/bin/zsh

# ==========================================================================
# ATALAIA SYSTEM - LIFECYCLE MANAGER
# Script para levantar/tirar la aplicación (Backend FastAPI + Frontend Java)
# ==========================================================================

# Variables en camelCase para adherencia estricta
scriptDir=$(cd "$(dirname "$0")" && pwd)

# Configuración de entorno para Java (JDK) y Maven si están en Homebrew
if [ -z "$JAVA_HOME" ] || [ ! -d "$JAVA_HOME" ]; then
    if [ -d "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" ]; then
        export JAVA_HOME="/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    elif [ -d "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" ]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    elif [ -d "/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home" ]; then
        export JAVA_HOME="/usr/local/opt/openjdk/libexec/openjdk.jdk/Contents/Home"
    elif [ -d "/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home" ]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk/libexec/openjdk.jdk/Contents/Home"
    fi
fi
if [ -n "$JAVA_HOME" ]; then
    export PATH="$JAVA_HOME/bin:/usr/local/bin:/opt/homebrew/bin:$PATH"
else
    export PATH="/usr/local/bin:/opt/homebrew/bin:$PATH"
fi

logsDir="$scriptDir/logs"
backendPidFile="$logsDir/backend.pid"
frontendPidFile="$logsDir/frontend.pid"
tailPid=""

# Asegurar que existe el directorio de logs
mkdir -p "$logsDir"

function cleanupAndExit() {
    echo -e "\n👋 Capturado Ctrl-C. Deteniendo todos los servicios de ATALA.ia..."
    if [ -n "$tailPid" ]; then
        kill "$tailPid" 2>/dev/null
    fi
    stopServices
    exit 0
}

function startServices() {
    # 1. Dar de baja servicios existentes preventivamente
    stopServices

    echo "=========================================================="
    echo "🚀 Levantando Sistema ATALA.ia (Aetherial UI)..."
    echo "=========================================================="

    # 2. Compilar el Frontend
    echo "📦 Compilando Frontend (Java 8 Maven)..."
    cd "$scriptDir/frontend" || exit 1
    if ! mvn clean package -DskipTests; then
        echo "❌ Error de compilación en el Frontend. Abortando inicio."
        cd "$scriptDir" || exit 1
        exit 1
    fi
    cd "$scriptDir" || exit 1

    # 3. Levantar el Backend (FastAPI)
    echo "⚡ Iniciando Backend en puerto 8004..."
    "$scriptDir/.venv/bin/python" "$scriptDir/backend/main.py" > "$logsDir/backend_output.log" 2>&1 &
    backendPid=$!
    echo "$backendPid" > "$backendPidFile"
    echo "✅ Backend levantado con éxito (PID: $backendPid)."

    # 4. Levantar el Frontend (Tomcat Embebido)
    echo "⚡ Iniciando Frontend (Tomcat Embebido) en puerto 8080..."
    java -jar "$scriptDir/frontend/target/correlation-frontend-1.0.0-SNAPSHOT.jar" > "$logsDir/frontend_output.log" 2>&1 &
    frontendPid=$!
    echo "$frontendPid" > "$frontendPidFile"
    echo "✅ Frontend (Tomcat) levantado con éxito (PID: $frontendPid)."

    echo "----------------------------------------------------------"
    echo "🎉 ¡Servicios iniciados con éxito!"
    echo "🌐 FastAPI Backend:   http://localhost:8004"
    echo "🌐 PrimeFaces Visual: http://localhost:8080/ATALA.ia/login.xhtml"
    echo "=========================================================="

    # 5. Configurar el trap para capturar Ctrl-C (SIGINT)
    trap cleanupAndExit INT

    echo "📊 Mostrando logs de Tomcat en tiempo real. Presiona Ctrl-C para detener todos los servicios..."
    echo "----------------------------------------------------------"
    tail -f "$logsDir/frontend_output.log" &
    tailPid=$!
    wait "$tailPid" 2>/dev/null
}

function stopServices() {
    echo "=========================================================="
    echo "🛑 Tirando Sistema ATALA.ia..."
    echo "=========================================================="

    # --- 1. APAGAR FRONTEND ---
    if [ -f "$frontendPidFile" ]; then
        frontendPid=$(cat "$frontendPidFile")
        echo "⚡ Deteniendo Frontend (PID: $frontendPid)..."
        kill "$frontendPid" 2>/dev/null
        # Dar un momento para cierre limpio
        sleep 2
        # Forzar si no ha cerrado
        if kill -0 "$frontendPid" 2>/dev/null; then
            echo "⚠️  El Frontend no respondió, forzando kill..."
            kill -9 "$frontendPid" 2>/dev/null
        fi
        rm -f "$frontendPidFile"
        echo "✅ Frontend detenido."
    else
        # Búsqueda preventiva por puerto 8080 si el pidfile no existe
        portPid=$(lsof -t -i:8080)
        if [ -n "$portPid" ]; then
            echo "⚡ Deteniendo proceso huérfano en puerto 8080 (PID: $portPid)..."
            kill "$portPid" 2>/dev/null
            sleep 1
            kill -9 "$portPid" 2>/dev/null
            echo "✅ Proceso en puerto 8080 detenido."
        else
            echo "ℹ️  No hay registros de Frontend activo."
        fi
    fi

    # --- 2. APAGAR BACKEND ---
    if [ -f "$backendPidFile" ]; then
        backendPid=$(cat "$backendPidFile")
        echo "⚡ Deteniendo Backend (PID: $backendPid)..."
        kill "$backendPid" 2>/dev/null
        sleep 1
        if kill -0 "$backendPid" 2>/dev/null; then
            echo "⚠️  El Backend no respondió, forzando kill..."
            kill -9 "$backendPid" 2>/dev/null
        fi
        rm -f "$backendPidFile"
        echo "✅ Backend detenido."
    else
        # Búsqueda preventiva por puerto 8004 si el pidfile no existe
        portPid=$(lsof -t -i:8004)
        if [ -n "$portPid" ]; then
            echo "⚡ Deteniendo proceso huérfano en puerto 8004 (PID: $portPid)..."
            kill "$portPid" 2>/dev/null
            sleep 1
            kill -9 "$portPid" 2>/dev/null
            echo "✅ Proceso en puerto 8004 detenido."
        else
            echo "ℹ️  No hay registros de Backend activo."
        fi
    fi

    echo "✅ Todos los servicios se han detenido."
    echo "=========================================================="
}

function showStatus() {
    echo "=========================================================="
    echo "📊 Estado del Sistema ATALA.ia"
    echo "=========================================================="

    # Estado Backend
    if [ -f "$backendPidFile" ]; then
        backendPid=$(cat "$backendPidFile")
        if kill -0 "$backendPid" 2>/dev/null; then
            echo "🟢 Backend (FastAPI):  ACTIVO (PID: $backendPid) | http://localhost:8004"
        else
            echo "🔴 Backend (FastAPI):  INACTIVO (PID muerto)"
        fi
    else
        echo "🔴 Backend (FastAPI):  INACTIVO"
    fi

    # Estado Frontend
    if [ -f "$frontendPidFile" ]; then
        frontendPid=$(cat "$frontendPidFile")
        if kill -0 "$frontendPid" 2>/dev/null; then
            echo "🟢 Frontend (Java):    ACTIVO (PID: $frontendPid) | http://localhost:8080/ATALA.ia/login.xhtml"
        else
            echo "🔴 Frontend (Java):    INACTIVO (PID muerto)"
        fi
    else
        echo "🔴 Frontend (Java):    INACTIVO"
    fi
    echo "=========================================================="
}

# Evaluar argumento de entrada
case "$1" in
    start)
        startServices
        ;;
    stop)
        stopServices
        ;;
    status)
        showStatus
        ;;
    restart)
        stopServices
        sleep 1
        startServices
        ;;
    *)
        echo "Uso: $0 {start|stop|status|restart}"
        exit 1
        ;;
esac

exit 0
