#!/bin/bash

# ==============================================================================
# ATALA.ia - Script de Gestión de Servicios Unificados
# Controla: MT5 Wine Bridge (8005), FastAPI Backend (8004) y Tomcat Frontend (8080)
# ==============================================================================

scriptDir=$(cd "$(dirname "$0")" && pwd)

# Detección de Java Home
if [ -z "$JAVA_HOME" ]; then
    if [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
        export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
    elif [ -d "/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home" ]; then
        export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
    fi
fi
if [ -n "$JAVA_HOME" ]; then
    export PATH="$JAVA_HOME/bin:$HOME/.local/share/maven/default-maven/bin:$HOME/.local/bin:/usr/local/bin:/opt/homebrew/bin:$PATH"
else
    export PATH="$HOME/.local/share/maven/default-maven/bin:$HOME/.local/bin:/usr/local/bin:/opt/homebrew/bin:$PATH"
fi

# Detección del intérprete de Python nativo
if [ -x "/home/jcolinm/.venvs/Sistema/bin/python" ]; then
    pythonCmd="/home/jcolinm/.venvs/Sistema/bin/python"
    uvicornCmd="/home/jcolinm/.venvs/Sistema/bin/uvicorn"
elif [ -x "$scriptDir/.venv/bin/python" ]; then
    pythonCmd="$scriptDir/.venv/bin/python"
    uvicornCmd="$scriptDir/.venv/bin/uvicorn"
else
    pythonCmd="python3"
    uvicornCmd="uvicorn"
fi

# Detección de Python en Wine
winePython="$HOME/.wine/drive_c/Python311/python.exe"

logsDir="$scriptDir/logs"
bridgePidFile="$logsDir/mt5_bridge.pid"
backendPidFile="$logsDir/backend.pid"
frontendPidFile="$logsDir/frontend.pid"
tailPid=""

mkdir -p "$logsDir"

cleanupAndExit() {
    echo -e "\n👋 Deteniendo servicios de ATALA.ia..."
    if [ -n "$tailPid" ]; then
        kill "$tailPid" 2>/dev/null
    fi
    stopServices
    exit 0
}

startServices() {
    local isDaemon="$1"

    # 1. Detener procesos existentes para evitar conflictos
    stopServices

    echo "=========================================================="
    echo "🚀 Levantando Sistema ATALA.ia (Aetherial UI)..."
    echo "=========================================================="

    # 2. Levantar MT5 Bridge si existe Wine Python (puerto 8005)
    if [ -f "$winePython" ]; then
        echo "⚡ Iniciando MT5 Wine Bridge en puerto 8005..."
        if [ -z "$DISPLAY" ] && command -v xvfb-run &>/dev/null; then
            xvfb-run -a wine "$winePython" "$scriptDir/middleware/api/mt5_bridge_server.py" > "$logsDir/mt5_bridge_output.log" 2>&1 &
        else
            WINEDEBUG=-all wine "$winePython" "$scriptDir/middleware/api/mt5_bridge_server.py" > "$logsDir/mt5_bridge_output.log" 2>&1 &
        fi
        bridgePid=$!
        echo "$bridgePid" > "$bridgePidFile"
        sleep 1
        echo "✅ MT5 Bridge levantado (PID: $bridgePid) | http://localhost:8005"
    fi

    # 3. Compilar Frontend sólo si no existe el JAR
    jarPath="$scriptDir/frontend/target/correlation-frontend-1.0.0-SNAPSHOT.jar"
    if [ ! -f "$jarPath" ]; then
        echo "📦 Preparando y compilando Frontend (Java Spring Boot + Maven)..."
        mkdir -p "$HOME/.build-cache/atalaia-frontend/target"
        if [ ! -L "$scriptDir/frontend/target" ]; then
            rm -rf "$scriptDir/frontend/target"
            ln -sfn "$HOME/.build-cache/atalaia-frontend/target" "$scriptDir/frontend/target"
        fi
        cd "$scriptDir/frontend" || exit 1
        mvn package -DskipTests
        cd "$scriptDir" || exit 1
    fi

    # 4. Levantar el Backend con Hot-Reload (FastAPI - puerto 8004)
    echo "⚡ Iniciando Backend FastAPI en puerto 8004 con Auto-Reload..."
    cd "$scriptDir" || exit 1
    "$uvicornCmd" backend.main:app --host 0.0.0.0 --port 8004 --reload > "$logsDir/backend_output.log" 2>&1 &
    backendPid=$!
    echo "$backendPid" > "$backendPidFile"
    echo "✅ Backend levantado con éxito (PID: $backendPid)."

    # 5. Levantar el Frontend (Tomcat Embebido - puerto 8080)
    if [ -f "$jarPath" ]; then
        echo "⚡ Iniciando Frontend (Tomcat Embebido) en puerto 8080..."
        java -jar "$jarPath" > "$logsDir/frontend_output.log" 2>&1 &
        frontendPid=$!
        echo "$frontendPid" > "$frontendPidFile"
        echo "✅ Frontend (Tomcat) levantado con éxito (PID: $frontendPid)."
    else
        echo "⚠️ No se encontró el JAR del Frontend en $jarPath"
    fi

    echo "----------------------------------------------------------"
    echo "🎉 ¡Servicios de ATALA.ia iniciados con éxito!"
    echo "🔌 MT5 Wine Bridge:   http://localhost:8005"
    echo "🌐 FastAPI Backend:   http://localhost:8004"
    echo "🌐 PrimeFaces Visual: http://localhost:8080/ATALA.ia/login.xhtml"
    echo "=========================================================="

    if [ "$isDaemon" = "daemon" ] || [ "$isDaemon" = "--daemon" ]; then
        trap cleanupAndExit INT TERM
        while true; do sleep 3600; done
    fi
}

stopServices() {
    echo "=========================================================="
    echo "🛑 Deteniendo Sistema ATALA.ia..."
    echo "=========================================================="

    # --- 1. APAGAR FRONTEND ---
    if [ -f "$frontendPidFile" ]; then
        frontendPid=$(cat "$frontendPidFile")
        echo "⚡ Deteniendo Frontend (PID: $frontendPid)..."
        kill "$frontendPid" 2>/dev/null
        sleep 1
        if kill -0 "$frontendPid" 2>/dev/null; then
            kill -9 "$frontendPid" 2>/dev/null
        fi
        rm -f "$frontendPidFile"
        echo "✅ Frontend detenido."
    fi
    portPid=$(lsof -t -i:8080 2>/dev/null)
    if [ -n "$portPid" ]; then
        kill -9 "$portPid" 2>/dev/null
    fi

    # --- 2. APAGAR BACKEND ---
    if [ -f "$backendPidFile" ]; then
        backendPid=$(cat "$backendPidFile")
        echo "⚡ Deteniendo Backend (PID: $backendPid)..."
        kill "$backendPid" 2>/dev/null
        sleep 1
        if kill -0 "$backendPid" 2>/dev/null; then
            kill -9 "$backendPid" 2>/dev/null
        fi
        rm -f "$backendPidFile"
        echo "✅ Backend detenido."
    fi
    portPid=$(lsof -t -i:8004 2>/dev/null)
    if [ -n "$portPid" ]; then
        kill -9 "$portPid" 2>/dev/null
    fi

    # --- 3. APAGAR MT5 BRIDGE ---
    if [ -f "$bridgePidFile" ]; then
        bridgePid=$(cat "$bridgePidFile")
        echo "⚡ Deteniendo MT5 Wine Bridge (PID: $bridgePid)..."
        kill "$bridgePid" 2>/dev/null
        sleep 1
        if kill -0 "$bridgePid" 2>/dev/null; then
            kill -9 "$bridgePid" 2>/dev/null
        fi
        rm -f "$bridgePidFile"
        echo "✅ MT5 Wine Bridge detenido."
    fi
    portPid=$(lsof -t -i:8005 2>/dev/null)
    if [ -n "$portPid" ]; then
        kill -9 "$portPid" 2>/dev/null
    fi

    echo "✅ Servicios de ATALA.ia detenidos."
    echo "=========================================================="
}

showStatus() {
    echo "=========================================================="
    echo "📊 Estado del Sistema ATALA.ia"
    echo "=========================================================="

    # Estado MT5 Bridge
    if [ -f "$bridgePidFile" ] && kill -0 "$(cat "$bridgePidFile" 2>/dev/null)" 2>/dev/null; then
        echo "🟢 MT5 Wine Bridge:   ACTIVO (PID: $(cat "$bridgePidFile")) | http://localhost:8005"
    else
        echo "🔴 MT5 Wine Bridge:   INACTIVO"
    fi

    # Estado Backend
    if [ -f "$backendPidFile" ] && kill -0 "$(cat "$backendPidFile" 2>/dev/null)" 2>/dev/null; then
        echo "🟢 Backend (FastAPI):  ACTIVO (PID: $(cat "$backendPidFile")) | http://localhost:8004"
    else
        echo "🔴 Backend (FastAPI):  INACTIVO"
    fi

    # Estado Frontend
    if [ -f "$frontendPidFile" ] && kill -0 "$(cat "$frontendPidFile" 2>/dev/null)" 2>/dev/null; then
        echo "🟢 Frontend (Java):    ACTIVO (PID: $(cat "$frontendPidFile")) | http://localhost:8080/ATALA.ia/login.xhtml"
    else
        echo "🔴 Frontend (Java):    INACTIVO"
    fi
    echo "=========================================================="
}

case "$1" in
    start|-start|--start)
        startServices "$2"
        ;;
    daemon|--daemon)
        startServices "daemon"
        ;;
    stop|-stop|--stop)
        stopServices
        ;;
    status|-status|--status)
        showStatus
        ;;
    restart|-restart|--restart)
        stopServices
        sleep 1
        startServices "$2"
        ;;
    *)
        echo "Uso: $0 {start|stop|status|restart|daemon}"
        exit 1
        ;;
esac

exit 0
