#!/bin/bash

scriptDir=$(cd "$(dirname "$0")" && pwd)

# Cargar variables de entorno del sistema
if [ -f "$scriptDir/.env" ]; then
    set -a
    source "$scriptDir/.env"
    set +a
fi

# Detección del JDK/Java nativo
if [ -n "$JAVA_HOME" ] && [ -x "$JAVA_HOME/bin/java" ]; then
    javaCmd="$JAVA_HOME/bin/java"
elif [ -x "$HOME/.local/share/jvm/default-jdk/bin/java" ]; then
    javaCmd="$HOME/.local/share/jvm/default-jdk/bin/java"
    export JAVA_HOME="$HOME/.local/share/jvm/default-jdk"
    export PATH="$JAVA_HOME/bin:$PATH"
elif [ -x "/home/jcolinm/.local/share/jvm/default-jdk/bin/java" ]; then
    javaCmd="/home/jcolinm/.local/share/jvm/default-jdk/bin/java"
    export JAVA_HOME="/home/jcolinm/.local/share/jvm/default-jdk"
    export PATH="$JAVA_HOME/bin:$PATH"
elif command -v java &>/dev/null; then
    javaCmd="java"
else
    javaCmd="java"
fi

if [ -x "$HOME/.local/share/maven/default-maven/bin/mvn" ]; then
    mvnCmd="$HOME/.local/share/maven/default-maven/bin/mvn"
elif [ -x "/home/jcolinm/.local/share/maven/default-maven/bin/mvn" ]; then
    mvnCmd="/home/jcolinm/.local/share/maven/default-maven/bin/mvn"
elif command -v mvn &>/dev/null; then
    mvnCmd="mvn"
else
    mvnCmd="mvn"
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
    local buildFlag="$2"

    # 1. Detener procesos existentes para evitar conflictos
    stopServices

    echo "=========================================================="
    echo "🚀 Levantando Sistema ATALA.ia (Aetherial UI)..."
    echo "=========================================================="

    # 1.1 Verificar disponibilidad de MySQL antes de levantar servicios
    echo "🔍 Verificando disponibilidad de MySQL en 127.0.0.1:3306..."
    for i in {1..30}; do
        if timeout 1 bash -c "</dev/tcp/127.0.0.1/3306" 2>/dev/null; then
            echo "✅ Conexión con MySQL establecida."
            break
        fi
        echo "⏳ Esperando a que MySQL esté disponible ($i/30)..."
        sleep 1
    done

    # 2. Levantar MT5 Bridge si existe Wine Python (puerto 8005)
    if [ -f "$winePython" ]; then
        echo "⚡ Iniciando MT5 Wine Bridge en puerto 8005..."
        if [ -z "$DISPLAY" ] && command -v xvfb-run &>/dev/null; then
            nohup xvfb-run -a wine "$winePython" "$scriptDir/middleware/api/mt5_bridge_server.py" > "$logsDir/mt5_bridge_output.log" 2>&1 &
        else
            nohup env WINEDEBUG=-all wine "$winePython" "$scriptDir/middleware/api/mt5_bridge_server.py" > "$logsDir/mt5_bridge_output.log" 2>&1 &
        fi
        bridgePid=$!
        disown $bridgePid 2>/dev/null || true
        echo "$bridgePid" > "$bridgePidFile"
        sleep 1
        echo "✅ MT5 Bridge levantado (PID: $bridgePid) | http://localhost:8005"
    fi

    # 3. Levantar el Backend con Hot-Reload (FastAPI - puerto 8004)
    echo "⚡ Iniciando Backend FastAPI en puerto 8004 con Auto-Reload..."
    cd "$scriptDir" || exit 1
    nohup "$uvicornCmd" backend.main:app --host 0.0.0.0 --port 8004 --reload > "$logsDir/backend_output.log" 2>&1 &
    backendPid=$!
    disown $backendPid 2>/dev/null || true
    echo "$backendPid" > "$backendPidFile"
    echo "✅ Backend levantado con éxito (PID: $backendPid)."

    # Verificar que el backend responda en puerto 8004
    for i in {1..15}; do
        if timeout 1 bash -c "</dev/tcp/127.0.0.1/8004" 2>/dev/null; then
            echo "✅ Backend FastAPI respondiendo en http://localhost:8004"
            break
        fi
        sleep 1
    done

    # 4. Preparar y compilar Frontend solo si falta el JAR o si se pide rebuild
    cacheTarget="$HOME/.build-cache/atalaia-frontend/target"
    mkdir -p "$cacheTarget"
    if [ ! -L "$scriptDir/frontend/target" ]; then
        rm -rf "$scriptDir/frontend/target" 2>/dev/null || true
        ln -sfn "$cacheTarget" "$scriptDir/frontend/target" 2>/dev/null || true
    fi

    jarPath="$cacheTarget/correlation-frontend-1.0.0-SNAPSHOT.jar"
    if [ ! -f "$jarPath" ]; then
        jarPath="$scriptDir/frontend/target/correlation-frontend-1.0.0-SNAPSHOT.jar"
    fi

    if [ ! -f "$jarPath" ] || [ "$buildFlag" = "build" ] || [ "$buildFlag" = "--build" ] || [ "$isDaemon" = "build" ]; then
        echo "📦 Compilando Frontend (Java Spring Boot + Maven)..."
        cd "$scriptDir/frontend" || exit 1
        "$mvnCmd" package -DskipTests || true
        cd "$scriptDir" || exit 1
        jarPath="$cacheTarget/correlation-frontend-1.0.0-SNAPSHOT.jar"
    else
        echo "⚡ Usando JAR existente de Frontend (Arranque Instantáneo)"
    fi

    # 5. Levantar el Frontend (Tomcat Embebido - puerto 8080)
    if [ -f "$jarPath" ]; then
        echo "⚡ Iniciando Frontend (Tomcat Embebido) en puerto 8080..."
        nohup "$javaCmd" -jar "$jarPath" > "$logsDir/frontend_output.log" 2>&1 &
        frontendPid=$!
        disown $frontendPid 2>/dev/null || true
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
        while true; do
            # Watchdog Backend FastAPI (Puerto 8004)
            if [ -n "$backendPid" ] && ! kill -0 "$backendPid" 2>/dev/null; then
                echo "⚠️ [Daemon Watchdog] Backend FastAPI (PID $backendPid) no responde o terminó. Reiniciando..."
                cd "$scriptDir" || exit 1
                nohup "$uvicornCmd" backend.main:app --host 0.0.0.0 --port 8004 --reload >> "$logsDir/backend_output.log" 2>&1 &
                backendPid=$!
                disown $backendPid 2>/dev/null || true
                echo "$backendPid" > "$backendPidFile"
            fi
            # Watchdog Frontend Tomcat (Puerto 8080)
            if [ -n "$frontendPid" ] && ! kill -0 "$frontendPid" 2>/dev/null; then
                echo "⚠️ [Daemon Watchdog] Frontend Tomcat (PID $frontendPid) no responde o terminó. Reiniciando..."
                nohup "$javaCmd" -jar "$jarPath" >> "$logsDir/frontend_output.log" 2>&1 &
                frontendPid=$!
                disown $frontendPid 2>/dev/null || true
                echo "$frontendPid" > "$frontendPidFile"
            fi
            sleep 5 & wait $!
        done
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
    wineserver -k 2>/dev/null || true
    pkill -9 -f "winedevice.exe" 2>/dev/null || true
    pkill -9 -f "terminal64.exe" 2>/dev/null || true
    pkill -9 -f "wine" 2>/dev/null || true

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
        startServices "$2" "$3"
        ;;
    daemon|--daemon)
        startServices "daemon" "$2"
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
        startServices "$2" "$3"
        ;;
    *)
        echo "Uso: $0 {start|stop|status|restart|daemon}"
        exit 1
        ;;
esac

exit 0
