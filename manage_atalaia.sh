#!/bin/zsh

# ==========================================================================
# ATALAIA SYSTEM - LIFECYCLE MANAGER
# Script para levantar/tirar la aplicación (Backend FastAPI + Frontend Java)
# ==========================================================================

# Variables en camelCase para adherencia estricta
scriptDir=$(cd "$(dirname "$0")" && pwd)
logsDir="$scriptDir/logs"
backendPidFile="$logsDir/backend.pid"
frontendPidFile="$logsDir/frontend.pid"

# Asegurar que existe el directorio de logs
mkdir -p "$logsDir"

function startServices() {
    echo "=========================================================="
    echo "🚀 Levantando Sistema ATALA.ia (Aetherial UI)..."
    echo "=========================================================="

    # --- 1. LEVANTAR BACKEND (FastAPI) ---
    if [ -f "$backendPidFile" ]; then
        backendPid=$(cat "$backendPidFile")
        if kill -0 "$backendPid" 2>/dev/null; then
            echo "⚠️  El Backend (FastAPI) ya está corriendo (PID: $backendPid)."
        else
            rm -f "$backendPidFile"
        fi
    fi

    if [ ! -f "$backendPidFile" ]; then
        echo "⚡ Iniciando Backend en puerto 8000..."
        # Ejecutar en segundo plano redirigiendo logs
        python3 "$scriptDir/backend/main.py" > "$logsDir/backend_output.log" 2>&1 &
        backendPid=$!
        echo "$backendPid" > "$backendPidFile"
        echo "✅ Backend levantado con éxito (PID: $backendPid)."
    fi

    # --- 2. LEVANTAR FRONTEND (Java Spring Boot/PrimeFaces) ---
    if [ -f "$frontendPidFile" ]; then
        frontendPid=$(cat "$frontendPidFile")
        if kill -0 "$frontendPid" 2>/dev/null; then
            echo "⚠️  El Frontend (Java Maven) ya está corriendo (PID: $frontendPid)."
        else
            rm -f "$frontendPidFile"
        fi
    fi

    if [ ! -f "$frontendPidFile" ]; then
        echo "⚡ Iniciando Frontend (Java 8 Maven) en puerto 8080..."
        # Entrar al directorio del frontend para levantar con Maven
        cd "$scriptDir/frontend" || exit 1
        mvn spring-boot:run > "$logsDir/frontend_output.log" 2>&1 &
        frontendPid=$!
        echo "$frontendPid" > "$frontendPidFile"
        cd "$scriptDir" || exit 1
        echo "✅ Frontend levantado con éxito (PID: $frontendPid)."
    fi

    echo "----------------------------------------------------------"
    echo "🎉 ¡Servicios iniciados con éxito!"
    echo "🌐 FastAPI Backend:   http://localhost:8000"
    echo "🌐 PrimeFaces Visual: http://localhost:8080/Atalaia/login.xhtml"
    echo "📊 Monitorea la consola con: tail -f logs/backend_output.log o logs/frontend_output.log"
    echo "=========================================================="
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
        # Búsqueda preventiva por puerto 8000 si el pidfile no existe
        portPid=$(lsof -t -i:8000)
        if [ -n "$portPid" ]; then
            echo "⚡ Deteniendo proceso huérfano en puerto 8000 (PID: $portPid)..."
            kill "$portPid" 2>/dev/null
            sleep 1
            kill -9 "$portPid" 2>/dev/null
            echo "✅ Proceso en puerto 8000 detenido."
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
            echo "🟢 Backend (FastAPI):  ACTIVO (PID: $backendPid) | http://localhost:8000"
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
            echo "🟢 Frontend (Java):    ACTIVO (PID: $frontendPid) | http://localhost:8080/Atalaia/login.xhtml"
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
