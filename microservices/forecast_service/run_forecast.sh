#!/bin/bash
scriptDir=$(cd "$(dirname "$0")" && pwd)
PYTHON_BIN="/home/jcolinm/.venvs/Sistema/bin/python"

if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

echo "[INFO] Iniciando ForecastService en puerto 8006..."
cd "$scriptDir" || exit 1
exec "$PYTHON_BIN" -m uvicorn main:app --host 0.0.0.0 --port 8006
