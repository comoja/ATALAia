#!/bin/bash
scriptDir=$(cd "$(dirname "$0")" && pwd)
projectRoot="$(cd "$scriptDir/.." && pwd)"

echo "======================================================="
echo "          INICIANDO TRADING BOT - DATASYMBOL           "
echo "======================================================="

PYTHON_BIN="/home/jcolinm/.venvs/Sistema/bin/python"
WATCHFILES_BIN="/home/jcolinm/.venvs/Sistema/bin/watchfiles"

if [ ! -x "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

echo "[INFO] Usando Python: $PYTHON_BIN"
cd "$projectRoot" || exit 1

if [ -x "$WATCHFILES_BIN" ]; then
    echo "[INFO] Modo Hot-Reload activo (watchfiles)"
    exec "$WATCHFILES_BIN" --filter python "$PYTHON_BIN $scriptDir/mainOrchestrator.py" "$scriptDir" "$projectRoot/middleware"
else
    exec "$PYTHON_BIN" "$scriptDir/mainOrchestrator.py" "$@"
fi
