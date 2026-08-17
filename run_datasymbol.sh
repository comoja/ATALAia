#!/bin/bash
scriptDir=$(cd "$(dirname "$0")" && pwd)

echo "======================================================="
echo "          INICIANDO TRADING BOT - DATASYMBOL           "
echo "======================================================="

if [ -x "$scriptDir/.venv/bin/python" ]; then
    PYTHON_BIN="$scriptDir/.venv/bin/python"
elif [ -x "$HOME/.venvs/Sistema/bin/python" ]; then
    PYTHON_BIN="$HOME/.venvs/Sistema/bin/python"
elif [ -x "$scriptDir/../.venv/bin/python" ]; then
    PYTHON_BIN="$scriptDir/../.venv/bin/python"
else
    PYTHON_BIN="python3"
fi

echo "[INFO] Usando Python: $PYTHON_BIN"
cd "$scriptDir" || exit 1
exec "$PYTHON_BIN" "$scriptDir/dataSymbol/mainOrchestrator.py" "$@"
