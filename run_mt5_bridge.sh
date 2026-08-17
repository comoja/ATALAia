#!/bin/bash
scriptDir=$(cd "$(dirname "$0")" && pwd)

echo "======================================================="
echo "       INICIANDO MT5 BRIDGE SERVER (WINE / LINUX)      "
echo "======================================================="

WINE_PYTHON="$HOME/.wine/drive_c/Python311/python.exe"

if [ ! -f "$WINE_PYTHON" ]; then
    echo "❌ No se encontró Python en Wine ($WINE_PYTHON)."
    exit 1
fi

WINEDEBUG=-all wine "$WINE_PYTHON" "$scriptDir/middleware/api/mt5_bridge_server.py"
