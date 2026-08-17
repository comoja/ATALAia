#!/bin/bash

# Script para ejecutar la sincronización diaria de precios de cierre (StockPrices)
# Puede ser configurado en crontab (ej. todos los días a las 23:00)
# 0 23 * * 1-5 /Volumes/TimeMachine/ATALAia/dataSymbol/run_daily_stock_prices.sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
PROJECT_ROOT="$(dirname "$DIR")"

echo "Iniciando proceso de sincronización diaria de StockPrices..."
cd "$PROJECT_ROOT"
source .venv/bin/activate
python dataSymbol/dailyStockPrices.py
echo "Proceso terminado."
