#!/bin/bash
# ==============================================================================
# SCRIPT DE MANTENIMIENTO SEMANAL AUTOMATIZADO - SISTEMA ATALAIA / SENTINEL
# ==============================================================================
# Configura las variables de entorno y ejecuta el backtesting dinámico
# para recalibrar symbolNotStrategia en MySQL.
# ==============================================================================

# Forzar el directorio raíz del proyecto en PYTHONPATH para evitar errores de imports
export PYTHONPATH="/Volumes/TimeMachine/ATALAia"

# Asegurar que existe el directorio de logs
mkdir -p "/Volumes/TimeMachine/ATALAia/logs"

# Ejecutar el orquestador de backtest maestro V6 con la ruta absoluta del intérprete de Python
/Library/Frameworks/Python.framework/Versions/3.14/bin/python3 /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_weekly_backtest_compounding_v6.py >> /Volumes/TimeMachine/ATALAia/logs/cron_maintenance.log 2>&1

echo "✅ Proceso de mantenimiento semanal finalizado correctamente a $(date)" >> /Volumes/TimeMachine/ATALAia/logs/cron_maintenance.log
