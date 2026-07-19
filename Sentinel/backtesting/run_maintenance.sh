#!/bin/bash
# ==============================================================================
# SCRIPT DE MANTENIMIENTO SEMANAL AUTOMATIZADO - SISTEMA ATALAIA / SENTINEL
# ==============================================================================
# Ejecuta el backtest global para actualizar la tabla symbolNotStrategia
# y posteriormente ejecuta la simulación de compounding semanal.
# ==============================================================================

export PYTHONPATH="/Volumes/TimeMachine/ATALAia"
export PYTHONWARNINGS="ignore"
PYTHON_BIN="/Volumes/TimeMachine/ATALAia/.venv/bin/python"
LOG_FILE="/Volumes/TimeMachine/ATALAia/logs/cron_maintenance.log"

mkdir -p "/Volumes/TimeMachine/ATALAia/logs"

echo "================================================================" >> $LOG_FILE
echo "🚀 Iniciando Mantenimiento Semanal a $(date)" >> $LOG_FILE
echo "================================================================" >> $LOG_FILE

echo "⏳ [1/3] Ejecutando Re-optimización de Parámetros en Rejilla Ampliada..." >> $LOG_FILE
$PYTHON_BIN /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_all_optimizations.py >> $LOG_FILE 2>&1

echo "⏳ [2/3] Ejecutando Evaluación Global de 2 Semanas (Actualizando Base de Datos)..." >> $LOG_FILE
$PYTHON_BIN /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_two_week_global_backtest.py >> $LOG_FILE 2>&1

echo "⏳ [3/3] Ejecutando Reporte Semanal Compuesto V6 (Generando PDF)..." >> $LOG_FILE
$PYTHON_BIN /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_weekly_backtest_compounding_v6.py >> $LOG_FILE 2>&1


echo "✅ Proceso de mantenimiento semanal finalizado correctamente a $(date)" >> $LOG_FILE
echo "================================================================" >> $LOG_FILE
