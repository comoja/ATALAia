#!/bin/bash

export PYTHONPATH="/Volumes/TimeMachine/ATALAia"
export PYTHONWARNINGS="ignore"
PYTHON_BIN="/Volumes/TimeMachine/ATALAia/.venv/bin/python"
LOG_FILE="/Volumes/TimeMachine/ATALAia/logs/cron_maintenance.log"

echo "================================================================" >> $LOG_FILE
echo "🚀 Retomando Mantenimiento Semanal a $(date)" >> $LOG_FILE
echo "================================================================" >> $LOG_FILE

scripts=(
    "run_patron4h_optimization.py"
    "run_ichimoku_optimization.py"
    "run_breakoutprobability_optimization.py"
    "run_breakoutny_optimization.py"
    "run_fvgdiario_optimization.py"
    "run_imbalance_optimization.py"
    "run_silverbullet_optimization.py"
    "run_speedbot_optimization.py"
)

for script in "${scripts[@]}"; do
    echo "⏳ Ejecutando optimizador pendiente: $script..." >> $LOG_FILE
    $PYTHON_BIN /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/$script >> $LOG_FILE 2>&1
done

echo "⏳ [2/3] Ejecutando Evaluación Global de 2 Semanas (Actualizando Base de Datos)..." >> $LOG_FILE
$PYTHON_BIN /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_two_week_global_backtest.py >> $LOG_FILE 2>&1

echo "⏳ [3/3] Ejecutando Reporte Semanal Compuesto V6 (Generando PDF)..." >> $LOG_FILE
$PYTHON_BIN /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_weekly_backtest_compounding_v6.py >> $LOG_FILE 2>&1

echo "✅ Proceso de mantenimiento semanal retomado finalizado correctamente a $(date)" >> $LOG_FILE
echo "================================================================" >> $LOG_FILE
