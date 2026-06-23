#!/usr/bin/env python3
"""
Orquestador central de optimizaciones para el mantenimiento semanal de Sentinel.
Ejecuta de forma secuencial todos los scripts de optimización en rejilla.
"""
import os
import sys
import subprocess
import logging
from datetime import datetime

# --- Path Setup ---
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

# Configurar logs
logDir = os.path.join(rutaRaiz, "logs")
os.makedirs(logDir, exist_ok=True)
logFile = os.path.join(logDir, "cron_maintenance.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    handlers=[
        logging.FileHandler(logFile, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("optimization_orchestrator")

# Lista de scripts de optimización a ejecutar
optimizationScripts = [
    "run_cruceema_optimization.py",
    "run_genericfvg_optimization.py",
    "run_sniper_optimization.py",
    "run_qtrend_optimization.py",
    "run_reversionmedia_optimization.py",
    "run_sesgobiashtf_optimization.py",
    "run_patron4h_optimization.py",
    "run_ichimoku_optimization.py",
    "run_breakoutprobability_optimization.py",
    "run_breakoutny_optimization.py",
    "run_fvgdiario_optimization.py",
    "run_imbalance_optimization.py",
    "run_silverbullet_optimization.py",
    "run_speedbot_optimization.py"
]

def runAllOptimizations() -> None:
    logger.info("================================================================")
    logger.info("🚀 INICIANDO RE-OPTIMIZACIÓN GENERAL DE PARÁMETROS SEMANAL")
    logger.info("================================================================")
    
    pythonBin = sys.executable
    backtestingDir = os.path.join(rutaRaiz, "Sentinel", "backtesting")
    
    successCount = 0
    failureCount = 0
    
    for scriptName in optimizationScripts:
        scriptPath = os.path.join(backtestingDir, scriptName)
        
        if not os.path.exists(scriptPath):
            logger.warning(f"⚠️ El script {scriptName} no existe en la ruta {backtestingDir}. Se omite.")
            continue
            
        logger.info(f"⏳ Ejecutando optimizador: {scriptName}...")
        startTime = datetime.now()
        
        try:
            # Ejecutar el script usando el mismo entorno de python actual
            result = subprocess.run(
                [pythonBin, scriptPath],
                cwd=rutaRaiz,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                check=True
            )
            
            elapsedTime = datetime.now() - startTime
            logger.info(f"✅ Optimizador {scriptName} finalizado con éxito en {elapsedTime.total_seconds():.1f}s.")
            successCount += 1
            
            # Registrar salida si tiene información relevante
            if result.stdout:
                lines = result.stdout.strip().split('\n')
                # Registrar las últimas 5 líneas de la salida para tener resumen
                summaryLines = [line for line in lines if "Mejor" in line or "Mejores" in line or "guardado" in line or "rentable" in line]
                for line in summaryLines[-5:]:
                    logger.info(f"   [Output] {line}")
                    
        except subprocess.CalledProcessError as e:
            elapsedTime = datetime.now() - startTime
            logger.error(f"❌ Error al ejecutar {scriptName} después de {elapsedTime.total_seconds():.1f}s.")
            logger.error(f"   [Error Output]: {e.stderr.strip() or e.stdout.strip()}")
            failureCount += 1
        except Exception as ex:
            logger.error(f"❌ Excepción inesperada ejecutando {scriptName}: {ex}")
            failureCount += 1
            
    logger.info("================================================================")
    logger.info(f"🏁 PROCESO COMPLETADO: {successCount} exitosos, {failureCount} fallidos.")
    logger.info("================================================================")

if __name__ == '__main__':
    runAllOptimizations()
