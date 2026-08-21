#!/usr/bin/env python3
"""
Orquestador central de optimizaciones para el mantenimiento de Sentinel.
Ejecuta de forma secuencial todos los scripts de optimización en rejilla
sobre los símbolos activos de Sentinel, garantizando la persistencia completa
en symbolstrategyconfig (enabled = TRUE para rentables, enabled = FALSE para no rentables).
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

# Lista de 15 scripts de optimización a ejecutar
optimizationScripts = [
    "run_cruceema_optimization.py",
    "run_genericfvg_optimization.py",
    "run_sniper_optimization.py",
    "run_qtrend_optimization.py",
    "run_reversionmedia_optimization.py",
    "run_sesgobiashtf_optimization.py",    
    "run_ichimoku_optimization.py",
    "run_breakoutprobability_optimization.py",
    "run_breakoutny_optimization.py",
    "run_fvgdiario_optimization.py",
    "run_imbalance_optimization.py",
    "run_silverbullet_optimization.py",
    "run_speedbot_optimization.py",
    "run_patron4h_optimization.py",
    "run_premiumconfluence_optimization.py"
]

def runAllOptimizations() -> None:
    logger.info("================================================================")
    logger.info("🚀 INICIANDO RE-OPTIMIZACIÓN GENERAL DE PARÁMETROS SENTINEL")
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
            
        logger.info(f"⏳ [{successCount + failureCount + 1}/{len(optimizationScripts)}] Ejecutando optimizador: {scriptName}...")
        startTime = datetime.now()
        
        try:
            env = os.environ.copy()
            env["PYTHONWARNINGS"] = "ignore"
            env["PYTHONUNBUFFERED"] = "1"
            
            process = subprocess.Popen(
                [pythonBin, scriptPath],
                cwd=rutaRaiz,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                env=env,
                bufsize=1
            )
            
            for line in iter(process.stdout.readline, ''):
                clean_line = line.strip()
                if clean_line:
                    logger.info(f"   [{scriptName}] {clean_line}")
                    
            process.stdout.close()
            return_code = process.wait()
            
            elapsedTime = datetime.now() - startTime
            if return_code == 0:
                logger.info(f"✅ Optimizador {scriptName} finalizado con éxito en {elapsedTime.total_seconds():.1f}s.")
                successCount += 1
            else:
                logger.error(f"❌ Error en {scriptName} (código de salida {return_code}) después de {elapsedTime.total_seconds():.1f}s.")
                failureCount += 1
                    
        except Exception as ex:
            elapsedTime = datetime.now() - startTime
            logger.error(f"❌ Excepción inesperada ejecutando {scriptName}: {ex}")
            failureCount += 1
            
    logger.info("================================================================")
    logger.info(f"🏁 PROCESO COMPLETADO: {successCount} exitosos, {failureCount} fallidos.")
    logger.info("================================================================")

if __name__ == '__main__':
    runAllOptimizations()
