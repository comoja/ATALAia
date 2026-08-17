import subprocess
import time
import sys
import os
import logging

# Configuración de logs para el guardián
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [WATCHDOG] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("watchdog.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_sentinel():
    """
    Ejecuta el proceso principal del bot y lo monitorea.
    """
    python_executable = sys.executable
    script_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'main.py'))
    
    logger.info(f"🛡️ Watchdog iniciando monitoreo de: {script_path}")
    
    while True:
        try:
            logger.info("🚀 Iniciando proceso Sentinel...")
            # Ejecutamos con un pipe para capturar errores si es necesario
            process = subprocess.Popen([python_executable, script_path])
            
            # Esperamos a que el proceso termine
            exit_code = process.wait()
            
            if exit_code == 0:
                logger.info("✅ Sentinel terminó legalmente (Exit Code 0). Reiniciando en 60s...")
            else:
                logger.error(f"❌ Sentinel CRASHED (Exit Code {exit_code}). Reiniciando en 10 segundos...")
            
            time.sleep(10) # Pausa antes de reiniciar
            
        except KeyboardInterrupt:
            logger.info("🛑 Watchdog detenido por el usuario.")
            process.terminate()
            break
        except Exception as e:
            logger.error(f"⚠️ Error fatal en Watchdog: {e}")
            time.sleep(30)

if __name__ == "__main__":
    run_sentinel()
