import logging


from scheduler.autoScheduler import startScheduler, isRestTime

# Cambiar a True cuando quieras operar en vivo
USE_LIVE = False
"""
if USE_LIVE:
    from execution.liveExecution import LiveExecution as ExecutionEngine
else:
    from execution.simulatedExecution import SimulatedExecution as ExecutionEngine
"""

# =============================

# =============================
# LOGGING
# =============================

# Configurar logging
logging.basicConfig(
    filename="ATALAia.log",
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(filename)s:%(lineno)d | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


# =============================
# INITIALIZATION
# =============================


# =============================
# MAIN TRADING CYCLE
# =============================

def run():

    if isRestTime():
        logging.info("Es hora de descansar. El bot se detendrá hasta el próximo ciclo.")
        return

    # Aquí iría la lógica principal del bot, por ejemplo:
    # 1. Obtener datos
    # 2. Analizar y generar señales
    # 3. Ejecutar órdenes
    # 4. Enviar alertas
    
   


# =============================
# ENTRY POINT
# =============================

if __name__ == "__main__":
    startScheduler(run)