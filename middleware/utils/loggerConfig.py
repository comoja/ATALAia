import logging
from logging.handlers import TimedRotatingFileHandler
import os

def setupLogging(logPara: str = "app", projectDir: str = None):
    if projectDir is None:
        projectDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    logDir = os.path.join(projectDir, 'logs')
    if not os.path.exists(logDir):
        os.makedirs(logDir)

    logFilename = os.path.join(logDir, f"{logPara}.log")
    
    # Manejador para archivo diario
    fileHandler = TimedRotatingFileHandler(
        logFilename, when="midnight", interval=1, backupCount=30, encoding='utf-8'
    )
    fileHandler.suffix = "%Y-%m-%d"
    
    # Manejador para consola
    consoleHandler = logging.StreamHandler()

    # Formato común
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(filename)s:%(lineno)d | %(message)s')
    fileHandler.setFormatter(formatter)
    consoleHandler.setFormatter(formatter)

    # Configuración raíz
    rootLogger = logging.getLogger()
    if not rootLogger.handlers:
        rootLogger.setLevel(logging.INFO)
        rootLogger.addHandler(fileHandler)
        rootLogger.addHandler(consoleHandler)

    # Silenciar logs de httpx (solo mostrar advertencias y errores)
    logging.getLogger("httpx").setLevel(logging.WARNING)
