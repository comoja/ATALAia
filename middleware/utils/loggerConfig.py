import logging
from logging.handlers import TimedRotatingFileHandler
import os
import warnings

# Silenciar advertencia ruidosa de scikit-learn sobre parallel.delayed en entornos Windows/multi-hilo
warnings.filterwarnings(
    "ignore",
    message=".*sklearn.utils.parallel.delayed.*"
)


class SafeTimedRotatingFileHandler(TimedRotatingFileHandler):
    """
    Manejador de archivos con rotación temporal que evita errores de permiso
    en Windows si el archivo de log está bloqueado por otro proceso.
    """
    def rotate(self, source: str, dest: str) -> None:
        try:
            if os.path.exists(source):
                os.rename(source, dest)
        except PermissionError:
            import shutil
            try:
                shutil.copy(source, dest)
                with open(source, 'w', encoding='utf-8') as fileObj:
                    fileObj.truncate(0)
            except Exception:
                pass


class ColorFormatter(logging.Formatter):
    """
    Formateador de logs que añade colores ANSI según el nivel del log.
    Útil para visualización en consola.
    """
    # Definición de colores
    GREY = "\x1b[38;20m"
    CYAN = "\x1b[36m"
    BLUE = "\x1b[34m"
    YELLOW = "\x1b[33m"
    RED = "\x1b[31m"
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"
    GREEN = "\x1b[32m"         # Verde estándar ANSI
    ORANGE = "\x1b[38;2;255;165;0m"  # Naranja oscuro / Oro (RGB: 255, 140, 0)
    DARK_ORANGE = "\x1b[38;2;255;140;0m"

    # Formato base
    log_format = '%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s'

    FORMATS = {
        logging.DEBUG: CYAN + log_format + RESET,
        logging.INFO: GREY + log_format + RESET,
        logging.WARNING: YELLOW + log_format + RESET,
        logging.ERROR: RED + log_format + RESET,
        logging.CRITICAL: BOLD_RED + log_format + RESET
    }

    def format(self, record):
        # Intentar obtener color personalizado desde el parámetro 'extra={"color": ...}'
        custom_color = getattr(record, 'color', None)
        
        if custom_color:
            # Soporte para nombres de colores en minúsculas
            color_map = {
                "grey": self.GREY, "cyan": self.CYAN, "blue": self.BLUE,
                "yellow": self.YELLOW, "red": self.RED, "bold_red": self.BOLD_RED, "green": self.GREEN,
                "orange": self.ORANGE, "dark_orange": self.DARK_ORANGE
            }
            color_code = color_map.get(custom_color.lower(), custom_color)
            log_fmt = color_code + self.log_format + self.RESET
        else:
            # Fallback al color predefinido para el nivel (Info, Warning, Error...)
            log_fmt = self.FORMATS.get(record.levelno, self.log_format)

        formatter = logging.Formatter(log_fmt, datefmt='%Y-%m-%d %H:%M:%S')
        return formatter.format(record)

def setupLogging(logPara: str = "app", projectDir: str | None = None, enableConsole: bool = True):
    if projectDir is None:
        projectDir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
    logDir = os.path.join(projectDir, 'logs')
    if not os.path.exists(logDir):
        os.makedirs(logDir)

    logFilename = os.path.join(logDir, f"{logPara}.log")
    
    # Usar el nombre del proceso como nombre de logger
    logger = logging.getLogger(logPara)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    
    # Manejador para archivo diario
    fileHandler = SafeTimedRotatingFileHandler(
        logFilename, when="midnight", interval=1, backupCount=30, encoding='utf-8'
    )
    fileHandler.suffix = "%Y-%m-%d"
    fileFormatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(name)s | %(filename)s:%(lineno)d | %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fileHandler.setFormatter(fileFormatter)
    logger.addHandler(fileHandler)
    
    # Manejador para consola (Con colores) - solo si está habilitado
    if enableConsole:
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(ColorFormatter())
        logger.addHandler(consoleHandler)

    # Silenciar logs de terceros
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram").setLevel(logging.WARNING)

