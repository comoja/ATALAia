import logging
from logging.handlers import TimedRotatingFileHandler
import os

def setup_logging():
    log_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'logs'))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_filename = os.path.join(log_dir, "ATALAia.log")
    
    # Manejador para archivo diario
    file_handler = TimedRotatingFileHandler(
        log_filename, when="midnight", interval=1, backupCount=30, encoding='utf-8'
    )
    file_handler.sufix = "%Y-%m-%d"
    
    # Manejador para consola
    console_handler = logging.StreamHandler()

    # Formato común
    formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(name)s | %(funcName)s | %(filename)s:%(lineno)d | %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Configuración raíz
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
