"""
Gestor de conectividad hacia el microservicio ConnectionPool.
Todas las operaciones con la base de datos se canalizan exclusivamente
mediante HTTP REST a través de ConnectionPool (http://127.0.0.1:8000/api/v1).
"""
import logging
import requests
from middleware.config.constants import CONNECTION_POOL_URL

logger = logging.getLogger(__name__)


def getConnection():
    """
    Función de compatibilidad. 
    ADVERTENCIA: Las conexiones directas a MySQL están desactivadas.
    Todas las interacciones de BD deben canalizarse mediante `_call_connection_pool` en `dbManager`.
    """
    logger.warning("⚠️ ADVERTENCIA: Se intentó abrir una conexión directa a MySQL. Toda operación debe canalizarse mediante ConnectionPool microservicio.")
    return None


def health_check() -> bool:
    """Verifica la salud del microservicio ConnectionPool."""
    try:
        res = requests.get(f"{CONNECTION_POOL_URL}/health", timeout=3)
        if res.status_code == 200 and res.json().get("status") == "online":
            return True
        return False
    except Exception as e:
        logger.warning(f"Health check a ConnectionPool falló: {e}")
        return False
