"""
Script de auto-reentrenamiento del modelo ML.
Designed to be run via cron or scheduled task.

Usage:
    # Run daily at 6am (will only retrain on Sundays)
    0 6 * * 0 /path/to/venv/bin/python /path/to/Sentinel/ml/auto_retrain.py
    
    # Or manually:
    python Sentinel/ml/auto_retrain.py
"""
import asyncio
import sys
import os
from datetime import datetime

# Add project root to path
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from Sentinel.ml.retrain_ml import retrain as retrain_classification
from Sentinel.ml.train_reg_model import train_reg as retrain_regression

# Configuración
RETRAIN_DAY_OF_WEEK = None  # None = diario, 0=Lunes, 6=Domingo
RETRAIN_HOUR = 1  # Hora de reentrenamiento (1am)
MIN_DATA_POINTS = 10000  # Mínimo de datos requeridos


async def should_retrain():
    """Verifica si debe ejecutarse el reentrenamiento hoy."""
    now = datetime.now()
    
    # Forzar reentrenamiento si hay argumentos
    if len(sys.argv) > 1 and sys.argv[1] == '--force':
        return True
    
    # Verificar si es el día configurado (si está configurado)
    if RETRAIN_DAY_OF_WEEK is not None and now.weekday() != RETRAIN_DAY_OF_WEEK:
        print(f"Hoy es {now.strftime('%A')} - No es día de reentrenamiento (día {RETRAIN_DAY_OF_WEEK})")
        return False
    
    # Verificar si es la hora configurada
    if now.hour != RETRAIN_HOUR:
        print(f"Ahora son las {now.hour}h - No es la hora de reentrenamiento ({RETRAIN_HOUR}h)")
        return False
    
    return True


async def auto_retrain():
    """Ejecuta el reentrenamiento automáticamente."""
    print("=" * 60)
    print("AUTO-REENTRENAMIENTO ML")
    print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # Verificar si debe reentrenar
    if not await should_retrain():
        return
    
    print("\n✅ Iniciando reentrenamiento...")
    
    # Reentrenar modelo de clasificación
    print("\n[1/2] Reentrenando modelo de clasificación...")
    try:
        await retrain_classification()
        print("✅ Modelo de clasificación reentrenado")
    except Exception as e:
        print(f"❌ Error en clasificación: {e}")
    
    # Reentrenar modelo de regresión
    print("\n[2/2] Reentrenando modelo de regresión...")
    try:
        await retrain_regression()
        print("✅ Modelo de regresión reentrenado")
    except Exception as e:
        print(f"❌ Error en regresión: {e}")
    
    print("\n" + "=" * 60)
    print("REENTRENAMIENTO COMPLETADO")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(auto_retrain())