import os
import sys
import asyncio
import logging

from unittest.mock import patch

# Mock isRestTime
import middleware.scheduler.autoScheduler as autoS
autoS.isRestTime = lambda: False  # Force market open

# Mock sleep para que no duerma 5 minutos
original_sleep = asyncio.sleep

async def mocked_sleep(seconds):
    if seconds >= 60:
        print(f"[MOCKED] Skipping sleep of {seconds} seconds.")
        raise KeyboardInterrupt()  # Forzar el fin después de un ciclo
    await original_sleep(seconds)

import Sentinel.main as main_module

async def run_test():
    try:
        with patch('asyncio.sleep', new=mocked_sleep):
            await main_module.main()
    except KeyboardInterrupt:
        print("Ciclo completado. Terminando exitosamente.")
    except Exception as e:
        print(f"Error detectado: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(run_test())
