#!/usr/bin/env python3
"""
Punto de entrada en raíz para el microservicio de optimización sabatina de RIDER (microCartOptimizer.py).
Delega la ejecución al módulo institucional en backend/services/microCartOptimizer.py.
"""

import os
import sys

rootDir = os.path.dirname(os.path.abspath(__file__))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.services.microCartOptimizer import main

if __name__ == "__main__":
    main()

