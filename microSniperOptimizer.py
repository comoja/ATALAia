#!/usr/bin/env python3
"""
Punto de entrada raíz para el Microservicio de Optimización y Calibración Adaptativa de SNIPER.
Delega al módulo institucional backend/services/microSniperOptimizer.py.
"""

import os
import sys

rootDir = os.path.dirname(os.path.abspath(__file__))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from backend.services.microSniperOptimizer import main

if __name__ == "__main__":
    main()
