#!/usr/bin/env python3
"""
Lanzador del Microservicio microRatio
====================================
"""
import sys
import os

projectRoot = os.path.abspath(os.path.dirname(__file__))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from backend.services.microRatio import main

if __name__ == "__main__":
    main()
