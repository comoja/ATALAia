import sqlite3
import pandas as pd

# Suponiendo que la DB esta en algun lugar o usan SQLAlchemy
import os
import sys
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path: sys.path.insert(0, rutaRaiz)
from middleware.database import dbManager

df = dbManager.get_historical_data("EUR/USD", "15min", 10)
if df is not None and not df.empty:
    print(df.tail(3))
    print("El timezone del index es:", df.index.tzinfo)
else:
    print("No hay df")
