from sqlalchemy import create_engine, Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base, sessionmaker

import sys
import os
# Añadimos la raíz del proyecto al sys.path para poder importar el middleware
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from middleware.config.constants import dbConfig

# Construimos la URL usando la configuración que ya tienes en middleware
SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{dbConfig['user']}:{dbConfig['password']}@{dbConfig['host']}/{dbConfig['database']}"

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class User(Base):
    """
    Tabla de administración de usuarios.
    """
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, index=True)
    hashed_password = Column(String(100))
    is_active = Column(Boolean, default=True)

class RatioSymbol(Base):
    """
    Tabla de administración del catálogo de pares 
    exclusivos para el modelo de correlación (diferentes a Sentinel).
    """
    __tablename__ = "RatioSymbol"

    id = Column(Integer, primary_key=True, index=True)
    pair_name = Column(String(20), unique=True, index=True) # ej. EURGBPUSD
    description = Column(String(100))
    is_active = Column(Boolean, default=True)

# Crea las tablas si no existen en la BD "ATALAia"
Base.metadata.create_all(bind=engine)
