from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float
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

class Role(Base):
    """
    Roles institucionales para control de acceso.
    """
    __tablename__ = "Role"

    idRole = Column(Integer, primary_key=True, autoincrement=True)
    nameRole = Column(String(50), unique=True, nullable=False)
    description = Column(String(255))

class Usuario(Base):
    """
    Tabla corporativa de usuarios de ATALAia.
    """
    __tablename__ = "Usuario"

    idUsuario      = Column(Integer, primary_key=True, autoincrement=True)
    username       = Column(String(50), unique=True, nullable=False)
    passwordHash   = Column(String(255), nullable=False)
    email          = Column(String(100))
    nombre         = Column(String(100), nullable=True)
    apellidoPaterno = Column(String(100), nullable=True)
    apellidoMaterno = Column(String(100), nullable=True)
    idRole         = Column(Integer, nullable=False)
    status         = Column(Integer, default=1)

class Menu(Base):
    """
    Navegación dinámica por roles.
    """
    __tablename__ = "Menu"

    idMenu = Column(Integer, primary_key=True, autoincrement=True)
    nameMenu = Column(String(50), nullable=False)
    url = Column(String(255), nullable=False)
    icon = Column(String(50))
    parentId = Column(Integer, nullable=True)

class RoleMenu(Base):
    """
    Relación de asignación de menús autorizados por rol.
    """
    __tablename__ = "RoleMenu"

    idRole = Column(Integer, primary_key=True)
    idMenu = Column(Integer, primary_key=True)

class RatioSymbol(Base):
    """
    Tabla de administración del catálogo de pares 
    exclusivos para el modelo de correlación.
    """
    __tablename__ = "RatioSymbol"

    symbol = Column(String(10), primary_key=True, index=True)
    Activo = Column(Integer, default=1)
    tipo = Column(String(10))

class Cuenta(Base):
    """
    Tabla corporativa de cuentas de trading en Sentinel/ATALAia.
    """
    __tablename__ = "Cuenta"

    idCuenta = Column(Integer, primary_key=True, autoincrement=True)
    Nombre = Column(String(100), nullable=False)
    Capital = Column(Float, default=0.0)
    ganancia = Column(Float, default=1.0) # Representa el riesgo base de la cuenta
    Activo = Column(Integer, default=1)
    TokenMsg = Column(String(255), nullable=True)
    idGrupoMsg = Column(String(50), nullable=True)
    riesgoPorOperacion = Column(Float, default=1.0)

class SentinelSymbol(Base):
    """
    Tabla de configuración de símbolos/instrumentos de Sentinel.
    """
    __tablename__ = "SentinelSymbol"

    symbol = Column(String(20), primary_key=True, index=True)
    Activo = Column(Integer, default=1)
    min_lots = Column(Float, default=1000.0)
    broker = Column(Integer, default=0)
    precioMaximo = Column(Float, nullable=True)
    precioMinimo = Column(Float, nullable=True)

# Crea las tablas si no existen en la BD "ATALAia"
Base.metadata.create_all(bind=engine)
