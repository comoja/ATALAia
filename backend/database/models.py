from sqlalchemy import create_engine, Column, Integer, String, Boolean, Float, ForeignKey, DateTime, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

import sys
import os
# Añadimos la raíz del proyecto al sys.path para poder importar el middleware
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "M1x&J34ny")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_DATABASE = os.getenv("DB_DATABASE", "atalaia")

SQLALCHEMY_DATABASE_URL = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=10,
    max_overflow=5,
    pool_recycle=1800,
    pool_pre_ping=True
)
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
    __tablename__ = "role"

    idRole = Column(Integer, primary_key=True, autoincrement=True)
    nameRole = Column(String(50), unique=True, nullable=False)
    description = Column(String(255))

class Usuario(Base):
    """
    Tabla corporativa de usuarios de ATALAia.
    """
    __tablename__ = "usuario"

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
    __tablename__ = "menu"

    idMenu = Column(Integer, primary_key=True, autoincrement=True)
    nameMenu = Column(String(50), nullable=False)
    url = Column(String(255), nullable=False)
    icon = Column(String(50))
    parentId = Column(Integer, nullable=True)

class RoleMenu(Base):
    """
    Relación de asignación de menús autorizados por rol.
    """
    __tablename__ = "rolemenu"

    idRole = Column(Integer, primary_key=True)
    idMenu = Column(Integer, primary_key=True)

class Symbol(Base):
    """
    Tabla unificada máster de administración de símbolos para ATALAia y Sentinel.
    """
    __tablename__ = "symbols"

    symbol = Column(String(10), primary_key=True, index=True)
    tipo = Column(String(10), nullable=False)
    activoRatio = Column(Integer, default=1)
    activoSentinel = Column(Integer, default=1)
    min_lots = Column(Float, default=1000.0)
    broker = Column(Integer, default=0)
    precioMaximo = Column(Float, nullable=True)
    precioMinimo = Column(Float, nullable=True)
    FOREX = Column(String(20), nullable=True)
    MT5 = Column(String(20), nullable=True)
    TradingView = Column(String(20), nullable=True)

class RatioSymbol(Base):
    """
    Vista/Tabla de administración del catálogo de pares de correlación.
    """
    __tablename__ = "ratiosymbol"

    symbol = Column(String(10), primary_key=True, index=True)
    Activo = Column(Integer, default=1)
    tipo = Column(String(10))

class Cuenta(Base):
    """
    Tabla corporativa de cuentas de trading en Sentinel/ATALAia.
    """
    __tablename__ = "cuenta"

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
    Vista/Tabla de configuración de símbolos de Sentinel.
    """
    __tablename__ = "sentinelsymbol"

    symbol = Column(String(20), primary_key=True, index=True)
    Activo = Column(Integer, default=1)
    min_lots = Column(Float, default=1000.0)
    broker = Column(Integer, default=0)
    precioMaximo = Column(Float, nullable=True)
    precioMinimo = Column(Float, nullable=True)

class UserRatio(Base):
    """
    Tabla de relación entre usuario, cuenta y ratios guardados a analizar.
    """
    __tablename__ = "user_ratios"
    __table_args__ = (UniqueConstraint("idUsuario", "idCuenta", "numerador", "denominador", name="ukUserRatioPair"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    idUsuario = Column(Integer, ForeignKey("usuario.idUsuario"), nullable=False, index=True)
    idCuenta = Column(Integer, ForeignKey("cuenta.idCuenta"), nullable=False, index=True)
    numerador = Column(String(20), nullable=False)
    denominador = Column(String(20), nullable=False)
    periodo = Column(String(20), nullable=False)
    dias = Column(Integer, default=180)
    EMARapida = Column(Integer, default=3)
    EMALenta = Column(Integer, default=20)
    operar = Column(Boolean, default=False)
    createdAt = Column(DateTime, default=datetime.utcnow)

class UsuarioCuenta(Base):
    """
    Tabla de relación entre usuarios y cuentas de trading (1 usuario administra N cuentas).
    """
    __tablename__ = "usuarioCuenta"
    __table_args__ = (
        UniqueConstraint("idUsuario", "idCuenta", name="ukUsuarioCuenta"),
    )

    idUsuarioCuenta = Column(Integer, primary_key=True, autoincrement=True)
    idUsuario = Column(Integer, ForeignKey("usuario.idUsuario"), nullable=False, index=True)
    idCuenta = Column(Integer, ForeignKey("cuenta.idCuenta"), nullable=False, index=True)
    activo = Column(Boolean, default=True, nullable=False)
    createdAt = Column(DateTime, default=datetime.utcnow)

# Crea las tablas si no existen en la BD "ATALAia"
Base.metadata.create_all(bind=engine)
