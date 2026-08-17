from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
import logging

from backend.database.models import SessionLocal, Usuario, Role, Menu, RoleMenu
from backend.services.security_service import verifyPassword

router = APIRouter(prefix="/auth", tags=["Autenticación"])
logger = logging.getLogger(__name__)

# Dependencia para base de datos
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Modelos Pydantic para intercambio de datos (usando camelCase estrictamente)
class LoginRequest(BaseModel):
    username: str
    password: str

class UserResponse(BaseModel):
    idUsuario: int
    username: str
    email: Optional[str] = None
    nombre: Optional[str] = None
    apellidoPaterno: Optional[str] = None
    apellidoMaterno: Optional[str] = None
    idRole: int
    nameRole: str
    status: int

class UsuarioComboItem(BaseModel):
    """DTO liviano para el combo selector de usuarios en el dashboard."""
    idUsuario: int
    nombreCompleto: str

class MenuResponse(BaseModel):
    idMenu: int
    nameMenu: str
    url: str
    icon: Optional[str] = None
    parentId: Optional[int] = None

@router.post("/login", response_model=UserResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    """
    Endpoint institucional para validación de credenciales.
    Verifica el hash SHA-256 en la base de datos y retorna el perfil seguro.
    """
    logger.info(f"Intento de inicio de sesión institucional para el usuario: {payload.username}")
    
    # 1. Buscar usuario
    usuario = db.query(Usuario).filter(Usuario.username == payload.username).first()
    if not usuario:
        logger.warning(f"Usuario no encontrado: {payload.username}")
        raise HTTPException(status_code=401, detail="Credenciales incorrectas o usuario no registrado")
    
    # 2. Verificar estado activo
    if usuario.status != 1:
        logger.warning(f"Intento de ingreso con usuario inactivo: {payload.username}")
        raise HTTPException(status_code=403, detail="Esta cuenta se encuentra desactivada temporalmente")
        
    # 3. Validar contraseña
    if not verifyPassword(payload.password, usuario.passwordHash):
        logger.warning(f"Contraseña incorrecta para el usuario: {payload.username}")
        raise HTTPException(status_code=401, detail="Credenciales incorrectas")
    
    # 4. Obtener nombre del rol
    roleInfo = db.query(Role).filter(Role.idRole == usuario.idRole).first()
    nameRole = roleInfo.nameRole if roleInfo else "Desconocido"
    
    logger.info(f"Autenticación exitosa. Usuario: {payload.username}, Rol: {nameRole}")
    
    return UserResponse(
        idUsuario=usuario.idUsuario,
        username=usuario.username,
        email=usuario.email,
        nombre=usuario.nombre,
        apellidoPaterno=usuario.apellidoPaterno,
        apellidoMaterno=usuario.apellidoMaterno,
        idRole=usuario.idRole,
        nameRole=nameRole,
        status=usuario.status
    )

@router.get("/menu/{idRole}", response_model=List[MenuResponse])
def getMenuByRole(idRole: int, db: Session = Depends(get_db)):
    """
    Carga dinámicamente los menús autorizados para un rol específico.
    """
    logger.info(f"Cargando menús dinámicos autorizados para el rol ID: {idRole}")
    
    try:
        # Consulta relacional con JOIN entre Menu y RoleMenu
        menusAutorizados = (
            db.query(Menu)
            .join(RoleMenu, Menu.idMenu == RoleMenu.idMenu)
            .filter(RoleMenu.idRole == idRole)
            .order_by(Menu.idMenu.asc())
            .all()
        )
        
        # Mapeo a respuesta DTO respetando camelCase
        listaMenu = []
        for menu in menusAutorizados:
            listaMenu.append(
                MenuResponse(
                    idMenu=menu.idMenu,
                    nameMenu=menu.nameMenu,
                    url=menu.url,
                    icon=menu.icon,
                    parentId=menu.parentId
                )
            )
        return listaMenu
        
    except Exception as e:
        logger.error(f"Error cargando menús dinámicos para el rol {idRole}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno al cargar la navegación")


@router.get("/usuarios", response_model=List[UsuarioComboItem])
def getUsuariosCombo(db: Session = Depends(get_db)):
    """
    Devuelve la lista de usuarios activos con nombre completo
    para el combo selector del dashboard (uso exclusivo del rol admin).
    """
    logger.info("Cargando lista de usuarios para combo del dashboard")
    try:
        usuarios = (
            db.query(Usuario)
            .filter(Usuario.status == 1)
            .order_by(Usuario.apellidoPaterno.asc(), Usuario.nombre.asc())
            .all()
        )

        listaUsuarios = []
        for u in usuarios:
            # Construir nombre completo con fallback al username si los campos son nulos
            partes = [p for p in [u.nombre, u.apellidoPaterno, u.apellidoMaterno] if p]
            nombreCompleto = " ".join(partes) if partes else u.username
            listaUsuarios.append(
                UsuarioComboItem(
                    idUsuario=u.idUsuario,
                    nombreCompleto=nombreCompleto
                )
            )
        return listaUsuarios

    except Exception as e:
        logger.error(f"Error cargando usuarios para combo: {str(e)}")
        raise HTTPException(status_code=500, detail="Error interno al cargar la lista de usuarios")
