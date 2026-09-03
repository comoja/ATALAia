from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import logging

from backend.database.models import SessionLocal, Usuario, Role, Menu, RoleMenu
from backend.services.security_service import (
    verifyPassword, 
    calculateSha256, 
    validatePasswordPolicy, 
    isPasswordExpired, 
    getDaysSincePasswordUpdate
)

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

class ChangePasswordRequest(BaseModel):
    username: str
    currentPassword: str
    newPassword: str
    confirmPassword: str

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
    passwordExpired: bool = False
    daysSincePasswordUpdate: int = 0
    mustChangePassword: bool = False

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

class ActionResponse(BaseModel):
    success: bool
    message: str
    user: Optional[UserResponse] = None

@router.post("/login", response_model=UserResponse)
def login(payload: LoginRequest, db: Session = Depends(get_db)):
    """
    Endpoint institucional para validación de credenciales.
    Verifica el hash SHA-256 en la base de datos, valida expiración periódica (cada 4 meses / 120 días)
    y retorna el perfil seguro con el estatus de vigencia de la clave.
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
    
    # 5. Evaluar expiración periódica de la clave (cada 120 días / 4 meses) o cambio forzado
    passExpired = isPasswordExpired(usuario.passwordUpdatedAt, maxDays=120) or bool(usuario.mustChangePassword)
    daysSinceUpdate = getDaysSincePasswordUpdate(usuario.passwordUpdatedAt)
    
    logger.info(f"Autenticación exitosa. Usuario: {payload.username}, Rol: {nameRole}, Clave Expirada: {passExpired} (hace {daysSinceUpdate} días)")
    
    return UserResponse(
        idUsuario=usuario.idUsuario,
        username=usuario.username,
        email=usuario.email,
        nombre=usuario.nombre,
        apellidoPaterno=usuario.apellidoPaterno,
        apellidoMaterno=usuario.apellidoMaterno,
        idRole=usuario.idRole,
        nameRole=nameRole,
        status=usuario.status,
        passwordExpired=passExpired,
        daysSincePasswordUpdate=daysSinceUpdate,
        mustChangePassword=bool(usuario.mustChangePassword)
    )

@router.post("/change-password", response_model=ActionResponse)
def changePassword(payload: ChangePasswordRequest, db: Session = Depends(get_db)):
    """
    Endpoint institucional para cambio seguro de contraseña.
    Valida:
    1. Que la contraseña actual coincida con el hash en base de datos.
    2. Que la nueva contraseña y su confirmación sean idénticas.
    3. Que la nueva contraseña sea diferente a la contraseña actual.
    4. Que la nueva contraseña cumpla los 4 requisitos de complejidad:
       - Mínimo 8 caracteres.
       - Al menos 1 número.
       - Mayúsculas y minúsculas.
       - Al menos 1 símbolo especial válido.
    5. Actualiza el hash SHA-256, actualiza passwordUpdatedAt al timestamp actual y limpia mustChangePassword.
    """
    logger.info(f"Solicitud de cambio de contraseña para usuario: {payload.username}")
    
    # 1. Buscar usuario
    usuario = db.query(Usuario).filter(Usuario.username == payload.username).first()
    if not usuario:
        logger.warning(f"Usuario no encontrado para cambio de clave: {payload.username}")
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
    if usuario.status != 1:
        raise HTTPException(status_code=403, detail="La cuenta se encuentra desactivada")
        
    # 2. Validar contraseña actual
    if not verifyPassword(payload.currentPassword, usuario.passwordHash):
        logger.warning(f"Contraseña actual incorrecta para usuario: {payload.username}")
        raise HTTPException(status_code=400, detail="La contraseña actual ingresada es incorrecta.")
        
    # 3. Validar confirmación
    if payload.newPassword != payload.confirmPassword:
        raise HTTPException(status_code=400, detail="La nueva contraseña y su confirmación no coinciden.")
        
    # 4. Validar que no sea igual a la actual
    if payload.newPassword == payload.currentPassword:
        raise HTTPException(status_code=400, detail="La nueva contraseña debe ser diferente a la contraseña actual.")
        
    # 5. Validar política estricta de complejidad
    isValid, errorMsg = validatePasswordPolicy(payload.newPassword)
    if not isValid:
        logger.warning(f"Contraseña rechazada por política para {payload.username}: {errorMsg}")
        raise HTTPException(status_code=400, detail=errorMsg)
        
    # 6. Calcular nuevo hash SHA-256 y actualizar fecha
    newHash = calculateSha256(payload.newPassword)
    usuario.passwordHash = newHash
    usuario.passwordUpdatedAt = datetime.now()
    usuario.mustChangePassword = 0
    
    db.commit()
    db.refresh(usuario)
    
    roleInfo = db.query(Role).filter(Role.idRole == usuario.idRole).first()
    nameRole = roleInfo.nameRole if roleInfo else "Desconocido"
    
    logger.info(f"✅ Contraseña actualizada exitosamente para usuario: {payload.username}")
    
    userResp = UserResponse(
        idUsuario=usuario.idUsuario,
        username=usuario.username,
        email=usuario.email,
        nombre=usuario.nombre,
        apellidoPaterno=usuario.apellidoPaterno,
        apellidoMaterno=usuario.apellidoMaterno,
        idRole=usuario.idRole,
        nameRole=nameRole,
        status=usuario.status,
        passwordExpired=False,
        daysSincePasswordUpdate=0,
        mustChangePassword=False
    )
    
    return ActionResponse(
        success=True,
        message="Contraseña actualizada exitosamente.",
        user=userResp
    )

@router.get("/menu/{idRole}", response_model=List[MenuResponse])
def getMenuByRole(idRole: int, db: Session = Depends(get_db)):
    """
    Carga dinámicamente los menús autorizados para un rol específico.
    """
    logger.info(f"Cargando menús dinámicos autorizados para el rol ID: {idRole}")
    
    try:
        menusAutorizados = (
            db.query(Menu)
            .join(RoleMenu, Menu.idMenu == RoleMenu.idMenu)
            .filter(RoleMenu.idRole == idRole)
            .order_by(Menu.idMenu.asc())
            .all()
        )
        
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
