import hashlib
import re
from datetime import datetime
from typing import Optional, Tuple

def calculateSha256(text: str) -> str:
    """
    Calcula el hash SHA-256 de un texto dado en formato hexadecimal.
    Asegura perfecta compatibilidad criptográfica con MessageDigest en Java.
    """
    if not text:
        return ""
    sha256 = hashlib.sha256()
    sha256.update(text.encode("utf-8"))
    return sha256.hexdigest()

def verifyPassword(plainPassword: str, passwordHash: str) -> bool:
    """
    Compara una contraseña en texto plano con el hash guardado en base de datos.
    """
    calculated = calculateSha256(plainPassword)
    return calculated == passwordHash

def validatePasswordPolicy(password: str) -> Tuple[bool, str]:
    """
    Valida los 4 criterios institucionales de seguridad de contraseñas:
    1. Mínimo 8 caracteres.
    2. Mínimo 1 número (0-9).
    3. Letras mayúsculas (A-Z) y minúsculas (a-z).
    4. Mínimo 1 símbolo / caracter especial válido.
    """
    if not password or len(password) < 8:
        return False, "La contraseña debe tener al menos 8 caracteres."
    
    if not re.search(r"[0-9]", password):
        return False, "La contraseña debe contener al menos 1 número."
    
    if not re.search(r"[A-Z]", password):
        return False, "La contraseña debe contener al menos 1 letra mayúscula."
    
    if not re.search(r"[a-z]", password):
        return False, "La contraseña debe contener al menos 1 letra minúscula."
    
    symbolPattern = r"[!@#$%^&*()_+\-=\[\]{}|;:,.<>?/~`'\"\\§±€£¥]"
    if not re.search(symbolPattern, password):
        return False, "La contraseña debe contener al menos 1 símbolo o caracter especial (!@#$%^&*...)."
    
    return True, "Contraseña segura y válida."

def isPasswordExpired(passwordUpdatedAt: Optional[datetime], maxDays: int = 120) -> bool:
    """
    Determina si la contraseña ha caducado (política de 4 meses / 120 días).
    """
    if not passwordUpdatedAt:
        return True
    
    delta = datetime.now() - passwordUpdatedAt
    return delta.days >= maxDays

def getDaysSincePasswordUpdate(passwordUpdatedAt: Optional[datetime]) -> int:
    """
    Calcula los días transcurridos desde la última actualización de contraseña.
    """
    if not passwordUpdatedAt:
        return 999
    delta = datetime.now() - passwordUpdatedAt
    return max(0, delta.days)
