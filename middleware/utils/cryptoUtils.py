import os
import base64
import hashlib
import logging
from typing import Optional, Any, List
from cryptography.fernet import Fernet, InvalidToken
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

def getMasterKey(customKey: Optional[str] = None) -> bytes:
    """
    Obtiene o deriva una clave Fernet de 32 bytes en formato base64 URL-safe.
    Prioridad:
    1. customKey pasada como argumento.
    2. WEBHOOK_ENCRYPTION_KEY en .env.
    3. WEBHOOK_VERIFY_TOKEN en .env (derivada con SHA256 si es texto plano o token).
    4. Fallback por defecto.
    """
    load_dotenv(override=True)
    rawKey = customKey or os.getenv("WEBHOOK_ENCRYPTION_KEY") or os.getenv("WEBHOOK_VERIFY_TOKEN") or "ATALAia_DEFAULT_SECRET_KEY_2026"
    
    # Si viene el token cifrado completo de .env, derivar la clave base
    if len(rawKey) == 44:
        try:
            base64.urlsafe_b64decode(rawKey.encode("utf-8"))
            return rawKey.encode("utf-8")
        except Exception:
            pass
            
    digest = hashlib.sha256(rawKey.encode("utf-8")).digest()
    return base64.urlsafe_b64encode(digest)

def getAllDecryptionKeys(customKey: Optional[str] = None) -> List[bytes]:
    """
    Obtiene todas las posibles claves Fernet para desencriptar tokens
    generados con diferentes variables o claves maestras.
    """
    load_dotenv(override=True)
    keys: List[bytes] = []
    
    # 1. Clave primaria
    primary = getMasterKey(customKey)
    keys.append(primary)
    
    # 2. Claves candidatas del entorno
    envCandidates = [
        customKey,
        os.getenv("WEBHOOK_ENCRYPTION_KEY"),
        os.getenv("WEBHOOK_VERIFY_TOKEN"),
        "ATALAIA_DEFAULT_SECRET_KEY_2026",
        "ATALAia_DEFAULT_SECRET_KEY_2026",
        "gAAAAABqhJ_Lj40DfJxumWs7amUTCdyfZ_j2xKV7erLZ_pqi0Kbd51y7tbnSY28osGhOhTUdelW6wCAwJqOS2I4R2c95qT_VVQ=="
    ]
    
    for raw in envCandidates:
        if not raw:
            continue
        if len(raw) == 44:
            try:
                base64.urlsafe_b64decode(raw.encode("utf-8"))
                k = raw.encode("utf-8")
                if k not in keys:
                    keys.append(k)
            except Exception:
                pass
        digest = hashlib.sha256(raw.encode("utf-8")).digest()
        kDerived = base64.urlsafe_b64encode(digest)
        if kDerived not in keys:
            keys.append(kDerived)
            
    return keys

def encryptValue(plainText: str, customKey: Optional[str] = None) -> str:
    """
    Encripta una cadena de texto generando un token base64 opaco (sin prefijos).
    Ejemplo: '2,comoja66@gmail.comauui,PruebaJ34ny.' -> 'gAAAAABqh...'
    """
    if not plainText:
        return ""
        
    key = getMasterKey(customKey)
    cipher = Fernet(key)
    
    # Si ya es un token válido desencriptable, no re-encriptar
    try:
        cipher.decrypt(plainText.strip().encode("utf-8"))
        return plainText.strip()
    except Exception:
        pass

    encryptedBytes = cipher.encrypt(plainText.encode("utf-8"))
    return encryptedBytes.decode("utf-8")

def decryptValue(cipherText: str, customKey: Optional[str] = None) -> str:
    """
    Intenta desencriptar una cadena de texto.
    Si la cadena es un token cifrado válido, devuelve el texto en claro.
    Si es texto normal (o no se puede desencriptar con ninguna clave), devuelve la cadena original sin alterarla.
    """
    if not cipherText or not isinstance(cipherText, str):
        return cipherText
        
    cleaned = cipherText.strip()
    if cleaned.lower().startswith("enc:"):
        cleaned = cleaned[4:].strip()
        
    for k in getAllDecryptionKeys(customKey):
        try:
            cipher = Fernet(k)
            decryptedBytes = cipher.decrypt(cleaned.encode("utf-8"))
            return decryptedBytes.decode("utf-8")
        except (InvalidToken, Exception):
            continue
            
    return cipherText

def buildEncryptedAccountToken(idCuenta: Any, loginUsuario: str, tokenAcceso: str, customKey: Optional[str] = None) -> str:
    """
    Construye la cadena triple de autenticación 'idCuenta,loginUsuario,tokenAcceso'
    y la encripta para enviarla de forma segura en las señales de Sentinel.
    """
    rawStr = f"{idCuenta},{loginUsuario},{tokenAcceso}"
    return encryptValue(rawStr, customKey)
