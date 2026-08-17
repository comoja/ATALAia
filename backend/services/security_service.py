import hashlib

def calculateSha256(text: str) -> str:
    """
    Calcula el hash SHA-256 de un texto dado en formato hexadecimal.
    Asegura perfecta compatibilidad criptográfica con MessageDigest en Java.
    """
    if not text:
        return ""
    # Codificar la cadena en UTF-8 y aplicar SHA-256
    sha256 = hashlib.sha256()
    sha256.update(text.encode("utf-8"))
    return sha256.hexdigest()

def verifyPassword(plainPassword: str, passwordHash: str) -> bool:
    """
    Compara una contraseña en texto plano con el hash guardado en base de datos.
    """
    calculated = calculateSha256(plainPassword)
    return calculated == passwordHash
