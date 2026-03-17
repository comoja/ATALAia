#!/usr/bin/env python3
"""
Script para convertir todo el código de middlend de snake_case a camelCase.
"""
import re
import os
from pathlib import Path

def to_camel_case(snake_str):
    """Convierte snake_case a camelCase."""
    if not snake_str or '_' not in snake_str:
        return snake_str
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])

def to_snake_case(name):
    """Convierte camelCase a snake_case."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def is_python_identifier(s):
    """Verifica si es un identificador válido de Python."""
    return s.isidentifier()

def replace_in_file(filepath):
    """Procesa un archivo y reemplaza snake_case con camelCase."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    original_content = content
    
    # Palabras clave que NO deben ser convertidas (constantes, palabras reservadas, etc.)
    # Esta es una aproximación -进行调整
    protected_words = {
        # Python keywords
        'True', 'False', 'None', 'and', 'or', 'not', 'if', 'else', 'elif',
        'for', 'while', 'try', 'except', 'finally', 'with', 'as', 'import',
        'from', 'class', 'def', 'return', 'yield', 'raise', 'pass', 'break',
        'continue', 'lambda', 'global', 'nonlocal', 'assert', 'del', 'in', 'is',
        # Common non-convertible
        'max', 'min', 'abs', 'len', 'str', 'int', 'float', 'list', 'dict',
        'set', 'tuple', 'range', 'enumerate', 'zip', 'map', 'filter', 'sum',
        'open', 'print', 'input', 'type', 'isinstance', 'hasattr', 'getattr',
        'setattr', 'delattr', 'callable', 'issubclass', 'super', 'object',
        # Numpy/pandas
        'pd', 'np', 'ta', 'df', 'Series', 'DataFrame', 
    }
    
    # Palabras que son completamente en mayúsculas (constantes) - no convertir
    # Primero, encontrar todas las palabras en CONSTANT_CASE
    constant_pattern = r'\b[A-Z][A-Z0-9_]+\b'
    constants = set(re.findall(constant_pattern, content))
    
    # Encontrar todas las palabras en snake_case (que no sean constantes)
    # Patrón: palabra con al menos una guión bajo, que no sea CONSTANT_CASE
    snake_pattern = r'\b([a-z][a-z0-9]*)_([a-z][a-z0-9]*(?:_[a-z0-9]+)*)\b'
    
    def replace_snake(match):
        full_match = match.group(0)
        
        # No convertir si es una palabra protegida
        if full_match in protected_words:
            return full_match
        
        # No convertir si es completamente mayúsculas
        if full_match.isupper():
            return full_match
        
        # Convertir a camelCase
        camel = to_camel_case(full_match)
        return camel
    
    # Aplicar el reemplazo
    content = re.sub(snake_pattern, replace_snake, content)
    
    # Manejar nombres de clases (CapitalCase -> camelCase, pero las clases se mantienen CapitalCase)
    # Solo cambiar funciones y variables
    
    if content != original_content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        return True
    return False

def main():
    middlend_path = Path('/Volumes/TOSHIBA5TB/Backup/desarrollo/ATALAia/middlend')
    
    files_changed = 0
    for filepath in middlend_path.rglob('*.py'):
        if '__pycache__' in str(filepath):
            continue
        if replace_in_file(filepath):
            print(f"Changed: {filepath}")
            files_changed += 1
    
    print(f"\nTotal files changed: {files_changed}")

if __name__ == '__main__':
    main()
