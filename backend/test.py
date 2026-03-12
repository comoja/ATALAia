import sys

def saludo():
    print("🐍 ¡Hola desde Python en Mac!")
    print(f"Versión: {sys.version.split()[0]}")

if __name__ == "__main__":
    saludo()
