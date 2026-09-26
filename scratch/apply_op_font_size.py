import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# Replace font-size: 1.35rem with font-size: 1.60rem in the Op column.
content = content.replace("font-size: 1.35rem; font-weight: 900;", "font-size: 1.60rem; font-weight: 900;")

with open(file_path, "w", encoding="utf-8") as f:
    f.write(content)

print("Applied font-size fix to Op column.")
