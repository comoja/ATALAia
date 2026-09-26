filepath = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java"
with open(filepath, "r") as f:
    content = f.read()

old_block = '''                        if (item.has("tipoEntrada") && !item.get("tipoEntrada").isNull()) dto.setTipoEntrada(item.get("tipoEntrada").asText());
                        else dto.setTipoEntrada("Selectiva");
                        if (item.has("borrado") && !item.get("borrado").isNull()) {'''

new_block = '''                        if (item.has("tipoEntrada") && !item.get("tipoEntrada").isNull()) dto.setTipoEntrada(item.get("tipoEntrada").asText());
                        else dto.setTipoEntrada("Selectiva");
                        if (item.has("createdAt") && !item.get("createdAt").isNull()) dto.setCreatedAt(item.get("createdAt").asText());
                        if (item.has("borrado") && !item.get("borrado").isNull()) {'''

if old_block in content:
    content = content.replace(old_block, new_block)
    print("✅ createdAt añadido a loadUserRatiosList")
else:
    print("❌ No se encontró el bloque a reemplazar")

with open(filepath, "w") as f:
    f.write(content)
print("✅ Archivo guardado")
