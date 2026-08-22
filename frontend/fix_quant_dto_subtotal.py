bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = '        private String exitReason;\n        private Boolean isWin;'
replacement = '        private String exitReason;\n        private Boolean isWin;\n        private Boolean isSubtotal = false;'

code = code.replace(target, replacement)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Added isSubtotal field to QuantTradeDto in DashboardBean.java!")
