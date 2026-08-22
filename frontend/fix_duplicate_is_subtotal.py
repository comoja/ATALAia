bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = '''        private Boolean isWin;
        private Boolean isSubtotal = false;'''

replacement = '''        private Boolean isWin;'''

if target in code:
    code = code.replace(target, replacement)
    with open(bean_path, 'w', encoding='utf-8') as f:
        f.write(code)
    print("Removed duplicate isSubtotal in SignalTradeDto!")
