bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = '''        private String exitReason;
        private Boolean isWin;'''

replacement = '''        private String exitReason;
        private Boolean isWin;
        private Integer multA;
        private Integer multB;
        private Integer unitsA;
        private Integer unitsB;
        private Double pipsA;
        private Double pipsB;'''

code = code.replace(target, replacement, 1)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Fixed SignalTradeDto fields in DashboardBean.java!")
