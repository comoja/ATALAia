bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = '''        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }
        public Boolean getIsSubtotal() { return isSubtotal != null && isSubtotal; }
        public void setIsSubtotal(Boolean isSubtotal) { this.isSubtotal = isSubtotal; }
    }

        // --- Propiedades del Backtest de Señales Gráficas ---'''

replacement = '''        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }
    }

        // --- Propiedades del Backtest de Señales Gráficas ---'''

if target in code:
    code = code.replace(target, replacement)
    with open(bean_path, 'w', encoding='utf-8') as f:
        f.write(code)
    print("Removed subtotal getters from QuantTradeDto!")
else:
    print("Target not found!")
