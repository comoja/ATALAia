bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = '''    public String getFormattedSignalBtInitialCapital() {
        if (signalBtCombinedMetrics != null && signalBtCombinedMetrics.getInitialCapital() != null) {
            double ic = signalBtCombinedMetrics.getInitialCapital();
            if (ic == Math.floor(ic)) {
                return String.format(java.util.Locale.US, "%,.0f", ic);
            } else {
                return String.format(java.util.Locale.US, "%,.2f", ic);
            }
        }
        return "800";
    }'''

replacement = '''    public Double getSelectedAccountCapital() {
        if (selectedAccountId != null && userAccountsCombo != null) {
            for (UserAccountDto acc : userAccountsCombo) {
                if (selectedAccountId.equals(acc.getIdCuenta()) && acc.getCapital() != null) {
                    return acc.getCapital();
                }
            }
        }
        if (signalBtCombinedMetrics != null && signalBtCombinedMetrics.getInitialCapital() != null) {
            return signalBtCombinedMetrics.getInitialCapital();
        }
        return 300.0;
    }

    public String getFormattedSignalBtInitialCapital() {
        Double cap = getSelectedAccountCapital();
        if (cap != null) {
            if (cap == Math.floor(cap)) {
                return String.format(java.util.Locale.US, "%,.0f", cap);
            } else {
                return String.format(java.util.Locale.US, "%,.2f", cap);
            }
        }
        return "300";
    }'''

code = code.replace(target, replacement, 1)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with dynamic getSelectedAccountCapital() using userAccountsCombo!")
