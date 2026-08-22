bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = '''    public String getFormattedSignalBtTotalPnl() {
        Double p = getSignalBtTotalPnl();
        return p != null ? String.format(java.util.Locale.US, "%,.2f", p) : "0.00";
    }'''

replacement = '''    public String getFormattedSignalBtTotalPnl() {
        Double p = getSignalBtTotalPnl();
        return p != null ? String.format(java.util.Locale.US, "%,.2f", p) : "0.00";
    }

    public String getFormattedSignalBtInitialCapital() {
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

if target in code:
    code = code.replace(target, replacement, 1)
    with open(bean_path, 'w', encoding='utf-8') as f:
        f.write(code)
    print("SUCCESS: Added getFormattedSignalBtInitialCapital to DashboardBean.java!")
else:
    print("ERROR: Target not found in DashboardBean.java!")
