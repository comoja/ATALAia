bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = '''    public Double getSignalBtTotalPnl() {
        if ("TRIANGLES".equals(this.signalBtStrategySelected)) {
            return signalBtTrianglesMetrics != null ? signalBtTrianglesMetrics.getNetProfit() : 0.0;
        } else {
            return signalBtCombinedMetrics != null ? signalBtCombinedMetrics.getNetProfit() : 0.0;
        }
    }'''

replacement = '''    public Double getSignalBtTotalPnl() {
        if ("TRIANGLES".equals(this.signalBtStrategySelected)) {
            return signalBtTrianglesMetrics != null ? signalBtTrianglesMetrics.getNetProfit() : 0.0;
        } else {
            return signalBtCombinedMetrics != null ? signalBtCombinedMetrics.getNetProfit() : 0.0;
        }
    }

    public String getFormattedSignalBtTotalPnl() {
        Double p = getSignalBtTotalPnl();
        return p != null ? String.format(java.util.Locale.US, "%,.2f", p) : "0.00";
    }'''

code = code.replace(target, replacement, 1)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Added getFormattedSignalBtTotalPnl to DashboardBean.java!")
