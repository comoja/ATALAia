bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

sum_getters = '''
    public Double getSignalBtTotalReturn() {
        if ("TRIANGLES".equals(this.signalBtStrategySelected)) {
            return signalBtTrianglesMetrics != null ? signalBtTrianglesMetrics.getTotalReturnPct() : 0.0;
        } else {
            return signalBtCombinedMetrics != null ? signalBtCombinedMetrics.getTotalReturnPct() : 0.0;
        }
    }

    public Double getSignalBtTotalPnl() {
        if ("TRIANGLES".equals(this.signalBtStrategySelected)) {
            return signalBtTrianglesMetrics != null ? signalBtTrianglesMetrics.getNetProfit() : 0.0;
        } else {
            return signalBtCombinedMetrics != null ? signalBtCombinedMetrics.getNetProfit() : 0.0;
        }
    }

    public Double getQuantTotalPnl() {
        if (quantMetrics != null && quantMetrics.getFinalCapital() != null && quantMetrics.getInitialCapital() != null) {
            return Math.round((quantMetrics.getFinalCapital() - quantMetrics.getInitialCapital()) * 100.0) / 100.0;
        }
        return 0.0;
    }
'''

if 'public Double getSignalBtTotalReturn()' not in code:
    code = code.replace(
        'public void onSignalStrategyChange() {',
        sum_getters + '\n    public void onSignalStrategyChange() {'
    )
    with open(bean_path, 'w', encoding='utf-8') as f:
        f.write(code)
    print("Added sum getters to DashboardBean.java!")
else:
    print("Sum getters already present.")
