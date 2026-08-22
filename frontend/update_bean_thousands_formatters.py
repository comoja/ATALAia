bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. SignalTradeDto formatters
target_st_end = '''        public Double getPnlB() { return pnlB; }
        public void setPnlB(Double pnlB) { this.pnlB = pnlB; }
    }'''

new_st_end = '''        public Double getPnlB() { return pnlB; }
        public void setPnlB(Double pnlB) { this.pnlB = pnlB; }
        public String getFormattedAllocatedCapital() {
            return allocatedCapital != null ? String.format(java.util.Locale.US, "%,.2f", allocatedCapital) : "0.00";
        }
        public String getFormattedUnitsA() {
            return unitsA != null ? String.format(java.util.Locale.US, "%,d", unitsA) : "0";
        }
        public String getFormattedUnitsB() {
            return unitsB != null ? String.format(java.util.Locale.US, "%,d", unitsB) : "0";
        }
        public String getFormattedPnl() {
            return pnl != null ? String.format(java.util.Locale.US, "%,.2f", pnl) : "0.00";
        }
        public String getFormattedPnlA() {
            return pnlA != null ? String.format(java.util.Locale.US, "%,.2f", pnlA) : "0.00";
        }
        public String getFormattedPnlB() {
            return pnlB != null ? String.format(java.util.Locale.US, "%,.2f", pnlB) : "0.00";
        }
    }'''

code = code.replace(target_st_end, new_st_end, 1)

# 2. SignalBacktestMetricsDto formatters
target_metrics_end = '''        public Double getReqMarginPerMinLot() { return reqMarginPerMinLot; }
        public void setReqMarginPerMinLot(Double reqMarginPerMinLot) { this.reqMarginPerMinLot = reqMarginPerMinLot; }
    }'''

new_metrics_end = '''        public Double getReqMarginPerMinLot() { return reqMarginPerMinLot; }
        public void setReqMarginPerMinLot(Double reqMarginPerMinLot) { this.reqMarginPerMinLot = reqMarginPerMinLot; }
        public String getFormattedInitialCapital() {
            return initialCapital != null ? String.format(java.util.Locale.US, "%,.2f", initialCapital) : "0.00";
        }
        public String getFormattedFinalCapital() {
            return finalCapital != null ? String.format(java.util.Locale.US, "%,.2f", finalCapital) : "0.00";
        }
        public String getFormattedNetProfit() {
            return netProfit != null ? String.format(java.util.Locale.US, "%,.2f", netProfit) : "0.00";
        }
        public String getFormattedReqMarginPerMinLot() {
            return reqMarginPerMinLot != null ? String.format(java.util.Locale.US, "%,.2f", reqMarginPerMinLot) : "0.00";
        }
    }'''

code = code.replace(target_metrics_end, new_metrics_end, 1)

# 3. DashboardBean formatted getters
target_dash_pnl = '''    public Double getSignalBtTotalPnl() {
        return signalBtTotalPnl;
    }'''

new_dash_pnl = '''    public Double getSignalBtTotalPnl() {
        return signalBtTotalPnl;
    }
    public String getFormattedSignalBtTotalPnl() {
        return signalBtTotalPnl != null ? String.format(java.util.Locale.US, "%,.2f", signalBtTotalPnl) : "0.00";
    }
    public String getFormattedSignalBtTotalInvested() {
        return signalBtTotalInvested != null ? String.format(java.util.Locale.US, "%,.2f", signalBtTotalInvested) : "0.00";
    }'''

code = code.replace(target_dash_pnl, new_dash_pnl, 1)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with thousands formatting methods (US comma separator)!")
