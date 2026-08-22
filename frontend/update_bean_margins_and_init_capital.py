bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Add marginA and marginB fields to SignalTradeDto
target_fields = '''        private Double pnlA;
        private Double pnlB;
        private Double accumCapital;
        private Boolean isOpen = false;'''

new_fields = '''        private Double pnlA;
        private Double pnlB;
        private Double marginA;
        private Double marginB;
        private Double accumCapital;
        private Boolean isOpen = false;'''

code = code.replace(target_fields, new_fields, 1)

# 2. Add getters and setters for marginA and marginB
target_dto_methods = '''        public Double getAccumCapital() { return accumCapital; }
        public void setAccumCapital(Double accumCapital) { this.accumCapital = accumCapital; }'''

new_dto_methods = '''        public Double getMarginA() { return marginA; }
        public void setMarginA(Double marginA) { this.marginA = marginA; }
        public Double getMarginB() { return marginB; }
        public void setMarginB(Double marginB) { this.marginB = marginB; }
        public String getFormattedMarginA() {
            return marginA != null ? String.format(java.util.Locale.US, "%,.2f", marginA) : "0.00";
        }
        public String getFormattedMarginB() {
            return marginB != null ? String.format(java.util.Locale.US, "%,.2f", marginB) : "0.00";
        }
        public Double getAccumCapital() { return accumCapital; }
        public void setAccumCapital(Double accumCapital) { this.accumCapital = accumCapital; }'''

code = code.replace(target_dto_methods, new_dto_methods, 1)

# 3. Add parsing for marginA and marginB in toTrades and cbTrades
target_parse = '''                                if (t.has("accumCapital") && !t.get("accumCapital").isNull()) st.setAccumCapital(t.get("accumCapital").asDouble());'''

new_parse = '''                                if (t.has("marginA") && !t.get("marginA").isNull()) st.setMarginA(t.get("marginA").asDouble());
                                if (t.has("marginB") && !t.get("marginB").isNull()) st.setMarginB(t.get("marginB").asDouble());
                                if (t.has("accumCapital") && !t.get("accumCapital").isNull()) st.setAccumCapital(t.get("accumCapital").asDouble());'''

code = code.replace(target_parse, new_parse)

# 4. Add getFormattedSignalBtInitialCapital() method in DashboardBean
target_bean_method = '''    public String getFormattedSignalBtTotalPnl() {
        Double pnl = getSignalBtTotalPnl();
        return pnl != null ? String.format(java.util.Locale.US, "%,.2f", pnl) : "0.00";
    }'''

new_bean_method = '''    public String getFormattedSignalBtTotalPnl() {
        Double pnl = getSignalBtTotalPnl();
        return pnl != null ? String.format(java.util.Locale.US, "%,.2f", pnl) : "0.00";
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

code = code.replace(target_bean_method, new_bean_method, 1)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with marginA, marginB and getFormattedSignalBtInitialCapital()!")
