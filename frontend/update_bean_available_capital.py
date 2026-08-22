bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Add availableCapital field to SignalTradeDto
target_fields = '''        private Double marginA;
        private Double marginB;
        private Double accumCapital;
        private Boolean isOpen = false;'''

new_fields = '''        private Double marginA;
        private Double marginB;
        private Double availableCapital;
        private Double accumCapital;
        private Boolean isOpen = false;'''

code = code.replace(target_fields, new_fields, 1)

# 2. Add getter, setter and formatter for availableCapital
target_methods = '''        public Double getAccumCapital() { return accumCapital; }
        public void setAccumCapital(Double accumCapital) { this.accumCapital = accumCapital; }'''

new_methods = '''        public Double getAvailableCapital() { return availableCapital; }
        public void setAvailableCapital(Double availableCapital) { this.availableCapital = availableCapital; }
        public String getFormattedAvailableCapital() {
            return availableCapital != null ? String.format(java.util.Locale.US, "%,.2f", availableCapital) : "0.00";
        }
        public Double getAccumCapital() { return accumCapital; }
        public void setAccumCapital(Double accumCapital) { this.accumCapital = accumCapital; }'''

code = code.replace(target_methods, new_methods, 1)

# 3. Add parsing for availableCapital in json loops
target_parsing = '''                                if (t.has("marginA") && !t.get("marginA").isNull()) st.setMarginA(t.get("marginA").asDouble());
                                if (t.has("marginB") && !t.get("marginB").isNull()) st.setMarginB(t.get("marginB").asDouble());
                                if (t.has("accumCapital") && !t.get("accumCapital").isNull()) st.setAccumCapital(t.get("accumCapital").asDouble());'''

new_parsing = '''                                if (t.has("marginA") && !t.get("marginA").isNull()) st.setMarginA(t.get("marginA").asDouble());
                                if (t.has("marginB") && !t.get("marginB").isNull()) st.setMarginB(t.get("marginB").asDouble());
                                if (t.has("availableCapital") && !t.get("availableCapital").isNull()) st.setAvailableCapital(t.get("availableCapital").asDouble());
                                if (t.has("accumCapital") && !t.get("accumCapital").isNull()) st.setAccumCapital(t.get("accumCapital").asDouble());'''

code = code.replace(target_parsing, new_parsing)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("SUCCESS: Updated DashboardBean.java with availableCapital!")
