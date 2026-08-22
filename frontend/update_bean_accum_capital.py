bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_field = '''        private Double pnlB;
        private Boolean isOpen = false;'''

new_field = '''        private Double pnlB;
        private Double accumCapital;
        private Boolean isOpen = false;'''

target_getter = '''        public Boolean getIsOpen() { return isOpen != null && isOpen; }
        public void setIsOpen(Boolean isOpen) { this.isOpen = isOpen; }'''

new_getter = '''        public Boolean getIsOpen() { return isOpen != null && isOpen; }
        public void setIsOpen(Boolean isOpen) { this.isOpen = isOpen; }
        public Double getAccumCapital() { return accumCapital; }
        public void setAccumCapital(Double accumCapital) { this.accumCapital = accumCapital; }
        public String getFormattedAccumCapital() {
            return accumCapital != null ? String.format(java.util.Locale.US, "%,.2f", accumCapital) : "0.00";
        }'''

code = code.replace(target_field, new_field, 1).replace(target_getter, new_getter, 1)

# Add parsing in toTrades and cbTrades
target_parse = '''                                if (t.has("pnlB") && !t.get("pnlB").isNull()) st.setPnlB(t.get("pnlB").asDouble());
                                if (t.has("isOpen")) st.setIsOpen(t.get("isOpen").asBoolean());'''

new_parse = '''                                if (t.has("pnlB") && !t.get("pnlB").isNull()) st.setPnlB(t.get("pnlB").asDouble());
                                if (t.has("accumCapital") && !t.get("accumCapital").isNull()) st.setAccumCapital(t.get("accumCapital").asDouble());
                                if (t.has("isOpen")) st.setIsOpen(t.get("isOpen").asBoolean());'''

code = code.replace(target_parse, new_parse)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with accumCapital support in DTO and JSON parsing!")
