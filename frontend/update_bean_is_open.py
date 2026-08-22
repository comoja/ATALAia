bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_field = '''        private Double pnlA;
        private Double pnlB;'''

new_field = '''        private Double pnlA;
        private Double pnlB;
        private Boolean isOpen = false;'''

target_getter = '''        public Double getPnlB() { return pnlB; }
        public void setPnlB(Double pnlB) { this.pnlB = pnlB; }'''

new_getter = '''        public Double getPnlB() { return pnlB; }
        public void setPnlB(Double pnlB) { this.pnlB = pnlB; }
        public Boolean getIsOpen() { return isOpen != null && isOpen; }
        public void setIsOpen(Boolean isOpen) { this.isOpen = isOpen; }'''

code = code.replace(target_field, new_field, 1).replace(target_getter, new_getter, 1)

# Update toTrades and cbTrades parsing
target_parse = '''                                if (t.has("pnlA") && !t.get("pnlA").isNull()) st.setPnlA(t.get("pnlA").asDouble());
                                if (t.has("pnlB") && !t.get("pnlB").isNull()) st.setPnlB(t.get("pnlB").asDouble());'''

new_parse = '''                                if (t.has("pnlA") && !t.get("pnlA").isNull()) st.setPnlA(t.get("pnlA").asDouble());
                                if (t.has("pnlB") && !t.get("pnlB").isNull()) st.setPnlB(t.get("pnlB").asDouble());
                                if (t.has("isOpen")) st.setIsOpen(t.get("isOpen").asBoolean());'''

code = code.replace(target_parse, new_parse)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with isOpen support!")
