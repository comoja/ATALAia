bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_fields = '''        private Double pipsA;
        private Double pipsB;'''

new_fields = '''        private Double pipsA;
        private Double pipsB;
        private Double pnlA;
        private Double pnlB;'''

target_getters = '''        public Double getPipsB() { return pipsB; }
        public void setPipsB(Double pipsB) { this.pipsB = pipsB; }
    }'''

new_getters = '''        public Double getPipsB() { return pipsB; }
        public void setPipsB(Double pipsB) { this.pipsB = pipsB; }
        public Double getPnlA() { return pnlA; }
        public void setPnlA(Double pnlA) { this.pnlA = pnlA; }
        public Double getPnlB() { return pnlB; }
        public void setPnlB(Double pnlB) { this.pnlB = pnlB; }
    }'''

code = code.replace(target_fields, new_fields, 1).replace(target_getters, new_getters, 1)

# Update toTrades and cbTrades parsing
target_parse_line = '''                                if (t.has("pipsA") && !t.get("pipsA").isNull()) st.setPipsA(t.get("pipsA").asDouble());
                                if (t.has("pipsB") && !t.get("pipsB").isNull()) st.setPipsB(t.get("pipsB").asDouble());'''

new_parse_line = '''                                if (t.has("pipsA") && !t.get("pipsA").isNull()) st.setPipsA(t.get("pipsA").asDouble());
                                if (t.has("pipsB") && !t.get("pipsB").isNull()) st.setPipsB(t.get("pipsB").asDouble());
                                if (t.has("pnlA") && !t.get("pnlA").isNull()) st.setPnlA(t.get("pnlA").asDouble());
                                if (t.has("pnlB") && !t.get("pnlB").isNull()) st.setPnlB(t.get("pnlB").asDouble());'''

code = code.replace(target_parse_line, new_parse_line)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with pnlA and pnlB support!")
