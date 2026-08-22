bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_prop = '        private String direction;'
replacement_prop = '''        private String direction;
        private Double allocatedCapital = 1500.0;'''

target_getter = '        public void setDirection(String direction) { this.direction = direction; }'
replacement_getter = '''        public void setDirection(String direction) { this.direction = direction; }
        public Double getAllocatedCapital() { return allocatedCapital; }
        public void setAllocatedCapital(Double allocatedCapital) { this.allocatedCapital = allocatedCapital; }'''

if 'allocatedCapital' not in code:
    code = code.replace(target_prop, replacement_prop)
    code = code.replace(target_getter, replacement_getter)

old_parse_t = 'if (t.has("direction")) st.setDirection(t.get("direction").asText());'
new_parse_t = '''if (t.has("direction")) st.setDirection(t.get("direction").asText());
                                if (t.has("allocatedCapital")) st.setAllocatedCapital(t.get("allocatedCapital").asDouble());'''
code = code.replace(old_parse_t, new_parse_t)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean with allocatedCapital in SignalTradeDto!")
