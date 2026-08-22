bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Add field and getter/setter in SignalTradeDto
target_field = '        private String signalType;'
new_field = '''        private String signalType;
        private Boolean isSubtotal = false;'''

target_getter = '''        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }'''

new_getter = '''        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }
        public Boolean getIsSubtotal() { return isSubtotal != null && isSubtotal; }
        public void setIsSubtotal(Boolean isSubtotal) { this.isSubtotal = isSubtotal; }'''

if target_field in code and 'isSubtotal' not in code:
    code = code.replace(target_field, new_field)

if target_getter in code and 'getIsSubtotal' not in code:
    code = code.replace(target_getter, new_getter)

# Update parser in loadQuantPairAnalysis
target_parse = 'if (t.has("isWin")) st.setIsWin(t.get("isWin").asBoolean());'
new_parse = '''if (t.has("isWin")) st.setIsWin(t.get("isWin").asBoolean());
                                if (t.has("isSubtotal")) st.setIsSubtotal(t.get("isSubtotal").asBoolean());'''

if target_parse in code and 'setIsSubtotal' not in code:
    code = code.replace(target_parse, new_parse)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with isSubtotal support!")
