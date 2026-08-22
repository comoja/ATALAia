bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_fields = '''        private String exitReason;
        private Boolean isWin;
        private Boolean isSubtotal = false;'''

new_fields = '''        private String exitReason;
        private Boolean isWin;
        private Boolean isSubtotal = false;
        private Integer multA;
        private Integer multB;
        private Integer unitsA;
        private Integer unitsB;
        private Double pipsA;
        private Double pipsB;'''

target_getters = '''        public Boolean getIsSubtotal() { return isSubtotal != null && isSubtotal; }
        public void setIsSubtotal(Boolean isSubtotal) { this.isSubtotal = isSubtotal; }
    }'''

new_getters = '''        public Boolean getIsSubtotal() { return isSubtotal != null && isSubtotal; }
        public void setIsSubtotal(Boolean isSubtotal) { this.isSubtotal = isSubtotal; }
        public Integer getMultA() { return multA; }
        public void setMultA(Integer multA) { this.multA = multA; }
        public Integer getMultB() { return multB; }
        public void setMultB(Integer multB) { this.multB = multB; }
        public Integer getUnitsA() { return unitsA; }
        public void setUnitsA(Integer unitsA) { this.unitsA = unitsA; }
        public Integer getUnitsB() { return unitsB; }
        public void setUnitsB(Integer unitsB) { this.unitsB = unitsB; }
        public Double getPipsA() { return pipsA; }
        public void setPipsA(Double pipsA) { this.pipsA = pipsA; }
        public Double getPipsB() { return pipsB; }
        public void setPipsB(Double pipsB) { this.pipsB = pipsB; }
    }'''

code = code.replace(target_fields, new_fields).replace(target_getters, new_getters)

# Update JSON parser in loadQuantPairAnalysis
target_parse = 'if (t.has("isSubtotal")) st.setIsSubtotal(t.get("isSubtotal").asBoolean());'
new_parse = '''if (t.has("isSubtotal")) st.setIsSubtotal(t.get("isSubtotal").asBoolean());
                                if (t.has("multA") && !t.get("multA").isNull()) st.setMultA(t.get("multA").asInt());
                                if (t.has("multB") && !t.get("multB").isNull()) st.setMultB(t.get("multB").asInt());
                                if (t.has("unitsA") && !t.get("unitsA").isNull()) st.setUnitsA(t.get("unitsA").asInt());
                                if (t.has("unitsB") && !t.get("unitsB").isNull()) st.setUnitsB(t.get("unitsB").asInt());
                                if (t.has("pipsA") && !t.get("pipsA").isNull()) st.setPipsA(t.get("pipsA").asDouble());
                                if (t.has("pipsB") && !t.get("pipsB").isNull()) st.setPipsB(t.get("pipsB").asDouble());'''

code = code.replace(target_parse, new_parse)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with multA, multB, unitsA, unitsB, pipsA, pipsB!")
