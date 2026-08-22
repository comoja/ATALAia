bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_fields = '''        private Double initialCapital = 10000.0;
        private Double finalCapital = 10000.0;'''

new_fields = '''        private Double initialCapital = 10000.0;
        private Double finalCapital = 10000.0;
        private Double allocationPct = 3.0;
        private Double reqMarginPerMinLot = 20.0;'''

target_getters = '''        public Double getFinalCapital() { return finalCapital; }
        public void setFinalCapital(Double finalCapital) { this.finalCapital = finalCapital; }'''

new_getters = '''        public Double getFinalCapital() { return finalCapital; }
        public void setFinalCapital(Double finalCapital) { this.finalCapital = finalCapital; }
        public Double getAllocationPct() { return allocationPct; }
        public void setAllocationPct(Double allocationPct) { this.allocationPct = allocationPct; }
        public Double getReqMarginPerMinLot() { return reqMarginPerMinLot; }
        public void setReqMarginPerMinLot(Double reqMarginPerMinLot) { this.reqMarginPerMinLot = reqMarginPerMinLot; }'''

code = code.replace(target_fields, new_fields).replace(target_getters, new_getters)

target_parse = 'if (cb.has("finalCapital") && !cb.get("finalCapital").isNull()) metrics.setFinalCapital(cb.get("finalCapital").asDouble());'
new_parse = '''if (cb.has("finalCapital") && !cb.get("finalCapital").isNull()) metrics.setFinalCapital(cb.get("finalCapital").asDouble());
                        if (cb.has("allocationPct") && !cb.get("allocationPct").isNull()) metrics.setAllocationPct(cb.get("allocationPct").asDouble());
                        if (cb.has("reqMarginPerMinLot") && !cb.get("reqMarginPerMinLot").isNull()) metrics.setReqMarginPerMinLot(cb.get("reqMarginPerMinLot").asDouble());'''

code = code.replace(target_parse, new_parse)

target_parse_tri = 'if (tr.has("finalCapital") && !tr.get("finalCapital").isNull()) metrics.setFinalCapital(tr.get("finalCapital").asDouble());'
new_parse_tri = '''if (tr.has("finalCapital") && !tr.get("finalCapital").isNull()) metrics.setFinalCapital(tr.get("finalCapital").asDouble());
                        if (tr.has("allocationPct") && !tr.get("allocationPct").isNull()) metrics.setAllocationPct(tr.get("allocationPct").asDouble());
                        if (tr.has("reqMarginPerMinLot") && !tr.get("reqMarginPerMinLot").isNull()) metrics.setReqMarginPerMinLot(tr.get("reqMarginPerMinLot").asDouble());'''

code = code.replace(target_parse_tri, new_parse_tri)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with reqMarginPerMinLot and allocationPct support!")
