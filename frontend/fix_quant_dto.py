bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target = '        private Double liveZScore = 0.0;'
replacement = '''        private Double liveZScore = 0.0;
        private Double initialCapital = 10000.0;
        private Double finalCapital = 10000.0;

        public Double getInitialCapital() { return initialCapital; }
        public void setInitialCapital(Double initialCapital) { this.initialCapital = initialCapital; }
        public Double getFinalCapital() { return finalCapital; }
        public void setFinalCapital(Double finalCapital) { this.finalCapital = finalCapital; }'''

if target in code and 'public Double getFinalCapital()' not in code:
    code = code.replace(target, replacement)

# Also parse initialCapital and finalCapital in loadQuantPairAnalysis
old_parse = 'if (bm.has("totalReturnPct")) metrics.setTotalReturnPct(bm.get("totalReturnPct").asDouble());'
new_parse = '''if (bm.has("totalReturnPct")) metrics.setTotalReturnPct(bm.get("totalReturnPct").asDouble());
                    if (bm.has("initialCapital")) metrics.setInitialCapital(bm.get("initialCapital").asDouble());
                    if (bm.has("finalCapital")) metrics.setFinalCapital(bm.get("finalCapital").asDouble());'''
code = code.replace(old_parse, new_parse)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Fixed QuantMetricsDto capital getters!")
