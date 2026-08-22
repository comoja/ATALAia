bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_prop = '    public static class QuantTradeDto implements java.io.Serializable {\n        private Integer tradeNum;\n        private String type;\n        private String entryDate;\n        private String exitDate;\n        private Double entryPrice;\n        private Double exitPrice;\n        private Integer durationBars;\n        private Double returnPct;\n        private Double pnl;\n        private String exitReason;\n        private Boolean isWin;'

replacement_prop = '''    public static class QuantTradeDto implements java.io.Serializable {
        private Integer tradeNum;
        private String type;
        private String entryDate;
        private String exitDate;
        private Double allocatedCapital = 10000.0;
        private Double entryPrice;
        private Double exitPrice;
        private Integer durationBars;
        private Double returnPct;
        private Double pnl;
        private String exitReason;
        private Boolean isWin;'''

if target_prop in code:
    code = code.replace(target_prop, replacement_prop)

target_getter = '        public String getExitDate() { return exitDate; }\n        public void setExitDate(String exitDate) { this.exitDate = exitDate; }'
replacement_getter = '''        public String getExitDate() { return exitDate; }
        public void setExitDate(String exitDate) { this.exitDate = exitDate; }
        public Double getAllocatedCapital() { return allocatedCapital; }
        public void setAllocatedCapital(Double allocatedCapital) { this.allocatedCapital = allocatedCapital; }'''

if target_getter in code:
    code = code.replace(target_getter, replacement_getter)

old_parse = 'if (t.has("exitDate")) qt.setExitDate(t.get("exitDate").asText());'
new_parse = '''if (t.has("exitDate")) qt.setExitDate(t.get("exitDate").asText());
                                if (t.has("allocatedCapital")) qt.setAllocatedCapital(t.get("allocatedCapital").asDouble());'''

if old_parse in code and 'qt.setAllocatedCapital' not in code:
    code = code.replace(old_parse, new_parse)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated QuantTradeDto in DashboardBean with allocatedCapital!")
