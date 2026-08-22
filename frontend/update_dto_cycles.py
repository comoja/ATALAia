bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_fields = '        private String signalType;'
replacement_fields = '''        private String signalType;
        private Integer cycleNum = 1;
        private Integer cycleTotalTrades = 1;
        private Double cycleTotalInvested = 2000.0;
        private Double cycleTotalPnl = 0.0;
        private Double cycleAvgReturnPct = 0.0;'''

if target_fields in code and 'cycleNum' not in code:
    code = code.replace(target_fields, replacement_fields)

target_methods = '        public Boolean getIsWin() { return isWin; }\n        public void setIsWin(Boolean isWin) { this.isWin = isWin; }'
replacement_methods = '''        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }
        public Integer getCycleNum() { return cycleNum; }
        public void setCycleNum(Integer cycleNum) { this.cycleNum = cycleNum; }
        public Integer getCycleTotalTrades() { return cycleTotalTrades; }
        public void setCycleTotalTrades(Integer cycleTotalTrades) { this.cycleTotalTrades = cycleTotalTrades; }
        public Double getCycleTotalInvested() { return cycleTotalInvested; }
        public void setCycleTotalInvested(Double cycleTotalInvested) { this.cycleTotalInvested = cycleTotalInvested; }
        public Double getCycleTotalPnl() { return cycleTotalPnl; }
        public void setCycleTotalPnl(Double cycleTotalPnl) { this.cycleTotalPnl = cycleTotalPnl; }
        public Double getCycleAvgReturnPct() { return cycleAvgReturnPct; }
        public void setCycleAvgReturnPct(Double cycleAvgReturnPct) { this.cycleAvgReturnPct = cycleAvgReturnPct; }'''

if target_methods in code and 'getCycleNum' not in code:
    code = code.replace(target_methods, replacement_methods)

# Update parser in loadQuantPairAnalysis
old_parse_block = 'if (t.has("isWin")) st.setIsWin(t.get("isWin").asBoolean());'
new_parse_block = '''if (t.has("isWin")) st.setIsWin(t.get("isWin").asBoolean());
                                if (t.has("cycleNum")) st.setCycleNum(t.get("cycleNum").asInt());
                                if (t.has("cycleTotalTrades")) st.setCycleTotalTrades(t.get("cycleTotalTrades").asInt());
                                if (t.has("cycleTotalInvested")) st.setCycleTotalInvested(t.get("cycleTotalInvested").asDouble());
                                if (t.has("cycleTotalPnl")) st.setCycleTotalPnl(t.get("cycleTotalPnl").asDouble());
                                if (t.has("cycleAvgReturnPct")) st.setCycleAvgReturnPct(t.get("cycleAvgReturnPct").asDouble());'''

code = code.replace(old_parse_block, new_parse_block)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean with cycle fields, getters and parser!")
