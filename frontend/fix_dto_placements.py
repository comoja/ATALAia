bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Make sure SignalTradeDto has cycle getters/setters
target_signal_end = '''        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }
        public Integer getCycleNum() { return cycleNum; }
        public void setCycleNum(Integer cycleNum) { this.cycleNum = cycleNum; }'''

proper_signal_end = '''        public Boolean getIsWin() { return isWin; }
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

if target_signal_end in code:
    code = code.replace(target_signal_end, proper_signal_end)

# Clean QuantTradeDto (remove cycle getters that don't belong there)
bad_quant_end = '''        public Boolean getIsWin() { return isWin; }
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
        public void setCycleAvgReturnPct(Double cycleAvgReturnPct) { this.cycleAvgReturnPct = cycleAvgReturnPct; }
    }'''

good_quant_end = '''        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }
    }'''

if bad_quant_end in code:
    code = code.replace(bad_quant_end, good_quant_end)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Cleaned DTO placements in DashboardBean.java!")
