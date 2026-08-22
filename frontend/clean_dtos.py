bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Replace SignalTradeDto with a clean version
signal_dto_clean = '''    public static class SignalTradeDto implements java.io.Serializable {
        private Integer tradeNum;
        private String signalType;
        private Integer cycleNum = 1;
        private Integer cycleTotalTrades = 1;
        private Double cycleTotalInvested = 2000.0;
        private Double cycleTotalPnl = 0.0;
        private Double cycleAvgReturnPct = 0.0;
        private String direction;
        private Double allocatedCapital = 2000.0;
        private String entryDate;
        private String exitDate;
        private Double entryPriceA;
        private Double exitPriceA;
        private Double entryPriceB;
        private Double exitPriceB;
        private Integer durationBars;
        private Double returnPct;
        private Double pnl;
        private String exitReason;
        private Boolean isWin;

        public Integer getTradeNum() { return tradeNum; }
        public void setTradeNum(Integer tradeNum) { this.tradeNum = tradeNum; }
        public String getSignalType() { return signalType; }
        public void setSignalType(String signalType) { this.signalType = signalType; }
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
        public String getDirection() { return direction; }
        public void setDirection(String direction) { this.direction = direction; }
        public Double getAllocatedCapital() { return allocatedCapital; }
        public void setAllocatedCapital(Double allocatedCapital) { this.allocatedCapital = allocatedCapital; }
        public String getEntryDate() { return entryDate; }
        public void setEntryDate(String entryDate) { this.entryDate = entryDate; }
        public String getExitDate() { return exitDate; }
        public void setExitDate(String exitDate) { this.exitDate = exitDate; }
        public Double getEntryPriceA() { return entryPriceA; }
        public void setEntryPriceA(Double entryPriceA) { this.entryPriceA = entryPriceA; }
        public Double getExitPriceA() { return exitPriceA; }
        public void setExitPriceA(Double exitPriceA) { this.exitPriceA = exitPriceA; }
        public Double getEntryPriceB() { return entryPriceB; }
        public void setEntryPriceB(Double entryPriceB) { this.entryPriceB = entryPriceB; }
        public Double getExitPriceB() { return exitPriceB; }
        public void setExitPriceB(Double exitPriceB) { this.exitPriceB = exitPriceB; }
        public Integer getDurationBars() { return durationBars; }
        public void setDurationBars(Integer durationBars) { this.durationBars = durationBars; }
        public Double getReturnPct() { return returnPct; }
        public void setReturnPct(Double returnPct) { this.returnPct = returnPct; }
        public Double getPnl() { return pnl; }
        public void setPnl(Double pnl) { this.pnl = pnl; }
        public String getExitReason() { return exitReason; }
        public void setExitReason(String exitReason) { this.exitReason = exitReason; }
        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }
    }'''

# Replace QuantTradeDto with a clean version
quant_dto_clean = '''    public static class QuantTradeDto implements java.io.Serializable {
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
        private Boolean isWin;

        public Integer getTradeNum() { return tradeNum; }
        public void setTradeNum(Integer tradeNum) { this.tradeNum = tradeNum; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public String getEntryDate() { return entryDate; }
        public void setEntryDate(String entryDate) { this.entryDate = entryDate; }
        public String getExitDate() { return exitDate; }
        public void setExitDate(String exitDate) { this.exitDate = exitDate; }
        public Double getAllocatedCapital() { return allocatedCapital; }
        public void setAllocatedCapital(Double allocatedCapital) { this.allocatedCapital = allocatedCapital; }
        public Double getEntryPrice() { return entryPrice; }
        public void setEntryPrice(Double entryPrice) { this.entryPrice = entryPrice; }
        public Double getExitPrice() { return exitPrice; }
        public void setExitPrice(Double exitPrice) { this.exitPrice = exitPrice; }
        public Integer getDurationBars() { return durationBars; }
        public void setDurationBars(Integer durationBars) { this.durationBars = durationBars; }
        public Double getReturnPct() { return returnPct; }
        public void setReturnPct(Double returnPct) { this.returnPct = returnPct; }
        public Double getPnl() { return pnl; }
        public void setPnl(Double pnl) { this.pnl = pnl; }
        public String getExitReason() { return exitReason; }
        public void setExitReason(String exitReason) { this.exitReason = exitReason; }
        public Boolean getIsWin() { return isWin; }
        public void setIsWin(Boolean isWin) { this.isWin = isWin; }
    }'''

import re
code = re.sub(r'    public static class SignalTradeDto implements java\.io\.Serializable \{.*?\n    \}', signal_dto_clean, code, flags=re.DOTALL)
code = re.sub(r'    public static class QuantTradeDto implements java\.io\.Serializable \{.*?\n    \}', quant_dto_clean, code, flags=re.DOTALL)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Properly replaced SignalTradeDto and QuantTradeDto in DashboardBean.java!")
