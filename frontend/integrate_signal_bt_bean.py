bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# 1. Add Signal DTOs
signal_dtos = '''    // --- DTOs para el Backtest de Señales Gráficas (Triángulos y Cuadros) ---
    public static class SignalBacktestMetricsDto implements java.io.Serializable {
        private String mode = "TRIANGLES_ONLY";
        private String modeLabel = "Solo Triángulos (Coincidentes)";
        private Integer totalTrades = 0;
        private Integer winningTrades = 0;
        private Integer losingTrades = 0;
        private Double winRate = 0.0;
        private Double profitFactor = 0.0;
        private Double sharpeRatio = 0.0;
        private Double maxDrawdown = 0.0;
        private Double totalReturnPct = 0.0;
        private Double netProfit = 0.0;
        private Double initialCapital = 10000.0;
        private Double finalCapital = 10000.0;

        public String getMode() { return mode; }
        public void setMode(String mode) { this.mode = mode; }
        public String getModeLabel() { return modeLabel; }
        public void setModeLabel(String modeLabel) { this.modeLabel = modeLabel; }
        public Integer getTotalTrades() { return totalTrades; }
        public void setTotalTrades(Integer totalTrades) { this.totalTrades = totalTrades; }
        public Integer getWinningTrades() { return winningTrades; }
        public void setWinningTrades(Integer winningTrades) { this.winningTrades = winningTrades; }
        public Integer getLosingTrades() { return losingTrades; }
        public void setLosingTrades(Integer losingTrades) { this.losingTrades = losingTrades; }
        public Double getWinRate() { return winRate; }
        public void setWinRate(Double winRate) { this.winRate = winRate; }
        public Double getProfitFactor() { return profitFactor; }
        public void setProfitFactor(Double profitFactor) { this.profitFactor = profitFactor; }
        public Double getSharpeRatio() { return sharpeRatio; }
        public void setSharpeRatio(Double sharpeRatio) { this.sharpeRatio = sharpeRatio; }
        public Double getMaxDrawdown() { return maxDrawdown; }
        public void setMaxDrawdown(Double maxDrawdown) { this.maxDrawdown = maxDrawdown; }
        public Double getTotalReturnPct() { return totalReturnPct; }
        public void setTotalReturnPct(Double totalReturnPct) { this.totalReturnPct = totalReturnPct; }
        public Double getNetProfit() { return netProfit; }
        public void setNetProfit(Double netProfit) { this.netProfit = netProfit; }
        public Double getInitialCapital() { return initialCapital; }
        public void setInitialCapital(Double initialCapital) { this.initialCapital = initialCapital; }
        public Double getFinalCapital() { return finalCapital; }
        public void setFinalCapital(Double finalCapital) { this.finalCapital = finalCapital; }
    }

    public static class SignalTradeDto implements java.io.Serializable {
        private Integer tradeNum;
        private String signalType;
        private String direction;
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
        public String getDirection() { return direction; }
        public void setDirection(String direction) { this.direction = direction; }
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
    }
'''

if 'public static class SignalBacktestMetricsDto' not in code:
    code = code.replace('public static class QuantMetricsDto', signal_dtos + '\n    public static class QuantMetricsDto')
    print("Added Signal Backtest DTOs to DashboardBean.java")

# 2. Add Signal Properties
signal_props = '''    // --- Propiedades del Backtest de Señales Gráficas ---
    private SignalBacktestMetricsDto signalBtTrianglesMetrics = new SignalBacktestMetricsDto();
    private SignalBacktestMetricsDto signalBtCombinedMetrics = new SignalBacktestMetricsDto();
    private String signalBtStrategySelected = "COMBINED"; // "TRIANGLES" o "COMBINED"
    private List<SignalTradeDto> signalBtTradesList = new ArrayList<>();
    private List<SignalTradeDto> signalBtTrianglesTrades = new ArrayList<>();
    private List<SignalTradeDto> signalBtCombinedTrades = new ArrayList<>();
    private String signalBtComparisonCurveJson = "[]";

    public SignalBacktestMetricsDto getSignalBtTrianglesMetrics() { return signalBtTrianglesMetrics; }
    public void setSignalBtTrianglesMetrics(SignalBacktestMetricsDto m) { this.signalBtTrianglesMetrics = m; }
    public SignalBacktestMetricsDto getSignalBtCombinedMetrics() { return signalBtCombinedMetrics; }
    public void setSignalBtCombinedMetrics(SignalBacktestMetricsDto m) { this.signalBtCombinedMetrics = m; }
    public String getSignalBtStrategySelected() { return signalBtStrategySelected; }
    public void setSignalBtStrategySelected(String s) { this.signalBtStrategySelected = s; }
    public List<SignalTradeDto> getSignalBtTradesList() { return signalBtTradesList; }
    public void setSignalBtTradesList(List<SignalTradeDto> l) { this.signalBtTradesList = l; }
    public List<SignalTradeDto> getSignalBtTrianglesTrades() { return signalBtTrianglesTrades; }
    public void setSignalBtTrianglesTrades(List<SignalTradeDto> l) { this.signalBtTrianglesTrades = l; }
    public List<SignalTradeDto> getSignalBtCombinedTrades() { return signalBtCombinedTrades; }
    public void setSignalBtCombinedTrades(List<SignalTradeDto> l) { this.signalBtCombinedTrades = l; }
    public String getSignalBtComparisonCurveJson() { return signalBtComparisonCurveJson; }
    public void setSignalBtComparisonCurveJson(String s) { this.signalBtComparisonCurveJson = s; }

    public void onSignalStrategyChange() {
        if ("TRIANGLES".equals(this.signalBtStrategySelected)) {
            this.signalBtTradesList = this.signalBtTrianglesTrades;
        } else {
            this.signalBtTradesList = this.signalBtCombinedTrades;
        }
        log.info("Estrategia de señales seleccionada: {}, Total trades: {}", this.signalBtStrategySelected, this.signalBtTradesList.size());
    }
'''

if 'signalBtTrianglesMetrics' not in code:
    code = code.replace('private QuantMetricsDto quantMetrics = new QuantMetricsDto();', signal_props + '\n    private QuantMetricsDto quantMetrics = new QuantMetricsDto();')
    print("Added Signal Backtest properties and switch method to DashboardBean.java")

# 3. Update loadQuantPairAnalysis to parse signalBacktest
parse_signal_bt = '''                if (root.has("signalBacktest")) {
                    JsonNode sbNode = root.get("signalBacktest");
                    
                    // 1. Triángulos Solos
                    if (sbNode.has("trianglesOnly")) {
                        JsonNode toNode = sbNode.get("trianglesOnly");
                        SignalBacktestMetricsDto toDto = new SignalBacktestMetricsDto();
                        if (toNode.has("mode")) toDto.setMode(toNode.get("mode").asText());
                        if (toNode.has("modeLabel")) toDto.setModeLabel(toNode.get("modeLabel").asText());
                        if (toNode.has("totalTrades")) toDto.setTotalTrades(toNode.get("totalTrades").asInt());
                        if (toNode.has("winningTrades")) toDto.setWinningTrades(toNode.get("winningTrades").asInt());
                        if (toNode.has("losingTrades")) toDto.setLosingTrades(toNode.get("losingTrades").asInt());
                        if (toNode.has("winRate")) toDto.setWinRate(toNode.get("winRate").asDouble());
                        if (toNode.has("profitFactor")) toDto.setProfitFactor(toNode.get("profitFactor").asDouble());
                        if (toNode.has("sharpeRatio")) toDto.setSharpeRatio(toNode.get("sharpeRatio").asDouble());
                        if (toNode.has("maxDrawdown")) toDto.setMaxDrawdown(toNode.get("maxDrawdown").asDouble());
                        if (toNode.has("totalReturnPct")) toDto.setTotalReturnPct(toNode.get("totalReturnPct").asDouble());
                        if (toNode.has("netProfit")) toDto.setNetProfit(toNode.get("netProfit").asDouble());
                        if (toNode.has("finalCapital")) toDto.setFinalCapital(toNode.get("finalCapital").asDouble());
                        this.signalBtTrianglesMetrics = toDto;

                        List<SignalTradeDto> toTrades = new ArrayList<>();
                        if (toNode.has("trades") && toNode.get("trades").isArray()) {
                            for (JsonNode t : toNode.get("trades")) {
                                SignalTradeDto st = new SignalTradeDto();
                                if (t.has("tradeNum")) st.setTradeNum(t.get("tradeNum").asInt());
                                if (t.has("signalType")) st.setSignalType(t.get("signalType").asText());
                                if (t.has("direction")) st.setDirection(t.get("direction").asText());
                                if (t.has("entryDate")) st.setEntryDate(t.get("entryDate").asText());
                                if (t.has("exitDate")) st.setExitDate(t.get("exitDate").asText());
                                if (t.has("entryPriceA")) st.setEntryPriceA(t.get("entryPriceA").asDouble());
                                if (t.has("exitPriceA")) st.setExitPriceA(t.get("exitPriceA").asDouble());
                                if (t.has("entryPriceB")) st.setEntryPriceB(t.get("entryPriceB").asDouble());
                                if (t.has("exitPriceB")) st.setExitPriceB(t.get("exitPriceB").asDouble());
                                if (t.has("durationBars")) st.setDurationBars(t.get("durationBars").asInt());
                                if (t.has("returnPct")) st.setReturnPct(t.get("returnPct").asDouble());
                                if (t.has("pnl")) st.setPnl(t.get("pnl").asDouble());
                                if (t.has("exitReason")) st.setExitReason(t.get("exitReason").asText());
                                if (t.has("isWin")) st.setIsWin(t.get("isWin").asBoolean());
                                toTrades.add(st);
                            }
                        }
                        this.signalBtTrianglesTrades = toTrades;
                    }

                    // 2. Combinado (Triángulos + Cuadros)
                    if (sbNode.has("combined")) {
                        JsonNode cbNode = sbNode.get("combined");
                        SignalBacktestMetricsDto cbDto = new SignalBacktestMetricsDto();
                        if (cbNode.has("mode")) cbDto.setMode(cbNode.get("mode").asText());
                        if (cbNode.has("modeLabel")) cbDto.setModeLabel(cbNode.get("modeLabel").asText());
                        if (cbNode.has("totalTrades")) cbDto.setTotalTrades(cbNode.get("totalTrades").asInt());
                        if (cbNode.has("winningTrades")) cbDto.setWinningTrades(cbNode.get("winningTrades").asInt());
                        if (cbNode.has("losingTrades")) cbDto.setLosingTrades(cbNode.get("losingTrades").asInt());
                        if (cbNode.has("winRate")) cbDto.setWinRate(cbNode.get("winRate").asDouble());
                        if (cbNode.has("profitFactor")) cbDto.setProfitFactor(cbNode.get("profitFactor").asDouble());
                        if (cbNode.has("sharpeRatio")) cbDto.setSharpeRatio(cbNode.get("sharpeRatio").asDouble());
                        if (cbNode.has("maxDrawdown")) cbDto.setMaxDrawdown(cbNode.get("maxDrawdown").asDouble());
                        if (cbNode.has("totalReturnPct")) cbDto.setTotalReturnPct(cbNode.get("totalReturnPct").asDouble());
                        if (cbNode.has("netProfit")) cbDto.setNetProfit(cbNode.get("netProfit").asDouble());
                        if (cbNode.has("finalCapital")) cbDto.setFinalCapital(cbNode.get("finalCapital").asDouble());
                        this.signalBtCombinedMetrics = cbDto;

                        List<SignalTradeDto> cbTrades = new ArrayList<>();
                        if (cbNode.has("trades") && cbNode.get("trades").isArray()) {
                            for (JsonNode t : cbNode.get("trades")) {
                                SignalTradeDto st = new SignalTradeDto();
                                if (t.has("tradeNum")) st.setTradeNum(t.get("tradeNum").asInt());
                                if (t.has("signalType")) st.setSignalType(t.get("signalType").asText());
                                if (t.has("direction")) st.setDirection(t.get("direction").asText());
                                if (t.has("entryDate")) st.setEntryDate(t.get("entryDate").asText());
                                if (t.has("exitDate")) st.setExitDate(t.get("exitDate").asText());
                                if (t.has("entryPriceA")) st.setEntryPriceA(t.get("entryPriceA").asDouble());
                                if (t.has("exitPriceA")) st.setExitPriceA(t.get("exitPriceA").asDouble());
                                if (t.has("entryPriceB")) st.setEntryPriceB(t.get("entryPriceB").asDouble());
                                if (t.has("exitPriceB")) st.setExitPriceB(t.get("exitPriceB").asDouble());
                                if (t.has("durationBars")) st.setDurationBars(t.get("durationBars").asInt());
                                if (t.has("returnPct")) st.setReturnPct(t.get("returnPct").asDouble());
                                if (t.has("pnl")) st.setPnl(t.get("pnl").asDouble());
                                if (t.has("exitReason")) st.setExitReason(t.get("exitReason").asText());
                                if (t.has("isWin")) st.setIsWin(t.get("isWin").asBoolean());
                                cbTrades.add(st);
                            }
                        }
                        this.signalBtCombinedTrades = cbTrades;
                    }

                    // Establecer lista activa según selección
                    if ("TRIANGLES".equals(this.signalBtStrategySelected)) {
                        this.signalBtTradesList = this.signalBtTrianglesTrades;
                    } else {
                        this.signalBtTradesList = this.signalBtCombinedTrades;
                    }

                    // Curva comparativa de equidad
                    this.signalBtComparisonCurveJson = sbNode.toString();
                }'''

if 'root.has("signalBacktest")' not in code:
    code = code.replace('this.quantMetrics = metrics;', parse_signal_bt + '\n                this.quantMetrics = metrics;')
    print("Added signalBacktest parser to loadQuantPairAnalysis in DashboardBean.java")

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("DashboardBean.java fully updated with Signal Backtest integration!")
