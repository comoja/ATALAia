import re

bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

quant_dtos = '''    // --- DTOs del Motor Cuantitativo (Pair Trading & Reversión a la Media) ---
    public static class QuantMetricsDto implements java.io.Serializable {
        private Double halfLife;
        private Double reversionSpeed;
        private String halfLifeDescription = "Calculando...";
        private Boolean isMeanReverting = false;
        private Double pValue;
        private Double rSquared;

        private Double dominantPeriod;
        private Integer periodsToMeanCross;

        private Integer totalFvgs = 0;
        private Integer activeFvgs = 0;

        private Integer totalTrades = 0;
        private Double winRate = 0.0;
        private Double profitFactor = 0.0;
        private Double sharpeRatio = 0.0;
        private Double maxDrawdown = 0.0;
        private Double totalReturnPct = 0.0;

        private Boolean hasLiveSignal = false;
        private String liveSignalType = "NONE";
        private String liveActionA = "HOLD";
        private String liveActionB = "HOLD";
        private Double liveZScore = 0.0;

        // Getters y Setters
        public Double getHalfLife() { return halfLife; }
        public void setHalfLife(Double halfLife) { this.halfLife = halfLife; }
        public Double getReversionSpeed() { return reversionSpeed; }
        public void setReversionSpeed(Double reversionSpeed) { this.reversionSpeed = reversionSpeed; }
        public String getHalfLifeDescription() { return halfLifeDescription; }
        public void setHalfLifeDescription(String halfLifeDescription) { this.halfLifeDescription = halfLifeDescription; }
        public Boolean getIsMeanReverting() { return isMeanReverting; }
        public void setIsMeanReverting(Boolean isMeanReverting) { this.isMeanReverting = isMeanReverting; }
        public Double getPValue() { return pValue; }
        public void setPValue(Double pValue) { this.pValue = pValue; }
        public Double getRSquared() { return rSquared; }
        public void setRSquared(Double rSquared) { this.rSquared = rSquared; }
        public Double getDominantPeriod() { return dominantPeriod; }
        public void setDominantPeriod(Double dominantPeriod) { this.dominantPeriod = dominantPeriod; }
        public Integer getPeriodsToMeanCross() { return periodsToMeanCross; }
        public void setPeriodsToMeanCross(Integer periodsToMeanCross) { this.periodsToMeanCross = periodsToMeanCross; }
        public Integer getTotalFvgs() { return totalFvgs; }
        public void setTotalFvgs(Integer totalFvgs) { this.totalFvgs = totalFvgs; }
        public Integer getActiveFvgs() { return activeFvgs; }
        public void setActiveFvgs(Integer activeFvgs) { this.activeFvgs = activeFvgs; }
        public Integer getTotalTrades() { return totalTrades; }
        public void setTotalTrades(Integer totalTrades) { this.totalTrades = totalTrades; }
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
        public Boolean getHasLiveSignal() { return hasLiveSignal; }
        public void setHasLiveSignal(Boolean hasLiveSignal) { this.hasLiveSignal = hasLiveSignal; }
        public String getLiveSignalType() { return liveSignalType; }
        public void setLiveSignalType(String liveSignalType) { this.liveSignalType = liveSignalType; }
        public String getLiveActionA() { return liveActionA; }
        public void setLiveActionA(String liveActionA) { this.liveActionA = liveActionA; }
        public String getLiveActionB() { return liveActionB; }
        public void setLiveActionB(String liveActionB) { this.liveActionB = liveActionB; }
        public Double getLiveZScore() { return liveZScore; }
        public void setLiveZScore(Double liveZScore) { this.liveZScore = liveZScore; }
    }

    public static class QuantFvgDto implements java.io.Serializable {
        private String datetime;
        private String type;
        private Double top;
        private Double bottom;
        private Double gapSize;
        private Boolean mitigated;
        private String mitigationDate;

        public String getDatetime() { return datetime; }
        public void setDatetime(String datetime) { this.datetime = datetime; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public Double getTop() { return top; }
        public void setTop(Double top) { this.top = top; }
        public Double getBottom() { return bottom; }
        public void setBottom(Double bottom) { this.bottom = bottom; }
        public Double getGapSize() { return gapSize; }
        public void setGapSize(Double gapSize) { this.gapSize = gapSize; }
        public Boolean getMitigated() { return mitigated; }
        public void setMitigated(Boolean mitigated) { this.mitigated = mitigated; }
        public String getMitigationDate() { return mitigationDate; }
        public void setMitigationDate(String mitigationDate) { this.mitigationDate = mitigationDate; }
    }

    public static class QuantTradeDto implements java.io.Serializable {
        private Integer tradeNum;
        private String type;
        private String entryDate;
        private String exitDate;
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
    }

    private QuantMetricsDto quantMetrics = new QuantMetricsDto();
    private List<QuantFvgDto> quantFvgsList = new ArrayList<>();
    private List<QuantTradeDto> quantTradesList = new ArrayList<>();
    private String quantTimeSeriesJson = "[]";
    private String equityCurveJson = "[]";

    public QuantMetricsDto getQuantMetrics() { return quantMetrics; }
    public void setQuantMetrics(QuantMetricsDto quantMetrics) { this.quantMetrics = quantMetrics; }
    public List<QuantFvgDto> getQuantFvgsList() { return quantFvgsList; }
    public void setQuantFvgsList(List<QuantFvgDto> quantFvgsList) { this.quantFvgsList = quantFvgsList; }
    public List<QuantTradeDto> getQuantTradesList() { return quantTradesList; }
    public void setQuantTradesList(List<QuantTradeDto> quantTradesList) { this.quantTradesList = quantTradesList; }
    public String getQuantTimeSeriesJson() { return quantTimeSeriesJson; }
    public void setQuantTimeSeriesJson(String quantTimeSeriesJson) { this.quantTimeSeriesJson = quantTimeSeriesJson; }
    public String getEquityCurveJson() { return equityCurveJson; }
    public void setEquityCurveJson(String equityCurveJson) { this.equityCurveJson = equityCurveJson; }
'''

if 'public static class QuantMetricsDto' not in code:
    code = code.replace('public static class UserAccountDto', quant_dtos + '\n    public static class UserAccountDto')
    print("Added Quant DTOs to DashboardBean.java")

quant_load_method = '''
    public void loadQuantPairAnalysis() {
        if (selectedPair == null || selectedPair.isEmpty() || selectedPair2 == null || selectedPair2.trim().isEmpty()) {
            return;
        }
        try {
            log.info("Ejecutando análisis cuantitativo y backtest para {} / {}...", selectedPair, selectedPair2);
            RestTemplate restTemplate = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();

            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            String startStr = (startDate != null) ? sdf.format(startDate) : "";
            String endStr = (endDate != null) ? sdf.format(endDate) : "";

            String url = String.format(
                    "%s/api/v1/quant/pair-analysis/%s?pairB=%s&timeframe=%s&days=%d&start_date=%s&end_date=%s",
                    backendUrl,
                    java.net.URLEncoder.encode(selectedPair, "UTF-8"),
                    java.net.URLEncoder.encode(selectedPair2, "UTF-8"),
                    timeframe, (daysBack != null ? daysBack : 180), startStr, endStr);

            String responseStr = restTemplate.getForObject(url, String.class);
            if (responseStr != null && !responseStr.isEmpty()) {
                JsonNode root = mapper.readTree(responseStr);
                QuantMetricsDto metrics = new QuantMetricsDto();

                if (root.has("halfLife")) {
                    JsonNode hl = root.get("halfLife");
                    if (hl.has("halfLife") && !hl.get("halfLife").isNull()) metrics.setHalfLife(hl.get("halfLife").asDouble());
                    if (hl.has("reversionSpeed") && !hl.get("reversionSpeed").isNull()) metrics.setReversionSpeed(hl.get("reversionSpeed").asDouble());
                    if (hl.has("halfLifeDescription")) metrics.setHalfLifeDescription(hl.get("halfLifeDescription").asText());
                    if (hl.has("isMeanReverting")) metrics.setIsMeanReverting(hl.get("isMeanReverting").asBoolean());
                    if (hl.has("pValue") && !hl.get("pValue").isNull()) metrics.setPValue(hl.get("pValue").asDouble());
                    if (hl.has("rSquared") && !hl.get("rSquared").isNull()) metrics.setRSquared(hl.get("rSquared").asDouble());
                }

                if (root.has("spectralAnalysis")) {
                    JsonNode sp = root.get("spectralAnalysis");
                    if (sp.has("dominantPeriod") && !sp.get("dominantPeriod").isNull()) metrics.setDominantPeriod(sp.get("dominantPeriod").asDouble());
                    if (sp.has("periodsToMeanCross") && !sp.get("periodsToMeanCross").isNull()) metrics.setPeriodsToMeanCross(sp.get("periodsToMeanCross").asInt());
                }

                if (root.has("fvgsSummary")) {
                    JsonNode fvgSum = root.get("fvgsSummary");
                    if (fvgSum.has("totalFvgs")) metrics.setTotalFvgs(fvgSum.get("totalFvgs").asInt());
                    if (fvgSum.has("activeFvgs")) metrics.setActiveFvgs(fvgSum.get("activeFvgs").asInt());

                    List<QuantFvgDto> fList = new ArrayList<>();
                    if (fvgSum.has("list") && fvgSum.get("list").isArray()) {
                        for (JsonNode item : fvgSum.get("list")) {
                            QuantFvgDto fDto = new QuantFvgDto();
                            if (item.has("datetime")) fDto.setDatetime(item.get("datetime").asText());
                            if (item.has("type")) fDto.setType(item.get("type").asText());
                            if (item.has("top")) fDto.setTop(item.get("top").asDouble());
                            if (item.has("bottom")) fDto.setBottom(item.get("bottom").asDouble());
                            if (item.has("gapSize")) fDto.setGapSize(item.get("gapSize").asDouble());
                            if (item.has("mitigated")) fDto.setMitigated(item.get("mitigated").asBoolean());
                            if (item.has("mitigationDate") && !item.get("mitigationDate").isNull()) fDto.setMitigationDate(item.get("mitigationDate").asText());
                            fList.add(fDto);
                        }
                    }
                    this.quantFvgsList = fList;
                }

                if (root.has("backtestMetrics")) {
                    JsonNode bm = root.get("backtestMetrics");
                    if (bm.has("totalTrades")) metrics.setTotalTrades(bm.get("totalTrades").asInt());
                    if (bm.has("winRate")) metrics.setWinRate(bm.get("winRate").asDouble());
                    if (bm.has("profitFactor")) metrics.setProfitFactor(bm.get("profitFactor").asDouble());
                    if (bm.has("sharpeRatio")) metrics.setSharpeRatio(bm.get("sharpeRatio").asDouble());
                    if (bm.has("maxDrawdown")) metrics.setMaxDrawdown(bm.get("maxDrawdown").asDouble());
                    if (bm.has("totalReturnPct")) metrics.setTotalReturnPct(bm.get("totalReturnPct").asDouble());

                    List<QuantTradeDto> tList = new ArrayList<>();
                    if (bm.has("trades") && bm.get("trades").isArray()) {
                        for (JsonNode item : bm.get("trades")) {
                            QuantTradeDto tDto = new QuantTradeDto();
                            if (item.has("tradeNum")) tDto.setTradeNum(item.get("tradeNum").asInt());
                            if (item.has("type")) tDto.setType(item.get("type").asText());
                            if (item.has("entryDate")) tDto.setEntryDate(item.get("entryDate").asText());
                            if (item.has("exitDate")) tDto.setExitDate(item.get("exitDate").asText());
                            if (item.has("entryPrice")) tDto.setEntryPrice(item.get("entryPrice").asDouble());
                            if (item.has("exitPrice")) tDto.setExitPrice(item.get("exitPrice").asDouble());
                            if (item.has("durationBars")) tDto.setDurationBars(item.get("durationBars").asInt());
                            if (item.has("returnPct")) tDto.setReturnPct(item.get("returnPct").asDouble());
                            if (item.has("pnl")) tDto.setPnl(item.get("pnl").asDouble());
                            if (item.has("exitReason")) tDto.setExitReason(item.get("exitReason").asText());
                            if (item.has("isWin")) tDto.setIsWin(item.get("isWin").asBoolean());
                            tList.add(tDto);
                        }
                    }
                    this.quantTradesList = tList;
                }

                if (root.has("liveSignal")) {
                    JsonNode ls = root.get("liveSignal");
                    if (ls.has("hasSignal")) metrics.setHasLiveSignal(ls.get("hasSignal").asBoolean());
                    if (ls.has("signalType")) metrics.setLiveSignalType(ls.get("signalType").asText());
                    if (ls.has("actionPairA")) metrics.setLiveActionA(ls.get("actionPairA").asText());
                    if (ls.has("actionPairB")) metrics.setLiveActionB(ls.get("actionPairB").asText());
                    if (ls.has("zScore")) metrics.setLiveZScore(ls.get("zScore").asDouble());
                }

                if (root.has("timeSeries")) {
                    this.quantTimeSeriesJson = root.get("timeSeries").toString();
                }

                if (root.has("equityCurve")) {
                    this.equityCurveJson = root.get("equityCurve").toString();
                }

                this.quantMetrics = metrics;
                log.info("Análisis cuantitativo cargado exitosamente: HalfLife={}, WinRate={}%", metrics.getHalfLife(), metrics.getWinRate());
            }
        } catch (Exception e) {
            log.error("Error al cargar análisis cuantitativo: {}", e.getMessage());
        }
    }
'''

if 'public void loadQuantPairAnalysis()' not in code:
    code = code.replace('public void analyzePair() {', quant_load_method + '\n    public void analyzePair() {')
    print("Added loadQuantPairAnalysis method to DashboardBean.java")

# Call loadQuantPairAnalysis() at the end of successful analyzePair()
code = code.replace('analysisResult = "Ratio sintético calculado: " + this.ratioLabel;', 'analysisResult = "Ratio sintético calculado: " + this.ratioLabel;\n                loadQuantPairAnalysis();')

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("DashboardBean.java fully updated with Quant engine integration!")
