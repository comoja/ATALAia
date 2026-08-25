package com.atalaia.correlation.beans;

import javax.annotation.PostConstruct;
import javax.enterprise.context.SessionScoped;
import javax.inject.Named;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;
import org.springframework.beans.factory.annotation.Value;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import org.primefaces.model.charts.ChartData;
import org.primefaces.model.charts.axes.cartesian.CartesianScales;
import org.primefaces.model.charts.axes.cartesian.linear.CartesianLinearAxes;
import org.primefaces.model.charts.bar.BarChartDataSet;
import org.primefaces.model.charts.bar.BarChartModel;
import org.primefaces.model.charts.bar.BarChartOptions;
import org.primefaces.model.charts.line.LineChartDataSet;
import org.primefaces.model.charts.line.LineChartModel;
import org.primefaces.model.charts.line.LineChartOptions;
import org.primefaces.model.charts.data.NumericPoint;

@Named("dashboardBean")
@SessionScoped
@Getter
@Setter
@Slf4j
public class DashboardBean implements Serializable {

    private static final long serialVersionUID = 1L;

    @javax.inject.Inject
    private SecurityBean securityBean;

    // --- Catalogo de Simbolos desde BD ---
    private List<RatioSymbolDto> availablePairs = new ArrayList<>();
            // --- DTOs del Motor Cuantitativo (Pair Trading & Reversión a la Media) ---
        // --- DTOs para el Backtest de Señales Gráficas (Triángulos y Cuadros) ---
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
        private Double allocationPct = 3.0;
        private Double reqMarginPerMinLot = 20.0;

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
        public Double getAllocationPct() { return allocationPct; }
        public void setAllocationPct(Double allocationPct) { this.allocationPct = allocationPct; }
        public Double getReqMarginPerMinLot() { return reqMarginPerMinLot; }
        public void setReqMarginPerMinLot(Double reqMarginPerMinLot) { this.reqMarginPerMinLot = reqMarginPerMinLot; }
        public String getFormattedInitialCapital() {
            return initialCapital != null ? String.format(java.util.Locale.US, "%,.2f", initialCapital) : "0.00";
        }
        public String getFormattedFinalCapital() {
            return finalCapital != null ? String.format(java.util.Locale.US, "%,.2f", finalCapital) : "0.00";
        }
        public String getFormattedNetProfit() {
            return netProfit != null ? String.format(java.util.Locale.US, "%,.2f", netProfit) : "0.00";
        }
        public String getFormattedReqMarginPerMinLot() {
            return reqMarginPerMinLot != null ? String.format(java.util.Locale.US, "%,.2f", reqMarginPerMinLot) : "0.00";
        }
    }

    public static class ActiveTradeItemDto implements java.io.Serializable {
        private int idTrade;
        private String symbol;
        private String direction;
        private double size;
        private double entryPrice;
        private double currentPrice;
        private double margin;
        private double pnl;
        private String openTime;

        public int getIdTrade() { return idTrade; }
        public void setIdTrade(int idTrade) { this.idTrade = idTrade; }
        public String getSymbol() { return symbol; }
        public void setSymbol(String symbol) { this.symbol = symbol; }
        public String getDirection() { return direction; }
        public void setDirection(String direction) { this.direction = direction; }
        public double getSize() { return size; }
        public void setSize(double size) { this.size = size; }
        public double getEntryPrice() { return entryPrice; }
        public void setEntryPrice(double entryPrice) { this.entryPrice = entryPrice; }
        public double getCurrentPrice() { return currentPrice; }
        public void setCurrentPrice(double currentPrice) { this.currentPrice = currentPrice; }
        public double getMargin() { return margin; }
        public void setMargin(double margin) { this.margin = margin; }
        public double getPnl() { return pnl; }
        public void setPnl(double pnl) { this.pnl = pnl; }
        public String getOpenTime() { return openTime; }
        public void setOpenTime(String openTime) { this.openTime = openTime; }

        public String getFormattedEntryPrice() { return String.format(java.util.Locale.US, "%,.5f", entryPrice); }
        public String getFormattedCurrentPrice() { return String.format(java.util.Locale.US, "%,.5f", currentPrice); }
        public String getFormattedMargin() { return String.format(java.util.Locale.US, "%,.2f", margin); }
        public String getFormattedPnl() { return String.format(java.util.Locale.US, "%,.2f", pnl); }
        public String getFormattedUnits() { return String.format(java.util.Locale.US, "%,.0f", size); }
    }

    public static class ActiveCycleSummaryDto implements java.io.Serializable {
        private String setup = "";
        private String pairA = "";
        private String pairB = "";
        private boolean hasActiveCycle = false;
        private int totalOpenTrades = 0;
        private double totalMargin = 0.0;
        private double marginA = 0.0;
        private double marginB = 0.0;
        private double totalPnl = 0.0;
        private double returnPct = 0.0;
        private double pnlA = 0.0;
        private double pnlB = 0.0;
        private String direction = "";
        private String firstEntryDate = "";
        private String lastEntryDate = "";
        private String timeframe = "1d";
        private String lastSignalType = "";
        private double availableCapital = 0.0;
        private double accumCapital = 0.0;
        private boolean isWin = true;
        private double sizeA = 0.0;
        private double sizeB = 0.0;
        private double avgEntryPriceA = 0.0;
        private double avgEntryPriceB = 0.0;
        private double currentPriceA = 0.0;
        private double currentPriceB = 0.0;
        private String dirA = "BUY";
        private String dirB = "SELL";
        private List<ActiveTradeItemDto> trades = new ArrayList<>();

        public String getSetup() { return setup; }
        public void setSetup(String setup) { this.setup = setup; }
        public String getPairA() { return pairA; }
        public void setPairA(String pairA) { this.pairA = pairA; }
        public String getPairB() { return pairB; }
        public void setPairB(String pairB) { this.pairB = pairB; }
        public boolean isHasActiveCycle() { return hasActiveCycle; }
        public void setHasActiveCycle(boolean hasActiveCycle) { this.hasActiveCycle = hasActiveCycle; }
        public int getTotalOpenTrades() { return totalOpenTrades; }
        public void setTotalOpenTrades(int totalOpenTrades) { this.totalOpenTrades = totalOpenTrades; }
        public double getTotalMargin() { return totalMargin; }
        public void setTotalMargin(double totalMargin) { this.totalMargin = totalMargin; }
        public double getMarginA() { return marginA; }
        public void setMarginA(double marginA) { this.marginA = marginA; }
        public double getMarginB() { return marginB; }
        public void setMarginB(double marginB) { this.marginB = marginB; }
        public double getTotalPnl() { return totalPnl; }
        public void setTotalPnl(double totalPnl) { this.totalPnl = totalPnl; }
        public double getReturnPct() { return returnPct; }
        public void setReturnPct(double returnPct) { this.returnPct = returnPct; }
        public double getPnlA() { return pnlA; }
        public void setPnlA(double pnlA) { this.pnlA = pnlA; }
        public double getPnlB() { return pnlB; }
        public void setPnlB(double pnlB) { this.pnlB = pnlB; }
        public String getDirection() { return direction; }
        public void setDirection(String direction) { this.direction = direction; }
        public String getFirstEntryDate() { return firstEntryDate; }
        public void setFirstEntryDate(String firstEntryDate) { this.firstEntryDate = firstEntryDate; }
        public String getLastEntryDate() { return lastEntryDate; }
        public void setLastEntryDate(String lastEntryDate) { this.lastEntryDate = lastEntryDate; }
        public String getTimeframe() { return timeframe; }
        public void setTimeframe(String timeframe) { this.timeframe = timeframe; }
        public String getLastSignalType() { return lastSignalType; }
        public void setLastSignalType(String lastSignalType) { this.lastSignalType = lastSignalType; }
        public double getAvailableCapital() { return availableCapital; }
        public void setAvailableCapital(double availableCapital) { this.availableCapital = availableCapital; }
        public double getAccumCapital() { return accumCapital; }
        public void setAccumCapital(double accumCapital) { this.accumCapital = accumCapital; }
        public boolean isIsWin() { return isWin; }
        public void setIsWin(boolean isWin) { this.isWin = isWin; }
        public double getSizeA() { return sizeA; }
        public void setSizeA(double sizeA) { this.sizeA = sizeA; }
        public double getSizeB() { return sizeB; }
        public void setSizeB(double sizeB) { this.sizeB = sizeB; }
        public double getAvgEntryPriceA() { return avgEntryPriceA; }
        public void setAvgEntryPriceA(double avgEntryPriceA) { this.avgEntryPriceA = avgEntryPriceA; }
        public double getAvgEntryPriceB() { return avgEntryPriceB; }
        public void setAvgEntryPriceB(double avgEntryPriceB) { this.avgEntryPriceB = avgEntryPriceB; }
        public double getCurrentPriceA() { return currentPriceA; }
        public void setCurrentPriceA(double currentPriceA) { this.currentPriceA = currentPriceA; }
        public double getCurrentPriceB() { return currentPriceB; }
        public void setCurrentPriceB(double currentPriceB) { this.currentPriceB = currentPriceB; }

        public String getFormattedSizeA() { return String.format(java.util.Locale.US, "%,.0f", sizeA); }
        public String getFormattedSizeB() { return String.format(java.util.Locale.US, "%,.0f", sizeB); }
        public String getFormattedAvgEntryPriceA() { return String.format(java.util.Locale.US, "%,.5f", avgEntryPriceA); }
        public String getFormattedAvgEntryPriceB() { return String.format(java.util.Locale.US, "%,.5f", avgEntryPriceB); }
        public String getFormattedCurrentPriceA() { return String.format(java.util.Locale.US, "%,.5f", currentPriceA); }
        public String getFormattedCurrentPriceB() { return String.format(java.util.Locale.US, "%,.5f", currentPriceB); }
        public String getDirA() { return dirA; }
        public void setDirA(String dirA) { this.dirA = dirA; }
        public String getDirB() { return dirB; }
        public void setDirB(String dirB) { this.dirB = dirB; }
        public boolean isBuyA() { return dirA != null && (dirA.equalsIgnoreCase("BUY") || dirA.toUpperCase().contains("LARG")); }
        public boolean isBuyB() { return dirB != null && (dirB.equalsIgnoreCase("BUY") || dirB.toUpperCase().contains("LARG")); }
        public boolean isWinA() { return pnlA >= 0; }
        public boolean isWinB() { return pnlB >= 0; }
        public String getColorA() { return isBuyA() ? "#15803d" : "#b91c1c"; }
        public String getColorB() { return isBuyB() ? "#15803d" : "#b91c1c"; }
        public String getPnlColorA() { return pnlA >= 0 ? "#15803d" : "#b91c1c"; }
        public String getPnlColorB() { return pnlB >= 0 ? "#15803d" : "#b91c1c"; }
        public String getPnlColor() { return totalPnl >= 0 ? "#15803d" : "#b91c1c"; }
        public String getBgColor() { return totalPnl >= 0 ? "#f0fdf4" : "#fef2f2"; }
        public String getBorderColor() { return totalPnl >= 0 ? "#bbf7d0" : "#fecaca"; }
        public String getPnlSignA() { return pnlA >= 0 ? "+" : ""; }
        public String getPnlSignB() { return pnlB >= 0 ? "+" : ""; }

        public List<ActiveTradeItemDto> getTrades() { return trades; }
        public void setTrades(List<ActiveTradeItemDto> trades) { this.trades = trades; }

        public String getPnlSign() { return totalPnl >= 0 ? "+" : ""; }
        public String getRetSign() { return returnPct >= 0 ? "+" : ""; }
        public String getFormattedTotalPnl() { return String.format(java.util.Locale.US, "%,.2f", totalPnl); }
        public String getFormattedReturnPct() { return String.format(java.util.Locale.US, "%,.1f", returnPct); }
        public String getFormattedTotalMargin() { return String.format(java.util.Locale.US, "%,.2f", totalMargin); }
        public String getFormattedMarginA() { return String.format(java.util.Locale.US, "%,.2f", marginA); }
        public String getFormattedMarginB() { return String.format(java.util.Locale.US, "%,.2f", marginB); }
        public String getFormattedPnlA() { return String.format(java.util.Locale.US, "%,.2f", pnlA); }
        public String getFormattedPnlB() { return String.format(java.util.Locale.US, "%,.2f", pnlB); }
        public String getShortDirection() {
            if (direction == null || direction.isEmpty()) return "N/A";
            return direction.replace("LONG ", "L:").replace("SHORT ", "S:").replace(" / ", " | ");
        }
    }

    public static class SignalTradeDto implements java.io.Serializable {
        private Integer tradeNum;
        private String signalType;
        private Boolean isSubtotal = false;
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
        private Integer multA;
        private Integer multB;
        private Integer unitsA;
        private Integer unitsB;
        private Double pipsA;
        private Double pipsB;
        private Double pnlA;
        private Double pnlB;
        private Double marginA;
        private Double marginB;
        private Double availableCapital;
        private Double accumCapital;
        private Boolean isOpen = false;

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
        public Boolean getIsSubtotal() { return isSubtotal != null && isSubtotal; }
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
        public Double getPnlA() { return pnlA; }
        public void setPnlA(Double pnlA) { this.pnlA = pnlA; }
        public Double getPnlB() { return pnlB; }
        public void setPnlB(Double pnlB) { this.pnlB = pnlB; }
        public Boolean getIsOpen() { return isOpen != null && isOpen; }
        public void setIsOpen(Boolean isOpen) { this.isOpen = isOpen; }
        public Double getMarginA() { return marginA; }
        public void setMarginA(Double marginA) { this.marginA = marginA; }
        public Double getMarginB() { return marginB; }
        public void setMarginB(Double marginB) { this.marginB = marginB; }
        public String getFormattedMarginA() {
            return marginA != null ? String.format(java.util.Locale.US, "%,.2f", marginA) : "0.00";
        }
        public String getFormattedMarginB() {
            return marginB != null ? String.format(java.util.Locale.US, "%,.2f", marginB) : "0.00";
        }
        public Double getAvailableCapital() { return availableCapital; }
        public void setAvailableCapital(Double availableCapital) { this.availableCapital = availableCapital; }
        public String getFormattedAvailableCapital() {
            return availableCapital != null ? String.format(java.util.Locale.US, "%,.2f", availableCapital) : "0.00";
        }
        public Double getAccumCapital() { return accumCapital; }
        public void setAccumCapital(Double accumCapital) { this.accumCapital = accumCapital; }
        public String getFormattedAccumCapital() {
            return accumCapital != null ? String.format(java.util.Locale.US, "%,.2f", accumCapital) : "0.00";
        }
        public String getFormattedAllocatedCapital() {
            return allocatedCapital != null ? String.format(java.util.Locale.US, "%,.2f", allocatedCapital) : "0.00";
        }
        public String getFormattedUnitsA() {
            return unitsA != null ? String.format(java.util.Locale.US, "%,d", unitsA) : "0";
        }
        public String getFormattedUnitsB() {
            return unitsB != null ? String.format(java.util.Locale.US, "%,d", unitsB) : "0";
        }
        public String getFormattedPnl() {
            return pnl != null ? String.format(java.util.Locale.US, "%,.2f", pnl) : "0.00";
        }
        public String getFormattedPnlA() {
            return pnlA != null ? String.format(java.util.Locale.US, "%,.2f", pnlA) : "0.00";
        }
        public String getFormattedPnlB() {
            return pnlB != null ? String.format(java.util.Locale.US, "%,.2f", pnlB) : "0.00";
        }
        public boolean isSquare() {
            return signalType != null && signalType.contains("CUADRO");
        }
        public boolean isTriangle() {
            return signalType != null && signalType.contains("TRIANGULO");
        }
        public boolean isTriangleUp() {
            return signalType != null && (signalType.contains("TRIANGULO_VERDE") || signalType.contains("UP"));
        }
        public String getSignalColor() {
            if (signalType == null) return "#1e293b";
            if (signalType.contains("VERDE")) return "#15803d";
            if (signalType.contains("ROJO")) return "#dc2626";
            return "#1e293b";
        }
        public String getSignalOriginPair() {
            if (signalType == null) return "";
            String s = signalType.replace("CUADRO_VERDE", "")
                                 .replace("CUADRO_ROJO", "")
                                 .replace("TRIANGULO_VERDE", "")
                                 .replace("TRIANGULO_ROJO", "")
                                 .replace("(EN CURSO)", "")
                                 .trim();
            if (s.startsWith("(") && s.endsWith(")")) {
                s = s.substring(1, s.length() - 1).trim();
            }
            return s.isEmpty() ? signalType : s;
        }
        public boolean isLongA() {
            if (direction == null) return true;
            return direction.startsWith("LONG");
        }
        public boolean isLongB() {
            if (direction == null) return false;
            return direction.contains("/ LONG");
        }
        public String getPairAColor() {
            return isLongA() ? "#15803d" : "#dc2626";
        }
        public String getPairBColor() {
            return isLongB() ? "#15803d" : "#dc2626";
        }
    }

    

    // --- Campos y DTOs para el Backtest de Señales Gráficas de Cruces EMA ---
    private SignalBacktestMetricsDto signalBtTrianglesMetrics = new SignalBacktestMetricsDto();
    private SignalBacktestMetricsDto signalBtCombinedMetrics = new SignalBacktestMetricsDto();
    private List<SignalTradeDto> signalBtTrianglesTrades = new ArrayList<>();
    private List<SignalTradeDto> signalBtCombinedTrades = new ArrayList<>();
    private List<SignalTradeDto> signalBtTradesList = new ArrayList<>();
    private String signalBtStrategySelected = "COMBINED";
    private String signalBtComparisonCurveJson = "{}";
    private List<ActiveCycleSummaryDto> allActiveCycles = new ArrayList<>();

    public Double getTotalActiveMargin() {
        double sum = 0.0;
        if (allActiveCycles != null) {
            for (ActiveCycleSummaryDto c : allActiveCycles) {
                sum += c.getTotalMargin();
            }
        }
        return sum;
    }

    public Double getTotalActivePnl() {
        double sum = 0.0;
        if (allActiveCycles != null) {
            for (ActiveCycleSummaryDto c : allActiveCycles) {
                sum += c.getTotalPnl();
            }
        }
        return sum;
    }

    public Double getTotalActiveCapital() {
        double cuentaCap = (getSelectedAccountCapital() != null) ? getSelectedAccountCapital() : 0.0;
        return cuentaCap;
    }

    public Double getTotalActiveEquity() {
        return getTotalActiveCapital() + getTotalActivePnl();
    }

    public Double getMarginIndicator() {
        double margin = getTotalActiveMargin();
        if (margin > 0) {
            return (getTotalActiveEquity() / margin) * 100.0;
        }
        return 0.0;
    }

    public String getFormattedTotalActiveCapital() {
        return String.format(java.util.Locale.US, "%,.2f", getTotalActiveCapital());
    }

    public String getFormattedTotalActiveMargin() {
        return String.format(java.util.Locale.US, "%,.2f", getTotalActiveMargin());
    }

    public String getFormattedTotalActivePnl() {
        return String.format(java.util.Locale.US, "%,.2f", Math.abs(getTotalActivePnl()));
    }

    public String getTotalActivePnlSign() {
        return (getTotalActivePnl() >= 0) ? "+" : "-";
    }

    public String getTotalActivePnlColor() {
        return (getTotalActivePnl() >= 0) ? "#15803d" : "#b91c1c";
    }

    public String getFormattedTotalActiveEquity() {
        return String.format(java.util.Locale.US, "%,.2f", getTotalActiveEquity());
    }

    public String getFormattedMarginIndicator() {
        return String.format(java.util.Locale.US, "%,.1f", getMarginIndicator());
    }

    public List<ActiveCycleSummaryDto> getAllActiveCycles() { return allActiveCycles; }
    public void setAllActiveCycles(List<ActiveCycleSummaryDto> allActiveCycles) { this.allActiveCycles = allActiveCycles; }

    public SignalBacktestMetricsDto getSignalBtTrianglesMetrics() { return signalBtTrianglesMetrics; }
    public void setSignalBtTrianglesMetrics(SignalBacktestMetricsDto signalBtTrianglesMetrics) { this.signalBtTrianglesMetrics = signalBtTrianglesMetrics; }
    public SignalBacktestMetricsDto getSignalBtCombinedMetrics() { return signalBtCombinedMetrics; }
    public void setSignalBtCombinedMetrics(SignalBacktestMetricsDto signalBtCombinedMetrics) { this.signalBtCombinedMetrics = signalBtCombinedMetrics; }
    public List<SignalTradeDto> getSignalBtTrianglesTrades() { return signalBtTrianglesTrades; }
    public void setSignalBtTrianglesTrades(List<SignalTradeDto> signalBtTrianglesTrades) { this.signalBtTrianglesTrades = signalBtTrianglesTrades; }
    public List<SignalTradeDto> getSignalBtCombinedTrades() { return signalBtCombinedTrades; }
    public void setSignalBtCombinedTrades(List<SignalTradeDto> signalBtCombinedTrades) { this.signalBtCombinedTrades = signalBtCombinedTrades; }
    public List<SignalTradeDto> getSignalBtTradesList() { return signalBtTradesList; }
    public void setSignalBtTradesList(List<SignalTradeDto> signalBtTradesList) { this.signalBtTradesList = signalBtTradesList; }
    public String getSignalBtStrategySelected() { return signalBtStrategySelected; }
    public void setSignalBtStrategySelected(String signalBtStrategySelected) {
        this.signalBtStrategySelected = signalBtStrategySelected;
        if ("TRIANGLES".equals(signalBtStrategySelected)) {
            this.signalBtTradesList = this.signalBtTrianglesTrades;
        } else {
            this.signalBtTradesList = this.signalBtCombinedTrades;
        }
    }
    public String getSignalBtComparisonCurveJson() { return signalBtComparisonCurveJson; }
    public Double getSelectedAccountCapital() {
        if (selectedAccountId != null && userAccountsCombo != null) {
            for (UserAccountDto acc : userAccountsCombo) {
                if (acc.getIdCuenta() != null && acc.getIdCuenta().equals(selectedAccountId)) {
                    if (acc.getCapital() != null && acc.getCapital() > 0) {
                        return acc.getCapital();
                    }
                }
            }
        }
        if (signalBtCombinedMetrics != null && signalBtCombinedMetrics.getInitialCapital() != null && signalBtCombinedMetrics.getInitialCapital() > 0) {
            return signalBtCombinedMetrics.getInitialCapital();
        }
        return 300.0;
    }

    public String getFormattedSignalBtInitialCapital() {
        Double cap = getSelectedAccountCapital();
        return String.format(java.util.Locale.US, "%,.2f", cap != null ? cap : 0.0);
    }

    public String getFormattedSignalBtFinalCapital() {
        return (signalBtCombinedMetrics != null && signalBtCombinedMetrics.getFinalCapital() != null)
                ? String.format(java.util.Locale.US, "%,.2f", signalBtCombinedMetrics.getFinalCapital())
                : "0.00";
    }

    public Double getSignalBtTotalReturn() {
        return (signalBtCombinedMetrics != null && signalBtCombinedMetrics.getTotalReturnPct() != null)
                ? signalBtCombinedMetrics.getTotalReturnPct()
                : 0.0;
    }

    public Double getSignalBtTotalPnl() {
        return (signalBtCombinedMetrics != null && signalBtCombinedMetrics.getNetProfit() != null)
                ? signalBtCombinedMetrics.getNetProfit()
                : 0.0;
    }

    public ActiveCycleSummaryDto getActiveCycleSummary() {
        if (allActiveCycles != null && !allActiveCycles.isEmpty()) {
            String pA = (selectedPair != null ? selectedPair.trim() : "");
            String pB = (selectedPair2 != null ? selectedPair2.trim() : "");
            String s1 = pA + " - " + pB;
            String s2 = pB + " - " + pA;
            for (ActiveCycleSummaryDto c : allActiveCycles) {
                if (c.getSetup() != null && (c.getSetup().equalsIgnoreCase(s1) || c.getSetup().equalsIgnoreCase(s2))) {
                    return c;
                }
                if ((c.getPairA() != null && c.getPairA().equalsIgnoreCase(pA) && c.getPairB() != null && c.getPairB().equalsIgnoreCase(pB))
                        || (c.getPairA() != null && c.getPairA().equalsIgnoreCase(pB) && c.getPairB() != null && c.getPairB().equalsIgnoreCase(pA))) {
                    return c;
                }
            }
        }
        ActiveCycleSummaryDto emptyDto = new ActiveCycleSummaryDto();
        emptyDto.setHasActiveCycle(false);
        return emptyDto;
    }

    public String getFormattedSignalBtTotalPnl() {
        Double netProfit = getSignalBtTotalPnl();
        return String.format(java.util.Locale.US, "%,.2f", netProfit != null ? netProfit : 0.0);
    }


    public void setSignalBtComparisonCurveJson(String signalBtComparisonCurveJson) { this.signalBtComparisonCurveJson = signalBtComparisonCurveJson; }

    public static class UserAccountDto implements java.io.Serializable {
        private Integer idUsuarioCuenta;
        private Integer idUsuario;
        private Integer idCuenta;
        private String nombreCuenta;
        private Double capital;
        private Boolean activo;

        public Integer getIdUsuarioCuenta() { return idUsuarioCuenta; }
        public void setIdUsuarioCuenta(Integer idUsuarioCuenta) { this.idUsuarioCuenta = idUsuarioCuenta; }
        public Integer getIdUsuario() { return idUsuario; }
        public void setIdUsuario(Integer idUsuario) { this.idUsuario = idUsuario; }
        public Integer getIdCuenta() { return idCuenta; }
        public void setIdCuenta(Integer idCuenta) { this.idCuenta = idCuenta; }
        public String getNombreCuenta() { return nombreCuenta; }
        public void setNombreCuenta(String nombreCuenta) { this.nombreCuenta = nombreCuenta; }
        public Double getCapital() { return capital; }
        public void setCapital(Double capital) { this.capital = capital; }
        public Boolean getActivo() { return activo; }
        public void setActivo(Boolean activo) { this.activo = activo; }
    }

    private List<UserAccountDto> userAccountsCombo = new ArrayList<>();
    private Integer selectedAccountId;

    public List<UserAccountDto> getUserAccountsCombo() {
        return userAccountsCombo;
    }

    public void setUserAccountsCombo(List<UserAccountDto> userAccountsCombo) {
        this.userAccountsCombo = userAccountsCombo;
    }

    public Integer getSelectedAccountId() {
        return selectedAccountId;
    }

    public void setSelectedAccountId(Integer selectedAccountId) {
        this.selectedAccountId = selectedAccountId;
    }

    public static class UserRatioDto implements java.io.Serializable {
        private Integer id;
        private Integer idUsuario;
        private Integer idCuenta;
        private String numerador;
        private String denominador;
        private String periodo;
        private Integer dias;
        private Integer emaRapida;
        private Integer emaLenta;
        private Boolean operar;
        private String createdAt;
        private Boolean hasOpenTrades = false;

        public Boolean getHasOpenTrades() { return hasOpenTrades; }
        public void setHasOpenTrades(Boolean hasOpenTrades) { this.hasOpenTrades = hasOpenTrades; }

        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }
        public Integer getIdUsuario() { return idUsuario; }
        public void setIdUsuario(Integer idUsuario) { this.idUsuario = idUsuario; }
        public Integer getIdCuenta() { return idCuenta; }
        public void setIdCuenta(Integer idCuenta) { this.idCuenta = idCuenta; }
        public String getNumerador() { return numerador; }
        public void setNumerador(String numerador) { this.numerador = numerador; }
        public String getDenominador() { return denominador; }
        public void setDenominador(String denominador) { this.denominador = denominador; }
        public String getPeriodo() { return periodo; }
        public void setPeriodo(String periodo) { this.periodo = periodo; }
        public Integer getDias() { return dias; }
        public void setDias(Integer dias) { this.dias = dias; }
        public Integer getEmaRapida() { return emaRapida; }
        public void setEmaRapida(Integer emaRapida) { this.emaRapida = emaRapida; }
        public Integer getEmaLenta() { return emaLenta; }
        public void setEmaLenta(Integer emaLenta) { this.emaLenta = emaLenta; }
        public Boolean getOperar() { return operar; }
        public void setOperar(Boolean operar) { this.operar = operar; }
        public String getCreatedAt() { return createdAt; }
        public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }
    }

        private String activeAccordionIndex = "0";

    public String getActiveAccordionIndex() {
        return activeAccordionIndex;
    }

    public void setActiveAccordionIndex(String activeAccordionIndex) {
        this.activeAccordionIndex = activeAccordionIndex;
    }

    public void onTabChange(org.primefaces.event.TabChangeEvent event) {
        if (event != null && event.getTab() != null) {
            String title = event.getTab().getTitle();
            if (title != null && title.contains("1.")) {
                this.activeAccordionIndex = "0";
            } else {
                this.activeAccordionIndex = "1";
            }
            log.info("Tab del acordeón cambiado a: {} (index={})", title, activeAccordionIndex);
        }
    }

    private List<UserRatioDto> userRatiosList = new ArrayList<>();
    private UserRatioDto selectedUserRatio;

    public List<UserRatioDto> getUserRatiosList() {
        return userRatiosList;
    }

    public void setUserRatiosList(List<UserRatioDto> userRatiosList) {
        this.userRatiosList = userRatiosList;
    }

    public UserRatioDto getSelectedUserRatio() {
        return selectedUserRatio;
    }

    public void setSelectedUserRatio(UserRatioDto selectedUserRatio) {
        this.selectedUserRatio = selectedUserRatio;
    }


    // --- Parámetros de Calibración (Entrada camelCase) ---
    private String selectedPair = "EUR/USD"; // Par A (Numerador)
    private String selectedPair2 = "GBP/USD"; // Par B (Denominador) — ratio sintético A/B
    private Double amplitude = 1.5;
    private Double freq = 0.05;
    private Double phase = 1.2;
    private Double offset = 0.2;
    private Double r = 0.06;
    private Double tYears = 45.0 / 252.0;
    private Integer sigmaWindow = 30;
    private Integer smaPeriodParam = 2;
    private Integer emaSlowPeriodParam = 15;
    private Integer histogramBins = 15;

    // --- Temporada y Periodo Histórico ---
    private String timeframe = "1d";
    private Integer daysBack = 180;
    private Boolean operar = false;
    private Boolean ratioExistsInDb = false;
    private boolean hasActiveTradesInDb = false;

    public boolean isHasActiveTradesInDb() { return hasActiveTradesInDb; }
    public boolean getHasActiveTradesInDb() { return hasActiveTradesInDb; }
    public void setHasActiveTradesInDb(boolean hasActiveTradesInDb) { this.hasActiveTradesInDb = hasActiveTradesInDb; }

    public Boolean getRatioExistsInDb() {
        return ratioExistsInDb;
    }

    public void setRatioExistsInDb(Boolean ratioExistsInDb) {
        this.ratioExistsInDb = ratioExistsInDb;
    }

    public Boolean getOperar() {
        return operar;
    }

    public void setOperar(Boolean operar) {
        this.operar = operar;
    }
    private java.util.Date startDate;
    private java.util.Date endDate;

    public Integer getDaysBack() {
        return daysBack;
    }

    public void setDaysBack(Integer daysBack) {
        this.daysBack = daysBack;
    }

    public void onDaysBackChange() {
        if (daysBack != null && daysBack > 0) {
            if (endDate == null) {
                endDate = new java.util.Date();
            }
            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.setTime(endDate);
            cal.add(java.util.Calendar.DAY_OF_YEAR, -daysBack);
            this.startDate = cal.getTime();
        }
    }

    public java.util.Date getStartDate() {
        return startDate;
    }

    public void setStartDate(java.util.Date startDate) {
        this.startDate = startDate;
    }

    public java.util.Date getEndDate() {
        return endDate;
    }

    public void setEndDate(java.util.Date endDate) {
        this.endDate = endDate;
    }

    public Double gettYears() {
        return this.tYears;
    }

    public void settYears(Double tYears) {
        this.tYears = tYears;
    } 

    // --- KPIs del Último Registro (Resultados de Salida) ---
    private Double currentPrice = 0.0; // Precio del ratio sintético (Par A / Par B)
    private Double syntheticRatioLatest = 0.0; // Mismo que currentPrice, alias semántico
    private Double logReturnLatest = 0.0;
    private Double vol7DAnnualized = 0.0;
    private Double vol60DAnnualized = 0.0;
    private Double strikeAvg20 = 0.0;
    private Double cicloStLatest = 0.0;
    private Double bsCallLatest = 0.0;
    private Double bsPutLatest = 0.0;

    // --- Label del ratio activo (ej. "EUR/USD / GBP/USD") ---
    private String ratioLabel = "Ratio";

    // --- Señal Direccional ---
    private String arbitrageSignal = "Sin Señal";
    private String arbitrageType = "NEUTRAL";

    // JSON String del historial para alimentar Chart.js
    private String historyJson = "[]";
    private String analysisResult;

    // --- Modelos para Análisis Conductual ---
    private BarChartModel prob3PasosModel = createEmptyBarChartModel();
    private BarChartModel velStModel = createEmptyBarChartModel();
    private BarChartModel velLtModel = createEmptyBarChartModel();

    // --- Distribución Estadística (Campana de Gauss) ---
    private Double zScoreA = 0.0;
    private Double zScoreB = 0.0;
    private Double zScoreDiff = 0.0;
    private LineChartModel gaussianModel = createEmptyLineChartModel();
    private BarChartModel histogramModel = createEmptyBarChartModel();
    private Double macdLineLatest = 0.0;
    private Double macdSignalLatest = 0.0;
    private Double macdHistLatest = 0.0;
    private LineChartModel macdModel = createEmptyLineChartModel();

    private static LineChartModel createEmptyLineChartModel() {
        LineChartModel model = new LineChartModel();
        ChartData data = new ChartData();
        model.setData(data);
        return model;
    }

    private static BarChartModel createEmptyBarChartModel() {
        BarChartModel model = new BarChartModel();
        ChartData data = new ChartData();
        model.setData(data);
        return model;
    }

    @Value("${atalaia.backend.url:http://localhost:8004}")
    private String backendUrl;

    @PostConstruct
    public void init() {
        log.info("Inicializando DashboardBean Holográfico (Aether UI)...");
        // Por defecto, fecha fin = hoy, fecha inicio = hace 180 días
        this.endDate = new java.util.Date();
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(this.endDate);
        cal.add(java.util.Calendar.DAY_OF_YEAR, -180);
        this.startDate = cal.getTime();

        loadCatalogo();
        loadUserAccounts();
        loadUserRatiosList();
        loadAllActiveCycles();
        fetchUserRatioDetails();
        analyzePair(); // Cargar datos iniciales
    }

    public void loadAllActiveCycles() {
        this.allActiveCycles = new ArrayList<>();
        Integer idCuenta = selectedAccountId;
        if (idCuenta == null) {
            return;
        }
        try {
            RestTemplate restTemplate = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();
            String url = backendUrl + "/api/v1/trades/active-summary/" + idCuenta;
            String responseStr = restTemplate.getForObject(url, String.class);
            if (responseStr != null && !responseStr.trim().isEmpty()) {
                JsonNode rootNode = mapper.readTree(responseStr);
                if (rootNode.isArray()) {
                    for (JsonNode n : rootNode) {
                        ActiveCycleSummaryDto dto = new ActiveCycleSummaryDto();
                        dto.setSetup(n.path("setup").asText(""));
                        dto.setPairA(n.path("pairA").asText(""));
                        dto.setPairB(n.path("pairB").asText(""));
                        dto.setHasActiveCycle(n.path("hasActiveCycle").asBoolean(true));
                        dto.setTotalOpenTrades(n.path("totalOpenTrades").asInt(0));
                        dto.setTotalMargin(n.path("totalMargin").asDouble(0.0));
                        dto.setMarginA(n.path("marginA").asDouble(0.0));
                        dto.setMarginB(n.path("marginB").asDouble(0.0));
                        dto.setPnlA(n.path("pnlA").asDouble(0.0));
                        dto.setPnlB(n.path("pnlB").asDouble(0.0));
                        dto.setTotalPnl(n.path("totalPnl").asDouble(0.0));
                        dto.setReturnPct(n.path("returnPct").asDouble(0.0));
                        dto.setDirection(n.path("direction").asText(""));
                        dto.setFirstEntryDate(n.path("firstEntryDate").asText(""));
                        dto.setLastEntryDate(n.path("lastEntryDate").asText(""));
                        dto.setTimeframe(n.path("timeframe").asText("1d"));
                        dto.setIsWin(dto.getTotalPnl() >= 0);
                        dto.setSizeA(n.path("sizeA").asDouble(0.0));
                        dto.setSizeB(n.path("sizeB").asDouble(0.0));
                        dto.setAvgEntryPriceA(n.path("avgEntryPriceA").asDouble(0.0));
                        dto.setAvgEntryPriceB(n.path("avgEntryPriceB").asDouble(0.0));
                        dto.setCurrentPriceA(n.path("currentPriceA").asDouble(0.0));
                        dto.setCurrentPriceB(n.path("currentPriceB").asDouble(0.0));
                        dto.setDirA(n.path("dirA").asText("BUY"));
                        dto.setDirB(n.path("dirB").asText("SELL"));

                        JsonNode tradesNode = n.path("trades");
                        if (tradesNode.isArray()) {
                            List<ActiveTradeItemDto> tList = new ArrayList<>();
                            for (JsonNode tn : tradesNode) {
                                ActiveTradeItemDto item = new ActiveTradeItemDto();
                                item.setIdTrade(tn.path("idTrade").asInt());
                                item.setSymbol(tn.path("symbol").asText(""));
                                item.setDirection(tn.path("direction").asText(""));
                                item.setSize(tn.path("size").asDouble());
                                item.setEntryPrice(tn.path("entryPrice").asDouble());
                                item.setCurrentPrice(tn.path("currentPrice").asDouble());
                                item.setMargin(tn.path("margin").asDouble());
                                item.setPnl(tn.path("pnl").asDouble());
                                item.setOpenTime(tn.path("openTime").asText(""));
                                tList.add(item);
                            }
                            dto.setTrades(tList);
                        }
                        allActiveCycles.add(dto);
                    }
                }
            }
            log.info("Cargados {} ciclos de posiciones activas para idCuenta {}", allActiveCycles.size(), idCuenta);
        } catch (Exception e) {
            log.error("Error al cargar allActiveCycles desde backend: {}", e.getMessage());
        }
    }

    public void selectRatioFromActiveCycle(ActiveCycleSummaryDto cycle) {
        if (cycle == null) return;
        if (cycle.getPairA() != null && !cycle.getPairA().isEmpty()) {
            this.selectedPair = cycle.getPairA();
        }
        if (cycle.getPairB() != null && !cycle.getPairB().isEmpty()) {
            this.selectedPair2 = cycle.getPairB();
        }
        if (cycle.getTimeframe() != null && !cycle.getTimeframe().isEmpty()) {
            this.timeframe = cycle.getTimeframe();
        }
        fetchUserRatioDetails();
        analyzePair();
    }

    public void onDatesOrTimeframeChange() {
        if (startDate == null) {
            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.add(java.util.Calendar.DAY_OF_YEAR, -180);
            startDate = cal.getTime();
        }
        if (endDate == null) {
            endDate = new java.util.Date();
        }
        if (timeframe == null) {
            timeframe = "1h";
        }
    }

    /**
     * Carga el catálogo de símbolos desde la base de datos a través de la API REST
     * del backend.
     */
    public void loadCatalogo() {
        try {
            log.info("Cargando catálogo de símbolos desde el backend...");
            RestTemplate restTemplate = new RestTemplate();
            String url = backendUrl + "/api/v1/catalogo";
            log.info("Llamando a FastAPI para catálogo: {}", url);

            RatioSymbolDto[] pairsArray = restTemplate.getForObject(url, RatioSymbolDto[].class);
            this.availablePairs = new ArrayList<>();
            if (pairsArray != null) {
                for (RatioSymbolDto pair : pairsArray) {
                    this.availablePairs.add(pair);
                }
            }
            log.info("Catálogo cargado con éxito. Total símbolos: {}", this.availablePairs.size());

            if (!this.availablePairs.isEmpty()) {
                boolean foundA = false, foundB = false;
                for (RatioSymbolDto p : this.availablePairs) {
                    if (p.getPairName() != null && p.getPairName().equals(selectedPair))
                        foundA = true;
                    if (p.getPairName() != null && p.getPairName().equals(selectedPair2))
                        foundB = true;
                }
                if (!foundA && this.availablePairs.size() >= 1)
                    selectedPair = this.availablePairs.get(0).getPairName();
                if (!foundB && this.availablePairs.size() >= 2)
                    selectedPair2 = this.availablePairs.get(1).getPairName();
                else if (!foundB && this.availablePairs.size() >= 1)
                    selectedPair2 = this.availablePairs.get(0).getPairName();
            }
        } catch (Exception e) {
            log.error("Error al cargar el catálogo de símbolos desde el backend", e);
            this.availablePairs = new ArrayList<>();
        }
    }

    
    public void loadCrucesEmaAnalysis() {
        if (selectedPair == null || selectedPair.isEmpty() || selectedPair2 == null || selectedPair2.trim().isEmpty()) {
            return;
        }
        try {
            log.info("Ejecutando análisis y backtest de Cruces EMA para {} / {}...", selectedPair, selectedPair2);
            RestTemplate restTemplate = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();

            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            String startStr = (startDate != null) ? sdf.format(startDate) : "";
            String endStr = (endDate != null) ? sdf.format(endDate) : "";

            Double curAccCap = getSelectedAccountCapital();
            String url = String.format(
                    "%s/api/v1/cruces-ema/pair-analysis/%s?pairB=%s&timeframe=%s&days=%d&start_date=%s&end_date=%s&smaPeriod=%d&sigmaWindow=%d" +
                    (selectedAccountId != null ? "&idCuenta=" + selectedAccountId : "") +
                    (curAccCap != null ? "&capital=" + curAccCap : "") +
                    "&leverage=100.0",
                    backendUrl,
                    java.net.URLEncoder.encode(selectedPair, "UTF-8"),
                    java.net.URLEncoder.encode(selectedPair2, "UTF-8"),
                    timeframe, (daysBack != null ? daysBack : 180), startStr, endStr,
                    (smaPeriodParam != null ? smaPeriodParam : 2), (emaSlowPeriodParam != null ? emaSlowPeriodParam : 15));

            String responseStr = restTemplate.getForObject(url, String.class);
            if (responseStr != null && !responseStr.isEmpty()) {
                JsonNode root = mapper.readTree(responseStr);
                if (root.has("signalBacktest")) {
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
                                if (t.has("allocatedCapital")) st.setAllocatedCapital(t.get("allocatedCapital").asDouble());
                                if (t.has("entryDate")) {
                                    String ed = t.get("entryDate").asText();
                                    if (ed != null) {
                                        if (ed.startsWith("Entradas: ")) {
                                            ed = ed.substring("Entradas: ".length());
                                        }
                                        if (ed.length() >= 10 && ed.contains(" ") && !ed.contains(" a ")) {
                                            ed = ed.substring(0, 10);
                                        }
                                    }
                                    st.setEntryDate(ed);
                                }
                                if (t.has("exitDate")) {
                                    String xd = t.get("exitDate").asText();
                                    if (xd != null && xd.length() >= 10 && xd.contains(" ")) {
                                        xd = xd.substring(0, 10);
                                    }
                                    st.setExitDate(xd);
                                }
                                if (t.has("entryPriceA")) st.setEntryPriceA(t.get("entryPriceA").asDouble());
                                if (t.has("exitPriceA")) st.setExitPriceA(t.get("exitPriceA").asDouble());
                                if (t.has("entryPriceB")) st.setEntryPriceB(t.get("entryPriceB").asDouble());
                                if (t.has("exitPriceB")) st.setExitPriceB(t.get("exitPriceB").asDouble());
                                if (t.has("durationBars")) st.setDurationBars(t.get("durationBars").asInt());
                                if (t.has("returnPct")) st.setReturnPct(t.get("returnPct").asDouble());
                                if (t.has("pnl")) st.setPnl(t.get("pnl").asDouble());
                                if (t.has("exitReason")) st.setExitReason(t.get("exitReason").asText());
                                if (t.has("isWin")) st.setIsWin(t.get("isWin").asBoolean());
                                if (t.has("cycleNum")) st.setCycleNum(t.get("cycleNum").asInt());
                                if (t.has("cycleTotalTrades")) st.setCycleTotalTrades(t.get("cycleTotalTrades").asInt());
                                if (t.has("cycleTotalInvested")) st.setCycleTotalInvested(t.get("cycleTotalInvested").asDouble());
                                if (t.has("cycleTotalPnl")) st.setCycleTotalPnl(t.get("cycleTotalPnl").asDouble());
                                if (t.has("cycleAvgReturnPct")) st.setCycleAvgReturnPct(t.get("cycleAvgReturnPct").asDouble());
                                if (t.has("isSubtotal")) st.setIsSubtotal(t.get("isSubtotal").asBoolean());
                                if (t.has("multA") && !t.get("multA").isNull()) st.setMultA(t.get("multA").asInt());
                                if (t.has("multB") && !t.get("multB").isNull()) st.setMultB(t.get("multB").asInt());
                                if (t.has("unitsA") && !t.get("unitsA").isNull()) st.setUnitsA(t.get("unitsA").asInt());
                                if (t.has("unitsB") && !t.get("unitsB").isNull()) st.setUnitsB(t.get("unitsB").asInt());
                                if (t.has("pipsA") && !t.get("pipsA").isNull()) st.setPipsA(t.get("pipsA").asDouble());
                                if (t.has("pipsB") && !t.get("pipsB").isNull()) st.setPipsB(t.get("pipsB").asDouble());
                                if (t.has("pnlA") && !t.get("pnlA").isNull()) st.setPnlA(t.get("pnlA").asDouble());
                                if (t.has("pnlB") && !t.get("pnlB").isNull()) st.setPnlB(t.get("pnlB").asDouble());
                                if (t.has("marginA") && !t.get("marginA").isNull()) st.setMarginA(t.get("marginA").asDouble());
                                if (t.has("marginB") && !t.get("marginB").isNull()) st.setMarginB(t.get("marginB").asDouble());
                                if (t.has("availableCapital") && !t.get("availableCapital").isNull()) st.setAvailableCapital(t.get("availableCapital").asDouble());
                                if (t.has("accumCapital") && !t.get("accumCapital").isNull()) st.setAccumCapital(t.get("accumCapital").asDouble());
                                if (t.has("isOpen")) st.setIsOpen(t.get("isOpen").asBoolean());
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
                                if (t.has("allocatedCapital")) st.setAllocatedCapital(t.get("allocatedCapital").asDouble());
                                if (t.has("entryDate")) {
                                    String ed = t.get("entryDate").asText();
                                    if (ed != null) {
                                        if (ed.startsWith("Entradas: ")) {
                                            ed = ed.substring("Entradas: ".length());
                                        }
                                        if (ed.length() >= 10 && ed.contains(" ") && !ed.contains(" a ")) {
                                            ed = ed.substring(0, 10);
                                        }
                                    }
                                    st.setEntryDate(ed);
                                }
                                if (t.has("exitDate")) {
                                    String xd = t.get("exitDate").asText();
                                    if (xd != null && xd.length() >= 10 && xd.contains(" ")) {
                                        xd = xd.substring(0, 10);
                                    }
                                    st.setExitDate(xd);
                                }
                                if (t.has("entryPriceA")) st.setEntryPriceA(t.get("entryPriceA").asDouble());
                                if (t.has("exitPriceA")) st.setExitPriceA(t.get("exitPriceA").asDouble());
                                if (t.has("entryPriceB")) st.setEntryPriceB(t.get("entryPriceB").asDouble());
                                if (t.has("exitPriceB")) st.setExitPriceB(t.get("exitPriceB").asDouble());
                                if (t.has("durationBars")) st.setDurationBars(t.get("durationBars").asInt());
                                if (t.has("returnPct")) st.setReturnPct(t.get("returnPct").asDouble());
                                if (t.has("pnl")) st.setPnl(t.get("pnl").asDouble());
                                if (t.has("exitReason")) st.setExitReason(t.get("exitReason").asText());
                                if (t.has("isWin")) st.setIsWin(t.get("isWin").asBoolean());
                                if (t.has("cycleNum")) st.setCycleNum(t.get("cycleNum").asInt());
                                if (t.has("cycleTotalTrades")) st.setCycleTotalTrades(t.get("cycleTotalTrades").asInt());
                                if (t.has("cycleTotalInvested")) st.setCycleTotalInvested(t.get("cycleTotalInvested").asDouble());
                                if (t.has("cycleTotalPnl")) st.setCycleTotalPnl(t.get("cycleTotalPnl").asDouble());
                                if (t.has("cycleAvgReturnPct")) st.setCycleAvgReturnPct(t.get("cycleAvgReturnPct").asDouble());
                                if (t.has("isSubtotal")) st.setIsSubtotal(t.get("isSubtotal").asBoolean());
                                if (t.has("multA") && !t.get("multA").isNull()) st.setMultA(t.get("multA").asInt());
                                if (t.has("multB") && !t.get("multB").isNull()) st.setMultB(t.get("multB").asInt());
                                if (t.has("unitsA") && !t.get("unitsA").isNull()) st.setUnitsA(t.get("unitsA").asInt());
                                if (t.has("unitsB") && !t.get("unitsB").isNull()) st.setUnitsB(t.get("unitsB").asInt());
                                if (t.has("pipsA") && !t.get("pipsA").isNull()) st.setPipsA(t.get("pipsA").asDouble());
                                if (t.has("pipsB") && !t.get("pipsB").isNull()) st.setPipsB(t.get("pipsB").asDouble());
                                if (t.has("pnlA") && !t.get("pnlA").isNull()) st.setPnlA(t.get("pnlA").asDouble());
                                if (t.has("pnlB") && !t.get("pnlB").isNull()) st.setPnlB(t.get("pnlB").asDouble());
                                if (t.has("marginA") && !t.get("marginA").isNull()) st.setMarginA(t.get("marginA").asDouble());
                                if (t.has("marginB") && !t.get("marginB").isNull()) st.setMarginB(t.get("marginB").asDouble());
                                if (t.has("availableCapital") && !t.get("availableCapital").isNull()) st.setAvailableCapital(t.get("availableCapital").asDouble());
                                if (t.has("accumCapital") && !t.get("accumCapital").isNull()) st.setAccumCapital(t.get("accumCapital").asDouble());
                                if (t.has("isOpen")) st.setIsOpen(t.get("isOpen").asBoolean());
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
                }
                log.info("Análisis de Cruces EMA cargado exitosamente.");
            }
        } catch (Exception e) {
            log.error("Error al cargar análisis cuantitativo: {}", e.getMessage());
        }
    }

    public void analyzePair() {
        onDatesOrTimeframeChange();
        if (selectedPair == null || selectedPair.isEmpty()) {
            analysisResult = "Por favor, seleccione un par válido.";
            return;
        }
        if (selectedPair2 == null || selectedPair2.trim().isEmpty()) {
            analysisResult = "Por favor, seleccione un denominador compatible (Par B).";
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_WARN,
                    "Selección Incompleta", "Por favor seleccione un denominador compatible (Par B)."));
            return;
        }

        try {
            log.info("Analizando ratio sintético: {} / {} con calibración en RAM...", selectedPair, selectedPair2);
            RestTemplate restTemplate = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();

            if (daysBack != null && daysBack > 0) {
                if (endDate == null) {
                    endDate = new java.util.Date();
                }
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime(endDate);
                cal.add(java.util.Calendar.DAY_OF_YEAR, -daysBack);
                this.startDate = cal.getTime();
            }

            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            String startStr = (startDate != null) ? sdf.format(startDate) : "";
            String endStr = (endDate != null) ? sdf.format(endDate) : "";

            // Construir URL para el endpoint de ratio de 2 pares
            String url = String.format(
                    "%s/api/v1/ratio/%s?pairB=%s&amplitude=%f&freq=%f&phase=%f&offset=%f&r=%f&tYears=%f&sigmaWindow=%d&smaPeriod=%d&emaSlowPeriod=%d&histogramBins=%d&tf=%s&start_date=%s&end_date=%s",
                    backendUrl,
                    java.net.URLEncoder.encode(selectedPair, "UTF-8"),
                    java.net.URLEncoder.encode(selectedPair2, "UTF-8"),
                    amplitude, freq, phase, offset, r, tYears, sigmaWindow, smaPeriodParam, emaSlowPeriodParam, histogramBins, timeframe, startStr, endStr);

            log.info("Llamando a FastAPI (ratio 2 pares): {}", url);
            String responseStr = restTemplate.getForObject(url, String.class);
            if (responseStr == null || responseStr.isEmpty()) {
                responseStr = "{\"success\": false, \"error\": \"Respuesta nula del servidor.\"}";
            }

            JsonNode rootNode = mapper.readTree(responseStr);
            if (rootNode.has("success") && rootNode.get("success").asBoolean()) {
                JsonNode latestNode = rootNode.get("latest");

                // Mapear KPIs de salida (el 'price' aquí es el ratio sintético A/B)
                this.currentPrice = latestNode.get("currentPrice").asDouble();
                this.syntheticRatioLatest = this.currentPrice; // alias semántico
                this.logReturnLatest = latestNode.get("logReturnLatest").asDouble();
                this.vol7DAnnualized = latestNode.get("vol7DAnnualized").asDouble();
                this.vol60DAnnualized = latestNode.get("vol60DAnnualized").asDouble();
                this.strikeAvg20 = latestNode.get("strikeAvg20").asDouble();
                this.cicloStLatest = latestNode.get("cicloStLatest").asDouble();
                this.bsCallLatest = latestNode.get("bsCallLatest").asDouble();
                this.bsPutLatest = latestNode.get("bsPutLatest").asDouble();
                if (latestNode.has("macdLineLatest")) {
                    this.macdLineLatest = latestNode.get("macdLineLatest").asDouble();
                    this.macdSignalLatest = latestNode.get("macdSignalLatest").asDouble();
                    this.macdHistLatest = latestNode.get("macdHistLatest").asDouble();
                }

                // Label del ratio activo que viene desde el backend
                if (rootNode.has("ratioLabel")) {
                    this.ratioLabel = rootNode.get("ratioLabel").asText();
                } else {
                    this.ratioLabel = selectedPair + " / " + selectedPair2;
                }

                if (rootNode.has("arbitrageSignal")) {
                    this.arbitrageSignal = rootNode.get("arbitrageSignal").asText();
                    this.arbitrageType = rootNode.get("arbitrageType").asText();
                }

                // Obtener historial de velas serializado
                this.historyJson = mapper.writeValueAsString(rootNode.get("history"));

                // --- Procesar Estadísticas de la Campana de Gauss ---
                if (rootNode.has("stats")) {
                    JsonNode statsNode = rootNode.get("stats");
                    this.zScoreA = statsNode.get("zA").asDouble();
                    this.zScoreB = statsNode.get("zB").asDouble();
                    this.zScoreDiff = statsNode.get("zDiff").asDouble();
                    
                    createGaussianModel(statsNode.get("bellCurve"));
                    if (statsNode.has("histogram")) {
                        createHistogramModel(statsNode.get("histogram"));
                    }
                }

                if (rootNode.has("history")) {
                    createMacdModel(rootNode.get("history"));
                }

                analysisResult = "";
                loadCrucesEmaAnalysis();
            } else {
                String errorMsg = rootNode.has("error") ? rootNode.get("error").asText() : "Error desconocido";
                analysisResult = "Fallo en motor de correlación: " + errorMsg;
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                                "Error de Procesamiento", errorMsg));
            }
            
            // --- Análisis conductual deshabilitado

        } catch (Exception e) {
            log.error("Error al calibrar con el backend holográfico", e);
            analysisResult = "Error conectando al engine de Python: " + e.getMessage();
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                            "Error de Conexión", e.getMessage()));
        }
    }

    public void optimizeCalibration() {
        if (selectedPair == null || selectedPair.isEmpty()) {
            analysisResult = "Por favor, seleccione un par válido.";
            return;
        }
        if (selectedPair2 == null || selectedPair2.trim().isEmpty()) {
            analysisResult = "Por favor, seleccione un denominador compatible (Par B) para optimizar.";
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_WARN,
                    "Selección Incompleta", "Por favor seleccione un denominador compatible (Par B) para optimizar."));
            return;
        }

        try {
            log.info("Optimizando parámetros del ciclo de ratio sintético: {} / {}...", selectedPair, selectedPair2);
            RestTemplate restTemplate = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();

            if (daysBack != null && daysBack > 0) {
                if (endDate == null) {
                    endDate = new java.util.Date();
                }
                java.util.Calendar cal = java.util.Calendar.getInstance();
                cal.setTime(endDate);
                cal.add(java.util.Calendar.DAY_OF_YEAR, -daysBack);
                this.startDate = cal.getTime();
            }

            java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd");
            String startStr = (startDate != null) ? sdf.format(startDate) : "";
            String endStr = (endDate != null) ? sdf.format(endDate) : "";

            String url = String.format(
                    "%s/api/v1/optimize/ratio/%s?pairB=%s&tf=%s&start_date=%s&end_date=%s",
                    backendUrl,
                    java.net.URLEncoder.encode(selectedPair, "UTF-8"),
                    java.net.URLEncoder.encode(selectedPair2, "UTF-8"),
                    timeframe, startStr, endStr);

            log.info("Llamando a FastAPI para optimización: {}", url);
            String responseStr = restTemplate.getForObject(url, String.class);
            if (responseStr == null || responseStr.isEmpty()) {
                responseStr = "{\"success\": false, \"error\": \"Respuesta nula del servidor.\"}";
            }

            JsonNode rootNode = mapper.readTree(responseStr);
            if (rootNode.has("success") && rootNode.get("success").asBoolean()) {
                this.amplitude = rootNode.get("amplitude").asDouble();
                this.freq = rootNode.get("freq").asDouble();
                this.phase = rootNode.get("phase").asDouble();
                this.offset = rootNode.get("offset").asDouble();

                log.info("Parámetros optimizados aplicados: Amp={}, Freq={}, Phase={}, Offset={}",
                        amplitude, freq, phase, offset);

                // Recalcular análisis con los parámetros optimizados
                analyzePair();
                
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                "Optimización Exitosa", "Calibración óptima de ciclo calculada y aplicada."));
            } else {
                String errorMsg = rootNode.has("error") ? rootNode.get("error").asText() : "Error desconocido";
                analysisResult = "Fallo en optimización: " + errorMsg;
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                                "Fallo de Optimización", errorMsg));
            }
        } catch (Exception e) {
            log.error("Error al optimizar la calibración con el backend", e);
            analysisResult = "Error conectando al engine de optimización de Python: " + e.getMessage();
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                            "Error de Conexión", e.getMessage()));
        }
    }

    public void guardarRatio() {
        try {
            Integer userId = null;
            if (securityBean != null) {
                userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
            }
            if (userId == null) {
                userId = 1;
            }

            RestTemplate restTemplate = new RestTemplate();
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

            java.util.Map<String, Object> payload = new java.util.HashMap<>();
            payload.put("idUsuario", userId);
            payload.put("idCuenta", selectedAccountId);
            payload.put("numerador", selectedPair);
            payload.put("denominador", selectedPair2);
            payload.put("periodo", timeframe != null ? timeframe : "1d");
            payload.put("dias", daysBack != null ? daysBack : 180);
            payload.put("EMARapida", smaPeriodParam != null ? smaPeriodParam : 2);
            payload.put("EMALenta", emaSlowPeriodParam != null ? emaSlowPeriodParam : 20);
            payload.put("operar", operar != null ? operar : false);

            org.springframework.http.HttpEntity<java.util.Map<String, Object>> requestEntity = new org.springframework.http.HttpEntity<>(payload, headers);

            String url = backendUrl + "/api/v1/user-ratios/guardar";
            log.info("Guardando ratio para idUsuario {}: {}/{} ({}, {} días) [EMARapida={}, EMALenta={}, Operar={}] en {}",
                    userId, selectedPair, selectedPair2, timeframe, daysBack, smaPeriodParam, emaSlowPeriodParam, operar, url);

            org.springframework.http.ResponseEntity<String> response = restTemplate.postForEntity(url, requestEntity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                this.ratioExistsInDb = true;
                loadUserRatiosList();
                String operarDesc = Boolean.TRUE.equals(operar) ? "Activado (SI)" : "Desactivado (NO)";
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                "Ratio Guardado Exitosamente",
                                "Se guardó la selección " + selectedPair + " / " + selectedPair2 +
                                " (" + timeframe + ", " + daysBack + " días) con EMARapida=" + smaPeriodParam +
                                ", EMALenta=" + emaSlowPeriodParam + ", Operar=" + operarDesc + "."));
            } else {
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                                "Error al Guardar", "No se pudo registrar el ratio."));
            }
        } catch (Exception e) {
            log.error("Excepción al guardar ratio: {}", e.getMessage(), e);
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                            "Error de Conexión", "Fallo al comunicar con el servidor backend: " + e.getMessage()));
        }
    }

                public void loadUserAccounts() {
        try {
            Integer userId = null;
            if (securityBean != null) {
                userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
            }
            if (userId == null) {
                userId = 1;
            }

            RestTemplate restTemplate = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();
            String url = backendUrl + "/api/v1/usuario-cuentas/" + userId;
            log.info("Cargando cuentas asociadas para idUsuario {} desde {}", userId, url);

            String responseStr = restTemplate.getForObject(url, String.class);
            List<UserAccountDto> list = new ArrayList<>();
            if (responseStr != null && !responseStr.isEmpty()) {
                JsonNode rootNode = mapper.readTree(responseStr);
                if (rootNode.isArray()) {
                    for (JsonNode item : rootNode) {
                        UserAccountDto dto = new UserAccountDto();
                        if (item.has("idUsuarioCuenta")) dto.setIdUsuarioCuenta(item.get("idUsuarioCuenta").asInt());
                        if (item.has("idUsuario")) dto.setIdUsuario(item.get("idUsuario").asInt());
                        if (item.has("idCuenta")) dto.setIdCuenta(item.get("idCuenta").asInt());
                        if (item.has("nombreCuenta")) dto.setNombreCuenta(item.get("nombreCuenta").asText());
                        if (item.has("capital") && !item.get("capital").isNull()) dto.setCapital(item.get("capital").asDouble());
                        if (item.has("activo") && !item.get("activo").isNull()) dto.setActivo(item.get("activo").asBoolean());
                        list.add(dto);
                    }
                }
            }
            this.userAccountsCombo = list;
            log.info("Cargadas {} cuentas para el usuario {}", list.size(), userId);

            if (!list.isEmpty()) {
                boolean accountFound = false;
                if (selectedAccountId != null) {
                    for (UserAccountDto acc : list) {
                        if (acc.getIdCuenta().equals(selectedAccountId)) {
                            accountFound = true;
                            break;
                        }
                    }
                }
                if (!accountFound) {
                    this.selectedAccountId = list.get(0).getIdCuenta();
                }
            } else {
                this.selectedAccountId = null;
            }
        } catch (Exception e) {
            log.error("Error al cargar cuentas del usuario: {}", e.getMessage());
        }
    }

    public void onCuentaChange() {
        log.info("Cuenta cambiada en panel lateral a idCuenta={}", selectedAccountId);
        loadUserRatiosList();
        loadAllActiveCycles();
        if (userRatiosList != null && !userRatiosList.isEmpty()) {
            UserRatioDto firstRatio = userRatiosList.get(0);
            onSelectUserRatio(firstRatio);
            this.activeAccordionIndex = "0";
        } else {
            this.ratioExistsInDb = false;
            this.operar = false;
            this.selectedUserRatio = null;
            this.activeAccordionIndex = "0";
            fetchUserRatioDetails();
            analyzePair();
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                            "Cuenta Seleccionada", "No hay ratios registrados para esta cuenta."));
        }

        try {
            if (org.primefaces.PrimeFaces.current() != null && org.primefaces.PrimeFaces.current().isAjaxRequest()) {
                org.primefaces.PrimeFaces.current().ajax().update("aetherForm:mainTabView:leftAccordion:userRatiosTable", "aetherForm:mainTabView:leftAccordion");
            }
        } catch (Exception ignored) {}
    }

    public void onUsuarioChange() {
        Integer userId = null;
        if (securityBean != null) {
            userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
        }
        log.info("Usuario seleccionado cambiado en cabecera de configuración: idUsuario={}", userId);
        loadUserAccounts();
        loadUserRatiosList();

        if (userRatiosList != null && !userRatiosList.isEmpty()) {
            UserRatioDto firstRatio = userRatiosList.get(0);
            log.info("Cargando primer ratio del usuario {}: {} / {}", userId, firstRatio.getNumerador(), firstRatio.getDenominador());
            onSelectUserRatio(firstRatio);
            this.activeAccordionIndex = "0"; // Mantener abierta la sección 1 para ver la tabla del usuario
        } else {
            log.info("El usuario {} no tiene ratios guardados. Restableciendo estado.", userId);
            this.ratioExistsInDb = false;
            this.operar = false;
            this.selectedUserRatio = null;
            this.activeAccordionIndex = "0";
            fetchUserRatioDetails();
            analyzePair();
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                            "Usuario Seleccionado", "El usuario seleccionado no tiene ratios guardados."));
        }

        try {
            if (org.primefaces.PrimeFaces.current() != null && org.primefaces.PrimeFaces.current().isAjaxRequest()) {
                org.primefaces.PrimeFaces.current().ajax().update("aetherForm:mainTabView:leftAccordion:userRatiosTable", "aetherForm:mainTabView:leftAccordion");
            }
        } catch (Exception ignored) {}
    }

    public void onOperarToggle() {
        log.info("Estado de Operar actualizado a {} para {} / {}. Guardando en BD...", operar, selectedPair, selectedPair2);
        guardarRatio();
    }

        public void loadUserRatiosList() {
        try {
            Integer userId = null;
            if (securityBean != null) {
                userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
            }
            if (userId == null) {
                userId = 1;
            }

            RestTemplate restTemplate = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();
            String url = backendUrl + "/api/v1/user-ratios/" + userId + (selectedAccountId != null ? "?idCuenta=" + selectedAccountId : "");
            log.info("Cargando lista de ratios guardados para idUsuario {} desde {}", userId, url);

            String responseStr = restTemplate.getForObject(url, String.class);
            List<UserRatioDto> list = new ArrayList<>();
            if (responseStr != null && !responseStr.isEmpty()) {
                JsonNode rootNode = mapper.readTree(responseStr);
                if (rootNode.isArray()) {
                    for (JsonNode item : rootNode) {
                        UserRatioDto dto = new UserRatioDto();
                        if (item.has("id")) dto.setId(item.get("id").asInt());
                        if (item.has("idUsuario")) dto.setIdUsuario(item.get("idUsuario").asInt());
                        if (item.has("idCuenta") && !item.get("idCuenta").isNull()) dto.setIdCuenta(item.get("idCuenta").asInt());
                        if (item.has("numerador")) dto.setNumerador(item.get("numerador").asText());
                        if (item.has("denominador")) dto.setDenominador(item.get("denominador").asText());
                        if (item.has("periodo")) dto.setPeriodo(item.get("periodo").asText());
                        if (item.has("dias") && !item.get("dias").isNull()) dto.setDias(item.get("dias").asInt());
                        if (item.has("EMARapida") && !item.get("EMARapida").isNull()) dto.setEmaRapida(item.get("EMARapida").asInt());
                        if (item.has("EMALenta") && !item.get("EMALenta").isNull()) dto.setEmaLenta(item.get("EMALenta").asInt());
                        if (item.has("operar") && !item.get("operar").isNull()) dto.setOperar(item.get("operar").asBoolean());
                        else dto.setOperar(false);
                        if (item.has("hasOpenTrades") && !item.get("hasOpenTrades").isNull()) dto.setHasOpenTrades(item.get("hasOpenTrades").asBoolean());
                        else dto.setHasOpenTrades(false);
                        list.add(dto);
                    }
                }
            }
                        this.userRatiosList = list;
            log.info("Cargados {} ratios guardados para el usuario {}", list.size(), userId);
            try {
                if (org.primefaces.PrimeFaces.current() != null && org.primefaces.PrimeFaces.current().isAjaxRequest()) {
                    org.primefaces.PrimeFaces.current().ajax().update("aetherForm:mainTabView:leftAccordion:userRatiosTable", "aetherForm:mainTabView:leftAccordion");
                }
            } catch (Exception ignored) {}
        } catch (Exception e) {
            log.error("Error al cargar lista de user_ratios: {}", e.getMessage());
        }
    }

        public void onRowSelect(org.primefaces.event.SelectEvent<UserRatioDto> event) {
        if (event != null && event.getObject() != null) {
            onSelectUserRatio(event.getObject());
        }
    }

    public void onSelectUserRatio(UserRatioDto ratio) {
        if (ratio == null) return;
        log.info("Seleccionado ratio desde acordeón: {} / {} (periodo={}, dias={}, fast={}, slow={}, operar={})",
                ratio.getNumerador(), ratio.getDenominador(), ratio.getPeriodo(), ratio.getDias(), ratio.getEmaRapida(), ratio.getEmaLenta(), ratio.getOperar());
        this.selectedUserRatio = ratio;
        if (ratio.getIdCuenta() != null) {
            this.selectedAccountId = ratio.getIdCuenta();
        }
        this.selectedPair = ratio.getNumerador();
        this.selectedPair2 = ratio.getDenominador();
        loadCorrelationsForSelectedPair();

        if (ratio.getPeriodo() != null && !ratio.getPeriodo().trim().isEmpty()) {
            this.timeframe = ratio.getPeriodo().trim();
        }
        if (ratio.getDias() != null && ratio.getDias() > 0) {
            this.daysBack = ratio.getDias();
        }
        if (ratio.getEmaRapida() != null && ratio.getEmaRapida() > 0) {
            this.smaPeriodParam = ratio.getEmaRapida();
        }
        if (ratio.getEmaLenta() != null && ratio.getEmaLenta() > 0) {
            this.emaSlowPeriodParam = ratio.getEmaLenta();
        }
        this.operar = Boolean.TRUE.equals(ratio.getOperar());
        this.hasActiveTradesInDb = Boolean.TRUE.equals(ratio.getHasOpenTrades());
        this.ratioExistsInDb = true;

        onDaysBackChange();
        analyzePair();

        javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                        "Ratio Cargado", "Se cargó la configuración de " + selectedPair + " / " + selectedPair2 + " (" + timeframe + ")"));
    }

    public void fetchUserRatioDetails() {
        log.info("Consultando BD al seleccionar Denominador para {} / {}", selectedPair, selectedPair2);
        try {
            Integer userId = null;
            if (securityBean != null) {
                userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
            }
            if (userId == null) {
                userId = 1;
            }

            RestTemplate restTemplate = new RestTemplate();
            ObjectMapper mapper = new ObjectMapper();
            String url = String.format("%s/api/v1/user-ratios/buscar?numerador=%s&denominador=%s" + (selectedAccountId != null ? "&idCuenta=" + selectedAccountId : "&idUsuario=" + userId),
                    backendUrl,
                    java.net.URLEncoder.encode(selectedPair != null ? selectedPair : "", "UTF-8"),
                    java.net.URLEncoder.encode(selectedPair2 != null ? selectedPair2 : "", "UTF-8"));

            log.info("Buscando relación guardada en backend: {}", url);
            String responseStr = restTemplate.getForObject(url, String.class);

            if (responseStr != null && !responseStr.isEmpty()) {
                JsonNode rootNode = mapper.readTree(responseStr);
                if (rootNode.has("found") && rootNode.get("found").asBoolean()) {
                    this.ratioExistsInDb = true;
                    if (rootNode.has("periodo") && !rootNode.get("periodo").isNull()) {
                        this.timeframe = rootNode.get("periodo").asText();
                    }
                    if (rootNode.has("dias") && !rootNode.get("dias").isNull()) {
                        this.daysBack = rootNode.get("dias").asInt();
                        onDaysBackChange();
                    }
                    if (rootNode.has("EMARapida") && !rootNode.get("EMARapida").isNull()) {
                        this.smaPeriodParam = rootNode.get("EMARapida").asInt();
                    }
                    if (rootNode.has("EMALenta") && !rootNode.get("EMALenta").isNull()) {
                        this.emaSlowPeriodParam = rootNode.get("EMALenta").asInt();
                    }
                    if (rootNode.has("operar") && !rootNode.get("operar").isNull()) {
                        this.operar = rootNode.get("operar").asBoolean();
                    } else {
                        this.operar = false;
                    }
                    if (rootNode.has("hasOpenTrades") && !rootNode.get("hasOpenTrades").isNull()) {
                        this.hasActiveTradesInDb = rootNode.get("hasOpenTrades").asBoolean();
                    } else {
                        this.hasActiveTradesInDb = false;
                    }
                    log.info("✅ Configuración recuperada de BD para {}/{}: timeframe={}, dias={}, EMARapida={}, EMALenta={}, operar={}",
                            selectedPair, selectedPair2, timeframe, daysBack, smaPeriodParam, emaSlowPeriodParam, operar);

                    javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                            new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                    "Configuración Cargada", "Se cargó la calibración guardada para " + selectedPair + " / " + selectedPair2));
                } else {
                    log.info("ℹ️ No existe registro en BD para {}/{}. Aplicando valores por defecto.", selectedPair, selectedPair2);
                    this.ratioExistsInDb = false;
                    this.smaPeriodParam = 2;
                    this.emaSlowPeriodParam = 15;
                    this.timeframe = "1h";
                    this.daysBack = 180;
                    this.operar = false;
                    this.hasActiveTradesInDb = false;
                    onDaysBackChange();
                }
            }
        } catch (Exception e) {
            log.error("Error al buscar la configuración guardada del ratio: {}", e.getMessage());
        }
    }

    public void borrarRatio() {
        if (this.hasActiveTradesInDb) {
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_WARN,
                            "Operación Bloqueada", "No se puede borrar el ratio porque tiene registros activos (no cerrados) en la tabla trades."));
            return;
        }
        try {
            Integer userId = null;
            if (securityBean != null) {
                userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
            }
            if (userId == null) {
                userId = 1;
            }

            RestTemplate restTemplate = new RestTemplate();
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

            java.util.Map<String, Object> payload = new java.util.HashMap<>();
            payload.put("idUsuario", userId);
            payload.put("idCuenta", selectedAccountId);
            payload.put("numerador", selectedPair);
            payload.put("denominador", selectedPair2);

            org.springframework.http.HttpEntity<java.util.Map<String, Object>> requestEntity = new org.springframework.http.HttpEntity<>(payload, headers);

            String url = backendUrl + "/api/v1/user-ratios/borrar";
            log.info("Eliminando ratio por índice compuesto para idUsuario {}: {}/{} en {}", userId, selectedPair, selectedPair2, url);

            org.springframework.http.ResponseEntity<String> response = restTemplate.postForEntity(url, requestEntity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                this.ratioExistsInDb = false;
                this.operar = false;
                loadUserRatiosList();
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                "Ratio Eliminado Exitosamente", "Se eliminó el ratio " + selectedPair + " / " + selectedPair2 + "."));
            } else {
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                                "Error al Borrar", "No se encontró el ratio para eliminar."));
            }
        } catch (Exception e) {
            log.error("Excepción al borrar ratio: {}", e.getMessage(), e);
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                            "Error de Conexión / Borrado", "No se pudo eliminar el ratio o no existía previamente."));
        }
    }

    public RatioSymbolDto findDtoByPairName(String pairName) {
        if (pairName == null || availablePairs == null) {
            return null;
        }
        for (RatioSymbolDto p : availablePairs) {
            if (pairName.equals(p.getPairName())) {
                return p;
            }
        }
        return null;
    }

    public List<RatioSymbolDto> getCompatibleDenominators() {
        List<RatioSymbolDto> list = new ArrayList<>();
        if (selectedPair == null || availablePairs == null) {
            return list;
        }
        for (RatioSymbolDto p : availablePairs) {
            if (!p.getPairName().equals(selectedPair)) {
                list.add(p);
            }
        }
        return list;
    }

    public boolean isDenominadorIncompatible(RatioSymbolDto p) {
        if (p == null || selectedPair == null) {
            return false;
        }
        return p.getPairName().equals(selectedPair);
    }

    @SuppressWarnings("unchecked")
    public void loadCorrelationsForSelectedPair() {
        if (selectedPair == null || selectedPair.isEmpty()) {
            return;
        }
        try {
            log.info("Cargando matriz de correlaciones desde backend para par base: {}", selectedPair);
            RestTemplate restTemplate = new RestTemplate();
            String url = backendUrl + "/api/v1/correlations/" + java.net.URLEncoder.encode(selectedPair, "UTF-8");

            // Recibir como mapa de String a Object
            java.util.Map<String, Object> corrMap = restTemplate.getForObject(url, java.util.Map.class);
            if (corrMap != null) {
                for (RatioSymbolDto p : availablePairs) {
                    if (p.getPairName() != null) {
                        Object val = corrMap.get(p.getPairName());
                        if (val != null) {
                            if (val instanceof Number) {
                                p.setCorrelationScore(((Number) val).doubleValue());
                            } else {
                                p.setCorrelationScore(Double.parseDouble(val.toString()));
                            }
                        } else {
                            // Si es el mismo par
                            if (selectedPair.equals(p.getPairName())) {
                                p.setCorrelationScore(1.0);
                            } else {
                                p.setCorrelationScore(0.0);
                            }
                        }
                    }
                }
                log.info("Correlaciones actualizadas para el par base: {}", selectedPair);
            }
        } catch (Exception e) {
            log.error("Error al cargar la matriz de correlaciones desde el backend", e);
        }
    }

    public String getCorrelationLabel(RatioSymbolDto p) {
        if (p == null)
            return "";
        if (selectedPair != null && selectedPair.equals(p.getPairName())) {
            return "(Base)";
        }
        if (isDenominadorIncompatible(p)) {
            return "(Incompatible)";
        }
        Double score = p.getCorrelationScore();
        if (score == null)
            score = 0.0;
        return String.format("(r: %+.2f)", score);
    }

    public String getCorrelationStyle(RatioSymbolDto p) {
        if (p == null)
            return "";
        if (selectedPair != null && selectedPair.equals(p.getPairName())) {
            return "color: #7E8C92; opacity: 0.5; text-decoration: line-through; padding: 3px 8px;";
        }
        if (isDenominadorIncompatible(p)) {
            return "color: #7E8C92; opacity: 0.5; text-decoration: line-through; padding: 3px 8px;";
        }
        Double r = p.getCorrelationScore();
        if (r == null)
            r = 0.0;

        if (r >= 0.75) {
            return "color: #059669; padding: 3px 8px; font-weight: bold;";
        } else if (r >= 0.40) {
            return "color: #65a30d; padding: 3px 8px;";
        } else if (r > -0.40) {
            return "color: #6b7280; padding: 3px 8px;";
        } else if (r > -0.75) {
            return "color: #d97706; padding: 3px 8px;";
        } else {
            return "color: #dc2626; padding: 3px 8px; font-weight: bold;";
        }
    }

    public String getCorrelationStyleByPairName(String pairName) {
        if (pairName == null)
            return "";
        RatioSymbolDto p = findDtoByPairName(pairName);
        return getCorrelationStyle(p);
    }

    public String getCorrelationLabelByPairName(String pairName) {
        if (pairName == null)
            return "";
        RatioSymbolDto p = findDtoByPairName(pairName);
        return getCorrelationLabel(p);
    }

    public void resetCalibrationDefaults() {
        log.info("Reseteando calibración a valores dinámicos por defecto...");
        if (selectedPair == null || selectedPair2 == null) {
            this.amplitude = 1.5;
            this.freq = 0.05;
            this.phase = 1.2;
            this.offset = 0.2;
            return;
        }

        String pairKey = (selectedPair.replace("/", "") + "_" + selectedPair2.replace("/", "")).toUpperCase();
        log.info("Clave de calibración dinámica: {}", pairKey);

        switch (pairKey) {
            case "EURUSD_GBPUSD":
            case "GBPUSD_EURUSD":
                this.amplitude = 1.5;
                this.freq = 0.05;
                this.phase = 1.2;
                this.offset = 0.2;
                break;
            case "EURUSD_USDCHF":
            case "USDCHF_EURUSD":
                this.amplitude = 1.5;
                this.freq = 0.05;
                this.phase = 4.34; // 1.2 + PI (espejo)
                this.offset = -0.2;
                break;
            case "AUDUSD_NZDUSD":
            case "NZDUSD_AUDUSD":
                this.amplitude = 1.2;
                this.freq = 0.04;
                this.phase = 1.0;
                this.offset = 0.1;
                break;
            case "XAUUSD_USDJPY":
            case "USDJPY_XAUUSD":
                this.amplitude = 2.0;
                this.freq = 0.08;
                this.phase = 4.5;
                this.offset = 0.3;
                break;
            case "XAUUSD_EURUSD":
            case "EURUSD_XAUUSD":
                this.amplitude = 1.8;
                this.freq = 0.06;
                this.phase = 0.8;
                this.offset = 0.15;
                break;
            case "GBPUSD_USDCHF":
            case "USDCHF_GBPUSD":
                this.amplitude = 1.6;
                this.freq = 0.05;
                this.phase = 4.34;
                this.offset = -0.15;
                break;
            default:
                // Lógica predictiva general por Pearson
                this.amplitude = 1.5;
                this.freq = 0.05;
                this.phase = 1.2;
                this.offset = 0.2;

                RatioSymbolDto den = findDtoByPairName(selectedPair2);
                if (den != null && den.getCorrelationScore() != null && den.getCorrelationScore() < 0) {
                    this.phase = 4.34; // 1.2 + 3.14 (desfase simétrico espejo)
                    this.offset = -0.2;
                    log.info("Fallback Pearson: Correlación negativa detectada (r: {}). Fase acoplada automáticamente a 4.34 y offset a -0.2.", den.getCorrelationScore());
                } else {
                    log.info("Fallback Pearson: Correlación positiva o neutral detectada. Fase por defecto a 1.2 y offset a 0.2.");
                }
                break;
        }
    }

    private void fetchAnalysisData() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String sym = this.selectedPair.replace("/", "-");
            String url = backendUrl + "/api/v1/analisis/conducta/" + sym + "?days=365";
            String response = restTemplate.getForObject(url, String.class);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode root = mapper.readTree(response);
            
            if (root.has("error")) {
                log.warn("Error en backend para análisis: " + root.get("error").asText());
                createEmptyModels();
                return;
            }

            prob3PasosModel = createBarModel(root.get("probabilidad_3_pasos"), "Probabilidades de Patrones (3 Días)", "rgba(54, 162, 235, 0.6)", "rgb(54, 162, 235)");
            velStModel = createBarModel(root.get("velocidad_st"), "Histograma Velocidad ST", "rgba(75, 192, 192, 0.6)", "rgb(75, 192, 192)");
            velLtModel = createBarModel(root.get("velocidad_lt"), "Histograma Velocidad LT", "rgba(153, 102, 255, 0.6)", "rgb(153, 102, 255)");
            
        } catch (Exception e) {
            log.error("Error al obtener datos de análisis conductual: ", e);
            createEmptyModels();
        }
    }

    private void createEmptyModels() {
        prob3PasosModel = createEmptyBarChartModel();
        velStModel = createEmptyBarChartModel();
        velLtModel = createEmptyBarChartModel();
    }

    private BarChartModel createBarModel(JsonNode dataNode, String label, String bgColor, String borderColor) {
        BarChartModel model = new BarChartModel();
        ChartData data = new ChartData();
        BarChartDataSet barDataSet = new BarChartDataSet();
        barDataSet.setLabel(label);
        
        List<Number> values = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        
        if (dataNode != null && dataNode.has("data") && dataNode.has("labels")) {
            JsonNode valuesNode = dataNode.get("data");
            for (JsonNode v : valuesNode) {
                values.add(v.asDouble());
            }
            JsonNode labelsNode = dataNode.get("labels");
            for (JsonNode l : labelsNode) {
                labels.add(l.asText());
            }
        }
        
        barDataSet.setData(values);
        
        List<String> bgColors = new ArrayList<>();
        bgColors.add(bgColor);
        barDataSet.setBackgroundColor(bgColors);
        
        List<String> borderColors = new ArrayList<>();
        borderColors.add(borderColor);
        barDataSet.setBorderColor(borderColors);
        barDataSet.setBorderWidth(1);
        
        data.addChartDataSet(barDataSet);
        data.setLabels(labels);
        model.setData(data);
        
        // Options
        BarChartOptions options = new BarChartOptions();
        CartesianScales cScales = new CartesianScales();
        CartesianLinearAxes linearAxes = new CartesianLinearAxes();
        linearAxes.setOffset(true);
        cScales.addYAxesData(linearAxes);
        options.setScales(cScales);
        
        model.setOptions(options);
        return model;
    }

    public void onNumeradorChange() {
        log.info("Numerador cambiado a: {}", selectedPair);
        RatioSymbolDto numeradorDto = findDtoByPairName(selectedPair);
        if (numeradorDto == null) {
            return;
        }

        // Cargar las nuevas correlaciones del numerador
        loadCorrelationsForSelectedPair();

        // Si el denominador actual es incompatible o es el mismo par, reajustar automáticamente
        if (selectedPair2 == null || selectedPair2.equals(selectedPair)) {
            // Encontrar el primer denominador compatible y diferente al numerador
            for (RatioSymbolDto p : availablePairs) {
                if (!p.getPairName().equals(selectedPair)) {
                    selectedPair2 = p.getPairName();
                    String msg = "Denominador ajustado dinámicamente a " + selectedPair2 + ".";
                    javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                            new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                    "Alineación de Correlación", msg));
                    break;
                }
            }
        }
        
        onDenominadorChange();
    }

    public void onDenominadorChange() {
        log.info("Selección de Denominador cambiada a: {}. Consultando BD...", selectedPair2);
        fetchUserRatioDetails();
        analyzePair();
    }

    /**
     * DTO para representar un símbolo de ratio cargado desde el backend.
     */
    @Getter
    @Setter
    public static class RatioSymbolDto implements Serializable {
        private static final long serialVersionUID = 1L;
        private String id;

        @com.fasterxml.jackson.annotation.JsonProperty("pair_name")
        private String pairName;

        private String desc;
        private String tipo;
        private Double correlationScore = 0.0;
    }

    private void createGaussianModel(JsonNode bellCurveNode) {
        gaussianModel = new LineChartModel();
        ChartData data = new ChartData();

        // Dataset 1: Campana de Gauss
        LineChartDataSet bellDataSet = new LineChartDataSet();
        bellDataSet.setLabel("Distribución Normal");
        bellDataSet.setBorderColor("rgba(75, 192, 192, 0.8)");
        bellDataSet.setBackgroundColor("rgba(75, 192, 192, 0.2)");
        bellDataSet.setShowLine(true);
        bellDataSet.setTension(0.4);
        bellDataSet.setPointRadius(0); // Ocultar puntos de la curva
        
        List<Object> bellPoints = new ArrayList<>();
        if (bellCurveNode != null && bellCurveNode.isArray()) {
            for (JsonNode pt : bellCurveNode) {
                bellPoints.add(new NumericPoint(pt.get("x").asDouble(), pt.get("y").asDouble()));
            }
        }
        bellDataSet.setData(bellPoints);
        data.addChartDataSet(bellDataSet);

        // Dataset 2: Par A
        LineChartDataSet pairADataSet = new LineChartDataSet();
        pairADataSet.setLabel("Par A (" + this.selectedPair + ")");
        pairADataSet.setBackgroundColor("rgba(54, 162, 235, 1)");
        pairADataSet.setBorderColor("rgba(54, 162, 235, 1)");
        pairADataSet.setPointRadius(6);
        
        // Calcular Y usando Math para la altura del punto
        double yA = (1.0 / Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * Math.pow(this.zScoreA, 2));
        List<Object> pointsA = new ArrayList<>();
        pointsA.add(new NumericPoint(this.zScoreA, yA));
        pairADataSet.setData(pointsA);
        data.addChartDataSet(pairADataSet);

        // Dataset 3: Par B
        LineChartDataSet pairBDataSet = new LineChartDataSet();
        pairBDataSet.setLabel("Par B (" + this.selectedPair2 + ")");
        pairBDataSet.setBackgroundColor("rgba(255, 159, 64, 1)");
        pairBDataSet.setBorderColor("rgba(255, 159, 64, 1)");
        pairBDataSet.setPointRadius(6);
        
        double yB = (1.0 / Math.sqrt(2 * Math.PI)) * Math.exp(-0.5 * Math.pow(this.zScoreB, 2));
        List<Object> pointsB = new ArrayList<>();
        pointsB.add(new NumericPoint(this.zScoreB, yB));
        pairBDataSet.setData(pointsB);
        data.addChartDataSet(pairBDataSet);

        gaussianModel.setData(data);

        // Opciones del gráfico
        LineChartOptions options = new LineChartOptions();
        CartesianScales cScales = new CartesianScales();
        CartesianLinearAxes linearAxes = new CartesianLinearAxes();
        linearAxes.setType("linear");
        linearAxes.setPosition("bottom");
        cScales.addXAxesData(linearAxes);
        options.setScales(cScales);

        gaussianModel.setOptions(options);
    }

    public Double getzScoreA() {
        return zScoreA;
    }

    public void setzScoreA(Double zScoreA) {
        this.zScoreA = zScoreA;
    }

    public Double getzScoreB() {
        return zScoreB;
    }

    public void setzScoreB(Double zScoreB) {
        this.zScoreB = zScoreB;
    }

    public Double getzScoreDiff() {
        return zScoreDiff;
    }

    public void setzScoreDiff(Double zScoreDiff) {
        this.zScoreDiff = zScoreDiff;
    }

    public LineChartModel getGaussianModel() {
        if (gaussianModel == null) {
            gaussianModel = createEmptyLineChartModel();
        }
        return gaussianModel;
    }

    public void setGaussianModel(LineChartModel gaussianModel) {
        this.gaussianModel = gaussianModel;
    }

    public BarChartModel getHistogramModel() {
        if (histogramModel == null) {
            histogramModel = createEmptyBarChartModel();
        }
        return histogramModel;
    }

    public void setHistogramModel(BarChartModel histogramModel) {
        this.histogramModel = histogramModel;
    }

    public BarChartModel getProb3PasosModel() {
        if (prob3PasosModel == null) {
            prob3PasosModel = createEmptyBarChartModel();
        }
        return prob3PasosModel;
    }

    public void setProb3PasosModel(BarChartModel prob3PasosModel) {
        this.prob3PasosModel = prob3PasosModel;
    }

    public BarChartModel getVelStModel() {
        if (velStModel == null) {
            velStModel = createEmptyBarChartModel();
        }
        return velStModel;
    }

    public void setVelStModel(BarChartModel velStModel) {
        this.velStModel = velStModel;
    }

    public BarChartModel getVelLtModel() {
        if (velLtModel == null) {
            velLtModel = createEmptyBarChartModel();
        }
        return velLtModel;
    }

    public void setVelLtModel(BarChartModel velLtModel) {
        this.velLtModel = velLtModel;
    }

    public Double getMacdLineLatest() {
        return macdLineLatest;
    }

    public void setMacdLineLatest(Double macdLineLatest) {
        this.macdLineLatest = macdLineLatest;
    }

    public Double getMacdSignalLatest() {
        return macdSignalLatest;
    }

    public void setMacdSignalLatest(Double macdSignalLatest) {
        this.macdSignalLatest = macdSignalLatest;
    }

    public Double getMacdHistLatest() {
        return macdHistLatest;
    }

    public void setMacdHistLatest(Double macdHistLatest) {
        this.macdHistLatest = macdHistLatest;
    }

    public LineChartModel getMacdModel() {
        if (macdModel == null) {
            macdModel = createEmptyLineChartModel();
        }
        return macdModel;
    }

    public void setMacdModel(LineChartModel macdModel) {
        this.macdModel = macdModel;
    }

    private void createMacdModel(JsonNode historyNode) {
        macdModel = createEmptyLineChartModel();
        if (historyNode == null || !historyNode.isArray()) return;

        ChartData data = new ChartData();

        List<Object> macdValues = new ArrayList<>();
        List<Object> signalValues = new ArrayList<>();
        List<Number> histValues = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        List<String> histBgColors = new ArrayList<>();
        List<String> histBorderColors = new ArrayList<>();

        for (JsonNode item : historyNode) {
            String dt = item.has("datetime") ? item.get("datetime").asText() : "";
            labels.add(dt);

            double macd = item.has("macdLine") ? item.get("macdLine").asDouble() : 0.0;
            double signal = item.has("macdSignal") ? item.get("macdSignal").asDouble() : 0.0;
            double hist = item.has("macdHist") ? item.get("macdHist").asDouble() : 0.0;

            macdValues.add(macd);
            signalValues.add(signal);
            histValues.add(hist);

            if (hist >= 0) {
                histBgColors.add("rgba(75, 192, 192, 0.65)");
                histBorderColors.add("rgba(75, 192, 192, 1.0)");
            } else {
                histBgColors.add("rgba(255, 99, 132, 0.65)");
                histBorderColors.add("rgba(255, 99, 132, 1.0)");
            }
        }

        // Dataset 1: Histograma MACD (Barras)
        BarChartDataSet histDataSet = new BarChartDataSet();
        histDataSet.setLabel("Histograma MACD");
        histDataSet.setData(histValues);
        histDataSet.setBackgroundColor(histBgColors);
        histDataSet.setBorderColor(histBorderColors);
        histDataSet.setBorderWidth(1);
        data.addChartDataSet(histDataSet);

        // Dataset 2: Línea MACD (12, 26)
        LineChartDataSet macdDataSet = new LineChartDataSet();
        macdDataSet.setLabel("Línea MACD (12,26)");
        macdDataSet.setData(macdValues);
        macdDataSet.setBorderColor("rgba(54, 162, 235, 1.0)");
        macdDataSet.setBackgroundColor("rgba(54, 162, 235, 0.1)");
        macdDataSet.setPointRadius(1);
        macdDataSet.setFill(false);
        data.addChartDataSet(macdDataSet);

        // Dataset 3: Línea de Señal (9)
        LineChartDataSet signalDataSet = new LineChartDataSet();
        signalDataSet.setLabel("Señal (9)");
        signalDataSet.setData(signalValues);
        signalDataSet.setBorderColor("rgba(255, 159, 64, 1.0)");
        signalDataSet.setBackgroundColor("rgba(255, 159, 64, 0.1)");
        signalDataSet.setPointRadius(1);
        signalDataSet.setFill(false);
        data.addChartDataSet(signalDataSet);

        data.setLabels(labels);

        LineChartOptions options = new LineChartOptions();
        CartesianScales cScales = new CartesianScales();
        CartesianLinearAxes linearAxes = new CartesianLinearAxes();
        linearAxes.setType("linear");
        linearAxes.setOffset(true);
        cScales.addYAxesData(linearAxes);
        options.setScales(cScales);

        macdModel.setData(data);
        macdModel.setOptions(options);
    }

    private void createHistogramModel(JsonNode histogramNode) {
        histogramModel = createEmptyBarChartModel();
        if (histogramNode == null || !histogramNode.isArray()) return;

        ChartData data = new ChartData();

        BarChartDataSet dataSet = new BarChartDataSet();
        dataSet.setLabel("Frecuencia Histórica");

        List<Number> values = new ArrayList<>();
        List<String> labels = new ArrayList<>();
        List<Object> currentMarkerValues = new ArrayList<>();
        List<String> bgColors = new ArrayList<>();
        List<String> borderColors = new ArrayList<>();

        for (JsonNode item : histogramNode) {
            labels.add(item.get("range").asText());
            int count = item.get("count").asInt();
            values.add(count);

            boolean isCurrent = item.has("isCurrent") && item.get("isCurrent").asBoolean();
            if (isCurrent) {
                bgColors.add("rgba(255, 51, 102, 0.8)"); // Rojo para el bloque actual
                borderColors.add("rgba(255, 51, 102, 1.0)");
            } else {
                bgColors.add("rgba(54, 162, 235, 0.65)"); // Azul para los demás
                borderColors.add("rgba(54, 162, 235, 1.0)");
            }
        }

        dataSet.setData(values);
        dataSet.setBackgroundColor(bgColors);
        dataSet.setBorderColor(borderColors);
        dataSet.setBorderWidth(1);

        data.addChartDataSet(dataSet);

        data.setLabels(labels);

        BarChartOptions options = new BarChartOptions();
        CartesianScales cScales = new CartesianScales();
        CartesianLinearAxes linearAxes = new CartesianLinearAxes();
        linearAxes.setType("linear");
        linearAxes.setOffset(true);
        cScales.addYAxesData(linearAxes);
        options.setScales(cScales);

        histogramModel.setData(data);
        histogramModel.setOptions(options);
    }
}
