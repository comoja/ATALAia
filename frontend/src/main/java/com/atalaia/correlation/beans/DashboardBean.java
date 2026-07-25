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

    // --- Catalogo de Simbolos desde BD ---
    private List<RatioSymbolDto> availablePairs = new ArrayList<>();

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
    private Integer smaPeriodParam = 3;
    private Integer emaSlowPeriodParam = 15;
    private Integer histogramBins = 50;

    // --- Temporada y Periodo Histórico ---
    private String timeframe = "1d";
    private java.util.Date startDate;
    private java.util.Date endDate;

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
    private BarChartModel prob3PasosModel;
    private BarChartModel velStModel;
    private BarChartModel velLtModel;

    // --- Distribución Estadística (Campana de Gauss) ---
    private Double zScoreA = 0.0;
    private Double zScoreB = 0.0;
    private Double zScoreDiff = 0.0;
    private LineChartModel gaussianModel;
    private BarChartModel histogramModel;
    private Double macdLineLatest = 0.0;
    private Double macdSignalLatest = 0.0;
    private Double macdHistLatest = 0.0;
    private LineChartModel macdModel;

    @Value("${atalaia.backend.url:http://localhost:8004}")
    private String backendUrl;

    @PostConstruct
    public void init() {
        log.info("Inicializando DashboardBean Holográfico (Aether UI)...");
        // Por defecto, fecha fin = hoy, fecha inicio = hace 1 año (365 días)
        this.endDate = new java.util.Date();
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(this.endDate);
        cal.add(java.util.Calendar.DAY_OF_YEAR, -365);
        this.startDate = cal.getTime();

        loadCatalogo();
        analyzePair(); // Cargar datos iniciales
    }

    public void onDatesOrTimeframeChange() {
        if (startDate == null) {
            java.util.Calendar cal = java.util.Calendar.getInstance();
            cal.add(java.util.Calendar.DAY_OF_YEAR, -365);
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

                analysisResult = "Ratio sintético calculado: " + this.ratioLabel;
            } else {
                String errorMsg = rootNode.has("error") ? rootNode.get("error").asText() : "Error desconocido";
                analysisResult = "Fallo en motor de correlación: " + errorMsg;
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                                "Error de Procesamiento", errorMsg));
            }
            
            // --- Cargar análisis conductual (nueva pestaña)
            fetchAnalysisData();

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
        prob3PasosModel = new BarChartModel();
        velStModel = new BarChartModel();
        velLtModel = new BarChartModel();
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
                    String msg = "Denominador ajustado dinámicamente a " + selectedPair2
                            + ".";
                    javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                            new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                    "Alineación de Correlación", msg));
                    break;
                }
            }
        }
        
        // Auto-calibrar fase y parámetros basados en correlación
        resetCalibrationDefaults();
    }

    public void onDenominadorChange() {
        log.info("Denominador cambiado a: {}", selectedPair2);
        
        // Auto-calibrar fase y parámetros basados en correlación
        resetCalibrationDefaults();
        
        // Recalcular análisis con el nuevo par seleccionado
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
        return gaussianModel;
    }

    public void setGaussianModel(LineChartModel gaussianModel) {
        this.gaussianModel = gaussianModel;
    }

    public BarChartModel getHistogramModel() {
        return histogramModel;
    }

    public void setHistogramModel(BarChartModel histogramModel) {
        this.histogramModel = histogramModel;
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
        return macdModel;
    }

    public void setMacdModel(LineChartModel macdModel) {
        this.macdModel = macdModel;
    }

    private void createMacdModel(JsonNode historyNode) {
        if (historyNode == null || !historyNode.isArray()) return;

        macdModel = new LineChartModel();
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
        if (histogramNode == null || !histogramNode.isArray()) return;

        histogramModel = new BarChartModel();
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
