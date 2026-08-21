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
    public static class UserRatioDto implements java.io.Serializable {
        private Integer id;
        private Integer idUsuario;
        private String numerador;
        private String denominador;
        private String periodo;
        private Integer dias;
        private Integer emaRapida;
        private Integer emaLenta;
        private Boolean operar;
        private String createdAt;

        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }
        public Integer getIdUsuario() { return idUsuario; }
        public void setIdUsuario(Integer idUsuario) { this.idUsuario = idUsuario; }
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
    private Integer smaPeriodParam = 3;
    private Integer emaSlowPeriodParam = 15;
    private Integer histogramBins = 15;

    // --- Temporada y Periodo Histórico ---
    private String timeframe = "1d";
    private Integer daysBack = 180;
    private Boolean operar = false;
    private Boolean ratioExistsInDb = false;

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
        loadUserRatiosList();
        loadUserRatiosList();
        fetchUserRatioDetails();
        analyzePair(); // Cargar datos iniciales
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

                analysisResult = "Ratio sintético calculado: " + this.ratioLabel;
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
            payload.put("numerador", selectedPair);
            payload.put("denominador", selectedPair2);
            payload.put("periodo", timeframe != null ? timeframe : "1d");
            payload.put("dias", daysBack != null ? daysBack : 180);
            payload.put("EMARapida", smaPeriodParam != null ? smaPeriodParam : 3);
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
            String url = backendUrl + "/api/v1/user-ratios/" + userId;
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
                        if (item.has("numerador")) dto.setNumerador(item.get("numerador").asText());
                        if (item.has("denominador")) dto.setDenominador(item.get("denominador").asText());
                        if (item.has("periodo")) dto.setPeriodo(item.get("periodo").asText());
                        if (item.has("dias") && !item.get("dias").isNull()) dto.setDias(item.get("dias").asInt());
                        if (item.has("EMARapida") && !item.get("EMARapida").isNull()) dto.setEmaRapida(item.get("EMARapida").asInt());
                        if (item.has("EMALenta") && !item.get("EMALenta").isNull()) dto.setEmaLenta(item.get("EMALenta").asInt());
                        if (item.has("operar") && !item.get("operar").isNull()) dto.setOperar(item.get("operar").asBoolean());
                        else dto.setOperar(false);
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
        this.selectedPair = ratio.getNumerador();
        loadCorrelationsForSelectedPair();
        this.selectedPair2 = ratio.getDenominador();
        if (ratio.getPeriodo() != null && !ratio.getPeriodo().isEmpty()) {
            this.timeframe = ratio.getPeriodo();
        }
        if (ratio.getDias() != null) {
            this.daysBack = ratio.getDias();
            onDaysBackChange();
        }
        if (ratio.getEmaRapida() != null) {
            this.smaPeriodParam = ratio.getEmaRapida();
        }
        if (ratio.getEmaLenta() != null) {
            this.emaSlowPeriodParam = ratio.getEmaLenta();
        }
        this.operar = Boolean.TRUE.equals(ratio.getOperar());
        this.ratioExistsInDb = true;

        analyzePair();

        javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                        "Ratio Cargado", "Se cargó la configuración de " + selectedPair + " / " + selectedPair2));
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
            String url = String.format("%s/api/v1/user-ratios/buscar?idUsuario=%d&numerador=%s&denominador=%s",
                    backendUrl, userId,
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
                    log.info("✅ Configuración recuperada de BD para {}/{}: timeframe={}, dias={}, EMARapida={}, EMALenta={}, operar={}",
                            selectedPair, selectedPair2, timeframe, daysBack, smaPeriodParam, emaSlowPeriodParam, operar);

                    javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                            new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                    "Configuración Cargada", "Se cargó la calibración guardada para " + selectedPair + " / " + selectedPair2));
                } else {
                    log.info("ℹ️ No existe registro en BD para {}/{}. Aplicando valores por defecto.", selectedPair, selectedPair2);
                    this.ratioExistsInDb = false;
                    this.smaPeriodParam = 3;
                    this.emaSlowPeriodParam = 15;
                    this.timeframe = "1d";
                    this.daysBack = 180;
                    this.operar = false;
                    onDaysBackChange();
                }
            }
        } catch (Exception e) {
            log.error("Error al buscar la configuración guardada del ratio: {}", e.getMessage());
        }
    }

    public void borrarRatio() {
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
