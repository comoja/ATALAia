package com.atalaia.correlation.beans;

import javax.annotation.PostConstruct;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.faces.view.ViewScoped;
import javax.inject.Named;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Named("mantenimientoBean")
@ViewScoped
@Getter
@Setter
@Slf4j
public class MantenimientoBean implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String backendApiUrl = "http://127.0.0.1:8004/api/v1";
    private transient RestTemplate restTemplate = new RestTemplate();
    private transient ObjectMapper objectMapper = new ObjectMapper();

    private List<ServiceStatusDto> services = new ArrayList<>();
    private SummaryDto summary = new SummaryDto();
    private List<TerminalEntryDto> terminalLogs = new ArrayList<>();
    
    private String selectedServiceFilter = "all";
    private boolean autoRefresh = true;
    private int pollInterval = 4; // segundos
    private String lastUpdatedStr = "-";

    @PostConstruct
    public void init() {
        log.info("Inicializando MantenimientoBean...");
        refreshAll();
    }

    private RestTemplate getRestTemplate() {
        if (restTemplate == null) {
            restTemplate = new RestTemplate();
        }
        return restTemplate;
    }

    private ObjectMapper getObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper;
    }

    public void refreshAll() {
        refreshServices();
        refreshTerminal();
        this.lastUpdatedStr = new SimpleDateFormat("HH:mm:ss").format(new Date());
    }

    public void pollUpdate() {
        if (autoRefresh) {
            refreshServices();
            refreshTerminal();
            this.lastUpdatedStr = new SimpleDateFormat("HH:mm:ss").format(new Date());
        }
    }

    public void refreshServices() {
        try {
            String url = backendApiUrl + "/system/services";
            String jsonResp = getRestTemplate().getForObject(url, String.class);
            JsonNode root = getObjectMapper().readTree(jsonResp);

            List<ServiceStatusDto> list = new ArrayList<>();
            JsonNode svcsNode = root.path("services");
            if (svcsNode.isArray()) {
                for (JsonNode sn : svcsNode) {
                    ServiceStatusDto dto = new ServiceStatusDto();
                    dto.setId(sn.path("id").asText(""));
                    dto.setName(sn.path("name").asText(""));
                    dto.setCategory(sn.path("category").asText(""));
                    dto.setDescription(sn.path("description").asText(""));
                    dto.setIcon(sn.path("icon").asText("pi pi-cog"));
                    dto.setPort(sn.path("port").asText("N/A"));
                    dto.setUnit(sn.path("unit").asText(""));
                    dto.setStatus(sn.path("status").asText("INACTIVO"));
                    dto.setActive(sn.path("active").asBoolean(false));
                    dto.setListening(sn.path("listening").asBoolean(false));
                    dto.setAutoStart(sn.path("autoStart").asText("N/A"));
                    dto.setPid(sn.path("pid").asText("-"));
                    dto.setStatusCss(sn.path("statusCss").asText("status-inactive"));
                    list.add(dto);
                }
            }
            this.services = list;

            JsonNode sumNode = root.path("summary");
            this.summary.setTotal(sumNode.path("total").asInt(list.size()));
            this.summary.setActive(sumNode.path("active").asInt(0));
            this.summary.setInactive(sumNode.path("inactive").asInt(0));
            this.summary.setListening(sumNode.path("listening").asInt(0));
            this.summary.setHealthy(sumNode.path("healthy").asBoolean(false));
            this.summary.setTimestamp(sumNode.path("timestamp").asText(new SimpleDateFormat("HH:mm:ss").format(new Date())));

        } catch (Exception e) {
            log.warn("Fallo conectando con FastAPI para /system/services, aplicando sondeo local de emergencia: {}", e.getMessage());
            fallbackLocalServiceCheck();
        }
    }

    public void refreshTerminal() {
        try {
            String url = backendApiUrl + "/system/terminal?service=" + selectedServiceFilter + "&lines=45";
            String jsonResp = getRestTemplate().getForObject(url, String.class);
            JsonNode root = getObjectMapper().readTree(jsonResp);

            List<TerminalEntryDto> entries = new ArrayList<>();
            JsonNode entriesNode = root.path("entries");
            if (entriesNode.isArray()) {
                for (JsonNode en : entriesNode) {
                    TerminalEntryDto t = new TerminalEntryDto();
                    t.setTime(en.path("time").asText(""));
                    t.setTag(en.path("tag").asText("SYS"));
                    t.setMessage(en.path("message").asText(""));
                    entries.add(t);
                }
            }
            this.terminalLogs = entries;

        } catch (Exception e) {
            log.debug("No se pudo obtener logs de terminal: {}", e.getMessage());
            if (this.terminalLogs.isEmpty()) {
                List<TerminalEntryDto> fallback = new ArrayList<>();
                fallback.add(new TerminalEntryDto(new SimpleDateFormat("HH:mm:ss").format(new Date()), "SYS", "[SYSTEM] Esperando conexión con el servicio FastAPI en 8004..."));
                this.terminalLogs = fallback;
            }
        }
    }

    public void onFilterChange() {
        refreshTerminal();
    }

    public void toggleAutoRefresh() {
        this.autoRefresh = !this.autoRefresh;
        String status = this.autoRefresh ? "Activado (4s)" : "Pausado";
        FacesContext.getCurrentInstance().addMessage(null, 
            new FacesMessage(FacesMessage.SEVERITY_INFO, "Monitoreo en Vivo", "Auto-refresco " + status));
    }

    public void clearTerminal() {
        this.terminalLogs.clear();
        this.terminalLogs.add(new TerminalEntryDto(new SimpleDateFormat("HH:mm:ss").format(new Date()), "CONSOLE", "[CONSOLE] Terminal limpiada por el operador institucional."));
    }

    public void restartService(String serviceId) {
        try {
            String url = backendApiUrl + "/system/services/" + serviceId + "/restart";
            getRestTemplate().postForObject(url, null, String.class);
            FacesContext.getCurrentInstance().addMessage(null, 
                new FacesMessage(FacesMessage.SEVERITY_INFO, "Reinicio Solicitado", "Comando de reinicio enviado para: " + serviceId));
            refreshAll();
        } catch (Exception e) {
            FacesContext.getCurrentInstance().addMessage(null, 
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error al Reiniciar", "No se pudo reiniciar " + serviceId + ": " + e.getMessage()));
        }
    }

    private void fallbackLocalServiceCheck() {
        List<ServiceStatusDto> list = new ArrayList<>();
        int actCount = 0;
        int listCount = 0;

        int[] ports = {3306, 8000, 8002, 8004, 8005, 8080};
        String[] names = {
            "Base de Datos MySQL", 
            "ConnectionPool Microservice", 
            "WebHook Server", 
            "ATALAia Backend Engine", 
            "MT5 Wine Bridge", 
            "Tomcat Webserver (Frontend)"
        };
        String[] categories = {
            "Infraestructura", 
            "Microservicios", 
            "Señales & Alertas", 
            "Core / Motor API", 
            "Conectividad Broker", 
            "Interfaz Gráfica"
        };
        String[] icons = {
            "pi pi-database", 
            "pi pi-server", 
            "pi pi-bolt", 
            "pi pi-code", 
            "pi pi-chart-line", 
            "pi pi-desktop"
        };

        for (int i = 0; i < ports.length; i++) {
            boolean listening = checkSocket(ports[i]);
            if (listening) {
                actCount++;
                listCount++;
            }
            ServiceStatusDto d = new ServiceStatusDto();
            d.setId("port-" + ports[i]);
            d.setName(names[i]);
            d.setCategory(categories[i]);
            d.setPort(String.valueOf(ports[i]));
            d.setIcon(icons[i]);
            d.setActive(listening);
            d.setListening(listening);
            d.setStatus(listening ? "ACTIVO" : "INACTIVO");
            d.setStatusCss(listening ? "status-active" : "status-inactive");
            d.setAutoStart("Habilitado");
            list.add(d);
        }

        this.services = list;
        this.summary.setTotal(list.size());
        this.summary.setActive(actCount);
        this.summary.setInactive(list.size() - actCount);
        this.summary.setListening(listCount);
        this.summary.setHealthy(actCount == list.size());
        this.summary.setTimestamp(new SimpleDateFormat("HH:mm:ss").format(new Date()));
    }

    private boolean checkSocket(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 250);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // ==========================================
    // DTOs Internos para Vista
    // ==========================================
    @Getter
    @Setter
    public static class ServiceStatusDto implements Serializable {
        private static final long serialVersionUID = 1L;
        private String id;
        private String name;
        private String category;
        private String description;
        private String icon;
        private String port;
        private String unit;
        private String status;
        private boolean active;
        private boolean listening;
        private String autoStart;
        private String pid;
        private String statusCss;
    }

    @Getter
    @Setter
    public static class SummaryDto implements Serializable {
        private static final long serialVersionUID = 1L;
        private int total = 0;
        private int active = 0;
        private int inactive = 0;
        private int listening = 0;
        private boolean healthy = true;
        private String timestamp = "-";
    }

    @Getter
    @Setter
    public static class TerminalEntryDto implements Serializable {
        private static final long serialVersionUID = 1L;
        private String time;
        private String tag;
        private String message;

        public TerminalEntryDto() {}
        public TerminalEntryDto(String time, String tag, String message) {
            this.time = time;
            this.tag = tag;
            this.message = message;
        }
    }
}
