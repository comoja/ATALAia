package com.atalaia.correlation.beans;

import javax.faces.view.ViewScoped;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.inject.Named;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.client.RestTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

@Named("configuracionBean")
@ViewScoped
@Getter
@Setter
@Slf4j
public class ConfiguracionBean implements Serializable {

    private static final long serialVersionUID = 1L;

    @javax.inject.Inject
    private SecurityBean securityBean;

    private List<CuentaDto> cuentas = new ArrayList<>();
    private List<SymbolDto> simbolos = new ArrayList<>();
    private List<EstrategiaSymbolDto> matrizRendimiento = new ArrayList<>();
    
    // Filtro por usuario para el Administrador (manejo similar al reporte jerárquico)
    private Integer selectedFiltroUsuarioId = null;
    private List<javax.faces.model.SelectItem> usuariosFiltroCombo = new ArrayList<>();

    public Integer getSelectedFiltroUsuarioId() { return selectedFiltroUsuarioId; }
    public void setSelectedFiltroUsuarioId(Integer selectedFiltroUsuarioId) { this.selectedFiltroUsuarioId = selectedFiltroUsuarioId; }

    public List<javax.faces.model.SelectItem> getUsuariosFiltroCombo() { return usuariosFiltroCombo; }
    public void setUsuariosFiltroCombo(List<javax.faces.model.SelectItem> usuariosFiltroCombo) { this.usuariosFiltroCombo = usuariosFiltroCombo; }

    // Estado de la consola de Backtesting
    private String consoleLog = "Consola inactiva. Seleccione una prueba para iniciar...";
    private boolean running = false;
    private Integer activePid = null;

    private final String backendApiUrl = "http://127.0.0.1:8004/api/v1";

    public void init() {
        log.info("Inicializando ConfiguracionBean...");
        loadUsuariosFiltroCombo();
        loadCuentas();
        loadSimbolos();
        loadMatrizRendimiento();
        checkBacktestStatus();
    }

    public void loadUsuariosFiltroCombo() {
        this.usuariosFiltroCombo.clear();
        this.usuariosFiltroCombo.add(new javax.faces.model.SelectItem(null, "👥 Todos los Usuarios"));
        try {
            RestTemplate rt = new RestTemplate();
            String uUrl = backendApiUrl + "/auth/usuarios";
            com.fasterxml.jackson.databind.JsonNode uArr = rt.getForObject(uUrl, com.fasterxml.jackson.databind.JsonNode.class);
            if (uArr != null && uArr.isArray()) {
                for (com.fasterxml.jackson.databind.JsonNode un : uArr) {
                    int uid = un.path("idUsuario").asInt();
                    String nom = un.path("nombreCompleto").asText(un.path("username").asText());
                    this.usuariosFiltroCombo.add(new javax.faces.model.SelectItem(uid, nom));
                }
            }
        } catch (Exception e) {
            log.warn("No se pudo cargar combo de usuarios para filtro en configuración: {}", e.getMessage());
        }
    }

    public void onTabChange(org.primefaces.event.TabChangeEvent event) {
        if (event != null && event.getTab() != null) {
            String title = event.getTab().getTitle();
            if (title != null) {
                if (title.contains("Cuentas")) {
                    loadCuentas();
                } else if (title.contains("Símbolos")) {
                    loadSimbolos();
                } else if (title.contains("Matriz")) {
                    loadMatrizRendimiento();
                }
                log.info("Pestaña de configuración cambiada a: {}, datos recargados desde BD", title);
            }
        }
    }

    public boolean isConcentradoraDisabled(CuentaDto cta) {
        if (cta != null && Boolean.TRUE.equals(cta.getConcentradora())) {
            return false;
        }
        if (cuentas != null) {
            for (CuentaDto c : cuentas) {
                if (c != null && (cta == null || !c.getIdCuenta().equals(cta.getIdCuenta()))) {
                    if (Boolean.TRUE.equals(c.getConcentradora())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public void loadCuentas() {
        try {
            boolean isAdmin = securityBean != null && securityBean.isAdmin();
            Integer targetUserId = null;
            if (isAdmin) {
                // El administrador puede ver todas las cuentas (targetUserId = null) o filtrar por un usuario específico
                targetUserId = this.selectedFiltroUsuarioId;
            } else {
                // Usuario regular: ve estrictamente solo sus cuentas asignadas
                targetUserId = securityBean != null ? (securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario()) : 1;
            }

            RestTemplate restTemplate = new RestTemplate();
            String url = backendApiUrl + "/config/cuentas" + (targetUserId != null && targetUserId > 0 ? "?idUsuario=" + targetUserId : "");
            log.info("Cargando cuentas para configuración (isAdmin={}, targetUserId={}) desde {}", isAdmin, targetUserId, url);
            CuentaDto[] response = restTemplate.getForObject(url, CuentaDto[].class);
            if (response != null) {
                this.cuentas = new ArrayList<>(Arrays.asList(response));
                log.info("Cuentas cargadas en configuración: {}", cuentas.size());
            }
        } catch (Exception e) {
            log.error("Error al cargar cuentas: {}", e.getMessage());
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error", "No se pudieron cargar las cuentas del backend."));
        }
    }

    public void guardarCuenta(CuentaDto cuenta) {
        if (cuenta != null) {
            double c = cuenta.getPctConservador() != null ? cuenta.getPctConservador() : 0.0;
            double m = cuenta.getPctModerado() != null ? cuenta.getPctModerado() : 0.0;
            double a = cuenta.getPctAgresivo() != null ? cuenta.getPctAgresivo() : 0.0;
            double sum = Math.round((c + m + a) * 100.0) / 100.0;
            if (Math.abs(sum - 100.0) > 0.05) {
                FacesContext.getCurrentInstance().addMessage(null,
                    new FacesMessage(FacesMessage.SEVERITY_ERROR, "Validación de Perfil",
                        "La suma de los porcentajes del perfil de inversionista debe ser exactamente 100% (Actual: " + sum + "%)."));
                return;
            }
        }
        try {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<CuentaDto> requestEntity = new HttpEntity<>(cuenta, headers);
            String url = backendApiUrl + "/config/cuentas/guardar";

            ResponseEntity<String> response = restTemplate.postForEntity(url, requestEntity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                FacesContext.getCurrentInstance().addMessage(null,
                    new FacesMessage(FacesMessage.SEVERITY_INFO, "\u00c9xito", "Cuenta '" + cuenta.getNombre() + "' guardada correctamente."));
                loadCuentas(); // Refrescar
            }
        } catch (Exception e) {
            log.error("Error al guardar cuenta: {}", e.getMessage());
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error", "No se pudo guardar la cuenta: " + e.getMessage()));
        }
    }

    public void loadSimbolos() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = backendApiUrl + "/config/simbolos";
            SymbolDto[] response = restTemplate.getForObject(url, SymbolDto[].class);
            if (response != null) {
                this.simbolos = new ArrayList<>(Arrays.asList(response));
                log.info("Símbolos cargados desde el backend: {}", simbolos.size());
            }
        } catch (Exception e) {
            log.error("Error al cargar símbolos: {}", e.getMessage());
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error", "No se pudieron cargar los símbolos del backend."));
        }
    }

    public void guardarSimbolo(SymbolDto simbolo) {
        try {
            RestTemplate restTemplate = new RestTemplate();
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<SymbolDto> requestEntity = new HttpEntity<>(simbolo, headers);
            String url = backendApiUrl + "/config/simbolos/guardar";

            ResponseEntity<String> response = restTemplate.postForEntity(url, requestEntity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                FacesContext.getCurrentInstance().addMessage(null,
                    new FacesMessage(FacesMessage.SEVERITY_INFO, "Éxito", "Símbolo '" + simbolo.getSymbol() + "' guardado correctamente."));
                loadSimbolos(); // Refrescar
            }
        } catch (Exception e) {
            log.error("Error al guardar símbolo: {}", e.getMessage());
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error", "No se pudo guardar el símbolo: " + e.getMessage()));
        }
    }

    public void loadMatrizRendimiento() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = backendApiUrl + "/config/estrategia-symbol";
            EstrategiaSymbolDto[] response = restTemplate.getForObject(url, EstrategiaSymbolDto[].class);
            if (response != null) {
                this.matrizRendimiento = new ArrayList<>(Arrays.asList(response));
                log.info("Matriz de rendimiento cargada desde el backend: {}", matrizRendimiento.size());
            }
        } catch (Exception e) {
            log.error("Error al cargar matriz de rendimiento: {}", e.getMessage());
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error", "No se pudieron cargar los datos de rendimiento de la base de datos."));
        }
    }

    public void runBacktest(String type) {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = backendApiUrl + "/backtest/run/" + type;
            ResponseEntity<BacktestRunResponse> response = restTemplate.postForEntity(url, null, BacktestRunResponse.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                this.running = true;
                this.activePid = response.getBody().getPid();
                this.consoleLog = "Iniciando backtesting " + type.toUpperCase() + " (PID: " + activePid + ")...";
                FacesContext.getCurrentInstance().addMessage(null,
                    new FacesMessage(FacesMessage.SEVERITY_INFO, "Iniciado", "Prueba de backtesting " + type + " lanzada con éxito."));
            }
        } catch (Exception e) {
            log.error("Error al lanzar backtesting {}: {}", type, e.getMessage());
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error", "No se pudo lanzar el backtesting: " + e.getMessage()));
        }
    }

    public void checkBacktestStatus() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = backendApiUrl + "/backtest/status";
            BacktestStatusResponse response = restTemplate.getForObject(url, BacktestStatusResponse.class);
            if (response != null) {
                this.running = response.isRunning();
                if (response.getConsoleLog() != null && !response.getConsoleLog().isEmpty()) {
                    this.consoleLog = response.getConsoleLog();
                } else if (!this.running) {
                    this.consoleLog = "Consola inactiva. Seleccione una prueba para iniciar...";
                }
            }
        } catch (Exception e) {
            log.error("Error al verificar status del backtesting: {}", e.getMessage());
        }
    }

    public void aplicarSugerenciasRiesgo() {
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = backendApiUrl + "/config/estrategia-symbol/aplicar";
            ResponseEntity<AplicarRiesgoResponse> response = restTemplate.postForEntity(url, null, AplicarRiesgoResponse.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                AplicarRiesgoResponse body = response.getBody();
                if (body != null) {
                    String msg = "Sugerencias de riesgo aplicadas correctamente. Excluidas: " 
                        + body.getExcluidos() + ", Reactivadas: " + body.getReactivados();
                    FacesContext.getCurrentInstance().addMessage(null,
                        new FacesMessage(FacesMessage.SEVERITY_INFO, "Éxito", msg));
                    loadMatrizRendimiento();
                } else {
                    FacesContext.getCurrentInstance().addMessage(null,
                        new FacesMessage(FacesMessage.SEVERITY_WARN, "Advertencia", "Respuesta vacía del backend al aplicar riesgos."));
                }
            }
        } catch (Exception e) {
            log.error("Error al aplicar sugerencias de riesgo: {}", e.getMessage());
            FacesContext.getCurrentInstance().addMessage(null,
                new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error", "No se pudieron aplicar las sugerencias de riesgo: " + e.getMessage()));
        }
    }


    // --- DTOs estáticos ---

    @Getter
    @Setter
    public static class AplicarRiesgoResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        private String status;
        private Integer excluidos;
        private Integer reactivados;
    }


    @Getter
    @Setter
    public static class CuentaDto implements Serializable {
        private static final long serialVersionUID = 1L;

        private Integer idCuenta;

        @JsonProperty("Nombre")
        private String nombre;

        @JsonProperty("correo")
        private String correo;

        public String getCorreo() { return correo; }
        public void setCorreo(String correo) { this.correo = correo; }

        @JsonProperty("Capital")
        private Double capital;

        private Double ganancia;

        @JsonProperty("Activo")
        private Integer activo;

        @JsonProperty("TokenMsg")
        private String tokenMsg;

        private String idGrupoMsg;
        private Double riesgoPorOperacion;
        private Double comision;

        @JsonProperty("indicadorMargen")
        private Double indicadorMargen = 200.0;

        public Double getIndicadorMargen() { return indicadorMargen != null ? indicadorMargen : 200.0; }
        public void setIndicadorMargen(Double indicadorMargen) { this.indicadorMargen = indicadorMargen; }

        @JsonProperty("Concentradora")
        private Boolean concentradora = false;

        public Boolean isConcentradora() { return Boolean.TRUE.equals(concentradora); }
        public Boolean getConcentradora() { return concentradora; }
        public void setConcentradora(Boolean concentradora) { this.concentradora = concentradora; }

        @JsonProperty("pctConservador")
        private Double pctConservador = 40.0;

        @JsonProperty("pctModerado")
        private Double pctModerado = 30.0;

        @JsonProperty("pctAgresivo")
        private Double pctAgresivo = 30.0;

        public Double getPctConservador() { return pctConservador != null ? pctConservador : 40.0; }
        public void setPctConservador(Double pctConservador) { this.pctConservador = pctConservador; }

        public Double getPctModerado() { return pctModerado != null ? pctModerado : 30.0; }
        public void setPctModerado(Double pctModerado) { this.pctModerado = pctModerado; }

        public Double getPctAgresivo() { return pctAgresivo != null ? pctAgresivo : 30.0; }
        public void setPctAgresivo(Double pctAgresivo) { this.pctAgresivo = pctAgresivo; }

        @JsonProperty("idUsuario")
        private Integer idUsuario;

        @JsonProperty("username")
        private String username;

        @JsonProperty("nombreUsuario")
        private String nombreUsuario;

        public Integer getIdUsuario() { return idUsuario; }
        public void setIdUsuario(Integer idUsuario) { this.idUsuario = idUsuario; }

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }

        public String getNombreUsuario() { return nombreUsuario != null ? nombreUsuario : (username != null ? username : ""); }
        public void setNombreUsuario(String nombreUsuario) { this.nombreUsuario = nombreUsuario; }

        @JsonProperty("idBroker")
        private Integer idBroker;

        @JsonProperty("nombreBroker")
        private String nombreBroker;

        @JsonProperty("brokerLogin")
        private String brokerLogin;

        public Integer getIdBroker() { return idBroker; }
        public void setIdBroker(Integer idBroker) { this.idBroker = idBroker; }

        public String getNombreBroker() { return nombreBroker != null ? nombreBroker : "Sin Bróker"; }
        public void setNombreBroker(String nombreBroker) { this.nombreBroker = nombreBroker; }

        public String getBrokerLogin() { return brokerLogin != null ? brokerLogin : ""; }
        public void setBrokerLogin(String brokerLogin) { this.brokerLogin = brokerLogin; }
    }

    @Getter
    @Setter
    public static class SymbolDto implements Serializable {
        private static final long serialVersionUID = 1L;

        private String symbol;

        @JsonProperty("Activo")
        private Integer activo;

        private Double min_lots;
        private Integer broker;
        private Double precioMaximo;
        private Double precioMinimo;
    }

    @Getter
    @Setter
    public static class BacktestRunResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        private String status;
        private String message;
        private Integer pid;
    }

    @Getter
    @Setter
    public static class BacktestStatusResponse implements Serializable {
        private static final long serialVersionUID = 1L;
        private boolean isRunning;
        private Integer exitCode;
        private String consoleLog;
    }

    @Getter
    @Setter
    public static class EstrategiaSymbolDto implements Serializable {
        private static final long serialVersionUID = 1L;
        private Integer idEstrategiaSymbol;
        private String symbol;
        private String strategy;
        private Integer totalTrades;
        private Integer wins;
        private Double winRate;
        private Double pnlNeto;
        private Double profitFactor;
        private Double expectancy;
        private Double maxDrawdown;
        private Double riesgoSugerido;
        private String fuente;
        private String periodoFecha;
        private String updatedAt;
    }
}
