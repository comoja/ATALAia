package com.atalaia.correlation.beans;

import javax.annotation.PostConstruct;
import javax.faces.view.ViewScoped;
import javax.inject.Named;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.primefaces.model.DefaultStreamedContent;
import org.primefaces.model.StreamedContent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Named("reportesBean")
@ViewScoped
@Getter
@Setter
@Slf4j
public class ReportesBean implements Serializable {

    private static final long serialVersionUID = 1L;

    @javax.inject.Inject
    private SecurityBean securityBean;

    @Value("${backend.api.url:http://127.0.0.1:8004}")
    private String backendUrl;

    // Filtros de Selección
    private Integer selectedAccountId;
    private Integer selectedAnio;
    private Integer selectedMes;

    private List<UserAccountDto> userAccounts = new ArrayList<>();
    private List<Integer> aniosDisponibles = new ArrayList<>();
    private List<MesDto> mesesDisponibles = new ArrayList<>();

    // Resumen Financiero del Periodo
    private String nombreCuenta = "";
    private String etiquetaPeriodo = "";
    private Double saldoInicial = 0.0;
    private Double saldoFinal = 0.0;
    private Double depositos = 0.0;
    private Double retiros = 0.0;
    private Double pnlBruto = 0.0;
    private Double pnlNeto = 0.0;
    private Double comisiones = 0.0;
    private Double rendimientoPct = 0.0;
    private Integer totalTrades = 0;
    private Integer tradesGanadores = 0;
    private Integer tradesPerdedores = 0;
    private Double winRate = 0.0;

    private List<TradeReportDto> tradesList = new ArrayList<>();
    private boolean consultado = false;

    // Captura de Movimiento de Capital (Depósito / Retiro)
    private String nuevoTipo = "DEPOSITO";
    private Double nuevoMonto;
    private LocalDate nuevaFecha = LocalDate.now();
    private String nuevoConcepto = "";
    private String nuevoFolio = "";

    // Listado de Movimientos de Capital
    private List<MovimientoCapitalDto> movimientosCapitalList = new ArrayList<>();

    public String getNuevoTipo() { return nuevoTipo; }
    public void setNuevoTipo(String nuevoTipo) { this.nuevoTipo = nuevoTipo; }

    public Double getNuevoMonto() { return nuevoMonto; }
    public void setNuevoMonto(Double nuevoMonto) { this.nuevoMonto = nuevoMonto; }

    public LocalDate getNuevaFecha() { return nuevaFecha; }
    public void setNuevaFecha(LocalDate nuevaFecha) { this.nuevaFecha = nuevaFecha; }

    public String getNuevoConcepto() { return nuevoConcepto; }
    public void setNuevoConcepto(String nuevoConcepto) { this.nuevoConcepto = nuevoConcepto; }

    public String getNuevoFolio() { return nuevoFolio; }
    public void setNuevoFolio(String nuevoFolio) { this.nuevoFolio = nuevoFolio; }

    public List<MovimientoCapitalDto> getMovimientosCapitalList() { return movimientosCapitalList; }
    public void setMovimientosCapitalList(List<MovimientoCapitalDto> movimientosCapitalList) { this.movimientosCapitalList = movimientosCapitalList; }

    @PostConstruct
    public void init() {
        log.info("Inicializando ReportesBean...");
        LocalDate now = LocalDate.now();
        this.selectedAnio = now.getYear();
        this.selectedMes = now.getMonthValue();

        // Inicializar años disponibles
        this.aniosDisponibles.clear();
        for (int y = now.getYear() - 2; y <= now.getYear() + 1; y++) {
            this.aniosDisponibles.add(y);
        }

        // Inicializar meses
        this.mesesDisponibles.clear();
        this.mesesDisponibles.add(new MesDto(1, "Enero"));
        this.mesesDisponibles.add(new MesDto(2, "Febrero"));
        this.mesesDisponibles.add(new MesDto(3, "Marzo"));
        this.mesesDisponibles.add(new MesDto(4, "Abril"));
        this.mesesDisponibles.add(new MesDto(5, "Mayo"));
        this.mesesDisponibles.add(new MesDto(6, "Junio"));
        this.mesesDisponibles.add(new MesDto(7, "Julio"));
        this.mesesDisponibles.add(new MesDto(8, "Agosto"));
        this.mesesDisponibles.add(new MesDto(9, "Septiembre"));
        this.mesesDisponibles.add(new MesDto(10, "Octubre"));
        this.mesesDisponibles.add(new MesDto(11, "Noviembre"));
        this.mesesDisponibles.add(new MesDto(12, "Diciembre"));

        loadUserAccounts();

        if (this.selectedAccountId != null) {
            consultarEstadoCuenta();
        }
    }

    /**
     * Regla de Negocio:
     * - Si es Administrador: puede ver TODAS las cuentas del sistema (/api/v1/config/cuentas).
     * - Si es Usuario regular: SOLO puede ver sus cuentas asignadas (/api/v1/usuario-cuentas/{userId}).
     */
    public void loadUserAccounts() {
        this.userAccounts.clear();
        Integer userId = securityBean != null ? securityBean.getIdUsuario() : null;
        boolean esAdmin = securityBean != null && securityBean.isAdmin();

        if (userId == null) {
            userId = 1;
            esAdmin = true;
        }

        log.info("Cargando cuentas para Reportes. userId={}, esAdmin={}", userId, esAdmin);

        RestTemplate restTemplate = new RestTemplate();
        ObjectMapper mapper = new ObjectMapper();

        try {
            if (esAdmin) {
                // Administrador: ver todas las cuentas del sistema
                String urlAll = backendUrl + "/api/v1/config/cuentas";
                log.info("Usuario Administrador: Consultando todas las cuentas desde {}", urlAll);
                String respAll = restTemplate.getForObject(urlAll, String.class);
                if (respAll != null && !respAll.trim().isEmpty()) {
                    JsonNode root = mapper.readTree(respAll);
                    if (root.isArray()) {
                        for (JsonNode n : root) {
                            UserAccountDto dto = new UserAccountDto();
                            dto.setIdCuenta(n.path("idCuenta").asInt());
                            String name = n.has("nombre") ? n.path("nombre").asText() : n.path("nombreCuenta").asText();
                            dto.setNombreCuenta(name != null && !name.isEmpty() ? name : ("Cuenta #" + dto.getIdCuenta()));
                            dto.setCapital(n.path("capital").asDouble(0.0));
                            dto.setActivo(n.path("activo").asBoolean(true));
                            this.userAccounts.add(dto);
                        }
                    }
                }
            } else {
                // Usuario regular: ver solo sus cuentas asignadas
                String urlUser = backendUrl + "/api/v1/usuario-cuentas/" + userId;
                log.info("Usuario Regular: Consultando cuentas asignadas desde {}", urlUser);
                String respUser = restTemplate.getForObject(urlUser, String.class);
                if (respUser != null && !respUser.trim().isEmpty()) {
                    JsonNode root = mapper.readTree(respUser);
                    if (root.isArray()) {
                        for (JsonNode n : root) {
                            UserAccountDto dto = new UserAccountDto();
                            dto.setIdCuenta(n.path("idCuenta").asInt());
                            String name = n.has("nombreCuenta") ? n.path("nombreCuenta").asText() : n.path("nombre").asText();
                            dto.setNombreCuenta(name != null && !name.isEmpty() ? name : ("Cuenta #" + dto.getIdCuenta()));
                            dto.setCapital(n.path("capital").asDouble(0.0));
                            dto.setActivo(n.path("activo").asBoolean(true));
                            this.userAccounts.add(dto);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error al cargar cuentas para reportes: {}", e.getMessage(), e);
        }

        log.info("Total cuentas cargadas para el usuario: {}", this.userAccounts.size());

        // Selección por defecto
        if (!this.userAccounts.isEmpty()) {
            boolean found = false;
            if (this.selectedAccountId != null) {
                for (UserAccountDto a : this.userAccounts) {
                    if (a.getIdCuenta().equals(this.selectedAccountId)) {
                        found = true;
                        break;
                    }
                }
            }
            if (!found) {
                // Preferir Cuenta #2 si está disponible para el usuario, de lo contrario la primera
                UserAccountDto defaultAcc = this.userAccounts.stream()
                        .filter(a -> a.getIdCuenta() == 2)
                        .findFirst()
                        .orElse(this.userAccounts.get(0));
                this.selectedAccountId = defaultAcc.getIdCuenta();
            }
            log.info("Cuenta seleccionada por defecto en Reportes: idCuenta={}", this.selectedAccountId);
        } else {
            this.selectedAccountId = null;
        }
    }

    public void consultarEstadoCuenta() {
        if (selectedAccountId == null) {
            log.warn("No se puede consultar estado de cuenta: selectedAccountId es null.");
            return;
        }
        try {
            RestTemplate restTemplate = new RestTemplate();
            String url = String.format("%s/api/v1/reportes/estado-cuenta/%d?anio=%d&mes=%d",
                    backendUrl, selectedAccountId, selectedAnio, selectedMes);

            log.info("Consultando estado de cuenta mensual desde backend: {}", url);
            String response = restTemplate.getForObject(url, String.class);
            if (response != null) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(response);
                JsonNode data = root.path("data");

                JsonNode cta = data.path("cuenta");
                this.nombreCuenta = cta.path("Nombre").asText("Cuenta #" + selectedAccountId);

                JsonNode per = data.path("periodo");
                this.etiquetaPeriodo = per.path("etiquetaPeriodo").asText();

                JsonNode res = data.path("resumenFinanciero");
                this.saldoInicial = res.path("saldoInicial").asDouble(0.0);
                this.saldoFinal = res.path("saldoFinal").asDouble(0.0);
                this.depositos = res.path("depositos").asDouble(0.0);
                this.retiros = res.path("retiros").asDouble(0.0);
                this.pnlBruto = res.path("pnlBruto").asDouble(0.0);
                this.pnlNeto = res.path("pnlNeto").asDouble(0.0);
                this.comisiones = res.path("comisiones").asDouble(0.0);
                this.rendimientoPct = res.path("rendimientoPct").asDouble(0.0);
                this.totalTrades = res.path("totalTrades").asInt(0);
                this.tradesGanadores = res.path("tradesGanadores").asInt(0);
                this.tradesPerdedores = res.path("tradesPerdedores").asInt(0);
                this.winRate = res.path("winRate").asDouble(0.0);

                this.movimientosCapitalList.clear();
                JsonNode movsNode = data.path("movimientosCapital");
                if (movsNode.isArray()) {
                    for (JsonNode m : movsNode) {
                        MovimientoCapitalDto mc = new MovimientoCapitalDto();
                        mc.setIdTrade(m.path("idTrade").asInt());
                        mc.setTipo(m.path("strategy").asText());
                        mc.setConcepto(m.path("setup").asText("-"));
                        mc.setMonto(Math.abs(m.path("pnl").asDouble()));
                        mc.setPnl(m.path("pnl").asDouble());
                        mc.setFecha(m.path("closeTime").asText());
                        mc.setFolio(m.path("ticketId").asText("-"));
                        this.movimientosCapitalList.add(mc);
                    }
                }

                this.tradesList.clear();
                JsonNode tradesNode = data.path("trades");
                if (tradesNode.isArray()) {
                    for (JsonNode t : tradesNode) {
                        TradeReportDto tr = new TradeReportDto();
                        tr.setIdTrade(t.path("idTrade").asInt());
                        tr.setSetup(t.path("setup").asText());
                        tr.setSymbol(t.path("symbol").asText());
                        tr.setDirection(t.path("direction").asText());
                        tr.setSize(t.path("size").asDouble());
                        tr.setEntryPrice(t.path("entryPrice").asDouble());
                        tr.setExitPrice(t.path("exitPrice").asDouble());
                        tr.setPnl(t.path("pnl").asDouble());
                        tr.setCommission(t.path("commission").asDouble());
                        tr.setCloseTime(t.path("closeTime").asText());
                        tr.setTicketId(t.path("ticketId").asText("-"));
                        this.tradesList.add(tr);
                    }
                }
                this.consultado = true;
                log.info("Estado de cuenta cargado con éxito: {} trades, Saldo Inicial={}, Saldo Final={}",
                        this.tradesList.size(), this.saldoInicial, this.saldoFinal);
            }
        } catch (Exception e) {
            log.error("Error al consultar estado de cuenta mensual: {}", e.getMessage(), e);
        }
    }

    public StreamedContent getPdfFile() {
        return descargarPdf();
    }

    public StreamedContent descargarPdf() {
        if (selectedAccountId == null) {
            return null;
        }
        try {
            String url = String.format("%s/api/v1/reportes/estado-cuenta/%d/pdf?anio=%d&mes=%d",
                    backendUrl, selectedAccountId, selectedAnio, selectedMes);

            log.info("Descargando PDF de estado de cuenta desde: {}", url);
            RestTemplate restTemplate = new RestTemplate();
            byte[] pdfBytes = restTemplate.getForObject(url, byte[].class);

            if (pdfBytes == null || pdfBytes.length == 0) {
                log.warn("El backend retornó 0 bytes para el PDF de estado de cuenta.");
                return null;
            }

            String filename = String.format("Estado_Cuenta_%d_%d_%02d.pdf", selectedAccountId, selectedAnio, selectedMes);

            return DefaultStreamedContent.builder()
                    .name(filename)
                    .contentType("application/pdf")
                    .stream(() -> new ByteArrayInputStream(pdfBytes))
                    .build();
        } catch (Exception e) {
            log.error("Error al descargar PDF de estado de cuenta: {}", e.getMessage(), e);
            return null;
        }
    }

    public void registrarMovimiento() {
        if (selectedAccountId == null) {
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_WARN,
                            "Cuenta requerida", "Por favor seleccione una cuenta de trading."));
            return;
        }
        if (nuevoMonto == null || nuevoMonto <= 0) {
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_WARN,
                            "Monto inválido", "El monto debe ser un valor numérico positivo mayor a cero."));
            return;
        }

        try {
            String url = backendUrl + "/api/v1/cuentas/" + selectedAccountId + "/movimientos";
            RestTemplate restTemplate = new RestTemplate();
            org.springframework.http.HttpHeaders headers = new org.springframework.http.HttpHeaders();
            headers.setContentType(org.springframework.http.MediaType.APPLICATION_JSON);

            java.util.Map<String, Object> reqBody = new java.util.HashMap<>();
            reqBody.put("tipo", nuevoTipo);
            reqBody.put("monto", nuevoMonto);
            if (nuevaFecha != null) {
                reqBody.put("fecha", nuevaFecha.toString() + " 12:00:00");
            }
            reqBody.put("concepto", nuevoConcepto != null ? nuevoConcepto.trim() : "");
            reqBody.put("folio", nuevoFolio != null ? nuevoFolio.trim() : "");

            org.springframework.http.HttpEntity<java.util.Map<String, Object>> entity =
                    new org.springframework.http.HttpEntity<>(reqBody, headers);

            org.springframework.http.ResponseEntity<String> response =
                    restTemplate.postForEntity(url, entity, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                String tipoLabel = "DEPOSITO".equalsIgnoreCase(nuevoTipo) ? "Depósito" : "Retiro";
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_INFO,
                                "Movimiento Registrado", tipoLabel + " de $" + String.format("%,.2f", nuevoMonto) + " USD aplicado exitosamente."));

                this.nuevoMonto = null;
                this.nuevoConcepto = "";
                this.nuevoFolio = "";
                this.nuevaFecha = LocalDate.now();

                loadUserAccounts();
                consultarEstadoCuenta();

                org.primefaces.PrimeFaces.current().executeScript("PF('dlgMovimiento').hide();");
            } else {
                javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                        new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                                "Error", "No se pudo registrar el movimiento: " + response.getBody()));
            }
        } catch (Exception e) {
            log.error("Error al registrar movimiento de capital: {}", e.getMessage(), e);
            javax.faces.context.FacesContext.getCurrentInstance().addMessage(null,
                    new javax.faces.application.FacesMessage(javax.faces.application.FacesMessage.SEVERITY_ERROR,
                            "Error", "Ocurrió un error al registrar el movimiento: " + e.getMessage()));
        }
    }

    public static class MovimientoCapitalDto implements Serializable {
        private Integer idTrade;
        private String tipo = "";
        private String concepto = "";
        private Double monto = 0.0;
        private Double pnl = 0.0;
        private String fecha = "";
        private String folio = "";

        public Integer getIdTrade() { return idTrade; }
        public void setIdTrade(Integer idTrade) { this.idTrade = idTrade; }
        public String getTipo() { return tipo; }
        public void setTipo(String tipo) { this.tipo = tipo; }
        public String getConcepto() { return concepto; }
        public void setConcepto(String concepto) { this.concepto = concepto; }
        public Double getMonto() { return monto; }
        public void setMonto(Double monto) { this.monto = monto; }
        public Double getPnl() { return pnl; }
        public void setPnl(Double pnl) { this.pnl = pnl; }
        public String getFecha() { return fecha; }
        public void setFecha(String fecha) { this.fecha = fecha; }
        public String getFolio() { return folio; }
        public void setFolio(String folio) { this.folio = folio; }
    }

    // DTOs Auxiliares
    public static class MesDto implements Serializable {
        private int id;
        private String nombre;

        public MesDto(int id, String nombre) {
            this.id = id;
            this.nombre = nombre;
        }

        public int getId() { return id; }
        public void setId(int id) { this.id = id; }
        public String getNombre() { return nombre; }
        public void setNombre(String nombre) { this.nombre = nombre; }
    }

    public static class UserAccountDto implements Serializable {
        private Integer idCuenta;
        private String nombreCuenta = "";
        private Double capital = 0.0;
        private Boolean activo = true;

        public Integer getIdCuenta() { return idCuenta; }
        public void setIdCuenta(Integer idCuenta) { this.idCuenta = idCuenta; }
        public String getNombreCuenta() { return nombreCuenta; }
        public void setNombreCuenta(String nombreCuenta) { this.nombreCuenta = nombreCuenta; }
        public Double getCapital() { return capital; }
        public void setCapital(Double capital) { this.capital = capital; }
        public Boolean getActivo() { return activo; }
        public void setActivo(Boolean activo) { this.activo = activo; }
    }

    public static class TradeReportDto implements Serializable {
        private Integer idTrade;
        private String setup = "";
        private String symbol = "";
        private String direction = "";
        private Double size = 0.0;
        private Double entryPrice = 0.0;
        private Double exitPrice = 0.0;
        private Double pnl = 0.0;
        private Double commission = 0.0;
        private String closeTime = "";
        private String ticketId = "";

        public Integer getIdTrade() { return idTrade; }
        public void setIdTrade(Integer idTrade) { this.idTrade = idTrade; }
        public String getSetup() { return setup; }
        public void setSetup(String setup) { this.setup = setup; }
        public String getSymbol() { return symbol; }
        public void setSymbol(String symbol) { this.symbol = symbol; }
        public String getDirection() { return direction; }
        public void setDirection(String direction) { this.direction = direction; }
        public Double getSize() { return size; }
        public void setSize(Double size) { this.size = size; }
        public Double getEntryPrice() { return entryPrice; }
        public void setEntryPrice(Double entryPrice) { this.entryPrice = entryPrice; }
        public Double getExitPrice() { return exitPrice; }
        public void setExitPrice(Double exitPrice) { this.exitPrice = exitPrice; }
        public Double getPnl() { return pnl; }
        public void setPnl(Double pnl) { this.pnl = pnl; }
        public Double getCommission() { return commission; }
        public void setCommission(Double commission) { this.commission = commission; }
        public String getCloseTime() { return closeTime; }
        public void setCloseTime(String closeTime) { this.closeTime = closeTime; }
        public String getTicketId() { return ticketId; }
        public void setTicketId(String ticketId) { this.ticketId = ticketId; }
    }
}
