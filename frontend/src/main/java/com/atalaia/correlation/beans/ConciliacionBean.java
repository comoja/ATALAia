package com.atalaia.correlation.beans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.faces.view.ViewScoped;
import javax.inject.Named;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@Named("conciliacionBean")
@ViewScoped
public class ConciliacionBean implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(ConciliacionBean.class);

    @Value("${backend.api.url:http://127.0.0.1:8004}")
    private String backendUrl;

    @javax.inject.Inject
    private SecurityBean securityBean;

    private boolean cargando = false;
    private boolean consultado = false;
    private String ultimaActualizacion = "";
    private boolean soloDiscrepancias = false;

    // Filtros
    private Integer filtroUsuarioId = null;

    // Resumen Global
    private KpiResumenDto kpiGlobal = new KpiResumenDto();

    // Jerarquía de Usuarios
    private List<UsuarioConciliacionDto> usuarios = new ArrayList<>();

    @PostConstruct
    public void init() {
        log.info("Inicializando ConciliacionBean...");
    }

    public void onFiltroUsuarioChange() {
        log.info("Cambio de usuario en filtro de conciliación: filtroUsuarioId={}", this.filtroUsuarioId);
        if (this.securityBean != null && this.securityBean.isAdmin()) {
            if (this.filtroUsuarioId != null && this.filtroUsuarioId > 0) {
                this.securityBean.setSelectedUserId(this.filtroUsuarioId);
            }
        }
        consultar();
    }

    public void consultar() {
        this.cargando = true;
        try {
            Integer targetUserId = null;
            if (this.securityBean != null) {
                if (!this.securityBean.isAdmin()) {
                    // Usuario regular: solo puede ver su propia conciliación
                    targetUserId = this.securityBean.getIdUsuario();
                    this.filtroUsuarioId = targetUserId;
                } else {
                    // Administrador: si filtroUsuarioId aún no está fijado, sincronizar con el seleccionado en cabecera
                    if (this.filtroUsuarioId == null && this.securityBean.getSelectedUserId() != null) {
                        this.filtroUsuarioId = this.securityBean.getSelectedUserId();
                    }
                    targetUserId = (this.filtroUsuarioId != null && this.filtroUsuarioId > 0) ? this.filtroUsuarioId : null;
                }
            } else if (this.filtroUsuarioId != null && this.filtroUsuarioId > 0) {
                targetUserId = this.filtroUsuarioId;
            }

            RestTemplate restTemplate = new RestTemplate();
            String url = backendUrl + "/api/v1/conciliacion/posiciones";
            if (targetUserId != null && targetUserId > 0) {
                url += "?idUsuario=" + targetUserId;
            }

            log.info("Consultando conciliación de posiciones en backend: {}", url);
            String response = restTemplate.getForObject(url, String.class);

            if (response != null) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(response);
                JsonNode dataNode = root.path("data");

                this.ultimaActualizacion = dataNode.path("timestamp").asText("");

                // Parsear KPIs Globales
                JsonNode kpiNode = dataNode.path("kpis");
                this.kpiGlobal = parseKpi(kpiNode);

                // Parsear Usuarios
                this.usuarios.clear();
                JsonNode usersArray = dataNode.path("usuarios");
                if (usersArray.isArray()) {
                    for (JsonNode uNode : usersArray) {
                        UsuarioConciliacionDto uDto = new UsuarioConciliacionDto();
                        uDto.setIdUsuario(uNode.path("idUsuario").asInt(0));
                        uDto.setNombre(uNode.path("nombre").asText("Sin Nombre"));
                        uDto.setEmail(uNode.path("email").asText(""));
                        uDto.setKpis(parseKpi(uNode.path("kpis")));

                        // Cuentas
                        JsonNode cArray = uNode.path("cuentas");
                        if (cArray.isArray()) {
                            for (JsonNode cNode : cArray) {
                                CuentaConciliacionDto cDto = new CuentaConciliacionDto();
                                cDto.setIdCuenta(cNode.path("idCuenta").asInt(0));
                                cDto.setNombreCuenta(cNode.path("nombreCuenta").asText(""));
                                cDto.setBrokerLogin(cNode.path("brokerLogin").asText(""));
                                cDto.setKpis(parseKpi(cNode.path("kpis")));

                                // Estrategias
                                JsonNode eArray = cNode.path("estrategias");
                                if (eArray.isArray()) {
                                    for (JsonNode eNode : eArray) {
                                        EstrategiaConciliacionDto eDto = new EstrategiaConciliacionDto();
                                        eDto.setEstrategia(eNode.path("estrategia").asText(""));
                                        eDto.setKpis(parseKpi(eNode.path("kpis")));

                                        // Símbolos
                                        JsonNode sArray = eNode.path("simbolos");
                                        if (sArray.isArray()) {
                                            for (JsonNode sNode : sArray) {
                                                SimboloConciliacionDto sDto = new SimboloConciliacionDto();
                                                sDto.setSymbol(sNode.path("symbol").asText(""));
                                                sDto.setKpis(parseKpi(sNode.path("kpis")));

                                                // Posiciones
                                                JsonNode pArray = sNode.path("posiciones");
                                                if (pArray.isArray()) {
                                                    for (JsonNode pNode : pArray) {
                                                        PosicionConciliacionDto pDto = new PosicionConciliacionDto();
                                                        pDto.setTicketId(pNode.path("ticketId").asText("-"));
                                                        pDto.setIdTrade(pNode.hasNonNull("idTrade") ? pNode.path("idTrade").asInt() : null);
                                                        pDto.setSymbol(pNode.path("symbol").asText(""));
                                                        pDto.setStatus(pNode.path("status").asText("MATCH"));
                                                        pDto.setDetalleDiferencia(pNode.path("detalleDiferencia").asText(""));
                                                        pDto.setDireccionAtalaia(pNode.path("direccionAtalaia").asText("-"));
                                                        pDto.setDireccionForex(pNode.path("direccionForex").asText("-"));
                                                        pDto.setLotesAtalaia(pNode.path("lotesAtalaia").asDouble(0.0));
                                                        pDto.setLotesForex(pNode.path("lotesForex").asDouble(0.0));
                                                        pDto.setPrecioAtalaia(pNode.path("precioAtalaia").asDouble(0.0));
                                                        pDto.setPrecioForex(pNode.path("precioForex").asDouble(0.0));
                                                        pDto.setFechaAtalaia(pNode.path("fechaAtalaia").asText("-"));
                                                        pDto.setFechaForex(pNode.path("fechaForex").asText("-"));
                                                        pDto.setLoginBroker(pNode.path("loginBroker").asText(""));
                                                        sDto.getPosiciones().add(pDto);
                                                    }
                                                }
                                                eDto.getSimbolos().add(sDto);
                                            }
                                        }
                                        cDto.getEstrategias().add(eDto);
                                    }
                                }
                                uDto.getCuentas().add(cDto);
                            }
                        }
                        this.usuarios.add(uDto);
                    }
                }
                this.consultado = true;
                log.info("Conciliación cargada exitosamente: {} usuarios auditados. Estado={}",
                        this.usuarios.size(), this.kpiGlobal.getEstadoGlobal());
            }
        } catch (Exception e) {
            log.error("Error al consultar conciliación de posiciones: {}", e.getMessage(), e);
        } finally {
            this.cargando = false;
        }
    }

    private KpiResumenDto parseKpi(JsonNode node) {
        KpiResumenDto kpi = new KpiResumenDto();
        if (node != null && !node.isMissingNode()) {
            kpi.setTotalAtalaia(node.path("totalAtalaia").asInt(0));
            kpi.setTotalForex(node.path("totalForex").asInt(0));
            kpi.setTotalConciliadas(node.path("totalConciliadas").asInt(0));
            kpi.setTotalHuerfanasAtalaia(node.path("totalHuerfanasAtalaia").asInt(0));
            kpi.setTotalHuerfanasForex(node.path("totalHuerfanasForex").asInt(0));
            kpi.setTotalDiferencias(node.path("totalDiferencias").asInt(0));
            kpi.setTotalCuentas(node.path("totalCuentas").asInt(0));
            kpi.setEstadoGlobal(node.path("estadoGlobal").asText(node.path("estado").asText("CONCILIADO")));
        }
        return kpi;
    }

    // Getters y Setters
    public boolean isCargando() { return cargando; }
    public void setCargando(boolean cargando) { this.cargando = cargando; }

    public boolean isConsultado() { return consultado; }
    public void setConsultado(boolean consultado) { this.consultado = consultado; }

    public String getUltimaActualizacion() { return ultimaActualizacion; }
    public void setUltimaActualizacion(String ultimaActualizacion) { this.ultimaActualizacion = ultimaActualizacion; }

    public boolean isSoloDiscrepancias() { return soloDiscrepancias; }
    public void setSoloDiscrepancias(boolean soloDiscrepancias) { this.soloDiscrepancias = soloDiscrepancias; }

    public Integer getFiltroUsuarioId() { return filtroUsuarioId; }
    public void setFiltroUsuarioId(Integer filtroUsuarioId) { this.filtroUsuarioId = filtroUsuarioId; }

    public KpiResumenDto getKpiGlobal() { return kpiGlobal; }
    public void setKpiGlobal(KpiResumenDto kpiGlobal) { this.kpiGlobal = kpiGlobal; }

    public List<UsuarioConciliacionDto> getUsuarios() {
        return usuarios;
    }
    public void setUsuarios(List<UsuarioConciliacionDto> usuarios) { this.usuarios = usuarios; }

    // ==========================================
    // DTOs INTERNOS ESTRUCTURADOS
    // ==========================================

    public static class KpiResumenDto implements Serializable {
        private int totalAtalaia = 0;
        private int totalForex = 0;
        private int totalConciliadas = 0;
        private int totalHuerfanasAtalaia = 0;
        private int totalHuerfanasForex = 0;
        private int totalDiferencias = 0;
        private int totalCuentas = 0;
        private String estadoGlobal = "CONCILIADO";

        public boolean isTieneDiscrepancias() {
            return totalHuerfanasAtalaia > 0 || totalHuerfanasForex > 0 || totalDiferencias > 0;
        }

        public int getTotalAtalaia() { return totalAtalaia; }
        public void setTotalAtalaia(int totalAtalaia) { this.totalAtalaia = totalAtalaia; }

        public int getTotalForex() { return totalForex; }
        public void setTotalForex(int totalForex) { this.totalForex = totalForex; }

        public int getTotalConciliadas() { return totalConciliadas; }
        public void setTotalConciliadas(int totalConciliadas) { this.totalConciliadas = totalConciliadas; }

        public int getTotalHuerfanasAtalaia() { return totalHuerfanasAtalaia; }
        public void setTotalHuerfanasAtalaia(int totalHuerfanasAtalaia) { this.totalHuerfanasAtalaia = totalHuerfanasAtalaia; }

        public int getTotalHuerfanasForex() { return totalHuerfanasForex; }
        public void setTotalHuerfanasForex(int totalHuerfanasForex) { this.totalHuerfanasForex = totalHuerfanasForex; }

        public int getTotalDiferencias() { return totalDiferencias; }
        public void setTotalDiferencias(int totalDiferencias) { this.totalDiferencias = totalDiferencias; }

        public int getTotalCuentas() { return totalCuentas; }
        public void setTotalCuentas(int totalCuentas) { this.totalCuentas = totalCuentas; }

        public String getEstadoGlobal() { return estadoGlobal; }
        public void setEstadoGlobal(String estadoGlobal) { this.estadoGlobal = estadoGlobal; }

        public String getEstado() { return estadoGlobal; }
        public void setEstado(String estado) { this.estadoGlobal = estado; }
    }

    public static class UsuarioConciliacionDto implements Serializable {
        private int idUsuario;
        private String nombre = "";
        private String email = "";
        private KpiResumenDto kpis = new KpiResumenDto();
        private List<CuentaConciliacionDto> cuentas = new ArrayList<>();

        public int getIdUsuario() { return idUsuario; }
        public void setIdUsuario(int idUsuario) { this.idUsuario = idUsuario; }

        public String getNombre() { return nombre; }
        public void setNombre(String nombre) { this.nombre = nombre; }

        public String getEmail() { return email; }
        public void setEmail(String email) { this.email = email; }

        public KpiResumenDto getKpis() { return kpis; }
        public void setKpis(KpiResumenDto kpis) { this.kpis = kpis; }

        public List<CuentaConciliacionDto> getCuentas() { return cuentas; }
        public void setCuentas(List<CuentaConciliacionDto> cuentas) { this.cuentas = cuentas; }
    }

    public static class CuentaConciliacionDto implements Serializable {
        private int idCuenta;
        private String nombreCuenta = "";
        private String brokerLogin = "";
        private KpiResumenDto kpis = new KpiResumenDto();
        private List<EstrategiaConciliacionDto> estrategias = new ArrayList<>();

        public int getIdCuenta() { return idCuenta; }
        public void setIdCuenta(int idCuenta) { this.idCuenta = idCuenta; }

        public String getNombreCuenta() { return nombreCuenta; }
        public void setNombreCuenta(String nombreCuenta) { this.nombreCuenta = nombreCuenta; }

        public String getBrokerLogin() { return brokerLogin; }
        public void setBrokerLogin(String brokerLogin) { this.brokerLogin = brokerLogin; }

        public KpiResumenDto getKpis() { return kpis; }
        public void setKpis(KpiResumenDto kpis) { this.kpis = kpis; }

        public List<EstrategiaConciliacionDto> getEstrategias() { return estrategias; }
        public void setEstrategias(List<EstrategiaConciliacionDto> estrategias) { this.estrategias = estrategias; }
    }

    public static class EstrategiaConciliacionDto implements Serializable {
        private String estrategia = "";
        private KpiResumenDto kpis = new KpiResumenDto();
        private List<SimboloConciliacionDto> simbolos = new ArrayList<>();

        public String getEstrategia() { return estrategia; }
        public void setEstrategia(String estrategia) { this.estrategia = estrategia; }

        public KpiResumenDto getKpis() { return kpis; }
        public void setKpis(KpiResumenDto kpis) { this.kpis = kpis; }

        public List<SimboloConciliacionDto> getSimbolos() { return simbolos; }
        public void setSimbolos(List<SimboloConciliacionDto> simbolos) { this.simbolos = simbolos; }
    }

    public static class SimboloConciliacionDto implements Serializable {
        private String symbol = "";
        private KpiResumenDto kpis = new KpiResumenDto();
        private List<PosicionConciliacionDto> posiciones = new ArrayList<>();

        public String getSymbol() { return symbol; }
        public void setSymbol(String symbol) { this.symbol = symbol; }

        public KpiResumenDto getKpis() { return kpis; }
        public void setKpis(KpiResumenDto kpis) { this.kpis = kpis; }

        public List<PosicionConciliacionDto> getPosiciones() { return posiciones; }
        public void setPosiciones(List<PosicionConciliacionDto> posiciones) { this.posiciones = posiciones; }
    }

    public static class PosicionConciliacionDto implements Serializable {
        private String ticketId = "";
        private Integer idTrade;
        private String symbol = "";
        private String status = "MATCH";
        private String detalleDiferencia = "";
        private String direccionAtalaia = "-";
        private String direccionForex = "-";
        private Double lotesAtalaia = 0.0;
        private Double lotesForex = 0.0;
        private Double precioAtalaia = 0.0;
        private Double precioForex = 0.0;
        private String fechaAtalaia = "-";
        private String fechaForex = "-";
        private String loginBroker = "";

        public String getTicketId() { return ticketId; }
        public void setTicketId(String ticketId) { this.ticketId = ticketId; }

        public Integer getIdTrade() { return idTrade; }
        public void setIdTrade(Integer idTrade) { this.idTrade = idTrade; }

        public String getSymbol() { return symbol; }
        public void setSymbol(String symbol) { this.symbol = symbol; }

        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }

        public String getDetalleDiferencia() { return detalleDiferencia; }
        public void setDetalleDiferencia(String detalleDiferencia) { this.detalleDiferencia = detalleDiferencia; }

        public String getDireccionAtalaia() { return direccionAtalaia; }
        public void setDireccionAtalaia(String direccionAtalaia) { this.direccionAtalaia = direccionAtalaia; }

        public String getDireccionForex() { return direccionForex; }
        public void setDireccionForex(String direccionForex) { this.direccionForex = direccionForex; }

        public Double getLotesAtalaia() { return lotesAtalaia; }
        public void setLotesAtalaia(Double lotesAtalaia) { this.lotesAtalaia = lotesAtalaia; }

        public Double getLotesForex() { return lotesForex; }
        public void setLotesForex(Double lotesForex) { this.lotesForex = lotesForex; }

        public Double getPrecioAtalaia() { return precioAtalaia; }
        public void setPrecioAtalaia(Double precioAtalaia) { this.precioAtalaia = precioAtalaia; }

        public Double getPrecioForex() { return precioForex; }
        public void setPrecioForex(Double precioForex) { this.precioForex = precioForex; }

        public String getFechaAtalaia() { return fechaAtalaia; }
        public void setFechaAtalaia(String fechaAtalaia) { this.fechaAtalaia = fechaAtalaia; }

        public String getFechaForex() { return fechaForex; }
        public void setFechaForex(String fechaForex) { this.fechaForex = fechaForex; }

        public String getLoginBroker() { return loginBroker; }
        public void setLoginBroker(String loginBroker) { this.loginBroker = loginBroker; }
    }
}

