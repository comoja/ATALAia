package com.atalaia.correlation.beans;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.primefaces.model.DefaultStreamedContent;
import org.primefaces.model.DefaultTreeNode;
import org.primefaces.model.StreamedContent;
import org.primefaces.model.TreeNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import javax.faces.application.FacesMessage;
import javax.faces.context.FacesContext;
import javax.faces.model.SelectItem;
import javax.faces.view.ViewScoped;
import javax.inject.Named;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

@Named("reporteJerarquicoBean")
@ViewScoped
@Getter
@Setter
@Slf4j
public class ReporteJerarquicoBean implements Serializable {

    private static final long serialVersionUID = 1L;

    @javax.inject.Inject
    private SecurityBean securityBean;

    @Value("${atalaia.backend.url:http://127.0.0.1:8004}")
    private String backendUrl;

    // Filtros de Fecha
    private Date fechaInicio;
    private Date fechaFin;

    // Filtros Opcionales
    private Integer selectedUsuarioId;
    private Integer selectedAccountId;
    private String selectedEstrategia;

    private List<SelectItem> usuariosCombo = new ArrayList<>();
    private List<SelectItem> cuentasCombo = new ArrayList<>();
    private List<SelectItem> estrategiasCombo = new ArrayList<>();

    // Árbol Principal de PrimeFaces
    private TreeNode<?> rootNode;

    // KPIs Generales del Periodo
    private Double kpiPnlNeto = 0.0;
    private Double kpiPnlBruto = 0.0;
    private Double kpiComisiones = 0.0;
    private Double kpiWinRate = 0.0;
    private Double kpiVolumen = 0.0;
    private Double kpiTasaAnualReal = 0.0;
    private Double kpiTasaAnualEquivalente = 0.0;
    private Integer kpiTotalTrades = 0;
    private Integer kpiTradesGanadores = 0;
    private Integer kpiTradesPerdedores = 0;
    private Integer kpiTotalUsuarios = 0;
    private Integer kpiTotalCuentas = 0;

    @PostConstruct
    public void init() {
        log.info("Inicializando ReporteJerarquicoBean...");
        LocalDate now = LocalDate.now();
        LocalDate firstDay = now.withDayOfMonth(1);

        this.fechaInicio = Date.from(firstDay.atStartOfDay(ZoneId.systemDefault()).toInstant());
        this.fechaFin = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());

        cargarCombosFiltro();
        consultarReporte();
    }

    public void cargarCombosFiltro() {
        try {
            // Cuentas desde SecurityBean o base
            this.cuentasCombo.clear();
            this.cuentasCombo.add(new SelectItem(null, "Todas las Cuentas"));

            this.usuariosCombo.clear();
            this.usuariosCombo.add(new SelectItem(null, "Todos los Usuarios"));

            this.estrategiasCombo.clear();
            this.estrategiasCombo.add(new SelectItem(null, "Todas las Estrategias"));
            this.estrategiasCombo.add(new SelectItem("RIDER", "RIDER"));
            this.estrategiasCombo.add(new SelectItem("RATIO ATALAia", "RATIOS (ATALAia)"));
            this.estrategiasCombo.add(new SelectItem("SNIPER", "SNIPER (Order Block & ML)"));
            this.estrategiasCombo.add(new SelectItem("COBERTURA", "COBERTURA"));
            this.estrategiasCombo.add(new SelectItem("COBERTURA INICIAL", "COBERTURA INICIAL"));

            // Cargar usuarios si es administrador
            RestTemplate rt = new RestTemplate();
            try {
                String uUrl = backendUrl + "/api/v1/auth/usuarios";
                JsonNode uArr = rt.getForObject(uUrl, JsonNode.class);
                if (uArr != null && uArr.isArray()) {
                    for (JsonNode un : uArr) {
                        int uid = un.path("idUsuario").asInt();
                        String nom = un.path("nombreCompleto").asText(un.path("username").asText());
                        this.usuariosCombo.add(new SelectItem(uid, nom));
                    }
                }
            } catch (Exception eu) {
                log.debug("No se pudo cargar combo de usuarios: {}", eu.getMessage());
            }

            // Cargar cuentas
            try {
                String cUrl = backendUrl + "/api/v1/config/cuentas";
                JsonNode cArr = rt.getForObject(cUrl, JsonNode.class);
                if (cArr != null && cArr.isArray()) {
                    for (JsonNode cn : cArr) {
                        int cid = cn.path("idCuenta").asInt();
                        String nom = cn.hasNonNull("nombre") && !cn.path("nombre").asText().isEmpty()
                                     ? cn.path("nombre").asText()
                                     : cn.path("Nombre").asText("Cuenta #" + cid);
                        this.cuentasCombo.add(new SelectItem(cid, nom + " (#" + cid + ")"));
                    }
                }
            } catch (Exception ec) {
                log.debug("No se pudo cargar combo de cuentas: {}", ec.getMessage());
            }
        } catch (Exception e) {
            log.error("Error cargando combos de filtro: {}", e.getMessage());
        }
    }

    public void consultarReporte() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String sInicio = (this.fechaInicio != null) ? sdf.format(this.fechaInicio) : sdf.format(new Date());
            String sFin = (this.fechaFin != null) ? sdf.format(this.fechaFin) : sdf.format(new Date());

            StringBuilder urlSb = new StringBuilder(backendUrl)
                    .append("/api/v1/reportes/jerarquico?fechaInicio=").append(sInicio)
                    .append("&fechaFin=").append(sFin);

            if (this.selectedUsuarioId != null) {
                urlSb.append("&idUsuario=").append(this.selectedUsuarioId);
            }
            if (this.selectedAccountId != null) {
                urlSb.append("&idCuenta=").append(this.selectedAccountId);
            }
            if (this.selectedEstrategia != null && !this.selectedEstrategia.trim().isEmpty()) {
                urlSb.append("&estrategia=").append(java.net.URLEncoder.encode(this.selectedEstrategia, "UTF-8"));
            }

            log.info("Consultando reporte jerárquico desde: {}", urlSb);
            RestTemplate rt = new RestTemplate();
            String jsonResp = rt.getForObject(urlSb.toString(), String.class);

            if (jsonResp != null && !jsonResp.isEmpty()) {
                ObjectMapper mapper = new ObjectMapper();
                JsonNode root = mapper.readTree(jsonResp);
                JsonNode data = root.path("data");

                // Parsear Resumen General KPIs
                JsonNode rg = data.path("resumenGeneral");
                this.kpiTotalTrades = rg.path("totalTrades").asInt(0);
                this.kpiTradesGanadores = rg.path("ganadores").asInt(0);
                this.kpiTradesPerdedores = rg.path("perdedores").asInt(0);
                this.kpiWinRate = rg.path("winRate").asDouble(0.0);
                this.kpiVolumen = rg.path("volumen").asDouble(0.0);
                this.kpiPnlBruto = rg.path("pnlBruto").asDouble(0.0);
                this.kpiComisiones = rg.path("comisiones").asDouble(0.0);
                this.kpiPnlNeto = rg.path("pnlNeto").asDouble(0.0);
                double rgTae = rg.hasNonNull("tasaAnualEquivalente") ? rg.path("tasaAnualEquivalente").asDouble(0.0) : rg.path("tasaAnualReal").asDouble(0.0);
                this.kpiTasaAnualEquivalente = rgTae;
                this.kpiTasaAnualReal = rgTae;

                // Construir Árbol Jerárquico TreeNode
                this.rootNode = new DefaultTreeNode(new ReporteItemDto("ROOT", "Raíz", "", "", 0.0, 0.0, 0.0, "", "", "ACTIVE", 0, 0, 0, 0.0, 0.0, 0.0, 0.0), null);
                this.rootNode.setExpanded(true);

                JsonNode usuariosArr = data.path("usuarios");
                this.kpiTotalUsuarios = usuariosArr.size();
                int accCount = 0;

                for (JsonNode uNode : usuariosArr) {
                    ReporteItemDto uDto = new ReporteItemDto(
                            "USUARIO",
                            uNode.path("nombreUsuario").asText("Sin Asignar"),
                            "@" + uNode.path("username").asText(""),
                            "-",
                            uNode.path("volumen").asDouble(0.0),
                            0.0, 0.0, "-", "-",
                            "USUARIO",
                            uNode.path("totalTrades").asInt(0),
                            uNode.path("ganadores").asInt(0),
                            uNode.path("perdedores").asInt(0),
                            uNode.path("winRate").asDouble(0.0),
                            uNode.path("pnlBruto").asDouble(0.0),
                            uNode.path("comisiones").asDouble(0.0),
                            uNode.path("pnlNeto").asDouble(0.0)
                    );
                    double uTae = uNode.hasNonNull("tasaAnualEquivalente") ? uNode.path("tasaAnualEquivalente").asDouble(0.0) : uNode.path("tasaAnualReal").asDouble(0.0);
                    uDto.setTasaAnualEquivalente(uTae);
                    uDto.setTasaAnualReal(uTae);
                    TreeNode uTree = new DefaultTreeNode(uDto, this.rootNode);
                    uTree.setExpanded(true); // Usuario expandido por defecto

                    JsonNode cuentasArr = uNode.path("cuentas");
                    accCount += cuentasArr.size();

                    for (JsonNode cNode : cuentasArr) {
                        ReporteItemDto cDto = new ReporteItemDto(
                                "CUENTA",
                                cNode.path("nombreCuenta").asText(""),
                                cNode.path("brokerType").asText("FOREX.com"),
                                "-",
                                cNode.path("volumen").asDouble(0.0),
                                0.0, 0.0, "-", "-",
                                "CUENTA",
                                cNode.path("totalTrades").asInt(0),
                                cNode.path("ganadores").asInt(0),
                                cNode.path("perdedores").asInt(0),
                                cNode.path("winRate").asDouble(0.0),
                                cNode.path("pnlBruto").asDouble(0.0),
                                cNode.path("comisiones").asDouble(0.0),
                                cNode.path("pnlNeto").asDouble(0.0)
                        );
                        double cTae = cNode.hasNonNull("tasaAnualEquivalente") ? cNode.path("tasaAnualEquivalente").asDouble(0.0) : cNode.path("tasaAnualReal").asDouble(0.0);
                        cDto.setTasaAnualEquivalente(cTae);
                        cDto.setTasaAnualReal(cTae);
                        TreeNode cTree = new DefaultTreeNode(cDto, uTree);
                        cTree.setExpanded(true); // Cuenta expandida por defecto

                        JsonNode stratArr = cNode.path("estrategias");
                        for (JsonNode stNode : stratArr) {
                            String stratName = stNode.path("estrategia").asText("");
                            ReporteItemDto stDto = new ReporteItemDto(
                                    "ESTRATEGIA",
                                    stratName,
                                    "Estrategia Institucional",
                                    "-",
                                    stNode.path("volumen").asDouble(0.0),
                                    0.0, 0.0, "-", "-",
                                    "ESTRATEGIA",
                                    stNode.path("totalTrades").asInt(0),
                                    stNode.path("ganadores").asInt(0),
                                    stNode.path("perdedores").asInt(0),
                                    stNode.path("winRate").asDouble(0.0),
                                    stNode.path("pnlBruto").asDouble(0.0),
                                    stNode.path("comisiones").asDouble(0.0),
                                    stNode.path("pnlNeto").asDouble(0.0)
                            );
                            double stTae = stNode.hasNonNull("tasaAnualEquivalente") ? stNode.path("tasaAnualEquivalente").asDouble(0.0) : stNode.path("tasaAnualReal").asDouble(0.0);
                            stDto.setTasaAnualEquivalente(stTae);
                            stDto.setTasaAnualReal(stTae);
                            TreeNode stTree = new DefaultTreeNode(stDto, cTree);
                            stTree.setExpanded(false); // Estrategia colapsada por defecto para limpieza visual

                            JsonNode instArr = stNode.path("instrumentos");
                            for (JsonNode insNode : instArr) {
                                String symName = insNode.path("instrumento").asText("");
                                ReporteItemDto insDto = new ReporteItemDto(
                                        "INSTRUMENTO",
                                        symName,
                                        "Activo / Par",
                                        "-",
                                        insNode.path("volumen").asDouble(0.0),
                                        0.0, 0.0, "-", "-",
                                        "INSTRUMENTO",
                                        insNode.path("totalTrades").asInt(0),
                                        insNode.path("ganadores").asInt(0),
                                        insNode.path("perdedores").asInt(0),
                                        insNode.path("winRate").asDouble(0.0),
                                        insNode.path("pnlBruto").asDouble(0.0),
                                        insNode.path("comisiones").asDouble(0.0),
                                        insNode.path("pnlNeto").asDouble(0.0)
                                );
                                double insTae = insNode.hasNonNull("tasaAnualEquivalente") ? insNode.path("tasaAnualEquivalente").asDouble(0.0) : insNode.path("tasaAnualReal").asDouble(0.0);
                                insDto.setTasaAnualEquivalente(insTae);
                                insDto.setTasaAnualReal(insTae);
                                TreeNode insTree = new DefaultTreeNode(insDto, stTree);
                                insTree.setExpanded(false);

                                JsonNode hechosArr = insNode.path("hechos");
                                for (JsonNode hNode : hechosArr) {
                                    ReporteItemDto hDto = new ReporteItemDto(
                                            "HECHO",
                                            "Hecho #" + hNode.path("idTrade").asInt() + " (Ticket: " + hNode.path("ticketId").asText("-") + ")",
                                            hNode.path("status").asText("CLOSED"),
                                            hNode.path("direction").asText(""),
                                            hNode.path("size").asDouble(0.0),
                                            hNode.path("entryPrice").asDouble(0.0),
                                            hNode.path("exitPrice").asDouble(0.0),
                                            hNode.path("openTime").asText("-"),
                                            hNode.path("closeTime").asText("-"),
                                            hNode.path("status").asText("CLOSED"),
                                            1,
                                            (hNode.path("pnlNeto").asDouble(0.0) > 0 ? 1 : 0),
                                            (hNode.path("pnlNeto").asDouble(0.0) < 0 ? 1 : 0),
                                            (hNode.path("pnlNeto").asDouble(0.0) > 0 ? 100.0 : 0.0),
                                            hNode.path("pnlBruto").asDouble(0.0),
                                            hNode.path("commission").asDouble(0.0),
                                            hNode.path("pnlNeto").asDouble(0.0)
                                    );
                                    double hTae = hNode.hasNonNull("tasaAnualEquivalente") ? hNode.path("tasaAnualEquivalente").asDouble(0.0) : hNode.path("tasaAnualReal").asDouble(0.0);
                                    hDto.setTasaAnualEquivalente(hTae);
                                    hDto.setTasaAnualReal(hTae);
                                    new DefaultTreeNode(hDto, insTree);
                                }
                            }
                        }
                    }
                }
                this.kpiTotalCuentas = accCount;
            }
        } catch (Exception e) {
            log.error("Error al consultar reporte jerárquico: {}", e.getMessage(), e);
            FacesContext.getCurrentInstance().addMessage(null,
                    new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error en Reporte", "No se pudo obtener el reporte jerárquico: " + e.getMessage()));
        }
    }

    // =========================================================================
    // ACCIONES RÁPIDAS DE FILTRO POR PERIODO
    // =========================================================================
    public void filtrarHoy() {
        LocalDate now = LocalDate.now();
        this.fechaInicio = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        this.fechaFin = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        consultarReporte();
    }

    public void filtrarEstaSemana() {
        LocalDate now = LocalDate.now();
        LocalDate monday = now.with(java.time.temporal.TemporalAdjusters.previousOrSame(java.time.DayOfWeek.MONDAY));
        this.fechaInicio = Date.from(monday.atStartOfDay(ZoneId.systemDefault()).toInstant());
        this.fechaFin = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        consultarReporte();
    }

    public void filtrarEsteMes() {
        LocalDate now = LocalDate.now();
        LocalDate firstDay = now.withDayOfMonth(1);
        this.fechaInicio = Date.from(firstDay.atStartOfDay(ZoneId.systemDefault()).toInstant());
        this.fechaFin = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        consultarReporte();
    }

    public void filtrarMesAnterior() {
        LocalDate now = LocalDate.now();
        LocalDate prevMonth = now.minusMonths(1);
        LocalDate firstDay = prevMonth.withDayOfMonth(1);
        LocalDate lastDay = prevMonth.withDayOfMonth(prevMonth.lengthOfMonth());
        this.fechaInicio = Date.from(firstDay.atStartOfDay(ZoneId.systemDefault()).toInstant());
        this.fechaFin = Date.from(lastDay.atStartOfDay(ZoneId.systemDefault()).toInstant());
        consultarReporte();
    }

    public void filtrarUltimos30Dias() {
        LocalDate now = LocalDate.now();
        LocalDate d30 = now.minusDays(30);
        this.fechaInicio = Date.from(d30.atStartOfDay(ZoneId.systemDefault()).toInstant());
        this.fechaFin = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        consultarReporte();
    }

    public void filtrarAnioActual() {
        LocalDate now = LocalDate.now();
        LocalDate firstDayYear = now.withDayOfYear(1);
        this.fechaInicio = Date.from(firstDayYear.atStartOfDay(ZoneId.systemDefault()).toInstant());
        this.fechaFin = Date.from(now.atStartOfDay(ZoneId.systemDefault()).toInstant());
        consultarReporte();
    }

    // =========================================================================
    // EXPANDIR / COLAPSAR ÁRBOL
    // =========================================================================
    public void expandAll() {
        if (rootNode != null) {
            setNodeExpanded(rootNode, true);
        }
    }

    public void collapseAll() {
        if (rootNode != null) {
            setNodeExpanded(rootNode, false);
            rootNode.setExpanded(true); // Dejar root visible
            for (TreeNode<?> child : rootNode.getChildren()) {
                child.setExpanded(true); // Dejar nivel usuario abierto
            }
        }
    }

    private void setNodeExpanded(TreeNode<?> node, boolean expanded) {
        node.setExpanded(expanded);
        for (TreeNode<?> child : node.getChildren()) {
            setNodeExpanded(child, expanded);
        }
    }

    // =========================================================================
    // EXPORTACIÓN A EXCEL
    // =========================================================================
    public StreamedContent getExcelFile() {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            String sInicio = (this.fechaInicio != null) ? sdf.format(this.fechaInicio) : sdf.format(new Date());
            String sFin = (this.fechaFin != null) ? sdf.format(this.fechaFin) : sdf.format(new Date());

            StringBuilder urlSb = new StringBuilder(backendUrl)
                    .append("/api/v1/reportes/jerarquico/excel?fechaInicio=").append(sInicio)
                    .append("&fechaFin=").append(sFin);

            if (this.selectedUsuarioId != null) {
                urlSb.append("&idUsuario=").append(this.selectedUsuarioId);
            }
            if (this.selectedAccountId != null) {
                urlSb.append("&idCuenta=").append(this.selectedAccountId);
            }
            if (this.selectedEstrategia != null && !this.selectedEstrategia.trim().isEmpty()) {
                urlSb.append("&estrategia=").append(java.net.URLEncoder.encode(this.selectedEstrategia, "UTF-8"));
            }

            RestTemplate rt = new RestTemplate();
            byte[] bytes = rt.getForObject(urlSb.toString(), byte[].class);

            if (bytes != null) {
                InputStream is = new ByteArrayInputStream(bytes);
                String fileName = "Reporte_Jerarquico_" + sInicio + "_" + sFin + ".xlsx";
                return DefaultStreamedContent.builder()
                        .name(fileName)
                        .contentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                        .stream(() -> is)
                        .build();
            }
        } catch (Exception e) {
            log.error("Error descargando Excel jerárquico: {}", e.getMessage(), e);
            FacesContext.getCurrentInstance().addMessage(null,
                    new FacesMessage(FacesMessage.SEVERITY_ERROR, "Error Excel", "No se pudo generar el archivo Excel."));
        }
        return null;
    }

    // =========================================================================
    // GETTERS FORMATEADOS PARA KPIS CON COMAS DE MILES
    // =========================================================================
    public String getFormattedKpiPnlNeto() {
        String sign = (kpiPnlNeto != null && kpiPnlNeto >= 0) ? "+" : "";
        double val = (kpiPnlNeto != null) ? kpiPnlNeto : 0.0;
        return String.format(Locale.US, "%s$%,.2f", sign, val);
    }

    public String getFormattedKpiPnlBruto() {
        String sign = (kpiPnlBruto != null && kpiPnlBruto >= 0) ? "+" : "";
        double val = (kpiPnlBruto != null) ? kpiPnlBruto : 0.0;
        return String.format(Locale.US, "%s$%,.2f", sign, val);
    }

    public String getFormattedKpiComisiones() {
        double val = (kpiComisiones != null) ? kpiComisiones : 0.0;
        return String.format(Locale.US, "$%,.2f", val);
    }

    public String getFormattedKpiVolumen() {
        double val = (kpiVolumen != null) ? kpiVolumen : 0.0;
        if (val >= 1000) {
            return String.format(Locale.US, "%,.0f u", val);
        }
        return String.format(Locale.US, "%,.2f u", val);
    }

    public String getFormattedKpiTotalTrades() {
        int val = (kpiTotalTrades != null) ? kpiTotalTrades : 0;
        return String.format(Locale.US, "%,d ops", val);
    }

    public String getFormattedKpiTasaAnualEquivalente() {
        Double val = (kpiTasaAnualEquivalente != null) ? kpiTasaAnualEquivalente : (kpiTasaAnualReal != null ? kpiTasaAnualReal : 0.0);
        String sign = val >= 0 ? "+" : "";
        return String.format(Locale.US, "%s%.2f%%", sign, val);
    }

    public String getFormattedKpiTasaAnualReal() {
        return getFormattedKpiTasaAnualEquivalente();
    }

    public String getRowStyleClass(ReporteItemDto item) {
        if (item == null || item.getTipo() == null) {
            return "node-hecho";
        }
        return "node-" + item.getTipo().toLowerCase().trim();
    }

    // =========================================================================
    // DTO DEL ITEM JERÁRQUICO
    // =========================================================================
    @Getter
    @Setter
    public static class ReporteItemDto implements Serializable {
        private String tipo; // USUARIO, CUENTA, ESTRATEGIA, INSTRUMENTO, HECHO
        private String concepto;
        private String subtitulo;
        private String direccion;
        private Double volumen;
        private Double entryPrice;
        private Double exitPrice;
        private String openTime;
        private String closeTime;
        private String status;
        private Integer totalTrades;
        private Integer ganadores;
        private Integer perdedores;
        private Double winRate;
        private Double pnlBruto;
        private Double comisiones;
        private Double pnlNeto;
        private Double tasaAnualReal = 0.0;
        private Double tasaAnualEquivalente = 0.0;

        public ReporteItemDto(String tipo, String concepto, String subtitulo, String direccion,
                              Double volumen, Double entryPrice, Double exitPrice, String openTime, String closeTime,
                              String status, Integer totalTrades, Integer ganadores, Integer perdedores,
                              Double winRate, Double pnlBruto, Double comisiones, Double pnlNeto) {
            this.tipo = tipo;
            this.concepto = concepto;
            this.subtitulo = subtitulo;
            this.direccion = direccion;
            this.volumen = volumen != null ? volumen : 0.0;
            this.entryPrice = entryPrice != null ? entryPrice : 0.0;
            this.exitPrice = exitPrice != null ? exitPrice : 0.0;
            this.openTime = openTime != null ? openTime : "-";
            this.closeTime = closeTime != null ? closeTime : "-";
            this.status = status != null ? status : "-";
            this.totalTrades = totalTrades != null ? totalTrades : 0;
            this.ganadores = ganadores != null ? ganadores : 0;
            this.perdedores = perdedores != null ? perdedores : 0;
            this.winRate = winRate != null ? winRate : 0.0;
            this.pnlBruto = pnlBruto != null ? pnlBruto : 0.0;
            this.comisiones = comisiones != null ? comisiones : 0.0;
            this.pnlNeto = pnlNeto != null ? pnlNeto : 0.0;
        }

        public String getIcono() {
            switch (tipo) {
                case "USUARIO": return "pi pi-user";
                case "CUENTA": return "pi pi-wallet";
                case "ESTRATEGIA": return "pi pi-compass";
                case "INSTRUMENTO": return "pi pi-chart-line";
                case "HECHO": return "pi pi-tag";
                default: return "pi pi-circle-fill";
            }
        }

        public String getIconoColor() {
            switch (tipo) {
                case "USUARIO": return "#38bdf8"; // Celeste Neón
                case "CUENTA": return "#10b981";  // Esmeralda
                case "ESTRATEGIA": return "#f59e0b"; // Ámbar
                case "INSTRUMENTO": return "#a855f7"; // Púrpura
                case "HECHO": return "#64748b";   // Slate
                default: return "#94a3b8";
            }
        }

        public String getBadgeBgColor() {
            switch (tipo) {
                case "USUARIO": return "#e0f2fe";
                case "CUENTA": return "#ecfdf5";
                case "ESTRATEGIA": return "#fef3c7";
                case "INSTRUMENTO": return "#f3e8ff";
                case "HECHO": return "#f1f5f9";
                default: return "#f1f5f9";
            }
        }

        public String getBadgeTextColor() {
            switch (tipo) {
                case "USUARIO": return "#0284c7";
                case "CUENTA": return "#059669";
                case "ESTRATEGIA": return "#d97706";
                case "INSTRUMENTO": return "#7e22ce";
                case "HECHO": return "#475569";
                default: return "#475569";
            }
        }

        public String getPnlNetoColor() {
            return (pnlNeto >= 0) ? "#059669" : "#dc2626";
        }

        public String getPnlNetoBg() {
            return (pnlNeto >= 0) ? "#ecfdf5" : "#fef2f2";
        }

        public String getFormattedPnlNeto() {
            String sign = (pnlNeto >= 0) ? "+" : "";
            return String.format(Locale.US, "%s$%,.2f", sign, pnlNeto);
        }

        public String getFormattedPnlBruto() {
            String sign = (pnlBruto >= 0) ? "+" : "";
            return String.format(Locale.US, "%s$%,.2f", sign, pnlBruto);
        }

        public String getFormattedComisiones() {
            return String.format(Locale.US, "$%,.2f", comisiones);
        }

        public String getFormattedVolumen() {
            if (volumen >= 1000) {
                return String.format(Locale.US, "%,.0f u", volumen);
            }
            return String.format(Locale.US, "%,.2f u", volumen);
        }

        public String getFormattedTasaAnualEquivalente() {
            Double val = (tasaAnualEquivalente != null) ? tasaAnualEquivalente : (tasaAnualReal != null ? tasaAnualReal : 0.0);
            String sign = val >= 0 ? "+" : "";
            return String.format(Locale.US, "%s%.2f%%", sign, val);
        }

        public String getTasaAnualEquivalenteColor() {
            Double val = (tasaAnualEquivalente != null) ? tasaAnualEquivalente : (tasaAnualReal != null ? tasaAnualReal : 0.0);
            return val >= 0 ? "#059669" : "#dc2626";
        }

        public String getFormattedTasaAnualReal() {
            return getFormattedTasaAnualEquivalente();
        }

        public String getTasaAnualRealColor() {
            return getTasaAnualEquivalenteColor();
        }

        public boolean isTotal() {
            return "USUARIO".equalsIgnoreCase(tipo);
        }

        public boolean isSubtotal() {
            return "CUENTA".equalsIgnoreCase(tipo) || "ESTRATEGIA".equalsIgnoreCase(tipo) || "INSTRUMENTO".equalsIgnoreCase(tipo);
        }

        public boolean isHecho() {
            return "HECHO".equalsIgnoreCase(tipo);
        }
    }
}
