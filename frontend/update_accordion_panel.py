import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Add UserRatioDto class and list fields
dto_code = '''    public static class UserRatioDto implements java.io.Serializable {
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
'''

if 'public static class UserRatioDto' not in code:
    code = code.replace('private List<RatioSymbolDto> availablePairs = new ArrayList<>();', 'private List<RatioSymbolDto> availablePairs = new ArrayList<>();\n' + dto_code)
    print("Added UserRatioDto and fields to DashboardBean.java")

# Add loadUserRatiosList and onSelectUserRatio methods
methods_code = '''    public void loadUserRatiosList() {
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
        } catch (Exception e) {
            log.error("Error al cargar lista de user_ratios: {}", e.getMessage());
        }
    }

    public void onSelectUserRatio(UserRatioDto ratio) {
        if (ratio == null) return;
        log.info("Seleccionado ratio desde acordeón: {} / {} (periodo={}, dias={}, fast={}, slow={}, operar={})",
                ratio.getNumerador(), ratio.getDenominador(), ratio.getPeriodo(), ratio.getDias(), ratio.getEmaRapida(), ratio.getEmaLenta(), ratio.getOperar());
        this.selectedUserRatio = ratio;
        this.selectedPair = ratio.getNumerador();
        updateCompatibleDenominators();
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
'''

if 'public void loadUserRatiosList()' not in code:
    code = code.replace('public void fetchUserRatioDetails() {', methods_code + '\n    public void fetchUserRatioDetails() {')
    print("Added loadUserRatiosList and onSelectUserRatio methods to DashboardBean.java")

# Ensure loadUserRatiosList() is called in init()
if 'loadUserRatiosList();' not in code:
    code = code.replace('loadCatalogo();\n        fetchUserRatioDetails();', 'loadCatalogo();\n        loadUserRatiosList();\n        fetchUserRatioDetails();')

# Call loadUserRatiosList() in guardarRatio() and borrarRatio()
code = code.replace(
    'this.ratioExistsInDb = true;\n                String operarDesc =',
    'this.ratioExistsInDb = true;\n                loadUserRatiosList();\n                String operarDesc ='
)
code = code.replace(
    'this.ratioExistsInDb = false;\n                this.operar = false;\n                javax.faces.context.FacesContext',
    'this.ratioExistsInDb = false;\n                this.operar = false;\n                loadUserRatiosList();\n                javax.faces.context.FacesContext'
)

# Call loadUserRatiosList() in onDenominadorChange()
code = code.replace(
    'fetchUserRatioDetails();\n        analyzePair();',
    'loadUserRatiosList();\n        fetchUserRatioDetails();\n        analyzePair();'
)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("DashboardBean.java updated successfully!")

# 2. Update dashboard.xhtml
dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Update grid width to 360px
dash = dash.replace('grid-template-columns: 320px 1fr;', 'grid-template-columns: 360px 1fr;')

# Replace left panel content with accordionPanel
panel_content_pattern = r'(<!-- COLUMNA IZQUIERDA: CONTROLES DE CALIBRACIÓN -->\s*<p:panel styleClass="aether-panel">\s*<f:facet name="header">[\s\S]*?</f:facet>)([\s\S]*?)(<h:outputText value="#\{dashboardBean\.analysisResult\}"[\s\S]*?</p:panel>)'

replacement_content = r'''\1
                <p:accordionPanel id="leftAccordion" multiple="true" activeIndex="0,1" styleClass="aether-accordion" style="margin-top: 4px;">
                    
                    <!-- PARTE 1: RATIOS GUARDADOS DEL USUARIO -->
                    <p:tab title="1. Ratios del Usuario">
                        <p:dataTable id="userRatiosTable"
                                     value="#{dashboardBean.userRatiosList}"
                                     var="r"
                                     emptyMessage="No hay ratios registrados"
                                     rowKey="#{r.id}"
                                     styleClass="aether-table-compact"
                                     style="width: 100%; margin-bottom: 6px;">
                            <p:column headerText="Ratio" style="font-weight: 700;">
                                <h:outputText value="#{r.numerador} / #{r.denominador}" style="font-size: 1.2rem;" />
                            </p:column>
                            <p:column headerText="TF" style="width: 36px; text-align: center;">
                                <h:outputText value="#{r.periodo}" style="font-size: 1.15rem;" />
                            </p:column>
                            <p:column headerText="EMAs" style="width: 48px; text-align: center;">
                                <h:outputText value="#{r.emaRapida}/#{r.emaLenta}" style="font-size: 1.15rem;" />
                            </p:column>
                            <p:column headerText="Op" style="width: 26px; text-align: center;">
                                <i class="#{r.operar ? 'pi pi-check-circle' : 'pi pi-times-circle'}" 
                                   style="color: #{r.operar ? '#10b981' : '#94a3b8'}; font-size: 1.15em;" 
                                   title="#{r.operar ? 'Operar: Activado' : 'Operar: Desactivado'}"></i>
                            </p:column>
                            <p:column style="width: 32px; text-align: center;">
                                <p:commandButton icon="pi pi-arrow-right"
                                                 action="#{dashboardBean.onSelectUserRatio(r)}"
                                                 update=":aetherForm"
                                                 oncomplete="renderAetherChart()"
                                                 styleClass="aether-btn-primary"
                                                 style="width: 26px !important; height: 26px !important; padding: 0 !important; border-radius: 4px !important;"
                                                 title="Cargar ratio a configuración" />
                            </p:column>
                        </p:dataTable>
                    </p:tab>

                    <!-- PARTE 2: CONFIGURACIÓN Y CALIBRACIÓN -->
                    <p:tab title="2. Calibración del Ratio">
                        <!-- Control Operar (Generar Órdenes) - Arriba de Numerador y Denominador, solo visible si existe en BD -->
                        <h:panelGroup id="operarContainerTop" layout="block" rendered="#{dashboardBean.ratioExistsInDb}" style="margin-bottom: 14px;">
                            <div style="padding: 8px 12px; background: rgba(241, 245, 249, 0.6); border: 1px solid #e2e8f0; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;">
                                <div style="display: flex; flex-direction: column;">
                                    <span style="font-size: 1.25rem; font-weight: 700; color: #1e293b;">Generar Órdenes (Operar)</span>
                                    <span style="font-size: 1.05rem; color: #64748b;">Habilita órdenes automáticas</span>
                                </div>
                                <p:selectBooleanCheckbox id="operarSwitch" value="#{dashboardBean.operar}">
                                    <p:ajax process="@this" update=":aetherForm:leftAccordion :aetherForm:growl" listener="#{dashboardBean.onOperarToggle}" />
                                </p:selectBooleanCheckbox>
                            </div>
                        </h:panelGroup>

                        <!-- Contenedor Flex para Pares A y B lado a lado -->
                        <div style="display: flex; gap: 10px; align-items: center; margin-bottom: 16px;">
                            <!-- Par A (Numerador) -->
                            <div style="flex: 1; min-width: 0;">
                                <div class="control-label">Numerador</div>
                                <p:selectOneMenu id="symbolSelect" value="#{dashboardBean.selectedPair}" styleClass="aether-input" style="width: 100%; box-sizing: border-box;">
                                    <f:selectItems value="#{dashboardBean.availablePairs}" var="p"
                                                   itemLabel="#{p.desc}" itemValue="#{p.pairName}" />
                                    <p:ajax process="@this" update="symbolSelect2 calibrationInputsPanel operarContainerTop" listener="#{dashboardBean.onNumeradorChange}" />
                                </p:selectOneMenu>
                            </div>

                            <!-- Divisor visual -->
                            <div class="ratio-divider" style="margin: 0; padding-top: 20px; flex-shrink: 0;">
                                <span class="ratio-formula-badge">÷</span>
                            </div>

                            <!-- Par B (Denominador) -->
                            <div style="flex: 1; min-width: 0;">
                                <div class="control-label">Denominador</div>
                                <p:selectOneMenu id="symbolSelect2" value="#{dashboardBean.selectedPair2}" var="p" styleClass="aether-input" style="width: 100%; box-sizing: border-box;">
                                    <f:selectItems value="#{dashboardBean.compatibleDenominators}" var="item"
                                                   itemLabel="#{item.desc}" itemValue="#{item.pairName}" />
                                    <p:column>
                                        <div style="display: block; width: 100%; border-radius: 4px; box-sizing: border-box; transition: all 0.2s ease;">
                                            <span style="color: #374151;">#{dashboardBean.findDtoByPairName(p).desc}</span>
                                        </div>
                                    </p:column>
                                    <p:ajax process="@this" update="calibrationInputsPanel operarContainerTop" listener="#{dashboardBean.onDenominadorChange}" />
                                </p:selectOneMenu>
                            </div>
                        </div>

                        <!-- EMA Rápida y EMA Lenta en el mismo renglón y Temporalidad -->
                        <h:panelGroup id="calibrationInputsPanel" layout="block" style="margin-bottom: 14px;">
                            <!-- Renglón con EMAs -->
                            <div class="aether-ema-row">
                                <div class="aether-ema-col">
                                    <div class="control-label" style="margin-bottom: 6px;">EMA Rápida</div>
                                    <p:inputNumber id="smaParam"
                                                   value="#{dashboardBean.smaPeriodParam}"
                                                   decimalPlaces="0"
                                                   minValue="1"
                                                   maxValue="999"
                                                   disabled="#{dashboardBean.operar}"
                                                   inputStyleClass="aether-input"
                                                   autocomplete="off">
                                        <p:ajax process="@this" update=":aetherForm:growl" />
                                    </p:inputNumber>
                                </div>
                                <div class="aether-ema-col">
                                    <div class="control-label" style="margin-bottom: 6px;">EMA Lenta</div>
                                    <p:inputNumber id="emaSlowParam"
                                                   value="#{dashboardBean.emaSlowPeriodParam}"
                                                   decimalPlaces="0"
                                                   minValue="1"
                                                   maxValue="999"
                                                   disabled="#{dashboardBean.operar}"
                                                   inputStyleClass="aether-input"
                                                   autocomplete="off">
                                        <p:ajax process="@this" update=":aetherForm:growl" />
                                    </p:inputNumber>
                                </div>
                            </div>
                            <!-- Temporalidad y Días hacia atrás (compactos) -->
                            <div class="aether-tf-days-row">
                                <div class="aether-tf-col">
                                    <div class="control-label" style="margin-bottom: 6px;">Temporalidad</div>
                                    <p:selectOneMenu id="timeframeSelect" value="#{dashboardBean.timeframe}" disabled="#{dashboardBean.operar}" styleClass="aether-input">
                                        <f:selectItem itemLabel="1 mes" itemValue="1month" />
                                        <f:selectItem itemLabel="1 Semana" itemValue="1week" />
                                        <f:selectItem itemLabel="1 Día" itemValue="1d" />
                                        <f:selectItem itemLabel="1 Hora (Default)" itemValue="1h" />
                                    </p:selectOneMenu>
                                </div>
                                <div class="aether-days-col">
                                    <div class="control-label" style="margin-bottom: 6px;">Días</div>
                                    <p:inputNumber id="daysBackParam"
                                                   value="#{dashboardBean.daysBack}"
                                                   decimalPlaces="0"
                                                   minValue="1"
                                                   maxValue="9999"
                                                   disabled="#{dashboardBean.operar}"
                                                   inputStyleClass="aether-input"
                                                   autocomplete="off">
                                        <p:ajax event="change" listener="#{dashboardBean.onDaysBackChange}" update="startDateInput" />
                                    </p:inputNumber>
                                </div>
                            </div>
                        </h:panelGroup>

                        <!-- Rango de Fechas -->
                        <h:panelGroup id="periodControls" layout="block" styleClass="aether-date-row">
                            <div class="aether-date-col">
                                <div class="control-label" style="margin-bottom: 6px;">Fecha Inicio</div>
                                <p:datePicker id="startDateInput"
                                              value="#{dashboardBean.startDate}"
                                              pattern="yyyy-MM-dd"
                                              showIcon="true"
                                              inputStyleClass="aether-input">
                                </p:datePicker>
                            </div>
                            <div class="aether-date-col">
                                <div class="control-label" style="margin-bottom: 6px;">Fecha Fin</div>
                                <p:datePicker id="endDateInput"
                                              value="#{dashboardBean.endDate}"
                                              pattern="yyyy-MM-dd"
                                              showIcon="true"
                                              inputStyleClass="aether-input">
                                </p:datePicker>
                            </div>
                        </h:panelGroup>

                        <!-- Botón de Ejecutar -->
                        <div style="margin-top: 20px; display: flex; flex-direction: column; gap: 10px;">
                            <p:commandButton value="Actualizar Calibración" 
                                             action="#{dashboardBean.analyzePair}" 
                                             update=":aetherForm" 
                                             oncomplete="renderAetherChart()"
                                             styleClass="aether-btn-primary" 
                                             style="width: 100%;" />

                            <div style="display: flex; gap: 10px; width: 100%;">
                                <p:commandButton value="Guardar Ratio"
                                                 action="#{dashboardBean.guardarRatio}"
                                                 update=":aetherForm:leftAccordion :aetherForm:growl"
                                                 styleClass="aether-btn-primary"
                                                 style="flex: 1;"
                                                 icon="pi pi-save" />

                                <p:commandButton value="Borrar Ratio"
                                                 action="#{dashboardBean.borrarRatio}"
                                                 update=":aetherForm:leftAccordion :aetherForm:growl"
                                                 styleClass="aether-btn-secondary"
                                                 style="flex: 1;"
                                                 icon="pi pi-trash" />
                            </div>
                        </div>
                    </p:tab>
                </p:accordionPanel>
\3'''

dash = re.sub(panel_content_pattern, replacement_content, dash)

# Update usuarioSelector ajax update
dash = dash.replace(
    'update="calibrationInputsPanel operarContainerTop" listener="#{dashboardBean.onDenominadorChange}"',
    'update=":aetherForm:leftAccordion" listener="#{dashboardBean.onDenominadorChange}"'
)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml updated with AccordionPanel in left panel!")
