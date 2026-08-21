import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Add UserAccountDto class and properties
user_acc_dto = '''    public static class UserAccountDto implements java.io.Serializable {
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
'''

if 'public static class UserAccountDto' not in code:
    code = code.replace('public static class UserRatioDto', user_acc_dto + '\n    public static class UserRatioDto')
    print("Added UserAccountDto and fields to DashboardBean.java")

# Add loadUserAccounts and onCuentaChange methods
acc_methods = '''    public void loadUserAccounts() {
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
'''

if 'public void loadUserAccounts()' not in code:
    code = code.replace('public void onUsuarioChange() {', acc_methods + '\n    public void onUsuarioChange() {')
    print("Added loadUserAccounts and onCuentaChange to DashboardBean.java")

# Update onUsuarioChange to call loadUserAccounts() first
code = code.replace(
    'log.info("Usuario seleccionado cambiado en cabecera de configuración: idUsuario={}", userId);\n\n        loadUserRatiosList();',
    'log.info("Usuario seleccionado cambiado en cabecera de configuración: idUsuario={}", userId);\n        loadUserAccounts();\n        loadUserRatiosList();'
)

# Update loadUserRatiosList to use selectedAccountId
old_url_build = 'String url = backendUrl + "/api/v1/user-ratios/" + userId;'
new_url_build = 'String url = backendUrl + "/api/v1/user-ratios/" + userId + (selectedAccountId != null ? "?idCuenta=" + selectedAccountId : "");'
code = code.replace(old_url_build, new_url_build)

# Update guardarRatio to include idCuenta
old_payload_user = 'payload.put("idUsuario", userId);'
new_payload_user = 'payload.put("idUsuario", userId);\n            payload.put("idCuenta", selectedAccountId);'
code = code.replace(old_payload_user, new_payload_user)

# Update borrarRatio to include idCuenta
old_del_payload = 'payload.put("idUsuario", userId);\n            payload.put("numerador", selectedPair);'
new_del_payload = 'payload.put("idUsuario", userId);\n            payload.put("idCuenta", selectedAccountId);\n            payload.put("numerador", selectedPair);'
code = code.replace(old_del_payload, new_del_payload)

# Update fetchUserRatioDetails to include idCuenta
old_fetch_url = 'String url = String.format("%s/api/v1/user-ratios/buscar?idUsuario=%d&numerador=%s&denominador=%s",'
new_fetch_url = 'String url = String.format("%s/api/v1/user-ratios/buscar?idUsuario=%d&numerador=%s&denominador=%s" + (selectedAccountId != null ? "&idCuenta=" + selectedAccountId : ""),'
code = code.replace(old_fetch_url, new_fetch_url)

# Call loadUserAccounts() in init()
code = code.replace('loadCatalogo();\n        loadUserRatiosList();', 'loadCatalogo();\n        loadUserAccounts();\n        loadUserRatiosList();')

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("DashboardBean.java updated with account selector and idCuenta persistence!")

# 2. Update dashboard.xhtml
dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Add cuentaSelector right inside tabUserRatios above userRatiosTable
old_tab_body = """                    <!-- PARTE 1: RATIOS GUARDADOS DEL USUARIO -->
                    <p:tab id="tabUserRatios" title="1. Ratios del Usuario">
                        <p:dataTable id="userRatiosTable" """

new_tab_body = """                    <!-- PARTE 1: RATIOS GUARDADOS DEL USUARIO -->
                    <p:tab id="tabUserRatios" title="1. Ratios del Usuario">
                        <!-- Selector Discreto de Cuenta del Usuario -->
                        <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 8px; padding: 2px 2px; gap: 8px;">
                            <div style="display: flex; align-items: center; gap: 6px; flex: 1; min-width: 0;">
                                <i class="pi pi-wallet" style="font-size: 1.15em; color: #64748b;"></i>
                                <span style="font-size: 1.12rem; font-weight: 700; color: #475569; white-space: nowrap;">Cuenta:</span>
                                <p:selectOneMenu id="cuentaSelector"
                                                 value="#{dashboardBean.selectedAccountId}"
                                                 styleClass="aether-input"
                                                 style="flex: 1; min-width: 120px; height: 28px !important; line-height: 28px !important; font-size: 1.12rem !important;">
                                    <f:selectItems value="#{dashboardBean.userAccountsCombo}"
                                                   var="c"
                                                   itemLabel="#{c.nombreCuenta}"
                                                   itemValue="#{c.idCuenta}" />
                                    <p:ajax process="@this" update=":aetherForm" oncomplete="renderAetherChart()" listener="#{dashboardBean.onCuentaChange}" />
                                </p:selectOneMenu>
                            </div>
                        </div>

                        <p:dataTable id="userRatiosTable" """

dash = dash.replace(old_tab_body, new_tab_body)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("dashboard.xhtml updated with cuentaSelector!")
