import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

on_usuario_change_code = '''    public void onUsuarioChange() {
        Integer userId = null;
        if (securityBean != null) {
            userId = securityBean.getSelectedUserId() != null ? securityBean.getSelectedUserId() : securityBean.getIdUsuario();
        }
        log.info("Usuario seleccionado cambiado en cabecera de configuración: idUsuario={}", userId);

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
'''

if 'public void onUsuarioChange()' not in code:
    code = code.replace('public void onOperarToggle() {', on_usuario_change_code + '\n    public void onOperarToggle() {')
    print("Added onUsuarioChange method to DashboardBean.java")

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

# 2. Update dashboard.xhtml
dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_usuario_ajax = '<p:ajax process="@this" update=":aetherForm:mainTabView:leftAccordion" listener="#{dashboardBean.onDenominadorChange}" />'
new_usuario_ajax = '<p:ajax process="@this" update=":aetherForm" oncomplete="renderAetherChart()" listener="#{dashboardBean.onUsuarioChange}" />'

if old_usuario_ajax in dash:
    dash = dash.replace(old_usuario_ajax, new_usuario_ajax)
    print("Updated usuarioSelector ajax listener to onUsuarioChange in dashboard.xhtml!")

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("onUsuarioChange integrated successfully!")
