import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Add server-side primefaces ajax update in loadUserRatiosList
primefaces_update_code = '''            this.userRatiosList = list;
            log.info("Cargados {} ratios guardados para el usuario {}", list.size(), userId);
            try {
                if (org.primefaces.PrimeFaces.current() != null && org.primefaces.PrimeFaces.current().isAjaxRequest()) {
                    org.primefaces.PrimeFaces.current().ajax().update("aetherForm:mainTabView:leftAccordion:userRatiosTable", "aetherForm:mainTabView:leftAccordion");
                }
            } catch (Exception ignored) {}'''

code = code.replace(
    'this.userRatiosList = list;\n            log.info("Cargados {} ratios guardados para el usuario {}", list.size(), userId);',
    primefaces_update_code
)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with server-side ajax update for userRatiosTable!")

# 2. Update dashboard.xhtml
dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Ensure p:tab id is set on Section 1
dash = dash.replace(
    '<p:tab title="1. Ratios del Usuario">',
    '<p:tab id="tabUserRatios" title="1. Ratios del Usuario">'
)
dash = dash.replace(
    '<p:tab title="2. Calibración del Ratio">',
    '<p:tab id="tabCalibration" title="2. Calibración del Ratio">'
)

# In guardarRatio and borrarRatio buttons, specify update explicitly
dash = dash.replace(
    '<p:commandButton value="Guardar Ratio"\n                                                 action="#{dashboardBean.guardarRatio}"\n                                                 update=":aetherForm"',
    '<p:commandButton value="Guardar Ratio"\n                                                 action="#{dashboardBean.guardarRatio}"\n                                                 update=":aetherForm:mainTabView:leftAccordion:userRatiosTable :aetherForm:mainTabView:leftAccordion :aetherForm:growl"'
)
dash = dash.replace(
    '<p:commandButton value="Borrar Ratio"\n                                                 action="#{dashboardBean.borrarRatio}"\n                                                 update=":aetherForm"',
    '<p:commandButton value="Borrar Ratio"\n                                                 action="#{dashboardBean.borrarRatio}"\n                                                 update=":aetherForm:mainTabView:leftAccordion:userRatiosTable :aetherForm:mainTabView:leftAccordion :aetherForm:growl"'
)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with explicit target updates for userRatiosTable!")
