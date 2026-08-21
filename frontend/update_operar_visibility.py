import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    bean_code = f.read()

# Add ratioExistsInDb property
if 'private Boolean ratioExistsInDb = false;' not in bean_code:
    target_prop = 'private Boolean operar = false;'
    replacement_prop = '''private Boolean operar = false;
    private Boolean ratioExistsInDb = false;

    public Boolean getRatioExistsInDb() {
        return ratioExistsInDb;
    }

    public void setRatioExistsInDb(Boolean ratioExistsInDb) {
        this.ratioExistsInDb = ratioExistsInDb;
    }'''
    bean_code = bean_code.replace(target_prop, replacement_prop)
    print("Added ratioExistsInDb property")

# Update guardarRatio: set ratioExistsInDb = true
bean_code = bean_code.replace(
    'if (response.getStatusCode().is2xxSuccessful()) {',
    'if (response.getStatusCode().is2xxSuccessful()) {\n                this.ratioExistsInDb = true;'
)

# Update borrarRatio: set ratioExistsInDb = false, operar = false
bean_code = bean_code.replace(
    'if (response.getStatusCode().is2xxSuccessful()) {',
    'if (response.getStatusCode().is2xxSuccessful()) {\n                this.ratioExistsInDb = false;\n                this.operar = false;'
)

# Update fetchUserRatioDetails:
bean_code = bean_code.replace(
    'if (rootNode.has("found") && rootNode.get("found").asBoolean()) {',
    'if (rootNode.has("found") && rootNode.get("found").asBoolean()) {\n                    this.ratioExistsInDb = true;'
)
bean_code = bean_code.replace(
    'log.info("ℹ️ No existe registro en BD para {}/{}. Aplicando valores por defecto.", selectedPair, selectedPair2);',
    'log.info("ℹ️ No existe registro en BD para {}/{}. Aplicando valores por defecto.", selectedPair, selectedPair2);\n                    this.ratioExistsInDb = false;'
)

# Update init() to fetch initial ratio details
if 'loadCatalogo();\n        fetchUserRatioDetails();' not in bean_code:
    bean_code = bean_code.replace('loadCatalogo();', 'loadCatalogo();\n        fetchUserRatioDetails();')
    print("Added fetchUserRatioDetails to init()")

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(bean_code)

print("DashboardBean.java updated with ratioExistsInDb!")

# 2. Update dashboard.xhtml
dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    dash_code = f.read()

# Wrap operarSwitch with rendered condition
old_operar_block = """                    <!-- Control Operar (Generar Órdenes) -->
                    <div style="margin-top: 14px; padding: 10px 14px; background: rgba(241, 245, 249, 0.6); border: 1px solid #e2e8f0; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;">
                        <div style="display: flex; flex-direction: column;">
                            <span style="font-size: 0.85em; font-weight: 700; color: #1e293b;">Generar Órdenes (Operar)</span>
                            <span style="font-size: 0.72em; color: #64748b;">Habilita órdenes automáticas para este ratio</span>
                        </div>
                        <p:selectBooleanCheckbox id="operarSwitch" value="#{dashboardBean.operar}">
                            <p:ajax process="@this" />
                        </p:selectBooleanCheckbox>
                    </div>"""

new_operar_block = """                    <!-- Control Operar (Generar Órdenes) - Solo visible si existe en BD -->
                    <h:panelGroup id="operarContainer" layout="block" rendered="#{dashboardBean.ratioExistsInDb}">
                        <div style="margin-top: 14px; padding: 10px 14px; background: rgba(241, 245, 249, 0.6); border: 1px solid #e2e8f0; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;">
                            <div style="display: flex; flex-direction: column;">
                                <span style="font-size: 0.85em; font-weight: 700; color: #1e293b;">Generar Órdenes (Operar)</span>
                                <span style="font-size: 0.72em; color: #64748b;">Habilita órdenes automáticas para este ratio</span>
                            </div>
                            <p:selectBooleanCheckbox id="operarSwitch" value="#{dashboardBean.operar}">
                                <p:ajax process="@this" />
                            </p:selectBooleanCheckbox>
                        </div>
                    </h:panelGroup>"""

if old_operar_block in dash_code:
    dash_code = dash_code.replace(old_operar_block, new_operar_block)
    print("Wrapped operarSwitch with rendered condition")

# Update commandButton updates to include calibrationInputsPanel
dash_code = dash_code.replace(
    'action="#{dashboardBean.guardarRatio}"\n                                         update=":aetherForm:growl"',
    'action="#{dashboardBean.guardarRatio}"\n                                         update="calibrationInputsPanel :aetherForm:growl"'
)
dash_code = dash_code.replace(
    'action="#{dashboardBean.borrarRatio}"\n                                         update=":aetherForm:growl"',
    'action="#{dashboardBean.borrarRatio}"\n                                         update="calibrationInputsPanel :aetherForm:growl"'
)

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(dash_code)

print("dashboard.xhtml updated successfully!")
