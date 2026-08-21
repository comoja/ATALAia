import re

dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Replace the panel header and user selector with facet header
old_panel_block = """            <!-- COLUMNA IZQUIERDA: CONTROLES DE CALIBRACIÓN -->
            <p:panel header="Configuración" styleClass="aether-panel">
                
                <!-- SELECTOR DE USUARIO: solo visible para el nivel de administrador -->
                <h:panelGroup layout="block" styleClass="aether-user-selector-container" style="margin-bottom: 22px;" rendered="#{securityBean.admin}">
                    <p:selectOneMenu id="usuarioSelector"
                                     value="#{securityBean.selectedUserId}"
                                     styleClass="aether-input"
                                     style="width: 100%; box-sizing: border-box;">
                        <f:selectItems value="#{securityBean.usuariosCombo}"
                                       var="u"
                                       itemLabel="#{u.nombreCompleto}"
                                       itemValue="#{u.idUsuario}" />
                        <p:ajax process="@this" update="calibrationInputsPanel" listener="#{dashboardBean.onDenominadorChange}" />
                    </p:selectOneMenu>
                    <i class="pi pi-users input-icon"></i>

                </h:panelGroup>
                <!-- Contenedor Flex para Pares A y B lado a lado -->
                <div style="display: flex; gap: 10px; align-items: center; margin-bottom: 22px;">"""

new_panel_block = """            <!-- COLUMNA IZQUIERDA: CONTROLES DE CALIBRACIÓN -->
            <p:panel styleClass="aether-panel">
                <f:facet name="header">
                    <div style="display: flex; align-items: center; justify-content: space-between; width: 100%;">
                        <span style="font-weight: 700; color: var(--text-primary);">Configuración</span>
                        <h:panelGroup layout="block" rendered="#{securityBean.admin}" style="display: flex; align-items: center; gap: 6px;">
                            <i class="pi pi-user" style="font-size: 0.85em; color: #64748b;"></i>
                            <p:selectOneMenu id="usuarioSelector"
                                             value="#{securityBean.selectedUserId}"
                                             styleClass="aether-input"
                                             style="min-width: 135px; font-size: 0.85em;">
                                <f:selectItems value="#{securityBean.usuariosCombo}"
                                               var="u"
                                               itemLabel="#{u.nombreCompleto}"
                                               itemValue="#{u.idUsuario}" />
                                <p:ajax process="@this" update="calibrationInputsPanel operarContainerTop" listener="#{dashboardBean.onDenominadorChange}" />
                            </p:selectOneMenu>
                        </h:panelGroup>
                    </div>
                </f:facet>

                <!-- Control Operar (Generar Órdenes) - Arriba de Numerador y Denominador, solo visible si existe en BD -->
                <h:panelGroup id="operarContainerTop" layout="block" rendered="#{dashboardBean.ratioExistsInDb}" style="margin-bottom: 18px;">
                    <div style="padding: 10px 14px; background: rgba(241, 245, 249, 0.6); border: 1px solid #e2e8f0; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;">
                        <div style="display: flex; flex-direction: column;">
                            <span style="font-size: 0.85em; font-weight: 700; color: #1e293b;">Generar Órdenes (Operar)</span>
                            <span style="font-size: 0.72em; color: #64748b;">Habilita órdenes automáticas para este ratio</span>
                        </div>
                        <p:selectBooleanCheckbox id="operarSwitch" value="#{dashboardBean.operar}">
                            <p:ajax process="@this" update="calibrationInputsPanel :aetherForm:growl" listener="#{dashboardBean.onOperarToggle}" />
                        </p:selectBooleanCheckbox>
                    </div>
                </h:panelGroup>

                <!-- Contenedor Flex para Pares A y B lado a lado -->
                <div style="display: flex; gap: 10px; align-items: center; margin-bottom: 22px;">"""

if old_panel_block in code:
    code = code.replace(old_panel_block, new_panel_block)
    print("Header facet with user selector and top operar control applied!")
else:
    print("Warning: old_panel_block not matched exactly, checking regex...")
    # fallback with regex
    code = re.sub(
        r'<!-- COLUMNA IZQUIERDA: CONTROLES DE CALIBRACIÓN -->\s*<p:panel header="Configuración" styleClass="aether-panel">[\s\S]*?<!-- Contenedor Flex para Pares A y B lado a lado -->\s*<div style="display: flex; gap: 10px; align-items: center; margin-bottom: 22px;">',
        new_panel_block,
        code
    )
    print("Applied regex replacement for header & top operar!")

# Remove lower operarContainer from calibrationInputsPanel
lower_operar_block = """                    <!-- Control Operar (Generar Órdenes) - Solo visible si existe en BD -->
                    <h:panelGroup id="operarContainer" layout="block" rendered="#{dashboardBean.ratioExistsInDb}">
                        <div style="margin-top: 14px; padding: 10px 14px; background: rgba(241, 245, 249, 0.6); border: 1px solid #e2e8f0; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;">
                            <div style="display: flex; flex-direction: column;">
                                <span style="font-size: 0.85em; font-weight: 700; color: #1e293b;">Generar Órdenes (Operar)</span>
                                <span style="font-size: 0.72em; color: #64748b;">Habilita órdenes automáticas para este ratio</span>
                            </div>
                            <p:selectBooleanCheckbox id="operarSwitch" value="#{dashboardBean.operar}">
                                <p:ajax process="@this" update="calibrationInputsPanel :aetherForm:growl" listener="#{dashboardBean.onOperarToggle}" />
                            </p:selectBooleanCheckbox>
                        </div>
                    </h:panelGroup>"""

if lower_operar_block in code:
    code = code.replace(lower_operar_block, '')
    print("Removed lower operarContainer from calibrationInputsPanel")

# Update symbolSelect and symbolSelect2 updates to include operarContainerTop
code = code.replace(
    'update="symbolSelect2 calibrationInputsPanel"',
    'update="symbolSelect2 calibrationInputsPanel operarContainerTop"'
)
code = code.replace(
    'update="calibrationInputsPanel" listener="#{dashboardBean.onDenominadorChange}"',
    'update="calibrationInputsPanel operarContainerTop" listener="#{dashboardBean.onDenominadorChange}"'
)

# Update buttons
code = code.replace(
    'action="#{dashboardBean.guardarRatio}"\n                                         update="calibrationInputsPanel :aetherForm:growl"',
    'action="#{dashboardBean.guardarRatio}"\n                                         update="calibrationInputsPanel operarContainerTop :aetherForm:growl"'
)
code = code.replace(
    'action="#{dashboardBean.borrarRatio}"\n                                         update="calibrationInputsPanel :aetherForm:growl"',
    'action="#{dashboardBean.borrarRatio}"\n                                         update="calibrationInputsPanel operarContainerTop :aetherForm:growl"'
)

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("dashboard.xhtml updated successfully with new layout!")
