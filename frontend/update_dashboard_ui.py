import re

dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Add CSS for Temporalidad and Días
css_to_add = """        /* Estilos compactos para Temporalidad y Días */
        .aether-tf-days-row {
            display: flex;
            gap: 16px;
            margin-bottom: 18px;
            align-items: flex-start;
        }

        .aether-tf-col {
            display: flex;
            flex-direction: column;
        }

        .aether-tf-col .ui-selectonemenu {
            width: 140px !important;
            box-sizing: border-box !important;
        }

        .aether-days-col {
            display: flex;
            flex-direction: column;
        }

        .aether-days-col .ui-inputnumber {
            width: 75px !important;
            display: inline-block !important;
        }

        .aether-days-col .ui-inputnumber input {
            width: 75px !important;
            text-align: center !important;
            box-sizing: border-box !important;
        }
"""

if '.aether-tf-days-row' not in content:
    content = content.replace('/* Estilos ajustados para Fechas', css_to_add + '\n        /* Estilos ajustados para Fechas')

# 2. Replace the HTML for Temporalidad and Días
old_tf_days_pattern = re.compile(
    r'<!-- Temporalidad y Días hacia atrás -->\s*<div class="aether-tf-days-row"[^>]*>[\s\S]*?</div>\s*</div>\s*</div>',
    re.MULTILINE
)

new_tf_days = """<!-- Temporalidad y Días hacia atrás (compactos) -->
                    <div class="aether-tf-days-row">
                        <div class="aether-tf-col">
                            <div class="control-label" style="margin-bottom: 6px;">Temporalidad</div>
                            <p:selectOneMenu id="timeframeSelect" value="#{dashboardBean.timeframe}" styleClass="aether-input">
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
                                           inputStyleClass="aether-input"
                                           autocomplete="off">
                                <p:ajax event="change" listener="#{dashboardBean.onDaysBackChange}" update="startDateInput" />
                            </p:inputNumber>
                        </div>
                    </div>"""

# Find and replace old tf_days
idx_tf = content.find('<!-- Temporalidad y Días hacia atrás -->')
if idx_tf != -1:
    idx_end_tf = content.find('</h:panelGroup>', idx_tf)
    if idx_end_tf != -1:
        content = content[:idx_tf] + new_tf_days + '\n                ' + content[idx_end_tf:]
        print("Replaced Temporalidad and Días section")

# 3. Remove KPI cards (Ratio sintetico, strike promedio, prima Call, prima put, ciclo ST)
idx_kpi = content.find('<!-- GRILLA DE KPIS HOLOGRÁFICOS (5 columnas) -->')
if idx_kpi != -1:
    idx_end_kpi = content.find('<!-- SEÑAL DIRECCIONAL DE ARBITRAJE -->', idx_kpi)
    if idx_end_kpi != -1:
        content = content[:idx_kpi] + content[idx_end_kpi:]
        print("Removed KPI cards section")

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(content)

print('dashboard.xhtml updated successfully!')
