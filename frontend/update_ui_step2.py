import re

# 1. Update dashboard.xhtml
dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace signal box with PAIRS TRADING title box (Numerador - Denominador)
old_signal_box = """                <!-- SEÑAL DIRECCIONAL DE ARBITRAJE -->
                <h:panelGroup layout="block" styleClass="kpi-card signal-box signal-#{dashboardBean.arbitrageType.toLowerCase()}">
                    <div class="kpi-label signal-label">Señal de Arbitraje Actual (Pairs Trading)</div>
                    <div class="kpi-value signal-value">
                        <h:outputText id="arbitrageSignalVal" value="#{dashboardBean.arbitrageSignal}" />
                    </div>
                </h:panelGroup>"""

new_pairs_header = """                <!-- ENCABEZADO PAIRS TRADING (NUMERADOR / DENOMINADOR) -->
                <div class="pairs-trading-header-box" style="margin-bottom: 14px; padding: 12px 20px; background: var(--bg-surface); border: 1px solid var(--border-soft); border-radius: 12px; display: flex; align-items: center; justify-content: space-between; box-shadow: var(--neu-shadow-flat);">
                    <div style="font-size: 1.15em; font-weight: 700; color: var(--text-primary); letter-spacing: 0.5px;">
                        PAIRS TRADING
                    </div>
                    <div style="font-size: 1.25em; font-weight: 700; color: var(--color-primary-hover, #3D7A6E);">
                        <h:outputText id="pairsHeaderTitle" value="#{dashboardBean.selectedPair} - #{dashboardBean.selectedPair2}" />
                    </div>
                </div>"""

if old_signal_box in content:
    content = content.replace(old_signal_box, new_pairs_header)
    print("Replaced signal box with PAIRS TRADING header box")
else:
    print("Old signal box not found directly, searching regex...")
    content = re.sub(r'<!-- SEÑAL DIRECCIONAL DE ARBITRAJE -->\s*<h:panelGroup[^>]*>[\s\S]*?</h:panelGroup>', new_pairs_header, content)

# Remove tabs: 'Estadísticas de Comportamiento' and 'Análisis Conductual'
idx_tab_est = content.find('<p:tab title="Estadísticas de Comportamiento">')
idx_tab_dist = content.find('<p:tab title="Distribución Estadística">')

if idx_tab_est != -1 and idx_tab_dist != -1:
    content = content[:idx_tab_est] + content[idx_tab_dist:]
    print("Removed Estadísticas de Comportamiento and Análisis Conductual tabs")
else:
    print(f"Indices for tabs: est={idx_tab_est}, dist={idx_tab_dist}")

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(content)

# 2. Update DashboardBean.java to avoid calculating behavioral analysis
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    bean_content = f.read()

# Comment out or remove fetchAnalysisData() call in analyzePair()
old_fetch = "// --- Cargar análisis conductual (nueva pestaña)\n            fetchAnalysisData();"
new_fetch = "// --- Análisis conductual deshabilitado"

if old_fetch in bean_content:
    bean_content = bean_content.replace(old_fetch, new_fetch)
    print("Disabled fetchAnalysisData in DashboardBean.java")
else:
    bean_content = bean_content.replace("fetchAnalysisData();", "// fetchAnalysisData();")
    print("Commented fetchAnalysisData() call")

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(bean_content)

print("Files updated successfully!")
