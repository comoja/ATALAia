import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    bean_code = f.read()

if 'public void onOperarToggle()' not in bean_code:
    toggle_method = '''    public void onOperarToggle() {
        log.info("Estado de Operar actualizado a {} para {} / {}. Guardando en BD...", operar, selectedPair, selectedPair2);
        guardarRatio();
    }'''
    # Place it right after guardarRatio
    idx = bean_code.find('public void fetchUserRatioDetails()')
    if idx != -1:
        bean_code = bean_code[:idx] + toggle_method + '\n\n    ' + bean_code[idx:]
        print("Added onOperarToggle method to DashboardBean.java")

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(bean_code)

# 2. Update dashboard.xhtml
dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    dash_code = f.read()

# Add disabled="#{dashboardBean.operar}" to smaParam
dash_code = dash_code.replace(
    '''<p:inputNumber id="smaParam"
                                           value="#{dashboardBean.smaPeriodParam}"
                                           decimalPlaces="0"
                                           minValue="1"
                                           maxValue="999"
                                           inputStyleClass="aether-input"''',
    '''<p:inputNumber id="smaParam"
                                           value="#{dashboardBean.smaPeriodParam}"
                                           decimalPlaces="0"
                                           minValue="1"
                                           maxValue="999"
                                           disabled="#{dashboardBean.operar}"
                                           inputStyleClass="aether-input"'''
)

# Add disabled="#{dashboardBean.operar}" to emaSlowParam
dash_code = dash_code.replace(
    '''<p:inputNumber id="emaSlowParam"
                                           value="#{dashboardBean.emaSlowPeriodParam}"
                                           decimalPlaces="0"
                                           minValue="1"
                                           maxValue="999"
                                           inputStyleClass="aether-input"''',
    '''<p:inputNumber id="emaSlowParam"
                                           value="#{dashboardBean.emaSlowPeriodParam}"
                                           decimalPlaces="0"
                                           minValue="1"
                                           maxValue="999"
                                           disabled="#{dashboardBean.operar}"
                                           inputStyleClass="aether-input"'''
)

# Add disabled="#{dashboardBean.operar}" to timeframeSelect
dash_code = dash_code.replace(
    '''<p:selectOneMenu id="timeframeSelect" value="#{dashboardBean.timeframe}" styleClass="aether-input">''',
    '''<p:selectOneMenu id="timeframeSelect" value="#{dashboardBean.timeframe}" disabled="#{dashboardBean.operar}" styleClass="aether-input">'''
)

# Add disabled="#{dashboardBean.operar}" to daysBackParam
dash_code = dash_code.replace(
    '''<p:inputNumber id="daysBackParam"
                                           value="#{dashboardBean.daysBack}"
                                           decimalPlaces="0"
                                           minValue="1"
                                           maxValue="9999"
                                           inputStyleClass="aether-input"''',
    '''<p:inputNumber id="daysBackParam"
                                           value="#{dashboardBean.daysBack}"
                                           decimalPlaces="0"
                                           minValue="1"
                                           maxValue="9999"
                                           disabled="#{dashboardBean.operar}"
                                           inputStyleClass="aether-input"'''
)

# Update operarSwitch ajax
dash_code = dash_code.replace(
    '''<p:selectBooleanCheckbox id="operarSwitch" value="#{dashboardBean.operar}">
                                <p:ajax process="@this" />
                            </p:selectBooleanCheckbox>''',
    '''<p:selectBooleanCheckbox id="operarSwitch" value="#{dashboardBean.operar}">
                                <p:ajax process="@this" update="calibrationInputsPanel :aetherForm:growl" listener="#{dashboardBean.onOperarToggle}" />
                            </p:selectBooleanCheckbox>'''
)

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(dash_code)

print("dashboard.xhtml updated with disabled binding on operar!")
