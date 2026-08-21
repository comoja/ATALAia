import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Add onRowSelect method
if 'public void onRowSelect(' not in code:
    row_select_method = '''    public void onRowSelect(org.primefaces.event.SelectEvent<UserRatioDto> event) {
        if (event != null && event.getObject() != null) {
            onSelectUserRatio(event.getObject());
        }
    }'''
    code = code.replace('public void onSelectUserRatio(UserRatioDto ratio) {', row_select_method + '\n\n    public void onSelectUserRatio(UserRatioDto ratio) {')
    print("Added onRowSelect method to DashboardBean.java")

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

# 2. Update dashboard.xhtml
dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Enhance userRatiosTable with selectionMode, rowSelect ajax, and scrollable
old_table_block = """                        <p:dataTable id="userRatiosTable"
                                     value="#{dashboardBean.userRatiosList}"
                                     var="r"
                                     emptyMessage="No hay ratios registrados"
                                     rowKey="#{r.id}"
                                     styleClass="aether-table-compact"
                                     style="width: 100%; margin-bottom: 6px;">"""

new_table_block = """                        <p:dataTable id="userRatiosTable"
                                     value="#{dashboardBean.userRatiosList}"
                                     var="r"
                                     selectionMode="single"
                                     selection="#{dashboardBean.selectedUserRatio}"
                                     rowKey="#{r.id}"
                                     emptyMessage="No hay ratios registrados"
                                     scrollable="true"
                                     scrollHeight="180px"
                                     styleClass="aether-table-compact"
                                     style="width: 100%; margin-bottom: 6px; cursor: pointer;">
                            <p:ajax event="rowSelect" listener="#{dashboardBean.onRowSelect}" update=":aetherForm" oncomplete="renderAetherChart()" />"""

dash = dash.replace(old_table_block, new_table_block)

# Enhance command buttons update in calibration tab to update full :aetherForm so chart and table stay perfectly in sync
dash = dash.replace(
    '<p:commandButton value="Guardar Ratio"\n                                                 action="#{dashboardBean.guardarRatio}"\n                                                 update=":aetherForm:leftAccordion :aetherForm:growl"',
    '<p:commandButton value="Guardar Ratio"\n                                                 action="#{dashboardBean.guardarRatio}"\n                                                 update=":aetherForm"\n                                                 oncomplete="renderAetherChart()"'
)
dash = dash.replace(
    '<p:commandButton value="Borrar Ratio"\n                                                 action="#{dashboardBean.borrarRatio}"\n                                                 update=":aetherForm:leftAccordion :aetherForm:growl"',
    '<p:commandButton value="Borrar Ratio"\n                                                 action="#{dashboardBean.borrarRatio}"\n                                                 update=":aetherForm"\n                                                 oncomplete="renderAetherChart()"'
)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Enhanced userRatiosTable with row selection and scrollable in dashboard.xhtml!")
