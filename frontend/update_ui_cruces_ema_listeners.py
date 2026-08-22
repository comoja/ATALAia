dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

target_tabview = '<p:tabView id="crucesEmaInnerTabs" onTabChange="setTimeout(function(){ renderSignalComparisonChart(); }, 150);">'
new_tabview = '<p:tabView id="crucesEmaInnerTabs" onTabShow="if (index === 0) setTimeout(renderSignalComparisonChart, 50);">'

dash = dash.replace(target_tabview, new_tabview)

# Ensure selectOneButton update is robust
target_update = '<p:ajax listener="#{dashboardBean.onSignalStrategyChange}" update="signalTradesTable" />'
new_update = '<p:ajax listener="#{dashboardBean.onSignalStrategyChange}" update="@([id$=signalTradesTable])" />'

dash = dash.replace(target_update, new_update)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with onTabShow listener and robust table update for Cruces EMA subtabs!")
