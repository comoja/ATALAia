import re

# 1. Update DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Add activeAccordionIndex and onTabChange
accordion_state_code = '''    private String activeAccordionIndex = "0";

    public String getActiveAccordionIndex() {
        return activeAccordionIndex;
    }

    public void setActiveAccordionIndex(String activeAccordionIndex) {
        this.activeAccordionIndex = activeAccordionIndex;
    }

    public void onTabChange(org.primefaces.event.TabChangeEvent event) {
        if (event != null && event.getTab() != null) {
            String title = event.getTab().getTitle();
            if (title != null && title.contains("1.")) {
                this.activeAccordionIndex = "0";
            } else {
                this.activeAccordionIndex = "1";
            }
            log.info("Tab del acordeón cambiado a: {} (index={})", title, activeAccordionIndex);
        }
    }
'''

if 'private String activeAccordionIndex' not in code:
    code = code.replace('private List<UserRatioDto> userRatiosList = new ArrayList<>();', accordion_state_code + '\n    private List<UserRatioDto> userRatiosList = new ArrayList<>();')
    print("Added activeAccordionIndex and onTabChange to DashboardBean.java")

# In onSelectUserRatio, set activeAccordionIndex = "1"
if 'this.activeAccordionIndex = "1";' not in code:
    code = code.replace(
        'this.ratioExistsInDb = true;',
        'this.ratioExistsInDb = true;\n        this.activeAccordionIndex = "1";'
    )
    print("Added activeAccordionIndex = '1' to onSelectUserRatio")

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

# 2. Update dashboard.xhtml
dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_acc_tag = '<p:accordionPanel id="leftAccordion" multiple="true" activeIndex="0,1" styleClass="aether-accordion" style="margin-top: 4px;">'
new_acc_tag = '''<p:accordionPanel id="leftAccordion" multiple="false" activeIndex="#{dashboardBean.activeAccordionIndex}" styleClass="aether-accordion" style="margin-top: 4px;">
                    <p:ajax event="tabChange" listener="#{dashboardBean.onTabChange}" />'''

if old_acc_tag in dash:
    dash = dash.replace(old_acc_tag, new_acc_tag)
    print("Updated p:accordionPanel to multiple='false' with activeIndex binding in dashboard.xhtml!")

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Accordion transition logic applied successfully!")
