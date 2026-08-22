bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_dates = '''                                if (t.has("entryDate")) st.setEntryDate(t.get("entryDate").asText());
                                if (t.has("exitDate")) st.setExitDate(t.get("exitDate").asText());'''

replacement_dates = '''                                if (t.has("entryDate")) {
                                    String ed = t.get("entryDate").asText();
                                    if (ed != null && ed.length() >= 10 && ed.contains(" ") && !ed.contains("Entradas")) {
                                        ed = ed.substring(0, 10);
                                    }
                                    st.setEntryDate(ed);
                                }
                                if (t.has("exitDate")) {
                                    String xd = t.get("exitDate").asText();
                                    if (xd != null && xd.length() >= 10 && xd.contains(" ")) {
                                        xd = xd.substring(0, 10);
                                    }
                                    st.setExitDate(xd);
                                }'''

code = code.replace(target_dates, replacement_dates)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java with clean YYYY-MM-DD date parsing!")
