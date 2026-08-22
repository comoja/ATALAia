bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_block = '''                                if (t.has("entryDate")) {
                                    String ed = t.get("entryDate").asText();
                                    if (ed != null && ed.length() >= 10 && ed.contains(" ") && !ed.contains("Entradas")) {
                                        ed = ed.substring(0, 10);
                                    }
                                    st.setEntryDate(ed);
                                }'''

new_block = '''                                if (t.has("entryDate")) {
                                    String ed = t.get("entryDate").asText();
                                    if (ed != null) {
                                        if (ed.startsWith("Entradas: ")) {
                                            ed = ed.substring("Entradas: ".length());
                                        }
                                        if (ed.length() >= 10 && ed.contains(" ") && !ed.contains(" a ")) {
                                            ed = ed.substring(0, 10);
                                        }
                                    }
                                    st.setEntryDate(ed);
                                }'''

code = code.replace(target_block, new_block)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java: removed 'Entradas: ' prefix from entryDate parsing!")
