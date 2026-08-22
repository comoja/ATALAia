bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    code = f.read()

# Target for toTrades loop
target_to = '''                                if (t.has("cycleTotalPnl")) st.setCycleTotalPnl(t.get("cycleTotalPnl").asDouble());
                                if (t.has("cycleAvgReturnPct")) st.setCycleAvgReturnPct(t.get("cycleAvgReturnPct").asDouble());
                                toTrades.add(st);'''

replacement_to = '''                                if (t.has("cycleTotalPnl")) st.setCycleTotalPnl(t.get("cycleTotalPnl").asDouble());
                                if (t.has("cycleAvgReturnPct")) st.setCycleAvgReturnPct(t.get("cycleAvgReturnPct").asDouble());
                                if (t.has("isSubtotal")) st.setIsSubtotal(t.get("isSubtotal").asBoolean());
                                if (t.has("multA") && !t.get("multA").isNull()) st.setMultA(t.get("multA").asInt());
                                if (t.has("multB") && !t.get("multB").isNull()) st.setMultB(t.get("multB").asInt());
                                if (t.has("unitsA") && !t.get("unitsA").isNull()) st.setUnitsA(t.get("unitsA").asInt());
                                if (t.has("unitsB") && !t.get("unitsB").isNull()) st.setUnitsB(t.get("unitsB").asInt());
                                if (t.has("pipsA") && !t.get("pipsA").isNull()) st.setPipsA(t.get("pipsA").asDouble());
                                if (t.has("pipsB") && !t.get("pipsB").isNull()) st.setPipsB(t.get("pipsB").asDouble());
                                toTrades.add(st);'''

# Target for cbTrades loop
target_cb = '''                                if (t.has("cycleTotalPnl")) st.setCycleTotalPnl(t.get("cycleTotalPnl").asDouble());
                                if (t.has("cycleAvgReturnPct")) st.setCycleAvgReturnPct(t.get("cycleAvgReturnPct").asDouble());
                                cbTrades.add(st);'''

replacement_cb = '''                                if (t.has("cycleTotalPnl")) st.setCycleTotalPnl(t.get("cycleTotalPnl").asDouble());
                                if (t.has("cycleAvgReturnPct")) st.setCycleAvgReturnPct(t.get("cycleAvgReturnPct").asDouble());
                                if (t.has("isSubtotal")) st.setIsSubtotal(t.get("isSubtotal").asBoolean());
                                if (t.has("multA") && !t.get("multA").isNull()) st.setMultA(t.get("multA").asInt());
                                if (t.has("multB") && !t.get("multB").isNull()) st.setMultB(t.get("multB").asInt());
                                if (t.has("unitsA") && !t.get("unitsA").isNull()) st.setUnitsA(t.get("unitsA").asInt());
                                if (t.has("unitsB") && !t.get("unitsB").isNull()) st.setUnitsB(t.get("unitsB").asInt());
                                if (t.has("pipsA") && !t.get("pipsA").isNull()) st.setPipsA(t.get("pipsA").asDouble());
                                if (t.has("pipsB") && !t.get("pipsB").isNull()) st.setPipsB(t.get("pipsB").asDouble());
                                cbTrades.add(st);'''

code = code.replace(target_to, replacement_to).replace(target_cb, replacement_cb)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated DashboardBean.java: Added units, mult, pips parsing to BOTH toTrades and cbTrades!")
