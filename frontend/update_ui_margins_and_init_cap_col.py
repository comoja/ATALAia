dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Update Margen Invertido column
target_margen_col = '''                            <p:column headerText="Margen Invertido ($)" style="width: 140px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 700; color: #1e293b; font-size: 1.0rem;">$#{st.formattedAllocatedCapital}</div>
                                    <div style="font-size: 0.76rem; color: #64748b; font-weight: 600; line-height: 1.2;">#{dashboardBean.selectedPair}: #{st.formattedUnitsA} lotes (#{st.multA}x)</div>
                                    <div style="font-size: 0.76rem; color: #64748b; font-weight: 600; line-height: 1.2;">#{dashboardBean.selectedPair2}: #{st.formattedUnitsB} lotes (#{st.multB}x)</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 3px 6px; border-radius: 5px; font-weight: 900; font-size: 1.02rem; display: inline-block;">
                                        $#{st.formattedAllocatedCapital}
                                    </span>
                                    <div style="font-size: 0.76rem; color: #334155; font-weight: 700; margin-top: 2px; line-height: 1.2;">#{dashboardBean.selectedPair}: #{st.formattedUnitsA} lotes</div>
                                    <div style="font-size: 0.76rem; color: #334155; font-weight: 700; line-height: 1.2;">#{dashboardBean.selectedPair2}: #{st.formattedUnitsB} lotes</div>
                                </h:panelGroup>
                            </p:column>'''

new_margen_col = '''                            <p:column headerText="Margen Invertido ($)" style="width: 148px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 800; color: #0f172a; font-size: 1.08rem;">$#{st.formattedAllocatedCapital}</div>
                                    <div style="font-size: 0.76rem; color: #475569; font-weight: 600; line-height: 1.2;">#{dashboardBean.selectedPair}: $#{st.formattedMarginA} (#{st.formattedUnitsA} lotes, #{st.multA}x)</div>
                                    <div style="font-size: 0.76rem; color: #475569; font-weight: 600; line-height: 1.2;">#{dashboardBean.selectedPair2}: $#{st.formattedMarginB} (#{st.formattedUnitsB} lotes, #{st.multB}x)</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 4px 8px; border-radius: 6px; font-weight: 900; font-size: 1.1rem; display: inline-block; box-shadow: 0 1px 3px rgba(0,0,0,0.15);">
                                        $#{st.formattedAllocatedCapital}
                                    </span>
                                    <div style="font-size: 0.76rem; color: #334155; font-weight: 700; margin-top: 2px; line-height: 1.2;">#{dashboardBean.selectedPair}: $#{st.formattedMarginA} (#{st.formattedUnitsA} lotes)</div>
                                    <div style="font-size: 0.76rem; color: #334155; font-weight: 700; line-height: 1.2;">#{dashboardBean.selectedPair2}: $#{st.formattedMarginB} (#{st.formattedUnitsB} lotes)</div>
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(target_margen_col, new_margen_col, 1)

# 2. Update Capital column headerText
target_cap_col = '<p:column headerText="Capital ($)" style="width: 90px; text-align: right;">'
new_cap_col = '<p:column headerText="Capital ($#{dashboardBean.formattedSignalBtInitialCapital})" style="width: 102px; text-align: right;">'

dash = dash.replace(target_cap_col, new_cap_col, 1)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml: Margen Invertido now includes individual pair margin ($) with bold total sum, and Capital header includes initial capital!")
