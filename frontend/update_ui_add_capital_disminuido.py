dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

target_block = '''                            <p:column headerText="Margen Invertido ($)" style="width: 148px; text-align: right;">
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

new_block = '''                            <p:column headerText="Margen Invertido ($)" style="width: 140px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <div style="font-weight: 800; color: #0f172a; font-size: 1.05rem;">$#{st.formattedAllocatedCapital}</div>
                                    <div style="font-size: 0.76rem; color: #475569; font-weight: 600; line-height: 1.2;">#{dashboardBean.selectedPair}: $#{st.formattedMarginA} (#{st.formattedUnitsA} lotes, #{st.multA}x)</div>
                                    <div style="font-size: 0.76rem; color: #475569; font-weight: 600; line-height: 1.2;">#{dashboardBean.selectedPair2}: $#{st.formattedMarginB} (#{st.formattedUnitsB} lotes, #{st.multB}x)</div>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #{st.isWin ? '#15803d' : '#b91c1c'}; color: #ffffff; padding: 3px 6px; border-radius: 5px; font-weight: 900; font-size: 1.05rem; display: inline-block; box-shadow: 0 1px 3px rgba(0,0,0,0.15);">
                                        $#{st.formattedAllocatedCapital}
                                    </span>
                                    <div style="font-size: 0.76rem; color: #334155; font-weight: 700; margin-top: 2px; line-height: 1.2;">#{dashboardBean.selectedPair}: $#{st.formattedMarginA} (#{st.formattedUnitsA} lotes)</div>
                                    <div style="font-size: 0.76rem; color: #334155; font-weight: 700; line-height: 1.2;">#{dashboardBean.selectedPair2}: $#{st.formattedMarginB} (#{st.formattedUnitsB} lotes)</div>
                                </h:panelGroup>
                            </p:column>
                            <p:column headerText="Cap. Disminuido ($)" style="width: 105px; text-align: right;">
                                <h:panelGroup rendered="#{!st.isSubtotal}">
                                    <span style="font-weight: 800; font-size: 1.02rem; color: #b45309;">
                                        $#{st.formattedAvailableCapital}
                                    </span>
                                </h:panelGroup>
                                <h:panelGroup rendered="#{st.isSubtotal}">
                                    <span style="background: #fef3c7; color: #b45309; border: 1px solid #fde68a; padding: 3px 6px; border-radius: 5px; font-weight: 900; font-size: 1.02rem; display: inline-block;">
                                        $#{st.formattedAvailableCapital}
                                    </span>
                                </h:panelGroup>
                            </p:column>'''

dash = dash.replace(target_block, new_block, 1)

# In footer, add second empty column for Cap. Disminuido
target_footer = '''            <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
            <p:column footerText="" style="background: #f8fafc;" />'''

new_footer = '''            <p:column colspan="7" style="text-align: right; font-weight: 800; font-size: 1.05rem; background: #f8fafc; color: #1e293b;" footerText="TOTAL NETO (SUMATORIA):" />
            <p:column footerText="" style="background: #f8fafc;" />
            <p:column footerText="" style="background: #f8fafc;" />'''

dash = dash.replace(target_footer, new_footer, 1)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("SUCCESS: Updated dashboard.xhtml with 'Cap. Disminuido ($)' column and matching footer!")
