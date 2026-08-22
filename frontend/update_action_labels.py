dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Update Quant tab description
dash = dash.replace(
    '<span style="font-size: 1.05rem; color: #64748b;">Capital Inicial: $10,000 USD | Costos: 3 bps</span>',
    '<span style="font-size: 1.05rem; color: #64748b;">Capital Inicial: $10,000 USD | Asignación: 100% (50% Par A / 50% Par B) | Costos: 3 bps</span>'
)

# 2. Update Signal Backtest tab description
dash = dash.replace(
    '<i class="pi pi-chart-line" style="margin-right: 6px; color: #0284c7;"></i>Comparativa de Curvas de Equidad (Capital Inicial: $10,000 USD | Salida: Cruce Media ●)',
    '<i class="pi pi-chart-line" style="margin-right: 6px; color: #0284c7;"></i>Comparativa de Curvas de Equidad (Capital: $10,000 USD | 50% Par A + 50% Par B | Salida: Cruce Media ●)'
)

# 3. Update Quant table "Tipo" column to show explicit legs
old_col_type = '''                                <p:column headerText="Tipo" style="width: 80px;">
                                    <span class="badge" style="padding: 2px 6px; border-radius: 4px; font-weight: 700; font-size: 1.0rem; background: #{t.type == 'LARGO_RATIO' ? '#dcfce7' : '#fee2e2'}; color: #{t.type == 'LARGO_RATIO' ? '#15803d' : '#b91c1c'};">
                                        #{t.type == 'LARGO_RATIO' ? 'COMPRA' : 'VENTA'}
                                    </span>
                                </p:column>'''

new_col_type = '''                                <p:column headerText="Operación (Ambos Pares)" style="width: 190px;">
                                    <span class="badge" style="padding: 3px 6px; border-radius: 4px; font-weight: 700; font-size: 0.95rem; background: #{t.type == 'LARGO_RATIO' ? '#dcfce7' : '#fee2e2'}; color: #{t.type == 'LARGO_RATIO' ? '#15803d' : '#b91c1c'};">
                                        #{t.type == 'LARGO_RATIO' ? ('COMPRA ' += dashboardBean.selectedPair += ' / VENTA ' += dashboardBean.selectedPair2) : ('VENTA ' += dashboardBean.selectedPair += ' / COMPRA ' += dashboardBean.selectedPair2)}
                                    </span>
                                </p:column>'''

dash = dash.replace(old_col_type, new_col_type)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated action labels and capital allocation in dashboard.xhtml!")
