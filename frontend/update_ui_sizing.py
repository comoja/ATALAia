import re

# 1. Update aetherial-ui.css
css_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/resources/css/aetherial-ui.css'
with open(css_path, 'r', encoding='utf-8') as f:
    css = f.read()

# Update .aether-input
old_input_css = """.aether-input {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-primary) !important;
    border-radius: 12px !important; /* Suave curvatura */
    padding: 10px 14px !important;
    font-family: 'Inter', sans-serif !important;
    box-shadow: var(--neu-shadow-inset) !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
}"""

new_input_css = """.aether-input {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-primary) !important;
    border-radius: 6px !important;
    padding: 4px 8px !important;
    font-size: 0.85rem !important;
    font-family: 'Inter', sans-serif !important;
    box-shadow: var(--neu-shadow-inset) !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
    box-sizing: border-box !important;
}

/* Ajustes compactos para PrimeFaces SelectOneMenu */
.ui-selectonemenu.aether-input {
    padding: 0 !important;
    min-height: 28px !important;
    height: 28px !important;
    display: inline-flex !important;
    align-items: center !important;
    border-radius: 6px !important;
}

.ui-selectonemenu.aether-input .ui-selectonemenu-label {
    padding: 3px 8px !important;
    font-size: 0.85rem !important;
    line-height: 1.2 !important;
    color: var(--text-primary) !important;
}

.ui-selectonemenu.aether-input .ui-selectonemenu-trigger {
    width: 22px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 0 !important;
}

.ui-selectonemenu-panel .ui-selectonemenu-items .ui-selectonemenu-item {
    padding: 4px 8px !important;
    font-size: 0.82rem !important;
}"""

css = css.replace(old_input_css, new_input_css)

# Update buttons
old_btn_primary = """.aether-btn-primary {
    background: var(--color-primary) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-green) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    border-radius: 12px !important;
    padding: 5px 16px !important;
    box-shadow: var(--neu-shadow-flat) !important;
    transition: all 0.2s ease !important;
    cursor: pointer;
}"""

new_btn_primary = """.aether-btn-primary {
    background: var(--color-primary) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-green) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    font-size: 0.82rem !important;
    border-radius: 6px !important;
    padding: 4px 10px !important;
    min-height: 28px !important;
    height: 28px !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    box-shadow: var(--neu-shadow-flat) !important;
    transition: all 0.2s ease !important;
    cursor: pointer;
}

.aether-btn-primary .ui-button-text {
    padding: 0 4px !important;
    line-height: 1.2 !important;
    font-size: 0.82rem !important;
}

.aether-btn-primary .ui-icon {
    font-size: 0.85em !important;
}"""

css = css.replace(old_btn_primary, new_btn_primary)

old_btn_sec = """.aether-btn-secondary {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-green) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    border-radius: 12px !important;
    padding: 5px 16px !important;
    box-shadow: var(--neu-shadow-flat) !important;
    transition: all 0.2s ease !important;
    cursor: pointer;
}"""

new_btn_sec = """.aether-btn-secondary {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-green) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    font-size: 0.82rem !important;
    border-radius: 6px !important;
    padding: 4px 10px !important;
    min-height: 28px !important;
    height: 28px !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    box-shadow: var(--neu-shadow-flat) !important;
    transition: all 0.2s ease !important;
    cursor: pointer;
}

.aether-btn-secondary .ui-button-text {
    padding: 0 4px !important;
    line-height: 1.2 !important;
    font-size: 0.82rem !important;
}

.aether-btn-secondary .ui-icon {
    font-size: 0.85em !important;
}"""

css = css.replace(old_btn_sec, new_btn_sec)

with open(css_path, 'w', encoding='utf-8') as f:
    f.write(css)

print("Updated aetherial-ui.css with compact proportions!")

# 2. Update dashboard.xhtml
dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# Update inline CSS in dashboard.xhtml
old_dash_css = """        .aether-ema-row {
            display: flex;
            gap: 16px;
            margin-bottom: 18px;
            align-items: flex-start;
        }

        .aether-ema-col {
            display: flex;
            flex-direction: column;
        }

        .aether-ema-col .ui-inputnumber {
            width: 75px !important;
            display: inline-block !important;
        }

        .aether-ema-col .ui-inputnumber input {
            width: 75px !important;
            text-align: center !important;
            box-sizing: border-box !important;
        }

                /* Estilos compactos para Temporalidad y Días */
        .aether-tf-days-row {
            display: flex;
            gap: 16px;
            margin-bottom: 18px;
            align-items: flex-start;
        }

        .aether-tf-col {
            display: flex;
            flex-direction: column;
        }

        .aether-tf-col .ui-selectonemenu {
            width: 140px !important;
            box-sizing: border-box !important;
        }

        .aether-days-col {
            display: flex;
            flex-direction: column;
        }

        .aether-days-col .ui-inputnumber {
            width: 75px !important;
            display: inline-block !important;
        }

        .aether-days-col .ui-inputnumber input {
            width: 75px !important;
            text-align: center !important;
            box-sizing: border-box !important;
        }

        /* Estilos ajustados para Fechas (Fecha Inicio / Fecha Fin) sin empalmes */
        .aether-date-row {
            display: flex;
            gap: 10px;
            margin-bottom: 22px;
            width: 100%;
            box-sizing: border-box;
        }

        .aether-date-col {
            flex: 1;
            min-width: 0;
        }

        .aether-date-col .ui-calendar,
        .aether-date-col .ui-datepicker {
            width: 100% !important;
            display: flex !important;
            box-sizing: border-box !important;
        }

        .aether-date-col .ui-inputfield {
            flex: 1 !important;
            min-width: 0 !important;
            width: 100% !important;
            box-sizing: border-box !important;
            padding: 8px 10px !important;
        }

        .aether-date-col .ui-datepicker-trigger {
            flex: 0 0 auto !important;
            border-radius: 8px !important;
            padding: 4px 8px !important;
        }"""

new_dash_css = """        .aether-ema-row {
            display: flex;
            gap: 12px;
            margin-bottom: 12px;
            align-items: flex-start;
        }

        .aether-ema-col {
            display: flex;
            flex-direction: column;
        }

        .aether-ema-col .ui-inputnumber {
            width: 65px !important;
            display: inline-block !important;
        }

        .aether-ema-col .ui-inputnumber input {
            width: 65px !important;
            height: 28px !important;
            padding: 2px 6px !important;
            font-size: 0.85rem !important;
            text-align: center !important;
            box-sizing: border-box !important;
            border-radius: 6px !important;
        }

        /* Estilos compactos para Temporalidad y Días */
        .aether-tf-days-row {
            display: flex;
            gap: 12px;
            margin-bottom: 12px;
            align-items: flex-start;
        }

        .aether-tf-col {
            display: flex;
            flex-direction: column;
        }

        .aether-tf-col .ui-selectonemenu {
            width: 130px !important;
            height: 28px !important;
            box-sizing: border-box !important;
        }

        .aether-days-col {
            display: flex;
            flex-direction: column;
        }

        .aether-days-col .ui-inputnumber {
            width: 65px !important;
            display: inline-block !important;
        }

        .aether-days-col .ui-inputnumber input {
            width: 65px !important;
            height: 28px !important;
            padding: 2px 6px !important;
            font-size: 0.85rem !important;
            text-align: center !important;
            box-sizing: border-box !important;
            border-radius: 6px !important;
        }

        /* Estilos ajustados para Fechas (Fecha Inicio / Fecha Fin) sin empalmes */
        .aether-date-row {
            display: flex;
            gap: 8px;
            margin-bottom: 16px;
            width: 100%;
            box-sizing: border-box;
        }

        .aether-date-col {
            flex: 1;
            min-width: 0;
        }

        .aether-date-col .ui-calendar,
        .aether-date-col .ui-datepicker {
            width: 100% !important;
            display: flex !important;
            box-sizing: border-box !important;
        }

        .aether-date-col .ui-inputfield {
            flex: 1 !important;
            min-width: 0 !important;
            width: 100% !important;
            height: 28px !important;
            box-sizing: border-box !important;
            padding: 3px 6px !important;
            font-size: 0.82rem !important;
            border-radius: 6px 0 0 6px !important;
        }

        .aether-date-col .ui-datepicker-trigger {
            flex: 0 0 auto !important;
            height: 28px !important;
            border-radius: 0 6px 6px 0 !important;
            padding: 2px 6px !important;
        }"""

dash = dash.replace(old_dash_css, new_dash_css)

# Update operarContainerTop padding & margin
dash = dash.replace(
    'style="margin-bottom: 18px;"',
    'style="margin-bottom: 12px;"'
)
dash = dash.replace(
    'style="padding: 10px 14px; background: rgba(241, 245, 249, 0.6); border: 1px solid #e2e8f0; border-radius: 8px; display: flex; align-items: center; justify-content: space-between;"',
    'style="padding: 6px 10px; background: rgba(241, 245, 249, 0.6); border: 1px solid #e2e8f0; border-radius: 6px; display: flex; align-items: center; justify-content: space-between;"'
)

# Update Numerador/Denominador gap & margin
dash = dash.replace(
    'style="display: flex; gap: 10px; align-items: center; margin-bottom: 22px;"',
    'style="display: flex; gap: 8px; align-items: center; margin-bottom: 14px;"'
)
dash = dash.replace(
    'style="margin-bottom: 22px;"',
    'style="margin-bottom: 12px;"'
)

# Update button block spacing
dash = dash.replace(
    'style="margin-top: 30px; display: flex; flex-direction: column; gap: 12px;"',
    'style="margin-top: 16px; display: flex; flex-direction: column; gap: 8px;"'
)
dash = dash.replace(
    'style="display: flex; gap: 10px; width: 100%;"',
    'style="display: flex; gap: 8px; width: 100%;"'
)

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml with compact control dimensions!")
