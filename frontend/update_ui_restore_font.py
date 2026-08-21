import re

# 1. Update aetherial-ui.css
css_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/resources/css/aetherial-ui.css'
with open(css_path, 'r', encoding='utf-8') as f:
    css = f.read()

# Restore font-size on .aether-input, .ui-selectonemenu, and buttons
old_input_css = """.aether-input {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-primary) !important;
    border-radius: 8px !important;
    padding: 6px 12px !important;
    font-size: 0.9rem !important;
    font-family: 'Inter', sans-serif !important;
    box-shadow: var(--neu-shadow-inset) !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
    box-sizing: border-box !important;
}

/* Ajustes equilibrados para PrimeFaces SelectOneMenu */
.ui-selectonemenu.aether-input {
    padding: 0 !important;
    min-height: 34px !important;
    height: 34px !important;
    display: inline-flex !important;
    align-items: center !important;
    border-radius: 8px !important;
}

.ui-selectonemenu.aether-input .ui-selectonemenu-label {
    padding: 6px 10px !important;
    font-size: 0.9rem !important;
    line-height: 1.2 !important;
    color: var(--text-primary) !important;
}

.ui-selectonemenu.aether-input .ui-selectonemenu-trigger {
    width: 26px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 0 !important;
}

.ui-selectonemenu-panel .ui-selectonemenu-items .ui-selectonemenu-item {
    padding: 6px 10px !important;
    font-size: 0.88rem !important;
}"""

new_input_css = """.aether-input {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-primary) !important;
    border-radius: 6px !important;
    padding: 4px 8px !important;
    font-size: 1.3rem !important;
    font-family: 'Inter', sans-serif !important;
    box-shadow: var(--neu-shadow-inset) !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
    box-sizing: border-box !important;
}

/* Ajustes compactos con font completo y nítido para PrimeFaces SelectOneMenu */
.ui-selectonemenu.aether-input {
    padding: 0 !important;
    min-height: 30px !important;
    height: 30px !important;
    display: inline-flex !important;
    align-items: center !important;
    border-radius: 6px !important;
}

.ui-selectonemenu.aether-input .ui-selectonemenu-label {
    padding: 3px 8px !important;
    font-size: 1.3rem !important;
    line-height: 1.2 !important;
    color: var(--text-primary) !important;
}

.ui-selectonemenu.aether-input .ui-selectonemenu-trigger {
    width: 24px !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 0 !important;
}

.ui-selectonemenu-panel .ui-selectonemenu-items .ui-selectonemenu-item {
    padding: 5px 10px !important;
    font-size: 1.25rem !important;
}"""

css = css.replace(old_input_css, new_input_css)

# Update buttons
old_btn_primary = """.aether-btn-primary {
    background: var(--color-primary) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-green) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    font-size: 0.88rem !important;
    border-radius: 8px !important;
    padding: 6px 14px !important;
    min-height: 34px !important;
    height: 34px !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    box-shadow: var(--neu-shadow-flat) !important;
    transition: all 0.2s ease !important;
    cursor: pointer;
}

.aether-btn-primary .ui-button-text {
    padding: 0 6px !important;
    line-height: 1.2 !important;
    font-size: 0.88rem !important;
}

.aether-btn-primary .ui-icon {
    font-size: 0.95em !important;
}"""

new_btn_primary = """.aether-btn-primary {
    background: var(--color-primary) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-green) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    font-size: 1.25rem !important;
    border-radius: 6px !important;
    padding: 4px 12px !important;
    min-height: 30px !important;
    height: 30px !important;
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
    font-size: 1.25rem !important;
}

.aether-btn-primary .ui-icon {
    font-size: 1.1em !important;
}"""

css = css.replace(old_btn_primary, new_btn_primary)

old_btn_sec = """.aether-btn-secondary {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-green) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    font-size: 0.88rem !important;
    border-radius: 8px !important;
    padding: 6px 14px !important;
    min-height: 34px !important;
    height: 34px !important;
    display: inline-flex !important;
    align-items: center !important;
    justify-content: center !important;
    box-shadow: var(--neu-shadow-flat) !important;
    transition: all 0.2s ease !important;
    cursor: pointer;
}

.aether-btn-secondary .ui-button-text {
    padding: 0 6px !important;
    line-height: 1.2 !important;
    font-size: 0.88rem !important;
}

.aether-btn-secondary .ui-icon {
    font-size: 0.95em !important;
}"""

new_btn_sec = """.aether-btn-secondary {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    color: var(--text-green) !important;
    font-family: 'Inter', sans-serif !important;
    font-weight: 700 !important;
    font-size: 1.25rem !important;
    border-radius: 6px !important;
    padding: 4px 12px !important;
    min-height: 30px !important;
    height: 30px !important;
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
    font-size: 1.25rem !important;
}

.aether-btn-secondary .ui-icon {
    font-size: 1.1em !important;
}"""

css = css.replace(old_btn_sec, new_btn_sec)

with open(css_path, 'w', encoding='utf-8') as f:
    f.write(css)

print("Updated aetherial-ui.css: font sizes restored to 1.3rem / 1.25rem with compact 30px height!")

# 2. Update dashboard.xhtml
dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_dash_css = """        .aether-ema-row {
            display: flex;
            gap: 14px;
            margin-bottom: 14px;
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
            height: 34px !important;
            padding: 4px 8px !important;
            font-size: 0.9rem !important;
            text-align: center !important;
            box-sizing: border-box !important;
            border-radius: 8px !important;
        }

        /* Estilos compactos para Temporalidad y Días */
        .aether-tf-days-row {
            display: flex;
            gap: 14px;
            margin-bottom: 14px;
            align-items: flex-start;
        }

        .aether-tf-col {
            display: flex;
            flex-direction: column;
        }

        .aether-tf-col .ui-selectonemenu {
            width: 135px !important;
            height: 34px !important;
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
            height: 34px !important;
            padding: 4px 8px !important;
            font-size: 0.9rem !important;
            text-align: center !important;
            box-sizing: border-box !important;
            border-radius: 8px !important;
        }

        /* Estilos ajustados para Fechas (Fecha Inicio / Fecha Fin) sin empalmes */
        .aether-date-row {
            display: flex;
            gap: 10px;
            margin-bottom: 18px;
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
            height: 34px !important;
            box-sizing: border-box !important;
            padding: 6px 10px !important;
            font-size: 0.88rem !important;
            border-radius: 8px 0 0 8px !important;
        }

        .aether-date-col .ui-datepicker-trigger {
            flex: 0 0 auto !important;
            height: 34px !important;
            border-radius: 0 8px 8px 0 !important;
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
            width: 70px !important;
            display: inline-block !important;
        }

        .aether-ema-col .ui-inputnumber input {
            width: 70px !important;
            height: 30px !important;
            padding: 2px 6px !important;
            font-size: 1.3rem !important;
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
            width: 135px !important;
            height: 30px !important;
            box-sizing: border-box !important;
        }

        .aether-days-col {
            display: flex;
            flex-direction: column;
        }

        .aether-days-col .ui-inputnumber {
            width: 70px !important;
            display: inline-block !important;
        }

        .aether-days-col .ui-inputnumber input {
            width: 70px !important;
            height: 30px !important;
            padding: 2px 6px !important;
            font-size: 1.3rem !important;
            text-align: center !important;
            box-sizing: border-box !important;
            border-radius: 6px !important;
        }

        /* Estilos ajustados para Fechas (Fecha Inicio / Fecha Fin) sin empalmes */
        .aether-date-row {
            display: flex;
            gap: 8px;
            margin-bottom: 14px;
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
            height: 30px !important;
            box-sizing: border-box !important;
            padding: 3px 8px !important;
            font-size: 1.25rem !important;
            border-radius: 6px 0 0 6px !important;
        }

        .aether-date-col .ui-datepicker-trigger {
            flex: 0 0 auto !important;
            height: 30px !important;
            border-radius: 0 6px 6px 0 !important;
            padding: 2px 6px !important;
        }"""

dash = dash.replace(old_dash_css, new_dash_css)

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated dashboard.xhtml: font sizes restored to 1.3rem / 1.25rem with compact 30px height!")
