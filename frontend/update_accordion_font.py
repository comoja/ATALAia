css_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/resources/css/aetherial-ui.css'
with open(css_path, 'r', encoding='utf-8') as f:
    css = f.read()

old_acc_css = """.aether-accordion .ui-accordion-header {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    border-radius: 8px !important;
    margin-bottom: 6px !important;
    padding: 8px 12px !important;
    font-size: 1.3rem !important;
    font-weight: 700 !important;
    color: var(--text-primary) !important;
}

.aether-accordion .ui-accordion-header.ui-state-active {
    background: var(--bg-surface-active) !important;
    border-color: var(--color-primary) !important;
}

.aether-accordion .ui-accordion-content {
    background: transparent !important;
    border: none !important;
    padding: 8px 2px !important;
}

.aether-table-compact .ui-datatable-tablewrapper {
    overflow-x: auto;
}

.aether-table-compact th {
    padding: 4px 6px !important;
    font-size: 1.15rem !important;
    text-align: center !important;
}

.aether-table-compact td {
    padding: 4px 6px !important;
    font-size: 1.2rem !important;
}"""

new_acc_css = """.aether-accordion .ui-accordion-header {
    background: var(--bg-surface) !important;
    border: 1px solid var(--border-soft) !important;
    border-radius: 6px !important;
    margin-bottom: 5px !important;
    padding: 6px 10px !important;
    font-size: 1.12rem !important;
    font-weight: 700 !important;
    color: var(--text-primary) !important;
}

.aether-accordion .ui-accordion-header a {
    font-size: 1.12rem !important;
    font-weight: 700 !important;
    padding: 0 !important;
}

.aether-accordion .ui-accordion-header.ui-state-active {
    background: var(--bg-surface-active) !important;
    border-color: var(--color-primary) !important;
}

.aether-accordion .ui-accordion-content {
    background: transparent !important;
    border: none !important;
    padding: 6px 2px !important;
}

.aether-table-compact .ui-datatable-tablewrapper {
    overflow-x: auto;
}

.aether-table-compact th {
    padding: 3px 5px !important;
    font-size: 1.05rem !important;
    text-align: center !important;
}

.aether-table-compact td {
    padding: 3px 5px !important;
    font-size: 1.1rem !important;
}"""

css = css.replace(old_acc_css, new_acc_css)

with open(css_path, 'w', encoding='utf-8') as f:
    f.write(css)

print("Updated aetherial-ui.css: reduced accordion headers font to 1.12rem!")
