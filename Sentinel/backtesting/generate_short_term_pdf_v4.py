"""
GENERADOR DE REPORTE PDF V4 - COMPOUNDING DE PORTAFOLIO GLOBAL
"""
import os
import sys
import pandas as pd
from fpdf import FPDF

sys.path.append("/Volumes/TimeMachine/ATALAia")

INITIAL_PORTFOLIO = 500.0
BASE_PATH = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/"


class PortfolioReportPdfV4(FPDF):
    """Reporte PDF V4 - Tema azul zafiro oscuro / acento cyan electrico."""

    def header(self) -> None:
        if self.page_no() > 1:
            self.set_text_color(120, 140, 160)
            self.set_font('Helvetica', 'B', 8)
            self.cell(0, 10, "SISTEMA ATALAIA / SENTINEL - AUDITORIA V4 (COMPOUNDING DE PORTAFOLIO GLOBAL)", 0, 0, 'L')
            self.cell(0, 10, "CONFIDENCIAL", 0, 1, 'R')
            self.set_fill_color(0, 210, 255)  # Cyan electrico
            self.rect(10, self.get_y(), 196, 0.6, 'F')
            self.ln(5)

    def footer(self) -> None:
        if self.page_no() > 1:
            self.set_y(-15)
            self.set_font('Helvetica', 'I', 8)
            self.set_text_color(120, 140, 160)
            self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, 'C')


def drawColorTable(pdf: FPDF, df: pd.DataFrame, headers: list, colWidths: list,
                    maxRows: int = 14, pnlColIdx: int = -1) -> None:
    """Dibuja tabla con cabecera oscura y filas alternadas. Colorea columna PnL."""
    pdf.set_font('Helvetica', 'B', 8.5)
    pdf.set_fill_color(18, 32, 50)
    pdf.set_text_color(0, 210, 255)
    for h, w in zip(headers, colWidths):
        pdf.cell(w, 7, h, 1, 0, 'C', True)
    pdf.ln(7)

    for i in range(min(maxRows, len(df))):
        row = df.iloc[i]
        fillColor = (240, 248, 255) if i % 2 == 0 else (225, 238, 250)
        pdf.set_fill_color(*fillColor)
        pdf.set_font('Helvetica', '', 8)
        pdf.set_text_color(30, 40, 55)

        for colIdx, (h, w) in enumerate(zip(headers, colWidths)):
            val = str(row.iloc[colIdx]) if colIdx < len(row) else ''
            isLast = (colIdx == len(headers) - 1)
            hasPnl = ('PnL' in h or 'Retorno' in h or 'Balance' in h and colIdx == pnlColIdx)

            if isLast or hasPnl:
                pdf.set_font('Helvetica', 'B', 8)
                if val.startswith('-'):
                    pdf.set_text_color(210, 40, 60)
                elif val.startswith('+') or (val.startswith('$') and not val.startswith('$-')):
                    pdf.set_text_color(0, 160, 90)
                else:
                    pdf.set_text_color(30, 40, 55)
            else:
                pdf.set_font('Helvetica', '', 8)
                pdf.set_text_color(30, 40, 55)

            pdf.cell(w, 6.5, val, 1, 0, 'C', True)
        pdf.ln(6.5)
    pdf.set_text_color(30, 40, 55)


def generatePortfolioPdfV4() -> None:
    """Genera el reporte PDF V4 de compounding de portafolio."""
    try:
        print("\n--- Iniciando Generacion de Reporte PDF V4 (Compounding Portafolio) ---")

        # Cargar CSVs
        csvFiles = {
            '5 Dias': BASE_PATH + 'backtest_short_term_v4_5_dias.csv',
            '4 Dias': BASE_PATH + 'backtest_short_term_v4_4_dias.csv',
            '3 Dias': BASE_PATH + 'backtest_short_term_v4_3_dias.csv',
            '2 Dias': BASE_PATH + 'backtest_short_term_v4_2_dias.csv',
            '1 Dia':  BASE_PATH + 'backtest_short_term_v4_1_dia.csv',
            'Summary': BASE_PATH + 'backtest_short_term_v4_portfolio_summary.csv',
        }
        for path in csvFiles.values():
            if not os.path.exists(path):
                raise FileNotFoundError(f"No encontrado: {path}")

        dfs = {k: pd.read_csv(v) for k, v in csvFiles.items()}
        dfSummary = dfs['Summary']
        print("CSVs V4 cargados correctamente.")

        # KPIs del portafolio
        balanceFinal = dfSummary['Balance_Fin'].iloc[-1]
        balanceInicial = dfSummary['Balance_Inicio'].iloc[0]
        pnlTotal = balanceFinal - balanceInicial
        retornoTotal = (pnlTotal / balanceInicial) * 100
        mejorDia = dfSummary.loc[dfSummary['PnL_Dia'].idxmax()]
        peorDia = dfSummary.loc[dfSummary['PnL_Dia'].idxmin()]

        # Top combos acumulados en todos los periodos
        allTrades = pd.concat([dfs[k] for k in ['5 Dias', '4 Dias', '3 Dias', '2 Dias', '1 Dia']])
        allTrades['pnlNum'] = allTrades['PnL Combo ($)'].str.replace('$', '', regex=False).str.replace(',', '').astype(float)
        topCombos = (
            allTrades.groupby(['Simbolo', 'Estrategia'])['pnlNum']
            .sum()
            .reset_index()
            .sort_values(by='pnlNum', ascending=False)
        )
        topCombos['PnL Acum. ($)'] = topCombos['pnlNum'].apply(lambda x: f"${x:+.2f}")

        # ── PDF ──────────────────────────────────────────────────────────────
        pdf = PortfolioReportPdfV4(orientation='P', unit='mm', format='letter')
        pdf.set_auto_page_break(auto=True, margin=15)

        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 1: PORTADA (Tema Azul Zafiro / Cyan)
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        # Fondo azul marino profundo
        pdf.set_fill_color(8, 15, 28)
        pdf.rect(0, 0, 216, 279, 'F')

        # Marco decorativo doble en cyan
        pdf.set_fill_color(0, 210, 255)
        pdf.rect(18, 18, 180, 2.5, 'F')
        pdf.rect(18, 18, 2.5, 243, 'F')
        pdf.set_fill_color(0, 130, 180)
        pdf.rect(195.5, 18, 2.5, 243, 'F')
        pdf.rect(18, 258.5, 180, 2.5, 'F')

        # Titulo sistema
        pdf.set_y(52)
        pdf.set_text_color(80, 130, 160)
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 8, "S I S T E M A   A T A L A I A   /   S E N T I N E L", 0, 1, 'C')

        # Titulo principal
        pdf.set_text_color(220, 240, 255)
        pdf.set_font('Helvetica', 'B', 26)
        pdf.set_y(72)
        pdf.multi_cell(0, 14, "AUDITORIA V4\nCOMPOUNDING DE\nPORTAFOLIO GLOBAL", 0, 'C')

        # Badge cyan
        pdf.set_fill_color(0, 60, 100)
        pdf.rect(38, 140, 140, 28, 'F')
        pdf.set_fill_color(0, 210, 255)
        pdf.rect(38, 140, 140, 2, 'F')
        pdf.rect(38, 166, 140, 2, 'F')

        pdf.set_y(148)
        pdf.set_text_color(0, 210, 255)
        pdf.set_font('Helvetica', 'B', 13)
        pdf.cell(0, 7, f"Balance Inicial: ${INITIAL_PORTFOLIO:.2f}  ->  Balance Final: ${balanceFinal:.2f}", 0, 1, 'C')
        pdf.set_font('Helvetica', '', 10)
        pdf.set_text_color(160, 210, 240)
        pdf.cell(0, 6, f"PnL Total: ${pnlTotal:+.2f} USD ({retornoTotal:+.1f}%)  |  5 Dias de Operacion", 0, 1, 'C')

        # KPIs portada
        pdf.set_y(182)
        pdf.set_font('Helvetica', '', 9.5)
        pdf.set_text_color(130, 175, 200)
        kpi = (
            f"Metodologia: PnL de TODOS los instrumentos suma al portafolio unico cada dia.\n"
            f"Mejor dia: {mejorDia['Periodo']} con PnL de ${mejorDia['PnL_Dia']:+.2f} USD "
            f"| Combos activos: {int(mejorDia['Combos_Activos'])}\n"
            f"Peor dia:  {peorDia['Periodo']} con PnL de ${peorDia['PnL_Dia']:+.2f} USD\n"
            f"Riesgo: 1% del portafolio por trade | RR Minimo: 1.5 | 14 Simbolos x 14 Estrategias"
        )
        pdf.multi_cell(0, 6, kpi.encode('latin-1', 'replace').decode('latin-1'), 0, 'C')

        # Firma
        pdf.set_y(248)
        pdf.set_font('Helvetica', 'B', 8.5)
        pdf.set_text_color(40, 80, 110)
        pdf.cell(0, 8, "VERSION V4 - DESARROLLADO POR ANTIGRAVITY AI - CONFIDENCIAL", 0, 1, 'C')

        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 2: EVOLUCION DEL PORTAFOLIO DIA A DIA + METODOLOGIA
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 16)
        pdf.cell(0, 10, "1. EVOLUCION DEL PORTAFOLIO - COMPOUNDING GLOBAL V4", 0, 1, 'L')
        pdf.ln(3)

        # Tabla evolucion diaria
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 130, 180)
        pdf.cell(0, 6, "[A] EVOLUCION DEL PORTAFOLIO (Portafolio Unico Compartido)", 0, 1, 'L')
        pdf.ln(2)

        headerEvo = ['Periodo', 'Bal. Inicio ($)', 'PnL del Dia ($)', 'Bal. Final ($)', 'Retorno Dia (%)', 'Combos Activos']
        colEvo = [28, 33, 33, 33, 35, 28]
        dfEvoDisp = dfSummary.copy()
        dfEvoDisp['Balance_Inicio'] = dfEvoDisp['Balance_Inicio'].apply(lambda x: f"${x:.2f}")
        dfEvoDisp['PnL_Dia'] = dfEvoDisp['PnL_Dia'].apply(lambda x: f"${x:+.2f}")
        dfEvoDisp['Balance_Fin'] = dfEvoDisp['Balance_Fin'].apply(lambda x: f"${x:.2f}")
        dfEvoDisp['Retorno_Dia_%'] = dfEvoDisp['Retorno_Dia_%'].apply(lambda x: f"{x:+.2f}%")
        dfEvoDisp['Combos_Activos'] = dfEvoDisp['Combos_Activos'].astype(int).astype(str)
        dfEvoDisp = dfEvoDisp[['Periodo', 'Balance_Inicio', 'PnL_Dia', 'Balance_Fin', 'Retorno_Dia_%', 'Combos_Activos']]
        drawColorTable(pdf, dfEvoDisp, headerEvo, colEvo, maxRows=5)
        pdf.ln(5)

        # Metodologia V4
        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_text_color(0, 130, 180)
        pdf.cell(0, 6, "[B] METODOLOGIA: COMPOUNDING DE PORTAFOLIO GLOBAL", 0, 1, 'L')
        pdf.set_font('Helvetica', '', 9.5)
        pdf.set_text_color(40, 44, 52)
        metodo = (
            "En el modelo V4, el portafolio Sentinel funciona como UNA SOLA CUENTA con $500.00 USD iniciales. "
            "Cada dia activo, el sistema ejecuta TODAS las combinaciones simbolo-estrategia no excluidas. "
            "El PnL de cada combinacion se suma al portafolio global del dia. Al cierre del dia, "
            "el balance acumulado (incluyendo TODOS los wins y losses de todos los instrumentos) "
            "se convierte en el capital del dia siguiente.\n\n"
            "Ejemplo concreto (Dia 5 -> Dia 4):\n"
            "  Balance inicio Dia 5: $500.00\n"
            "  XAU/USD + SilverBullet genera: +$XX.XX | BTC/USD + SpeedBot: +$XX.XX\n"
            "  EUR/USD + Ichimoku: +$XX.XX | ... (todos los combos activos)\n"
            "  Suma total del dia: $YY.YY\n"
            "  Balance inicio Dia 4: $500.00 + $YY.YY = $ZZZ.ZZ\n"
            "  El riesgo del Dia 4 se recalcula al 1% de $ZZZ.ZZ."
        )
        pdf.multi_cell(0, 5, metodo.encode('latin-1', 'replace').decode('latin-1'), 0, 'L')
        pdf.ln(4)

        # Comparativa V3 vs V4
        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_text_color(200, 80, 0)
        pdf.cell(0, 6, "[C] DIFERENCIA CLAVE: V3 vs V4", 0, 1, 'L')
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(40, 44, 52)
        diff = (
            "V3 (Balance Independiente):  Cada (simbolo, estrategia) tenia su propio balance de $500. "
            "El rendimiento era individual y no se consolidaba. Mejor balance terminal: $565.70 (XAU/USD+SilverBullet).\n\n"
            "V4 (Balance de Portafolio):  UN SOLO balance de $500 para todo el sistema. "
            "El PnL de XAU/USD + BTC/USD + EUR/USD + todos los demas se SUMA al portafolio. "
            "Esto refleja la realidad de una cuenta de trading donde todos los bots comparten el mismo capital. "
            "El efecto compuesto es MUCHO mas potente porque el portafolio crece con la suma de todos los "
            "instrumentos ganadores simultaneamente."
        )
        pdf.multi_cell(0, 5, diff.encode('latin-1', 'replace').decode('latin-1'), 0, 'L')

        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 3: TOP COMBOS Y MATRICES DE 5D y 4D
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 15)
        pdf.cell(0, 10, "2. RANKING DE COMBINACIONES Y MATRICES 5D / 4D (V4)", 0, 1, 'L')
        pdf.ln(2)

        # Top 15 combos acumulados
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 130, 180)
        pdf.cell(0, 6, "[A] TOP 15 COMBINACIONES POR PnL ACUMULADO (5 DIAS DE PORTAFOLIO)", 0, 1, 'L')
        pdf.ln(2)
        headersTop = ['Simbolo', 'Estrategia', 'PnL Acum. ($)']
        colTop = [50, 70, 76]
        drawColorTable(pdf, topCombos.head(15)[['Simbolo', 'Estrategia', 'PnL Acum. ($)']], headersTop, colTop, maxRows=15)
        pdf.ln(4)

        # Top 7 del dia 5
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 150, 90)
        pdf.cell(0, 6, "[B] TOP 7 COMBOS EN PERIODO 5 DIAS (Primer dia de portafolio)", 0, 1, 'L')
        pdf.ln(2)
        df5d = dfs['5 Dias'].head(7)[['Simbolo', 'Estrategia', 'Win Rate', 'Total Trades', 'Riesgo/Trade ($)', 'PnL Combo ($)']]
        headers5d = ['Simbolo', 'Estrategia', 'Win Rate', 'Trades', 'Riesgo ($)', 'PnL Combo ($)']
        colWidths5d = [30, 42, 22, 18, 28, 36]
        drawColorTable(pdf, df5d, headers5d, colWidths5d, maxRows=7)
        pdf.ln(4)

        # Top 7 del dia 4
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 100, 180)
        pdf.cell(0, 6, "[C] TOP 7 COMBOS EN PERIODO 4 DIAS (Balance compuesto activo)", 0, 1, 'L')
        pdf.ln(2)
        df4d = dfs['4 Dias'].head(7)[['Simbolo', 'Estrategia', 'Win Rate', 'Total Trades', 'Riesgo/Trade ($)', 'PnL Combo ($)']]
        drawColorTable(pdf, df4d, headers5d, colWidths5d, maxRows=7)

        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 4: MATRICES DE 3D, 2D y 1D
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 15)
        pdf.cell(0, 10, "3. MATRICES DE RENDIMIENTO V4 - 3, 2 Y 1 DIA (PORTAFOLIO VIVO)", 0, 1, 'L')
        pdf.ln(2)

        for label, color, dayDf in [
            ('3 DIAS', (0, 150, 90),  dfs['3 Dias']),
            ('2 DIAS', (0, 100, 180), dfs['2 Dias']),
            ('1 DIA',  (200, 80, 0),  dfs['1 Dia']),
        ]:
            pdf.set_font('Helvetica', 'B', 10)
            pdf.set_text_color(*color)
            pdf.cell(0, 6, f"[{'ABC'[['3 DIAS', '2 DIAS', '1 DIA'].index(label)]}] TOP 5 COMBOS - {label} (Balance portafolio acumulado)", 0, 1, 'L')
            pdf.ln(2)
            dfDisp = dayDf.head(5)[['Simbolo', 'Estrategia', 'Win Rate', 'Total Trades', 'Riesgo/Trade ($)', 'PnL Combo ($)']]
            drawColorTable(pdf, dfDisp, headers5d, colWidths5d, maxRows=5)
            pdf.ln(4)

        # Bottom 5 (mayor drawdown)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(200, 50, 70)
        pdf.cell(0, 6, "[D] BOTTOM 5 COMBOS DEL DIA 1 (Mayor drawdown al portafolio)", 0, 1, 'L')
        pdf.ln(2)
        dfBot = dfs['1 Dia'].tail(5)[['Simbolo', 'Estrategia', 'Win Rate', 'Total Trades', 'Riesgo/Trade ($)', 'PnL Combo ($)']]
        drawColorTable(pdf, dfBot, headers5d, colWidths5d, maxRows=5)

        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 5: DIRECTRICES V4 + RESUMEN BD
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 15)
        pdf.cell(0, 10, "4. DIRECTRICES TACTICAS V4 Y ACTUALIZACION DE BASE DE DATOS", 0, 1, 'L')
        pdf.ln(3)

        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_text_color(0, 150, 200)
        pdf.cell(0, 6, "[*] PLAN DE ACCION V4 PARA PORTAFOLIO VIVO ($500 -> COMPUESTO)", 0, 1, 'L')
        pdf.ln(2)

        directrices = [
            ("1. Implementar Sentinel como gestor de portafolio unico:",
             f" El portafolio inicio en ${INITIAL_PORTFOLIO:.2f} USD y crece de forma acumulada con TODOS los bots. "
             f"El balance terminal es ${balanceFinal:.2f} USD (PnL: ${pnlTotal:+.2f}, {retornoTotal:+.1f}%). "
             "Configurar el motor Sentinel con un balance de cuenta unico y dejar que cada bot use 1% del balance real."),
            ("2. Combos de alto impacto en el portafolio (mayor PnL acumulado):",
             " XAU/USD+SilverBullet y BTC/USD+SpeedBot lideran el PnL acumulado en todos los periodos. "
             "Patron4h aporta consistencia transversal en multiples pares. "
             "Mantener estos bots SIEMPRE activos; son los motores del compounding."),
            ("3. Nuevas exclusiones V4 aplicadas en la BD:",
             " USD/HKD excluido de todos los bots de scalping e imbalances (par pegged, sin expansion). "
             "GBP/CAD excluido de FVGDiario (correlacion USD/CAD genera falsos setups). "
             "NZD/USD excluido de EMA20200 (baja volatilidad drena portafolio). "
             "SMA20_200 y Sniper permanecen DESHABILITADAS globalmente en strategyConfig."),
            ("4. Monitorizacion del portafolio cada 24 horas:",
             " Con el modelo V4, verificar el balance real de la cuenta al inicio de cada dia y actualizar "
             "el sizing de riesgo en Sentinel (1% del balance real). Si el portafolio supera $550 en 5 dias, "
             "el bot automaticamente opera con $5.50 de riesgo por combo en lugar de $5.00."),
            ("5. Patron4h post-refactor como motor SMC Top-Down:",
             " Tras la integracion con technical.detect_fvgs, Patron4h es ahora el bot mas consistente "
             "en terminos de combinaciones activas. Aporta PnL en 13 de los 14 simbolos activos, "
             "siendo el mayor contribuidor transversal del portafolio en el modelo V4.")
        ]

        for title, desc in directrices:
            pdf.set_font('Helvetica', 'B', 9.5)
            pdf.cell(0, 5.5, title.encode('latin-1', 'replace').decode('latin-1'), 0, 1, 'L')
            pdf.set_font('Helvetica', '', 9)
            pdf.multi_cell(0, 4.5, desc.encode('latin-1', 'replace').decode('latin-1'), 0, 'L')
            pdf.ln(2.5)

        # Banner final
        pdf.ln(3)
        pdf.set_fill_color(8, 25, 45)
        pdf.rect(10, pdf.get_y(), 196, 18, 'F')
        pdf.set_fill_color(0, 210, 255)
        pdf.rect(10, pdf.get_y(), 196, 2, 'F')
        pdf.set_y(pdf.get_y() + 4)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(0, 210, 255)
        pdf.cell(0, 5, f"   [+] AUDITORIA V4 COMPLETADA | Portafolio: ${balanceFinal:.2f} ({retornoTotal:+.1f}% en 5 dias)", 0, 1, 'L')
        pdf.set_font('Helvetica', 'I', 8)
        pdf.set_text_color(80, 160, 200)
        pdf.cell(0, 5, "   BD actualizada con exclusiones V4. SMA20_200 y Sniper deshabilitadas. USD/HKD y GBP/CAD parcialmente excluidos.", 0, 1, 'L')

        # Guardar PDF
        pdfPath = BASE_PATH + "reporte_backtest_corto_plazo_v4.pdf"
        pdf.output(pdfPath, 'F')
        print(f"✅ Reporte PDF V4 generado en {pdfPath}")

    except Exception as e:
        print(f"❌ Error generando PDF V4: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    generatePortfolioPdfV4()
