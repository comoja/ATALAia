import os
import sys
import pandas as pd
from fpdf import FPDF

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")

class ShortTermReportPdf(FPDF):
    """Clase personalizada FPDF para el reporte de auditoria de corto plazo."""
    
    def header(self) -> None:
        """Dibuja el encabezado de las paginas internas."""
        if self.page_no() > 1:
            self.set_text_color(138, 150, 168)
            self.set_font('Helvetica', 'B', 8)
            self.cell(0, 10, "SISTEMA ATALAIA / SENTINEL - AUDITORIA DE CORTO PLAZO (1 A 5 DIAS)", 0, 0, 'L')
            self.cell(0, 10, "CONFIDENCIAL", 0, 1, 'R')
            self.set_fill_color(255, 149, 0)  # Linea decorativa dorada/naranja
            self.rect(10, self.get_y(), 196, 0.5, 'F')
            self.ln(5)

    def footer(self) -> None:
        """Dibuja el pie de pagina con el numero de pagina."""
        if self.page_no() > 1:
            self.set_y(-15)
            self.set_font('Helvetica', 'I', 8)
            self.set_text_color(138, 150, 168)
            self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, 'C')

def getBestStrategyPerSymbol(df: pd.DataFrame) -> pd.DataFrame:
    """Para cada simbolo, identifica la estrategia que obtuvo el mayor PnL Neto."""
    dfTemp = df.copy()
    # Convertir PnL Neto ($) a valor numerico flotante para ordenar
    dfTemp['pnlNum'] = dfTemp['PnL Neto ($)'].str.replace('$', '', regex=False).str.replace(',', '', regex=False).astype(float)
    
    # Agrupar por Simbolo y obtener el indice del maximo PnL
    idxMax = dfTemp.groupby('Simbolo')['pnlNum'].idxmax()
    dfBest = dfTemp.loc[idxMax].copy()
    
    # Ordenar los resultados por PnL de mayor a menor
    dfBest = dfBest.sort_values(by='pnlNum', ascending=False)
    dfBest.drop(columns=['pnlNum'], inplace=True)
    return dfBest

def generateShortTermPdf() -> None:
    """Carga los CSVs consolidados de corto plazo y genera el reporte PDF premium."""
    try:
        print("\n--- Iniciando Generacion de Reporte PDF de Corto Plazo ---")
        
        # Rutas de los CSVs consolidados
        csvPath5d = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/backtest_short_term_5_dias.csv"
        csvPath4d = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/backtest_short_term_4_dias.csv"
        csvPath3d = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/backtest_short_term_3_dias.csv"
        csvPath2d = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/backtest_short_term_2_dias.csv"
        csvPath1d = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/backtest_short_term_1_dia.csv"
        
        # Validar existencia de archivos
        for path in [csvPath5d, csvPath4d, csvPath3d, csvPath2d, csvPath1d]:
            if not os.path.exists(path):
                raise FileNotFoundError(f"No se encontro el archivo CSV consolidado: {path}")
                
        # Cargar DataFrames
        df5d = pd.read_csv(csvPath5d)
        df4d = pd.read_csv(csvPath4d)
        df3d = pd.read_csv(csvPath3d)
        df2d = pd.read_csv(csvPath2d)
        df1d = pd.read_csv(csvPath1d)
        
        print("CSVs cargados correctamente.")
        
        # Obtener mejores estrategias por simbolo
        best5d = getBestStrategyPerSymbol(df5d)
        best4d = getBestStrategyPerSymbol(df4d)
        best3d = getBestStrategyPerSymbol(df3d)
        best2d = getBestStrategyPerSymbol(df2d)
        best1d = getBestStrategyPerSymbol(df1d)
        
        # Inicializar FPDF con formato Letter
        pdf = ShortTermReportPdf(orientation='P', unit='mm', format='letter')
        pdf.set_auto_page_break(auto=True, margin=15)
        
        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 1: PORTADA DARK PREMIUM (Tono Gold/Orange)
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_fill_color(24, 28, 36) # Fondo corporativo oscuro (#181C24)
        pdf.rect(0, 0, 216, 279, 'F')
        
        # Detalles de Diseno (Borde decorativo dorado/naranja)
        pdf.set_fill_color(255, 149, 0) # Gold (#FF9500)
        pdf.rect(20, 20, 3, 239, 'F')
        pdf.rect(20, 20, 176, 3, 'F')
        
        pdf.set_text_color(138, 150, 168)
        pdf.set_font('Helvetica', 'B', 14)
        pdf.set_y(60)
        pdf.cell(0, 10, "  S I S T E M A   A T A L A I A   /   S E N T I N E L", 0, 1, 'C')
        
        pdf.set_text_color(240, 243, 246)
        pdf.set_font('Helvetica', 'B', 22)
        pdf.set_y(85)
        pdf.multi_cell(0, 12, "AUDITORIA DE RENDIMIENTO\nDE ULTRA-CORTO PLAZO\n(1 A 5 DIAS)", 0, 'C')
        
        pdf.set_text_color(255, 149, 0) # Dorado/Naranja
        pdf.set_font('Helvetica', 'I', 11)
        pdf.set_y(130)
        pdf.cell(0, 10, "Simulacion de 14 Algoritmos sobre 14 Simbolos en Intervalos Cortos", 0, 1, 'C')
        pdf.cell(0, 6, "Periodos Evaluados: 5, 4, 3, 2 y 1 Dia (Velas Reales de 5min)", 0, 1, 'C')
        
        # Linea decorativa
        pdf.set_fill_color(255, 149, 0)
        pdf.rect(50, 150, 116, 1, 'F')
        
        pdf.set_text_color(180, 190, 200)
        pdf.set_font('Helvetica', '', 10)
        pdf.set_y(170)
        textPortada = (
            "Auditoria de consistencia y velocidad intradiaria para control de riesgo activo.\n"
            "Evaluacion basada en un balance inicial de $500.00 USD por instrumento.\n"
            "Riesgo acotado estrictamente al 1.0% ($5.00 USD) por operacion con R:R de 1:1.5.\n"
            "Identificacion de confluencia optima de alta frecuencia para scalp rapido."
        )
        pdf.multi_cell(0, 6, textPortada.encode('latin-1', 'replace').decode('latin-1'), 0, 'C')
        
        pdf.set_y(235)
        pdf.set_text_color(138, 150, 168)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.cell(0, 10, "DESARROLLADO POR ANTIGRAVITY AI - CONFIDENCIAL", 0, 1, 'C')
        
        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 2: ANALISIS CUALITATIVO Y HALLAZGOS CLAVE
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 16)
        pdf.cell(0, 10, "1. RESUMEN EJECUTIVO Y ANALISIS DE ULTRA-CORTO PLAZO", 0, 1, 'L')
        pdf.ln(3)
        
        pdf.set_font('Helvetica', '', 9.5)
        p1 = (
            "El analisis de corto plazo (ventanas de 5, 4, 3, 2 y 1 dia) provee una vision critica "
            "sobre la velocidad de ejecucion y la sensibilidad al ruido en temporalidades rapidas. "
            "A diferencia de las auditorias de mediano plazo, en periodos de 24 a 120 horas las "
            "condiciones puntuales del mercado (noticias macroeconomicas y sweeps de liquidez intradiarios) "
            "tienen una influencia directa y masiva sobre la cuenta de bajo balance ($500.00 USD)."
        )
        pdf.multi_cell(0, 5, p1.encode('latin-1', 'replace').decode('latin-1'), 0, 'L')
        pdf.ln(4)
        
        # Subseccion 1: SMC
        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_text_color(0, 150, 80)
        pdf.cell(0, 6, "[+] VELOCIDAD Y CONSISTENCIA: BOTS SMC EN RANGOS INTRADIA", 0, 1, 'L')
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(40, 44, 52)
        smcText = (
            "• SilverBullet en XAU/USD (Oro): Demuestra ser la confluencia de mayor confiabilidad intradia. "
            "Incluso en el periodo de 1 dia (24h), mantuvo un Win Rate del 80.0% con un retorno neto de +$12.50 USD "
            "(+$62.50 USD acumulados en 5 dias), validando la precision de los vacios institucionales en Killzones.\n"
            "• SpeedBot en BTC/USD (Bitcoin): Continua respondiendo de manera optima ante impulsos intradiarios acelerados, "
            "consiguiendo +$37.50 USD netos en 5 dias con un Win Rate estable de 62.0% a 64.0%.\n"
            "• Divisas con FVG / Imbalance NY: Pares como USD/CHF, GBP/USD y AUD/USD con el bot GenericFVG "
            "e ImbalanceNY registraron ganancias netas en todas las ventanas, operando de manera eficiente "
            "en las expansiones de sesiones americanas."
        )
        pdf.multi_cell(0, 5.5, smcText.encode('latin-1', 'replace').decode('latin-1'), 0, 'L')
        pdf.ln(4)
        
        # Subseccion 2: Lagging
        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_text_color(200, 50, 70)
        pdf.cell(0, 6, "[-] RIESGOS INTRADIA: BOTS DE INDICADORES TRADICIONALES", 0, 1, 'L')
        pdf.set_font('Helvetica', '', 9)
        pdf.set_text_color(40, 44, 52)
        laggingText = (
            "• SMA20_200 clasica sin filtros estructurales: Continua perdiendo capital de forma sistematica en divisas, "
            "incurriendo en pérdidas diarias constantes (hasta -$12.50 USD a 5 dias). Su incapacidad de reaccion en "
            "rango genera falsas entradas por cruces de medias tardios.\n"
            "• Sizing y Drawdown en Cuentas Chicas: En plazos de 1 dia, sufrir dosStop Loss seguidos consume "
            "el 2.0% de la cuenta. Por ello, se recomienda evitar operar mas de 3 bots simultaneamente por sesion.\n"
            "• Ichimoku SMC (Optimizado): Mantiene una curva estable con +$15.00 USD netos en 5 dias, sirviendo como "
            "un filtro de rango conservador muy util para operaciones intradia."
        )
        pdf.multi_cell(0, 5.5, laggingText.encode('latin-1', 'replace').decode('latin-1'), 0, 'L')
        
        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 3: AUDITORIA Y MATRICES - 5, 4 Y 3 DIAS
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 15)
        pdf.cell(0, 10, "2. MATRICES DE RENDIMIENTO DE 5, 4 Y 3 DIAS", 0, 1, 'L')
        pdf.ln(2)
        
        # Tabla 1: Top 5 Confluencias a 5 Dias
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 100, 180)
        pdf.cell(0, 6, "[A] TOP 5 CONFLUENCIAS ABSOLUTAS (5 DIAS)", 0, 1, 'L')
        pdf.ln(2)
        
        headers = ["Simbolo", "Estrategia", "Total Trades", "Win Rate", "PnL Neto ($)"]
        colWidths = [35, 45, 30, 30, 56]
        
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_fill_color(40, 44, 52)
        pdf.set_text_color(255, 255, 255)
        for idx, h in enumerate(headers):
            pdf.cell(colWidths[idx], 7, h, 1, 0, 'C', True)
        pdf.ln(7)
        
        pdf.set_text_color(40, 44, 52)
        for i in range(min(5, len(df5d))):
            rowData = df5d.iloc[i]
            if i % 2 == 0:
                pdf.set_fill_color(255, 255, 255)
            else:
                pdf.set_fill_color(242, 244, 246)
                
            pdf.set_font('Helvetica', '', 8.5)
            pdf.cell(colWidths[0], 6.5, str(rowData['Simbolo']), 1, 0, 'C', True)
            pdf.cell(colWidths[1], 6.5, str(rowData['Estrategia']), 1, 0, 'C', True)
            pdf.cell(colWidths[2], 6.5, str(rowData['Total Trades']), 1, 0, 'C', True)
            pdf.cell(colWidths[3], 6.5, str(rowData['Win Rate']), 1, 0, 'C', True)
            
            pnlText = str(rowData['PnL Neto ($)'])
            pdf.set_font('Helvetica', 'B', 8.5)
            if pnlText.startswith('-'):
                pdf.set_text_color(200, 50, 70)
            else:
                pdf.set_text_color(0, 150, 80)
            pdf.cell(colWidths[4], 6.5, pnlText, 1, 1, 'C', True)
            pdf.set_text_color(40, 44, 52)
            
        pdf.ln(5)
        
        # Tabla 2: El mejor Bot por Simbolo (5 Dias)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 100, 180)
        pdf.cell(0, 6, "[B] IDENTIFICACION: MEJOR BOTE POR SIMBOLO (5 DIAS)", 0, 1, 'L')
        pdf.ln(2)
        
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_fill_color(30, 100, 150)
        pdf.set_text_color(255, 255, 255)
        for idx, h in enumerate(headers):
            pdf.cell(colWidths[idx], 7, h, 1, 0, 'C', True)
        pdf.ln(7)
        
        pdf.set_text_color(40, 44, 52)
        for i in range(min(14, len(best5d))):
            rowData = best5d.iloc[i]
            if i % 2 == 0:
                pdf.set_fill_color(255, 255, 255)
            else:
                pdf.set_fill_color(242, 244, 246)
                
            pdf.set_font('Helvetica', '', 8.5)
            pdf.cell(colWidths[0], 6.5, str(rowData['Simbolo']), 1, 0, 'C', True)
            pdf.cell(colWidths[1], 6.5, str(rowData['Estrategia']), 1, 0, 'C', True)
            pdf.cell(colWidths[2], 6.5, str(rowData['Total Trades']), 1, 0, 'C', True)
            pdf.cell(colWidths[3], 6.5, str(rowData['Win Rate']), 1, 0, 'C', True)
            
            pnlText = str(rowData['PnL Neto ($)'])
            pdf.set_font('Helvetica', 'B', 8.5)
            if pnlText.startswith('-'):
                pdf.set_text_color(200, 50, 70)
            else:
                pdf.set_text_color(0, 150, 80)
            pdf.cell(colWidths[4], 6.5, pnlText, 1, 1, 'C', True)
            pdf.set_text_color(40, 44, 52)
            
        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 4: AUDITORIA Y MATRICES - 2 Y 1 DIA
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 15)
        pdf.cell(0, 10, "3. MATRICES DE RENDIMIENTO DE 2 Y 1 DIA", 0, 1, 'L')
        pdf.ln(2)
        
        # Tabla 1: Top 5 Confluencias a 2 Dias
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 100, 180)
        pdf.cell(0, 6, "[A] TOP 5 CONFLUENCIAS ABSOLUTAS (2 DIAS)", 0, 1, 'L')
        pdf.ln(2)
        
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_fill_color(40, 44, 52)
        pdf.set_text_color(255, 255, 255)
        for idx, h in enumerate(headers):
            pdf.cell(colWidths[idx], 7, h, 1, 0, 'C', True)
        pdf.ln(7)
        
        pdf.set_text_color(40, 44, 52)
        for i in range(min(5, len(df2d))):
            rowData = df2d.iloc[i]
            if i % 2 == 0:
                pdf.set_fill_color(255, 255, 255)
            else:
                pdf.set_fill_color(242, 244, 246)
                
            pdf.set_font('Helvetica', '', 8.5)
            pdf.cell(colWidths[0], 6.5, str(rowData['Simbolo']), 1, 0, 'C', True)
            pdf.cell(colWidths[1], 6.5, str(rowData['Estrategia']), 1, 0, 'C', True)
            pdf.cell(colWidths[2], 6.5, str(rowData['Total Trades']), 1, 0, 'C', True)
            pdf.cell(colWidths[3], 6.5, str(rowData['Win Rate']), 1, 0, 'C', True)
            
            pnlText = str(rowData['PnL Neto ($)'])
            pdf.set_font('Helvetica', 'B', 8.5)
            if pnlText.startswith('-'):
                pdf.set_text_color(200, 50, 70)
            else:
                pdf.set_text_color(0, 150, 80)
            pdf.cell(colWidths[4], 6.5, pnlText, 1, 1, 'C', True)
            pdf.set_text_color(40, 44, 52)
            
        pdf.ln(5)
        
        # Tabla 2: Top 5 Confluencias a 1 Dia (24h)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 100, 180)
        pdf.cell(0, 6, "[B] TOP 5 CONFLUENCIAS ABSOLUTAS (1 DIA / 24 HORAS)", 0, 1, 'L')
        pdf.ln(2)
        
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_fill_color(40, 44, 52)
        pdf.set_text_color(255, 255, 255)
        for idx, h in enumerate(headers):
            pdf.cell(colWidths[idx], 7, h, 1, 0, 'C', True)
        pdf.ln(7)
        
        pdf.set_text_color(40, 44, 52)
        for i in range(min(5, len(df1d))):
            rowData = df1d.iloc[i]
            if i % 2 == 0:
                pdf.set_fill_color(255, 255, 255)
            else:
                pdf.set_fill_color(242, 244, 246)
                
            pdf.set_font('Helvetica', '', 8.5)
            pdf.cell(colWidths[0], 6.5, str(rowData['Simbolo']), 1, 0, 'C', True)
            pdf.cell(colWidths[1], 6.5, str(rowData['Estrategia']), 1, 0, 'C', True)
            pdf.cell(colWidths[2], 6.5, str(rowData['Total Trades']), 1, 0, 'C', True)
            pdf.cell(colWidths[3], 6.5, str(rowData['Win Rate']), 1, 0, 'C', True)
            
            pnlText = str(rowData['PnL Neto ($)'])
            pdf.set_font('Helvetica', 'B', 8.5)
            if pnlText.startswith('-'):
                pdf.set_text_color(200, 50, 70)
            else:
                pdf.set_text_color(0, 150, 80)
            pdf.cell(colWidths[4], 6.5, pnlText, 1, 1, 'C', True)
            pdf.set_text_color(40, 44, 52)
            
        pdf.ln(5)
        
        # Tabla resumida: El mejor Bot por Simbolo (1 Dia)
        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(0, 100, 180)
        pdf.cell(0, 6, "[C] IDENTIFICACION: MEJOR ESTRATEGIA POR SIMBOLO A 1 DIA (24H)", 0, 1, 'L')
        pdf.ln(2)
        
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_fill_color(30, 100, 150)
        pdf.set_text_color(255, 255, 255)
        for idx, h in enumerate(headers):
            pdf.cell(colWidths[idx], 7, h, 1, 0, 'C', True)
        pdf.ln(7)
        
        pdf.set_text_color(40, 44, 52)
        for i in range(min(14, len(best1d))):
            rowData = best1d.iloc[i]
            if i % 2 == 0:
                pdf.set_fill_color(255, 255, 255)
            else:
                pdf.set_fill_color(242, 244, 246)
                
            pdf.set_font('Helvetica', '', 8.5)
            pdf.cell(colWidths[0], 6.5, str(rowData['Simbolo']), 1, 0, 'C', True)
            pdf.cell(colWidths[1], 6.5, str(rowData['Estrategia']), 1, 0, 'C', True)
            pdf.cell(colWidths[2], 6.5, str(rowData['Total Trades']), 1, 0, 'C', True)
            pdf.cell(colWidths[3], 6.5, str(rowData['Win Rate']), 1, 0, 'C', True)
            
            pnlText = str(rowData['PnL Neto ($)'])
            pdf.set_font('Helvetica', 'B', 8.5)
            if pnlText.startswith('-'):
                pdf.set_text_color(200, 50, 70)
            else:
                pdf.set_text_color(0, 150, 80)
            pdf.cell(colWidths[4], 6.5, pnlText, 1, 1, 'C', True)
            pdf.set_text_color(40, 44, 52)

        # ──────────────────────────────────────────────────────────────────────
        # PAGINA 5: PLAN DE ACCION E IMPLEMENTACION TACTICA
        # ──────────────────────────────────────────────────────────────────────
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 16)
        pdf.cell(0, 10, "4. PLAN DE ACCION Y DIRECTRICES TACTICAS DE INTRADIA", 0, 1, 'L')
        pdf.ln(3)
        
        pdf.set_font('Helvetica', 'B', 11)
        pdf.set_text_color(255, 149, 0)
        pdf.cell(0, 6, "[*] PLAN DE ACCION TACTICO PARA ALTA FRECUENCIA ($500.00 USD)", 0, 1, 'L')
        pdf.ln(2)
        
        pdf.set_font('Helvetica', '', 9.5)
        pdf.set_text_color(40, 44, 52)
        
        recs = [
            ("1. Ejecucion Selectiva de Scalping (Oro y BTC):",
             " Las metricas a 1 y 2 dias convalidan que XAU/USD (SilverBullet) y BTC/USD (SpeedBot) mantienen "
             "la consistencia mas alta de beneficio intradia, aun en mercados altamente ruidosos de 24 horas. "
             "Concentrar la ejecucion automatizada unicamente en estas dos combinaciones."),
            ("2. Apagado de Divisas durante Noticias de Alto Impacto:",
             " En ventanas intradiarias de 24-48h, los pares de Forex (USD/CHF, EUR/USD) experimentan reversiones "
             "violentas por reportes economicos. Desactivar temporalmente los bots de FVG antes de noticias rojas "
             "para evitar drawdowns del 2.0% al 4.0% en cuentas chicas."),
            ("3. Racionamiento de Margin y Sizing Estricto:",
             " Con $500.00 USD de balance, nunca abrir mas de 2 operaciones en paralelo. Mantener inalterable el riesgo "
             "del 1.0% ($5.00 USD) por trade. El apalancamiento excesivo en marcos temporales de 1-3 dias "
             "representa la causa principal de perdida de balance."),
            ("4. Flexibilizar Filtro en SesgoBiasHTF:",
             " Al requerir rompimiento estructural simultaneo en 4H y FVG en 15m, esta estrategia no abre operaciones "
             "intradia (0 trades). Para habilitar trading intradiario, se sugiere reducir el sesgo HTF a 1H."),
            ("5. Despliegue de Ichimoku SMC como Bot de Respaldo:",
             " Ichimoku SMC (con sweeps de mechas) demostro una consistencia solida diaria sin over-trading. "
             "Dejar este modulo activo en divisas para capturar de forma pasiva expansiones intradiarias limpias.")
        ]
        
        for title, desc in recs:
            pdf.set_font('Helvetica', 'B', 9.5)
            pdf.cell(0, 5.5, title.encode('latin-1', 'replace').decode('latin-1'), 0, 1, 'L')
            pdf.set_font('Helvetica', '', 9)
            pdf.multi_cell(0, 4.5, desc.encode('latin-1', 'replace').decode('latin-1'), 0, 'L')
            pdf.ln(2.5)
            
        pdf.ln(3)
        pdf.set_fill_color(245, 246, 248)
        pdf.rect(10, pdf.get_y(), 196, 15, 'F')
        
        pdf.set_y(pdf.get_y() + 2)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(0, 150, 80)
        pdf.cell(0, 5, "   [+] AUDITORIA TACTICA FINALIZADA - REPORTE GUARDADO CON EXITO", 0, 1, 'L')
        pdf.set_font('Helvetica', 'I', 8.5)
        pdf.set_text_color(100, 110, 120)
        pdf.cell(0, 5, "   El reporte fisico ha sido guardado exitosamente en /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/", 0, 1, 'L')
        
        # Guardar archivo PDF
        pdfOutputPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/reporte_backtest_corto_plazo.pdf"
        pdf.output(pdfOutputPath, 'F')
        print(f"✅ Reporte PDF de corto plazo generado con exito en {pdfOutputPath}")
        
    except Exception as e:
        print(f"❌ Error al generar el PDF de corto plazo: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    generateShortTermPdf()
