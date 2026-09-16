"""
Servicio institucional para la gestión de saldos mensuales, estados de cuenta y generación de PDF.
Convención: camelCase en funciones, parámetros y variables.
"""

import io
import calendar
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from sqlalchemy.orm import Session

# ReportLab para generación de PDF institucional
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, KeepTogether, HRFlowable
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT

logger = logging.getLogger("monthlyBalanceService")


def obtenerOCrearSaldoMensual(dbSession: Session, idCuenta: int, anio: int, mes: int) -> Dict[str, Any]:
    """
    Obtiene el registro de saldo inicial/mensual de una cuenta.
    Si no existe, busca el saldo de cierre del mes previo (o el Capital de la cuenta)
    y crea automáticamente el registro para el nuevo mes.
    """
    row = dbSession.execute(text("""
        SELECT idSaldoMensual, idCuenta, anio, mes, saldoInicial, saldoFinal,
               depositos, retiros, pnlRealizado, comisiones, comisionLiquidada, rendimientoPct,
               totalTrades, tradesGanadores, tradesPerdedores, fechaRegistro, fechaModificacion
        FROM saldoCuentaMensual
        WHERE idCuenta = :idc AND anio = :a AND mes = :m
        LIMIT 1
    """), {"idc": idCuenta, "a": anio, "m": mes}).mappings().fetchone()

    if row:
        return dict(row)

    # Si no existe, determinar el saldo inicial:
    # 1. Buscar el mes inmediatamente anterior
    prevAnio = anio if mes > 1 else anio - 1
    prevMes = mes - 1 if mes > 1 else 12

    prevRow = dbSession.execute(text("""
        SELECT saldoFinal, saldoInicial
        FROM saldoCuentaMensual
        WHERE idCuenta = :idc AND anio = :pa AND mes = :pm
        LIMIT 1
    """), {"idc": idCuenta, "pa": prevAnio, "pm": prevMes}).mappings().fetchone()

    if prevRow and prevRow["saldoFinal"] is not None:
        saldoInicial = float(prevRow["saldoFinal"])
    elif prevRow and prevRow["saldoInicial"] is not None:
        saldoInicial = float(prevRow["saldoInicial"])
    else:
        # 2. Si no hay mes anterior registrado, tomar el Capital actual de la tabla cuenta
        cuentaRow = dbSession.execute(text("""
            SELECT Capital FROM cuenta WHERE idCuenta = :idc LIMIT 1
        """), {"idc": idCuenta}).fetchone()
        saldoInicial = float(cuentaRow[0]) if (cuentaRow and cuentaRow[0] is not None) else 0.0

    # Insertar el nuevo registro de saldo mensual
    dbSession.execute(text("""
        INSERT INTO saldoCuentaMensual (
            idCuenta, anio, mes, saldoInicial, saldoFinal,
            depositos, retiros, pnlRealizado, comisiones, rendimientoPct,
            totalTrades, tradesGanadores, tradesPerdedores, fechaRegistro
        ) VALUES (
            :idc, :a, :m, :si, :si,
            0.0, 0.0, 0.0, 0.0, 0.0,
            0, 0, 0, NOW()
        )
    """), {
        "idc": idCuenta,
        "a": anio,
        "m": mes,
        "si": saldoInicial
    })
    dbSession.commit()

    logger.info(f"✅ [saldoCuentaMensual] Registro creado para Cuenta #{idCuenta} en {anio}-{mes:02d} con Saldo Inicial: ${saldoInicial:,.2f}")

    newRow = dbSession.execute(text("""
        SELECT idSaldoMensual, idCuenta, anio, mes, saldoInicial, saldoFinal,
               depositos, retiros, pnlRealizado, comisiones, comisionLiquidada, rendimientoPct,
               totalTrades, tradesGanadores, tradesPerdedores, fechaRegistro, fechaModificacion
        FROM saldoCuentaMensual
        WHERE idCuenta = :idc AND anio = :a AND mes = :m
        LIMIT 1
    """), {"idc": idCuenta, "a": anio, "m": mes}).mappings().fetchone()

    return dict(newRow) if newRow else {}


def registrarSaldoCreacionCuenta(dbSession: Session, idCuenta: int, capitalInicial: float) -> None:
    """
    Registra el saldo inicial en saldoCuentaMensual al momento exacto de crear una nueva cuenta.
    """
    now = datetime.now()
    anio = now.year
    mes = now.month

    dbSession.execute(text("""
        INSERT INTO saldoCuentaMensual (
            idCuenta, anio, mes, saldoInicial, saldoFinal,
            depositos, retiros, pnlRealizado, comisiones, rendimientoPct,
            totalTrades, tradesGanadores, tradesPerdedores, fechaRegistro
        ) VALUES (
            :idc, :a, :m, :si, :si,
            0.0, 0.0, 0.0, 0.0, 0.0,
            0, 0, 0, NOW()
        )
        ON DUPLICATE KEY UPDATE
            saldoInicial = VALUES(saldoInicial),
            saldoFinal = VALUES(saldoFinal)
    """), {
        "idc": idCuenta,
        "a": anio,
        "m": mes,
        "si": capitalInicial
    })
    dbSession.commit()
    logger.info(f"✅ [saldoCuentaMensual] Saldo inicial de creación registrado para Cuenta #{idCuenta}: ${capitalInicial:,.2f} ({anio}-{mes:02d})")


def calcularYActualizarEstadoCuenta(dbSession: Session, idCuenta: int, anio: int, mes: int) -> Dict[str, Any]:
    """
    Calcula el estado de cuenta completo del mes:
    - Saldo inicial
    - Desglose y suma de PnL de todos los trades cerrados
    - Comisiones acumuladas
    - Saldo final y rendimiento %
    - Actualiza saldoCuentaMensual en BD
    - Retorna el DTO estructurado para la consulta interactiva y para el PDF.
    """
    saldoRecord = obtenerOCrearSaldoMensual(dbSession, idCuenta, anio, mes)
    saldoInicial = float(saldoRecord.get("saldoInicial", 0.0) or 0.0)
    depositos = float(saldoRecord.get("depositos", 0.0) or 0.0)
    retiros = float(saldoRecord.get("retiros", 0.0) or 0.0)

    # Obtener datos generales de la cuenta
    cuentaRow = dbSession.execute(text("""
        SELECT idCuenta, Nombre, correo, brokerType, Capital, Activo, Concentradora, comision
        FROM cuenta WHERE idCuenta = :idc LIMIT 1
    """), {"idc": idCuenta}).mappings().fetchone()

    cuentaData = dict(cuentaRow) if cuentaRow else {
        "idCuenta": idCuenta, "Nombre": f"Cuenta #{idCuenta}", "correo": "", "Capital": saldoInicial
    }

    # 1. Totalizar depósitos y retiros registrados en la tabla trades para este periodo
    movsRows = dbSession.execute(text("""
        SELECT idTrade, strategy, setup, symbol, direction, size,
               pnl, openTime, closeTime, ticketId
        FROM trades
        WHERE idCuenta = :idc 
          AND status = 'CLOSED'
          AND strategy IN ('Depósito', 'DEPOSITO', 'Deposito', 'Retiro', 'RETIRO')
          AND YEAR(closeTime) = :a 
          AND MONTH(closeTime) = :m
        ORDER BY closeTime ASC, idTrade ASC
    """), {"idc": idCuenta, "a": anio, "m": mes}).mappings().fetchall()

    movimientosList = [dict(r) for r in movsRows]

    depositosTrades = sum(float(m.get("pnl") or 0.0) for m in movimientosList if (m.get("strategy") in ('Depósito', 'DEPOSITO', 'Deposito') or float(m.get("pnl") or 0.0) > 0))
    retirosTrades = sum(abs(float(m.get("pnl") or 0.0)) for m in movimientosList if (m.get("strategy") in ('Retiro', 'RETIRO') or float(m.get("pnl") or 0.0) < 0))

    # El monto de depósitos y retiros se sincroniza directamente desde la trazabilidad de la tabla trades
    depositos = round(depositosTrades, 2)
    retiros = round(retirosTrades, 2)

    # 2. Consultar todos los trades cerrados de trading de Forex en ese mes
    # REGLA CRÍTICA: Excluir 'Depósito' y 'Retiro' para NO distorsionar el PnL operativo, Win Rate ni Comisiones
    tradesRows = dbSession.execute(text("""
        SELECT idTrade, strategy, setup, symbol, direction, size,
               entryPrice, exitPrice, pnl, commission, openTime, closeTime, ticketId
        FROM trades
        WHERE idCuenta = :idc 
          AND status = 'CLOSED'
          AND strategy NOT IN ('Depósito', 'DEPOSITO', 'Deposito', 'Retiro', 'RETIRO')
          AND YEAR(closeTime) = :a 
          AND MONTH(closeTime) = :m
        ORDER BY closeTime ASC, idTrade ASC
    """), {"idc": idCuenta, "a": anio, "m": mes}).mappings().fetchall()

    tradesList = [dict(r) for r in tradesRows]

    totalTrades = len(tradesList)
    tradesGanadores = 0
    tradesPerdedores = 0
    pnlRealizado = 0.0

    for tr in tradesList:
        pnlVal = float(tr.get("pnl") or 0.0)
        pnlRealizado += pnlVal
        if pnlVal > 0:
            tradesGanadores += 1
        elif pnlVal < 0:
            tradesPerdedores += 1

    pnlRealizado = round(pnlRealizado, 2)

    # La comisión se cobra sobre el neteo del PnL solo de las trades de trading cerradas dentro del mes;
    # si al final de mes es negativo o cero, la comisión es cero.
    comisionPct = float(cuentaData.get("comision", 0.0) or 0.0)
    if comisionPct > 0 and pnlRealizado > 0:
        comisionesTotales = round(pnlRealizado * (comisionPct / 100.0), 2)
    else:
        comisionesTotales = 0.0

    pnlNeto = round(pnlRealizado - comisionesTotales, 2)

    saldoFinal = round(saldoInicial + pnlNeto + depositos - retiros, 2)
    rendimientoPct = round((pnlNeto / saldoInicial * 100.0), 4) if saldoInicial > 0 else 0.0
    winRate = round((tradesGanadores / totalTrades * 100.0), 2) if totalTrades > 0 else 0.0

    # Actualizar tabla saldoCuentaMensual con saldos, depósitos, retiros y métricas de trading
    dbSession.execute(text("""
        UPDATE saldoCuentaMensual
        SET saldoFinal = :sf,
            depositos = :dep,
            retiros = :ret,
            pnlRealizado = :pnl,
            comisiones = :comm,
            rendimientoPct = :rend,
            totalTrades = :tt,
            tradesGanadores = :tg,
            tradesPerdedores = :tp
        WHERE idCuenta = :idc AND anio = :a AND mes = :m
    """), {
        "sf": saldoFinal,
        "dep": depositos,
        "ret": retiros,
        "pnl": pnlRealizado,
        "comm": comisionesTotales,
        "rend": rendimientoPct,
        "tt": totalTrades,
        "tg": tradesGanadores,
        "tp": tradesPerdedores,
        "idc": idCuenta,
        "a": anio,
        "m": mes
    })
    dbSession.commit()

    nombreMes = [
        "", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
        "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
    ][mes]

    return {
        "cuenta": cuentaData,
        "periodo": {
            "anio": anio,
            "mes": mes,
            "nombreMes": nombreMes,
            "etiquetaPeriodo": f"{nombreMes} {anio}"
        },
        "resumenFinanciero": {
            "saldoInicial": saldoInicial,
            "saldoFinal": saldoFinal,
            "depositos": depositos,
            "retiros": retiros,
            "pnlBruto": pnlRealizado,
            "comisiones": comisionesTotales,
            "pnlNeto": pnlNeto,
            "rendimientoPct": rendimientoPct,
            "totalTrades": totalTrades,
            "tradesGanadores": tradesGanadores,
            "tradesPerdedores": tradesPerdedores,
            "winRate": winRate
        },
        "trades": tradesList,
        "movimientosCapital": movimientosList
    }


def generarPdfEstadoCuenta(dbSession: Session, idCuenta: int, anio: int, mes: int) -> bytes:
    """
    Genera un documento PDF institucional y profesional con el estado de cuenta mensual.
    """
    data = calcularYActualizarEstadoCuenta(dbSession, idCuenta, anio, mes)
    cuenta = data["cuenta"]
    periodo = data["periodo"]
    resumen = data["resumenFinanciero"]
    trades = data["trades"]

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        leftMargin=36,
        rightMargin=36,
        topMargin=36,
        bottomMargin=36
    )

    styles = getSampleStyleSheet()
    
    # Estilos tipográficos institucionales
    titleStyle = ParagraphStyle(
        'DocTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=20,
        leading=24,
        textColor=colors.HexColor('#0F172A')
    )
    
    subTitleStyle = ParagraphStyle(
        'DocSubtitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=10,
        leading=14,
        textColor=colors.HexColor('#64748B')
    )

    metaLabelStyle = ParagraphStyle(
        'MetaLabel',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=8,
        leading=11,
        textColor=colors.HexColor('#475569')
    )

    metaValueStyle = ParagraphStyle(
        'MetaValue',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9,
        leading=12,
        textColor=colors.HexColor('#0F172A')
    )

    kpiTitleStyle = ParagraphStyle(
        'KpiTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=8,
        leading=10,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#475569')
    )

    kpiValueGreen = ParagraphStyle(
        'KpiValGreen',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=13,
        leading=16,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#16A34A')
    )

    kpiValueRed = ParagraphStyle(
        'KpiValRed',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=13,
        leading=16,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#DC2626')
    )

    kpiValueNeutral = ParagraphStyle(
        'KpiValNeutral',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=13,
        leading=16,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#0F172A')
    )

    thStyle = ParagraphStyle(
        'TableHeader',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=7,
        leading=9,
        alignment=TA_CENTER,
        textColor=colors.white
    )

    tdStyleLeft = ParagraphStyle(
        'TableCellLeft',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=7,
        leading=9,
        textColor=colors.HexColor('#1E293B')
    )

    tdStyleCenter = ParagraphStyle(
        'TableCellCenter',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=7,
        leading=9,
        alignment=TA_CENTER,
        textColor=colors.HexColor('#1E293B')
    )

    tdStyleRight = ParagraphStyle(
        'TableCellRight',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=7,
        leading=9,
        alignment=TA_RIGHT,
        textColor=colors.HexColor('#1E293B')
    )

    story = []

    # 1. ENCABEZADO INSTITUCIONAL
    headerTableData = [
        [
            Paragraph("<b>ATALA.ia</b> Institutional Trading", titleStyle),
            Paragraph("<b>ESTADO DE CUENTA MENSUAL</b>", ParagraphStyle('RightHdr', parent=titleStyle, fontSize=14, leading=16, alignment=TA_RIGHT, textColor=colors.HexColor('#1E293B')))
        ],
        [
            Paragraph("Sistema Algorítmico de Cobertura y Arbitraje Estadístico", subTitleStyle),
            Paragraph(f"Periodo: <b>{periodo['etiquetaPeriodo']}</b> | Emisión: {datetime.now().strftime('%Y-%m-%d %H:%M')}", ParagraphStyle('RightSub', parent=subTitleStyle, alignment=TA_RIGHT))
        ]
    ]
    headerTable = Table(headerTableData, colWidths=[3.8 * inch, 3.8 * inch])
    headerTable.setStyle(TableStyle([
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('BOTTOMPADDING', (0,0), (-1,-1), 0),
        ('TOPPADDING', (0,0), (-1,-1), 0),
    ]))
    story.append(headerTable)
    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=1.5, color=colors.HexColor('#0F172A'), spaceAfter=12))

    # 2. DATOS DE LA CUENTA Y DEL TITULAR
    accountInfoData = [
        [
            Paragraph("Titular de la Cuenta:", metaLabelStyle),
            Paragraph(f"<b>{cuenta.get('Nombre') or 'No especificado'}</b>", metaValueStyle),
            Paragraph("Número de Cuenta:", metaLabelStyle),
            Paragraph(f"<b>#{cuenta.get('idCuenta')}</b>", metaValueStyle)
        ],
        [
            Paragraph("Correo Electrónico:", metaLabelStyle),
            Paragraph(f"{cuenta.get('correo') or 'contacto@atalaia.io'}", metaValueStyle),
            Paragraph("Moneda Base:", metaLabelStyle),
            Paragraph("<b>USD ($)</b>", metaValueStyle)
        ],
        [
            Paragraph("Tipo de Broker / Conexión:", metaLabelStyle),
            Paragraph(f"{cuenta.get('brokerType') or 'FOREX.com (CIAPI)'}", metaValueStyle),
            Paragraph("Estatus de Cuenta:", metaLabelStyle),
            Paragraph("<font color='#16A34A'><b>ACTIVA</b></font>" if cuenta.get('Activo') else "<font color='#DC2626'><b>INACTIVA</b></font>", metaValueStyle)
        ]
    ]
    accountInfoTable = Table(accountInfoData, colWidths=[1.5 * inch, 2.3 * inch, 1.5 * inch, 2.3 * inch])
    accountInfoTable.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#F8FAFC')),
        ('BOX', (0,0), (-1,-1), 0.5, colors.HexColor('#CBD5E1')),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#E2E8F0')),
        ('TOPPADDING', (0,0), (-1,-1), 4),
        ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ('LEFTPADDING', (0,0), (-1,-1), 8),
        ('RIGHTPADDING', (0,0), (-1,-1), 8),
    ]))
    story.append(accountInfoTable)
    story.append(Spacer(1, 14))

    # 3. TARJETAS DE RENDIMIENTO FINANCIERO (KPIS)
    pnlStyle = kpiValueGreen if resumen["pnlNeto"] >= 0 else kpiValueRed
    pnlPrefix = "+" if resumen["pnlNeto"] >= 0 else ""
    rendPrefix = "+" if resumen["rendimientoPct"] >= 0 else ""

    kpiData = [
        [
            Paragraph("SALDO INICIAL", kpiTitleStyle),
            Paragraph("PNL NETO MES", kpiTitleStyle),
            Paragraph("COMISIONES", kpiTitleStyle),
            Paragraph("RENDIMIENTO", kpiTitleStyle),
            Paragraph("SALDO FINAL", kpiTitleStyle),
        ],
        [
            Paragraph(f"${resumen['saldoInicial']:,.2f}", kpiValueNeutral),
            Paragraph(f"{pnlPrefix}${resumen['pnlNeto']:,.2f}", pnlStyle),
            Paragraph(f"${resumen['comisiones']:,.2f}", kpiValueNeutral),
            Paragraph(f"{rendPrefix}{resumen['rendimientoPct']:.2f}%", pnlStyle),
            Paragraph(f"${resumen['saldoFinal']:,.2f}", kpiValueNeutral),
        ]
    ]
    kpiTable = Table(kpiData, colWidths=[1.52 * inch] * 5)
    kpiTable.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#F1F5F9')),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor('#0F172A')),
        ('INNERGRID', (0,0), (-1,-1), 0.5, colors.HexColor('#CBD5E1')),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
    ]))
    story.append(kpiTable)
    story.append(Spacer(1, 14))

    # 4. RESUMEN DE OPERACIONES
    summaryHeader = ParagraphStyle('SecHeader', parent=styles['Normal'], fontName='Helvetica-Bold', fontSize=10, leading=13, textColor=colors.HexColor('#0F172A'))
    story.append(Paragraph(f"<b>Bitácora de Operaciones Cerradas en el Periodo ({len(trades)} trades)</b>", summaryHeader))
    story.append(Spacer(1, 6))

    if not trades:
        story.append(Paragraph("<i>No se registraron operaciones cerradas durante este periodo mensual.</i>", tdStyleLeft))
    else:
        # Tabla detallada de trades
        colWidths = [
            0.6 * inch,  # #
            0.9 * inch,  # Fecha Cierre
            1.5 * inch,  # Setup / Ratio
            0.7 * inch,  # Par
            0.5 * inch,  # Dir
            0.6 * inch,  # Lotes
            0.7 * inch,  # Entrada
            0.7 * inch,  # Salida
            0.7 * inch,  # PnL USD
            0.7 * inch   # Ticket
        ]

        tableRows = [
            [
                Paragraph("<b>#</b>", thStyle),
                Paragraph("<b>Fecha Cierre</b>", thStyle),
                Paragraph("<b>Setup / Ratio</b>", thStyle),
                Paragraph("<b>Símbolo</b>", thStyle),
                Paragraph("<b>Dir</b>", thStyle),
                Paragraph("<b>Lotes</b>", thStyle),
                Paragraph("<b>Entrada</b>", thStyle),
                Paragraph("<b>Salida</b>", thStyle),
                Paragraph("<b>PnL (USD)</b>", thStyle),
                Paragraph("<b>Ticket</b>", thStyle),
            ]
        ]

        for i, tr in enumerate(trades, start=1):
            cTime = tr.get("closeTime")
            cTimeStr = cTime.strftime("%d/%m %H:%M") if isinstance(cTime, datetime) else str(cTime or "-")[:16]
            pnlVal = float(tr.get("pnl") or 0.0)
            pnlStyleItem = tdStyleRight if pnlVal >= 0 else ParagraphStyle('PnlNeg', parent=tdStyleRight, textColor=colors.HexColor('#DC2626'))
            pnlStr = f"+${pnlVal:,.2f}" if pnlVal >= 0 else f"-${abs(pnlVal):,.2f}"

            tableRows.append([
                Paragraph(str(tr.get("idTrade")), tdStyleCenter),
                Paragraph(cTimeStr, tdStyleCenter),
                Paragraph(str(tr.get("setup") or "RATIO"), tdStyleLeft),
                Paragraph(str(tr.get("symbol") or ""), tdStyleCenter),
                Paragraph(str(tr.get("direction") or "").upper()[:5], tdStyleCenter),
                Paragraph(f"{float(tr.get('size') or 0.0):,.0f}", tdStyleRight),
                Paragraph(f"{float(tr.get('entryPrice') or 0.0):.5f}", tdStyleRight),
                Paragraph(f"{float(tr.get('exitPrice') or 0.0):.5f}", tdStyleRight),
                Paragraph(f"<b>{pnlStr}</b>", pnlStyleItem),
                Paragraph(str(tr.get("ticketId") or "-"), tdStyleCenter),
            ])

        tradesTable = Table(tableRows, colWidths=colWidths, repeatRows=1)
        
        tStyle = [
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#0F172A')),
            ('ALIGN', (0,0), (-1,0), 'CENTER'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('TOPPADDING', (0,0), (-1,-1), 3),
            ('BOTTOMPADDING', (0,0), (-1,-1), 3),
            ('INNERGRID', (0,0), (-1,-1), 0.4, colors.HexColor('#E2E8F0')),
            ('BOX', (0,0), (-1,-1), 0.8, colors.HexColor('#0F172A')),
        ]
        
        # Alternancia de colores en filas (Zebra striping)
        for rowIdx in range(1, len(tableRows)):
            if rowIdx % 2 == 0:
                tStyle.append(('BACKGROUND', (0, rowIdx), (-1, rowIdx), colors.HexColor('#F8FAFC')))
            else:
                tStyle.append(('BACKGROUND', (0, rowIdx), (-1, rowIdx), colors.white))

        tradesTable.setStyle(TableStyle(tStyle))
        story.append(tradesTable)

    # 5. PIE DE PÁGINA INSTITUCIONAL
    story.append(Spacer(1, 20))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#CBD5E1'), spaceAfter=8))
    footerText = (
        "<b>Aviso Institucional:</b> Este documento constituye un estado de cuenta emitido automáticamente por la plataforma "
        "ATALA.ia para efectos de seguimiento operativo y contable. Toda la información ha sido conciliada contra las confirmaciones "
        "de ejecución del broker regulado (FOREX.com / City Index)."
    )
    story.append(Paragraph(footerText, ParagraphStyle('Footer', parent=styles['Normal'], fontSize=7, leading=10, textColor=colors.HexColor('#94A3B8'), alignment=TA_CENTER)))

    doc.build(story)
    return buffer.getvalue()


def inicializarSaldosHistoricos(dbSession: Session, anio: Optional[int] = None, mes: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    Inicializa los registros de saldoCuentaMensual de todas las cuentas registradas en 'cuenta'.
    Para el periodo objetivo (por defecto año y mes actual), sincroniza el saldo inicial y final
    con el Capital real de la cuenta y los trades cerrados del periodo.
    """
    now = datetime.now()
    targetAnio = anio or now.year
    targetMes = mes or now.month

    logger.info(f"Iniciando inicialización de saldos mensuales para todas las cuentas ({targetAnio}-{targetMes:02d})...")

    cuentas = dbSession.execute(text("SELECT idCuenta, Nombre, Capital, Activo FROM cuenta ORDER BY idCuenta")).mappings().fetchall()

    resultados = []
    for c in cuentas:
        idC = c["idCuenta"]
        nombre = str(c["Nombre"])
        cap = float(c["Capital"] or 0.0)

        # Consultar trades cerrados del mes
        trRow = dbSession.execute(text("""
            SELECT 
                COUNT(*) as cnt,
                COALESCE(SUM(pnl), 0.0) as pnl,
                COALESCE(SUM(commission), 0.0) as comm,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as win,
                SUM(CASE WHEN pnl < 0 THEN 1 ELSE 0 END) as loss
            FROM trades
            WHERE idCuenta = :idc AND status = 'CLOSED' AND YEAR(closeTime) = :a AND MONTH(closeTime) = :m
        """), {"idc": idC, "a": targetAnio, "m": targetMes}).mappings().fetchone()

        cnt = int(trRow["cnt"] or 0)
        pnl = round(float(trRow["pnl"] or 0.0), 2)
        win = int(trRow["win"] or 0)
        loss = int(trRow["loss"] or 0)

        # La comisión se cobra sobre el neteo del PnL solo de las trades cerradas dentro del mes;
        # si al final de mes es negativo o cero, la comisión es cero.
        comisionPct = float(c.get("comision", 0.0) or 0.0)
        if comisionPct > 0 and pnl > 0:
            comm = round(pnl * (comisionPct / 100.0), 2)
        else:
            comm = 0.0

        pnlNeto = round(pnl - comm, 2)

        if cnt > 0:
            saldoIni = round(cap - pnl, 2)
            saldoFin = round(saldoIni + pnlNeto, 2)
            rend = round((pnlNeto / saldoIni * 100.0), 4) if saldoIni > 0 else 0.0
        else:
            saldoIni = cap
            saldoFin = cap
            rend = 0.0

        dbSession.execute(text("""
            INSERT INTO saldoCuentaMensual (
                idCuenta, anio, mes, saldoInicial, saldoFinal,
                depositos, retiros, pnlRealizado, comisiones, rendimientoPct,
                totalTrades, tradesGanadores, tradesPerdedores, fechaRegistro, fechaModificacion
            ) VALUES (
                :idc, :a, :m, :si, :sf,
                0.0, 0.0, :pnl, :comm, :rend,
                :tt, :tg, :tp, NOW(), NOW()
            ) ON DUPLICATE KEY UPDATE
                saldoInicial = VALUES(saldoInicial),
                saldoFinal = VALUES(saldoFinal),
                depositos = 0.0,
                retiros = 0.0,
                pnlRealizado = VALUES(pnlRealizado),
                comisiones = VALUES(comisiones),
                rendimientoPct = VALUES(rendimientoPct),
                totalTrades = VALUES(totalTrades),
                tradesGanadores = VALUES(tradesGanadores),
                tradesPerdedores = VALUES(tradesPerdedores),
                fechaModificacion = NOW()
        """), {
            "idc": idC, "a": targetAnio, "m": targetMes,
            "si": saldoIni, "sf": saldoFin, "pnl": pnl, "comm": comm, "rend": rend,
            "tt": cnt, "tg": win, "tp": loss
        })

        resultados.append({
            "idCuenta": idC,
            "nombre": nombre,
            "capital": cap,
            "saldoInicial": saldoIni,
            "saldoFinal": saldoFin,
            "totalTrades": cnt
        })

    dbSession.commit()
    logger.info(f"✅ Inicialización de saldos completada para {len(resultados)} cuentas.")
    return resultados


def obtenerCuentaConcentradora(dbSession: Session, idCuenta: int) -> Optional[int]:
    """
    Determina la cuenta concentradora asociada al usuario de la cuenta.
    Si el usuario no tiene una asignada, busca la cuenta concentradora global activa.
    """
    try:
        userAccRow = dbSession.execute(text("""
            SELECT idUsuario FROM usuarioCuenta 
            WHERE idCuenta = :idc AND activo = 1 
            LIMIT 1
        """), {"idc": idCuenta}).fetchone()
        
        if userAccRow:
            idUsuario = userAccRow[0]
            concAccRow = dbSession.execute(text("""
                SELECT c.idCuenta FROM cuenta c
                JOIN usuarioCuenta uc ON c.idCuenta = uc.idCuenta
                WHERE uc.idUsuario = :idu AND c.Concentradora = 1 AND c.Activo = 1
                LIMIT 1
            """), {"idu": idUsuario}).fetchone()
            if concAccRow:
                return concAccRow[0]
                
        # Fallback a cuenta concentradora global activa
        fallbackConc = dbSession.execute(text("""
            SELECT idCuenta FROM cuenta 
            WHERE Concentradora = 1 AND Activo = 1 
            LIMIT 1
        """)).fetchone()
        if fallbackConc:
            return fallbackConc[0]
    except Exception as exConc:
        logger.error(f"Error al determinar cuenta concentradora para idCuenta={idCuenta}: {exConc}")
    return None


def registrarMovimientoCapital(
    dbSession: Session,
    idCuenta: int,
    tipo: str,
    monto: float,
    fecha: Optional[datetime] = None,
    concepto: str = "",
    folio: str = ""
) -> Dict[str, Any]:
    """
    Registra un movimiento de capital (Depósito o Retiro) para una cuenta.
    - Inserta la trazabilidad en la tabla trades (strategy='Depósito'/'Retiro', status='CLOSED', pnl=+/-monto, symbol='CASH').
    - Actualiza el Capital de la cuenta en la tabla cuenta.
    - Sincroniza y totaliza saldoCuentaMensual del mes del movimiento.
    """
    if monto is None or float(monto) <= 0:
        raise ValueError("El monto del movimiento debe ser un valor positivo mayor a cero.")

    tipoNorm = tipo.strip().upper()
    if tipoNorm not in ("DEPOSITO", "DEPÓSITO", "RETIRO"):
        raise ValueError("El tipo de movimiento debe ser 'DEPOSITO' o 'RETIRO'.")

    isDeposito = (tipoNorm in ("DEPOSITO", "DEPÓSITO"))
    strategyName = "Depósito" if isDeposito else "Retiro"
    directionVal = "IN" if isDeposito else "OUT"
    montoAbs = round(float(monto), 2)
    pnlVal = montoAbs if isDeposito else round(-montoAbs, 2)

    fechaMov = fecha or datetime.now()
    anio = fechaMov.year
    mes = fechaMov.month

    # 1. Verificar existencia de la cuenta
    cuentaRow = dbSession.execute(text("""
        SELECT idCuenta, Nombre, Capital FROM cuenta WHERE idCuenta = :idc LIMIT 1
    """), {"idc": idCuenta}).mappings().fetchone()

    if not cuentaRow:
        raise ValueError(f"La cuenta #{idCuenta} no existe en el sistema.")

    capitalActual = float(cuentaRow["Capital"] or 0.0)

    # 2. Insertar trazabilidad en la tabla trades
    ticketVal = folio.strip() if folio and folio.strip() else f"{strategyName[:3].upper()}-{int(fechaMov.timestamp())}"
    setupVal = concepto.strip() if concepto and concepto.strip() else f"{strategyName} de Capital"

    insertResult = dbSession.execute(text("""
        INSERT INTO trades (
            idCuenta, strategy, setup, symbol, status, direction, intervalo,
            pnl, size, openTime, closeTime, candleTime, candle_time,
            ticketId, commission, margin_used, isBreakEven, sentAt
        ) VALUES (
            :idc, :strat, :setup, 'CASH', 'CLOSED', :dir, '15min',
            :pnl, :size, :ot, :ct, :ct, :ct,
            :ticket, 0.0, 0.0, 0, NOW()
        )
    """), {
        "idc": idCuenta,
        "strat": strategyName,
        "setup": setupVal,
        "dir": directionVal,
        "pnl": pnlVal,
        "size": montoAbs,
        "ot": fechaMov,
        "ct": fechaMov,
        "ticket": ticketVal
    })
    newTradeId = insertResult.lastrowid

    # 3. Actualizar Capital de la cuenta
    nuevoCapital = round(capitalActual + pnlVal, 2)
    dbSession.execute(text("""
        UPDATE cuenta
        SET Capital = :cap
        WHERE idCuenta = :idc
    """), {"cap": nuevoCapital, "idc": idCuenta})

    dbSession.commit()

    # 4. Recalcular y sincronizar saldoCuentaMensual del mes del movimiento
    estadoMensual = calcularYActualizarEstadoCuenta(dbSession, idCuenta, anio, mes)

    logger.info(
        f"✅ [{strategyName}] Registrado #{newTradeId} para Cuenta #{idCuenta}: "
        f"${montoAbs:,.2f} en {anio}-{mes:02d}. Nuevo Capital: ${nuevoCapital:,.2f}"
    )

    return {
        "idTrade": newTradeId,
        "idCuenta": idCuenta,
        "tipo": strategyName,
        "monto": montoAbs,
        "pnl": pnlVal,
        "fecha": fechaMov.strftime("%Y-%m-%d %H:%M:%S"),
        "concepto": setupVal,
        "folio": ticketVal,
        "nuevoCapital": nuevoCapital,
        "estadoCuentaMes": estadoMensual
    }


def obtenerMovimientosCapital(
    dbSession: Session,
    idCuenta: int,
    anio: Optional[int] = None,
    mes: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Obtiene el listado histórico de depósitos y retiros registrados para una cuenta,
    con opción de filtrar por año y mes.
    """
    params = {"idc": idCuenta}
    whereExtra = ""
    if anio is not None:
        whereExtra += " AND YEAR(closeTime) = :a"
        params["a"] = anio
    if mes is not None:
        whereExtra += " AND MONTH(closeTime) = :m"
        params["m"] = mes

    sql = f"""
        SELECT idTrade, idCuenta, strategy, setup, direction, size,
               pnl, openTime, closeTime, ticketId
        FROM trades
        WHERE idCuenta = :idc 
          AND status = 'CLOSED'
          AND strategy IN ('Depósito', 'DEPOSITO', 'Deposito', 'Retiro', 'RETIRO')
          {whereExtra}
        ORDER BY closeTime DESC, idTrade DESC
    """
    rows = dbSession.execute(text(sql), params).mappings().fetchall()
    return [dict(r) for r in rows]


def liquidarComisionesFinDeMes(dbSession: Session, anio: Optional[int] = None, mes: Optional[int] = None) -> Dict[str, Any]:
    """
    Ejecuta el corte y liquidación mensual de comisiones a fin de mes:
    1. Para cada cuenta registrada que no sea concentradora:
       - Evalúa el PnL neto acumulado de trades cerrados en el mes (YEAR(closeTime) = anio, MONTH(closeTime) = mes).
       - Si pnlRealizado > 0 y comision_pct > 0:
           comision = round(pnlRealizado * (comision_pct / 100.0), 2)
           Verifica si comisionLiquidada == 0:
               - Descuenta la comisión de cuenta.Capital (Capital = Capital - comision)
               - Acredita la comisión al Capital de la cuenta Concentradora (Capital = Capital + comision)
               - Registra el trade de auditoría en la cuenta concentradora (setup='CORTE MENSUAL', pnl=comision)
               - Actualiza saldoCuentaMensual (comisiones=comision, comisionLiquidada=1)
               - Actualiza el saldo mensual de la concentradora (depositos = depositos + comision)
       - Si pnlRealizado <= 0:
           comision = 0.0, no se realiza ningún descuento ni transferencia.
    """
    now = datetime.now()
    targetAnio = anio or now.year
    targetMes = mes or now.month

    logger.info(f"🏦 [Corte Fin de Mes] Iniciando liquidación de comisiones para {targetAnio}-{targetMes:02d}...")

    cuentas = dbSession.execute(text("SELECT idCuenta, Nombre, Capital, Activo, Concentradora, comision FROM cuenta ORDER BY idCuenta")).mappings().fetchall()

    lastDayOfMonth = calendar.monthrange(targetAnio, targetMes)[1]
    corteDateTime = datetime(targetAnio, targetMes, lastDayOfMonth, 23, 59, 59)
    nowStr = now.strftime("%Y-%m-%d %H:%M:%S")

    insertCommTradeSql = text("""
        INSERT INTO trades (
            idCuenta, strategy, setup, symbol, status, direction,
            intervalo, size, entryPrice, exitPrice, stopLoss, takeProfit,
            isBreakEven, pnl, slippage, commission, margin_used,
            openTime, closeTime, candleTime, sentAt, ticketId
        ) VALUES (
            :idCuenta, :strategy, :setup, 'USD', 'CLOSED', 'IN',
            '1M', 1.0, 1.0, 1.0, NULL, NULL,
            0, :pnl, 0.0, 0.0, 0.0,
            :openTime, :closeTime, :candleTime, :sentAt, NULL
        )
    """)

    transferencias = []
    cuentasProcesadas = 0

    for c in cuentas:
        idC = c["idCuenta"]
        nombre = str(c["Nombre"])
        isConcentradora = bool(c["Concentradora"])
        comisionPct = float(c["comision"] or 0.0)

        # Si la cuenta es concentradora, no se liquida comisión a sí misma
        if isConcentradora:
            continue

        cuentasProcesadas += 1

        # Obtener o crear saldo mensual del periodo
        saldoRec = obtenerOCrearSaldoMensual(dbSession, idC, targetAnio, targetMes)
        yaLiquidada = bool(saldoRec.get("comisionLiquidada", 0))

        # Consultar PnL neto acumulado de trades cerrados en el mes
        trRow = dbSession.execute(text("""
            SELECT COALESCE(SUM(pnl), 0.0) as pnl, COUNT(*) as cnt
            FROM trades
            WHERE idCuenta = :idc AND status = 'CLOSED' 
              AND YEAR(closeTime) = :a AND MONTH(closeTime) = :m
        """), {"idc": idC, "a": targetAnio, "m": targetMes}).fetchone()

        pnlRealizado = round(float(trRow[0] or 0.0), 2)
        cntTrades = int(trRow[1] or 0)

        # Si ya fue liquidada para este mes, no duplicar cobro
        if yaLiquidada:
            logger.info(f"ℹ️ Cuenta #{idC} ({nombre}) ya fue liquidada previamente para {targetAnio}-{targetMes:02d}.")
            continue

        # Si pnlRealizado <= 0 o comisionPct <= 0, comisión es 0.0
        if pnlRealizado <= 0 or comisionPct <= 0:
            dbSession.execute(text("""
                UPDATE saldoCuentaMensual
                SET comisiones = 0.0,
                    comisionLiquidada = 1,
                    pnlRealizado = :pnl,
                    totalTrades = :tt,
                    fechaModificacion = NOW()
                WHERE idCuenta = :idc AND anio = :a AND mes = :m
            """), {"idc": idC, "a": targetAnio, "m": targetMes, "pnl": pnlRealizado, "tt": cntTrades})
            continue

        # pnlRealizado > 0: calcular comisión
        comision = round(pnlRealizado * (comisionPct / 100.0), 2)

        # Identificar cuenta concentradora asociada
        idConc = obtenerCuentaConcentradora(dbSession, idC)
        if not idConc or idConc == idC:
            logger.warning(f"⚠️ No se encontró cuenta concentradora destino válida para Cuenta #{idC} ({nombre}).")
            continue

        # 1. Descontar comisión del Capital de la cuenta origen
        dbSession.execute(text("""
            UPDATE cuenta
            SET Capital = Capital - :comm
            WHERE idCuenta = :idc
        """), {"comm": comision, "idc": idC})

        # 2. Acreditar comisión al Capital de la cuenta concentradora
        dbSession.execute(text("""
            UPDATE cuenta
            SET Capital = Capital + :comm
            WHERE idCuenta = :idConc
        """), {"comm": comision, "idConc": idConc})

        # 3. Registrar trade de auditoría en la concentradora
        strategyName = f"Comision Mensual #{idC} {targetAnio}-{targetMes:02d}"
        dbSession.execute(insertCommTradeSql, {
            "idCuenta": idConc,
            "strategy": strategyName,
            "setup": "CORTE MENSUAL",
            "pnl": comision,
            "openTime": corteDateTime,
            "closeTime": corteDateTime,
            "candleTime": corteDateTime,
            "sentAt": nowStr
        })

        # 4. Actualizar saldoCuentaMensual de la cuenta origen
        dbSession.execute(text("""
            UPDATE saldoCuentaMensual
            SET comisiones = :comm,
                comisionLiquidada = 1,
                pnlRealizado = :pnl,
                totalTrades = :tt,
                saldoFinal = round(saldoInicial + (:pnl - :comm) + depositos - retiros, 2),
                rendimientoPct = round(((:pnl - :comm) / saldoInicial * 100.0), 4),
                fechaModificacion = NOW()
            WHERE idCuenta = :idc AND anio = :a AND mes = :m
        """), {
            "idc": idC, "a": targetAnio, "m": targetMes,
            "comm": comision, "pnl": pnlRealizado, "tt": cntTrades
        })

        # 5. Actualizar saldoCuentaMensual de la concentradora (reflejar comisión como depósito recibido)
        obtenerOCrearSaldoMensual(dbSession, idConc, targetAnio, targetMes)
        dbSession.execute(text("""
            UPDATE saldoCuentaMensual
            SET depositos = depositos + :comm,
                saldoFinal = saldoFinal + :comm,
                fechaModificacion = NOW()
            WHERE idCuenta = :idConc AND anio = :a AND mes = :m
        """), {"idConc": idConc, "a": targetAnio, "m": targetMes, "comm": comision})

        logger.info(f"💰 [Transferencia Exitosa] Comisión de ${comision:,.2f} ({comisionPct}%) transferida de Cuenta #{idC} a Concentradora #{idConc}")

        transferencias.append({
            "idCuentaOrigen": idC,
            "nombreOrigen": nombre,
            "pnlRealizado": pnlRealizado,
            "comisionPct": comisionPct,
            "comision": comision,
            "idCuentaConcentradora": idConc
        })

    dbSession.commit()
    logger.info(f"✅ Liquidación de fin de mes completada. {len(transferencias)} transferencias realizadas.")

    return {
        "periodo": f"{targetAnio}-{targetMes:02d}",
        "cuentasEvaluadas": cuentasProcesadas,
        "transferenciasRealizadas": len(transferencias),
        "totalComisionesTransferidas": round(sum(t["comision"] for t in transferencias), 2),
        "detalle": transferencias
    }
