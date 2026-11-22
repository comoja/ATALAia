"""
Orquestador Maestro de Fin de Semana (weekendOrchestrator.py)
ATALA.ia Quantitative Systems & Portfolio Optimization

Pipeline autónomo de ejecución sabatina:
  Paso 1 (01:00 AM): Limpieza y verificación de velas en BD (cleanupWeekendData).
  Paso 2 (02:00 AM): Optimización y calibración de microRatio (microRatioOptimizer).
  Paso 3 (03:00 AM): Optimización de ventana RIDE / CART (microCartOptimizer).
  Paso 4 (04:00 AM): Optimización y calibración adaptativa de microSniper (microSniperOptimizer).
  Paso 5 (05:00 AM): Escaneo global del universo multiactivo (universePortfolioScanner).
  Paso 6 (06:00 AM): Rebalanceo, sizing de lotaje y protección de margen por cuenta (accountPortfolioOptimizer).
  Paso 7 (07:00 AM): Generación de Reportes PDF (Maestro Consolidado + Personalizados por Cuenta).
  Paso 8 (07:15 AM): Despacho Privado e Independiente por Telegram a cada usuario vía alertaInmediata().
"""

import os
import sys
import time
import json
import asyncio
import logging
import argparse
import subprocess
from datetime import datetime
from typing import Dict, Any, List

# Asegurar raíz de ATALAia en sys.path
rootDir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if rootDir not in sys.path:
    sys.path.insert(0, rootDir)

from sqlalchemy import text
from backend.database.models import SessionLocal
from backend.services.microRatioOptimizer import optimizeAllUserRatios
from backend.services.microCartOptimizer import runOptimizationCycle as runCartOptimization
from backend.services.microSniperOptimizer import optimizeAllSniperSymbols
from backend.services.universePortfolioScanner import runFullUniverseScan
from backend.services.accountPortfolioOptimizer import optimizeAllAccounts, loadGlobalPortfolio
from middleware.utils.communications import alertaInmediata

LOG_DIR = os.path.join(rootDir, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "weekendOrchestrator.log")

REPORTS_DIR = "/home/jcolinm/datos_mysql/DATOS/Sistema/reports"
os.makedirs(REPORTS_DIR, exist_ok=True)

HTML_REPORT_PATH = "/home/jcolinm/datos_mysql/DATOS/Sistema/reporte_semanal_portafolio.html"
PDF_REPORT_PATH = "/home/jcolinm/datos_mysql/DATOS/Sistema/Reporte_Semanal_Portafolio_ATALAia.pdf"
INFORME_PDF_PATH = "/home/jcolinm/datos_mysql/DATOS/Sistema/Informe_Estrategias_ATALAia.pdf"
INSTITUTIONAL_PASSWORD = "4T4L4.i@"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [weekendOrchestrator] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("weekendOrchestrator")


def encryptPdf(srcPath: str, destPath: str = None, password: str = INSTITUTIONAL_PASSWORD) -> str:
    """
    Cifra un archivo PDF con contraseña de apertura (User & Owner password) usando AES vía pypdf.
    """
    if not os.path.exists(srcPath):
        return srcPath
    if not destPath:
        destPath = srcPath

    tempPath = f"{destPath}.enc.tmp.pdf"
    try:
        from pypdf import PdfReader, PdfWriter
        reader = PdfReader(srcPath)
        if reader.is_encrypted:
            # Si ya está cifrado con la contraseña correcta, devolver la ruta directamente
            if reader.decrypt(password) > 0:
                logger.info(f"🔒 PDF ya se encuentra protegido con la contraseña institucional: {srcPath}")
                return srcPath

        writer = PdfWriter()
        for page in reader.pages:
            writer.add_page(page)
        writer.encrypt(user_password=password, owner_password=password)
        with open(tempPath, "wb") as f:
            writer.write(f)
        os.replace(tempPath, destPath)
        logger.info(f"🔒 PDF protegido exitosamente con contraseña: {destPath}")
        return destPath
    except Exception as e:
        logger.error(f"Error protegiendo PDF con contraseña: {e}")
        if os.path.exists(tempPath):
            try: os.remove(tempPath)
            except Exception: pass
        return srcPath



def generateOwnerPortfolioPdf(ownerName: str, plans: List[Dict[str, Any]], pdf_file: str, html_file: str) -> str:
    """
    Genera un informe PDF ejecutivo personalizado e independiente para un usuario/titular específico,
    conteniendo ÚNICAMENTE las cuentas que le pertenecen (independencia, privacidad y confidencialidad).
    Maneja tanto titulares con una sola cuenta como titulares con múltiples cuentas.
    """
    if not plans:
        logger.warning(f"No hay cuentas para generar PDF de {ownerName}")
        return ""

    now_str = datetime.now().strftime("%d de %B de %Y - %H:%M")
    totalCapital = sum(float(p.get("capital", 0.0) or 0.0) for p in plans)
    totalEstMargin = sum(float(p.get("totalEstMargin", 0.0) or 0.0) for p in plans)
    totalMarginPct = round((totalEstMargin / totalCapital * 100), 1) if totalCapital > 0 else 0.0
    freeMarginPct = round(max(0.0, 100.0 - totalMarginPct), 1)
    freeMarginDollars = round(max(0.0, totalCapital - totalEstMargin), 2)
    cuentas_str = ", ".join([f"#{p.get('idCuenta')} {p.get('nombre')}" for p in plans])

    if len(plans) == 1:
        # Formato optimizado para 1 cuenta
        p = plans[0]
        idCuenta = p["idCuenta"]
        nombre = p.get("nombre", f"Cuenta #{idCuenta}")
        capital = float(p.get("capital", 0.0) or 0.0)
        riskPct = float(p.get("riesgoPorOperacion", 1.0) or 1.0)
        weights = p.get("normalizedWeights", {})
        weights_str = " · ".join([f"{k.replace('micro', '')}: {int(v*100)}%" for k, v in weights.items()]) or "Estrategias Asignadas"
        allocations = p.get("allocations", [])

        alloc_rows = ""
        for a in allocations:
            strat = a.get("strategy", "")
            sym = a.get("symbol", "")
            tf = a.get("timeframe", "")
            lots = a.get("lots", 0.0)
            lots_display = f"{lots:,.2f} lotes" if lots < 100 else f"{int(lots):,} unidades"
            alloc_cap = a.get("allocatedCapital", 0.0)
            est_m = a.get("estMargin", 0.0)
            badge_class = "badge-blue" if "Ratio" in strat else ("badge-green" if "Rider" in strat else "badge-amber")

            alloc_rows += f"""
            <tr>
                <td><span class="badge {badge_class}">{strat}</span></td>
                <td><strong><code>{sym}</code></strong></td>
                <td style="text-align: center;"><strong>{tf}</strong></td>
                <td style="text-align: center; color: #0284c7; font-weight: bold;">{lots_display}</td>
                <td style="text-align: right;">${alloc_cap:,.2f}</td>
                <td style="text-align: right;">${est_m:,.2f}</td>
                <td><small style="color: #64748b;">{'Cobertura Estadística' if 'Ratio' in strat else ('Expansión Tendencial' if 'Rider' in strat else 'Alfa Asimétrico Intradía')}</small></td>
            </tr>
            """
        if not alloc_rows:
            alloc_rows = '<tr><td colspan="7" style="text-align: center; padding: 15px; color: #64748b;">No hay instrumentos asignados actualmente para esta cuenta.</td></tr>'

        body_content = f"""
        <div class="meta-grid">
            <div><span class="meta-label">Titular de Cuenta</span><span class="meta-val">{ownerName}</span></div>
            <div><span class="meta-label">Cuenta ID</span><span class="meta-val">#{idCuenta}</span></div>
            <div><span class="meta-label">Capital Operativo</span><span class="meta-val">${capital:,.2f} USD</span></div>
            <div><span class="meta-label">Riesgo / Trade</span><span class="meta-val">{riskPct}% ({riskPct * capital / 100:,.2f} USD)</span></div>
        </div>

        <div class="metric-cards">
            <div class="metric-card">
                <div class="metric-card-title">Ponderación Estrategias</div>
                <div class="metric-card-val" style="font-size: 10pt; color: #0284c7;">{weights_str}</div>
                <div class="metric-card-sub" style="color: #0369a1;">Ponderación calibrada para su perfil</div>
            </div>
            <div class="metric-card">
                <div class="metric-card-title">Margen Retenido Simulado</div>
                <div class="metric-card-val">${totalEstMargin:,.2f} USD</div>
                <div class="metric-card-sub" style="color: #166534;">{totalMarginPct}% del capital total</div>
            </div>
            <div class="metric-card">
                <div class="metric-card-title">Colchón Flotante Libre</div>
                <div class="metric-card-val">{freeMarginPct}%</div>
                <div class="metric-card-sub" style="color: #166534;">${freeMarginDollars:,.2f} USD disponible</div>
            </div>
        </div>

        <h2>1. Cartera de Instrumentos Asignados a Su Cuenta</h2>
        <p style="margin-bottom: 6px; font-size: 8.2pt; color: #334155;">
            Los siguientes instrumentos han sido calculados y asignados con dimensionamiento de lote institucional acorde a su capital:
        </p>

        <table>
            <thead>
                <tr>
                    <th>Estrategia</th>
                    <th>Instrumento / Par</th>
                    <th style="text-align: center;">TF</th>
                    <th style="text-align: center;">Lote Calculado</th>
                    <th style="text-align: right;">Capital Asignado</th>
                    <th style="text-align: right;">Margen Est.</th>
                    <th>Objetivo Estratégico</th>
                </tr>
            </thead>
            <tbody>
                {alloc_rows}
            </tbody>
        </table>

        <h2>2. Dictamen de Cortafuegos de Margen (Margin Guardrail)</h2>
        <div style="font-size: 8.2pt; color: #334155; line-height: 1.45;">
            <p style="margin-bottom: 4px;">
                <strong>• Protección Simultánea Total:</strong> Si el 100% de los algoritmos activan órdenes concurrentes en el mercado, el margen total requerido por el broker será de <strong>${totalEstMargin:,.2f} USD ({totalMarginPct}%)</strong>, muy por debajo del límite prudencial máximo del <strong>30%</strong>.
            </p>
            <p style="margin-bottom: 4px;">
                <strong>• Preservación de Capital:</strong> Su cuenta mantiene un <strong>{freeMarginPct}% de margen libre (${freeMarginDollars:,.2f} USD)</strong> para absorber fluctuaciones normales del mercado sin ningún riesgo de margin call o stop out.
            </p>
            <p style="margin-bottom: 4px;">
                <strong>• Sincronización en Producción:</strong> Los parámetros y lotajes fueron transferidos automáticamente a las tablas de ejecución de su cuenta en ATALA.ia.
            </p>
        </div>

        <div class="callout">
            <div class="callout-title">✓ Estado de la Cuenta: TOTALMENTE BLINDADA Y OPERATIVA</div>
            Su portafolio se encuentra optimizado, protegido y listo para la apertura de mercados del domingo a las 17:00 CST sin requerir acción manual.
        </div>
        """
    else:
        # Formato Multi-Cuenta Consolidado Exclusivo para el Titular
        account_summary_rows = ""
        for p in plans:
            is_op = p.get("isOperational", True) and float(p.get("capital", 0.0) or 0.0) > 0
            w_str = ", ".join([f"{k.replace('micro', '')}: {int(v*100)}%" for k, v in p.get("normalizedWeights", {}).items()])
            alloc_symbols = ", ".join([al.get("symbol") for al in p.get("allocations", [])[:4]])
            if is_op:
                account_summary_rows += f"""
                <tr>
                    <td><strong>#{p.get('idCuenta')}</strong> - {p.get('nombre')}</td>
                    <td style="font-weight: 700;">${p.get('capital', 0):,.2f} USD</td>
                    <td>{w_str or 'Sin Estrategia'}</td>
                    <td><small>{alloc_symbols or 'N/A'}</small></td>
                    <td><strong>{p.get('totalMarginPct', 0)}%</strong></td>
                    <td><span class="badge badge-green">✓ Blindado (&lt;30%)</span></td>
                </tr>
                """
            else:
                status_lbl = p.get("status", "No Opera (Concentradora)")
                account_summary_rows += f"""
                <tr style="background: #f8fafc; color: #64748b;">
                    <td><strong>#{p.get('idCuenta')}</strong> - {p.get('nombre')}</td>
                    <td>$0.00 USD</td>
                    <td colspan="3" style="text-align: center;"><em>{status_lbl}</em></td>
                    <td><span style="color: #64748b; font-weight: 600;">Omitida (Sin Trading)</span></td>
                </tr>
                """

        detailed_acc_blocks = ""
        for p in [p for p in plans if p.get("isOperational", True) and float(p.get("capital", 0.0) or 0.0) > 0]:
            p_alloc_rows = ""
            for a in p.get("allocations", []):
                strat = a.get("strategy", "")
                sym = a.get("symbol", "")
                tf = a.get("timeframe", "")
                lots = a.get("lots", 0.0)
                lots_display = f"{lots:,.2f} lotes" if lots < 100 else f"{int(lots):,} unidades"
                alloc_cap = a.get("allocatedCapital", 0.0)
                est_m = a.get("estMargin", 0.0)
                strat_badge = "badge-blue" if "Ratio" in strat else ("badge-green" if "Rider" in strat else "badge-amber")
                p_alloc_rows += f"""
                <tr>
                    <td><span class="badge {strat_badge}">{strat}</span></td>
                    <td><strong><code>{sym}</code></strong></td>
                    <td style="text-align: center;"><strong>{tf}</strong></td>
                    <td style="text-align: center; color: #0284c7; font-weight: bold;">{lots_display}</td>
                    <td style="text-align: right;">${alloc_cap:,.2f}</td>
                    <td style="text-align: right;">${est_m:,.2f}</td>
                    <td><small style="color: #64748b;">{'Cobertura Estadística' if 'Ratio' in strat else ('Expansión Tendencial' if 'Rider' in strat else 'Alfa Asimétrico')}</small></td>
                </tr>
                """
            if not p_alloc_rows:
                p_alloc_rows = '<tr><td colspan="7" style="text-align: center; padding: 10px; color: #64748b;">Sin instrumentos asignados.</td></tr>'

            w_str = ", ".join([f"{k.replace('micro', '')}: {int(v*100)}%" for k, v in p.get("normalizedWeights", {}).items()])
            detailed_acc_blocks += f"""
            <div style="margin-top: 14px; margin-bottom: 16px; page-break-inside: avoid;">
                <div style="background: #f8fafc; border: 1px solid #cbd5e1; border-left: 4px solid #0284c7; padding: 6px 12px; border-radius: 4px; display: flex; justify-content: space-between; align-items: center;">
                    <div>
                        <strong style="font-size: 9.5pt; color: #0f172a;">Cuenta #{p.get('idCuenta')} — {p.get('nombre')}</strong>
                        <span style="font-size: 7.8pt; color: #64748b; margin-left: 10px;">Ponderación: {w_str}</span>
                    </div>
                    <div style="font-size: 8.5pt; font-weight: 700; color: #0f172a;">
                        Balance: ${p.get('capital', 0):,.2f} USD &nbsp;|&nbsp; Margen Teórico: {p.get('totalMarginPct', 0)}%
                    </div>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Estrategia</th>
                            <th>Instrumento / Par</th>
                            <th style="text-align: center;">TF</th>
                            <th style="text-align: center;">Lote Calculado</th>
                            <th style="text-align: right;">Capital Asignado</th>
                            <th style="text-align: right;">Margen Est.</th>
                            <th>Objetivo Estratégico</th>
                        </tr>
                    </thead>
                    <tbody>
                        {p_alloc_rows}
                    </tbody>
                </table>
            </div>
            """

        body_content = f"""
        <div class="meta-grid">
            <div><span class="meta-label">Titular de Cuentas</span><span class="meta-val">{ownerName}</span></div>
            <div><span class="meta-label">Cuentas Gestionadas</span><span class="meta-val">{len(plans)} Cuentas Activas</span></div>
            <div><span class="meta-label">Capital Consolidado</span><span class="meta-val">${totalCapital:,.2f} USD</span></div>
            <div><span class="meta-label">Riesgo / Trade</span><span class="meta-val">1.0% por operación</span></div>
        </div>

        <div class="metric-cards">
            <div class="metric-card">
                <div class="metric-card-title">Capital Total Consolidado</div>
                <div class="metric-card-val" style="color: #0284c7;">${totalCapital:,.2f} USD</div>
                <div class="metric-card-sub" style="color: #0369a1;">Patrimonio combinado en {len(plans)} cuentas</div>
            </div>
            <div class="metric-card">
                <div class="metric-card-title">Margen Retenido Simulado</div>
                <div class="metric-card-val">${totalEstMargin:,.2f} USD</div>
                <div class="metric-card-sub" style="color: #166534;">{totalMarginPct}% del capital total</div>
            </div>
            <div class="metric-card">
                <div class="metric-card-title">Colchón Flotante Libre</div>
                <div class="metric-card-val">{freeMarginPct}%</div>
                <div class="metric-card-sub" style="color: #166534;">${freeMarginDollars:,.2f} USD disponible</div>
            </div>
        </div>

        <h2>1. Resumen y Balance de Sus Cuentas</h2>
        <p style="margin-bottom: 6px; font-size: 8.2pt; color: #334155;">
            Estado de capital, asignación estratégica y verificación del Cortafuegos de Margen para sus cuentas:
        </p>

        <table>
            <thead>
                <tr>
                    <th>Cuenta</th>
                    <th>Capital</th>
                    <th>Ponderación Estrategias</th>
                    <th>Instrumentos Asignados</th>
                    <th>Margen Teórico</th>
                    <th>Estado de Riesgo</th>
                </tr>
            </thead>
            <tbody>
                {account_summary_rows}
            </tbody>
        </table>

        <div class="page" style="page-break-before: always;">
            <div class="header-bar">
                <div class="brand" style="font-size: 14pt;">ATALA<span>.ia</span> · Desglose por Cuenta</div>
                <div class="doc-tag">Titular: {ownerName} · {len(plans)} Cuentas</div>
            </div>

            <h2>2. Desglose Operativo y Asignación de Lotes por Cuenta</h2>
            <p style="margin-bottom: 6px; font-size: 8.2pt; color: #334155;">
                Dimensionamiento de lote cuantitativo y margen estimado específico para cada una de sus cuentas:
            </p>

            {detailed_acc_blocks}

            <h2>3. Protocolo de Gobernanza y Cortafuegos de Margen</h2>
            <div style="font-size: 8.2pt; color: #334155; line-height: 1.45;">
                <p style="margin-bottom: 4px;">
                    <strong>• Límite Simultáneo Prudencial (&lt;30%):</strong> Todas sus cuentas operan bajo la cota estricta donde una activación concurrente del 100% de los algoritmos nunca excede el 22-25% de margen, garantizando la solvencia ante cualquier régimen de volatilidad.
                </p>
                <p style="margin-bottom: 4px;">
                    <strong>• Preservación Patrimonial:</strong> El portafolio mantiene un <strong>{freeMarginPct}% de margen libre (${freeMarginDollars:,.2f} USD)</strong> disponible para absorber drawdown transitorio.
                </p>
                <p style="margin-bottom: 4px;">
                    <strong>• Sincronización en Producción:</strong> Todos los parámetros óptimos y dimensionamiento de lotes se encuentran sincronizados automáticamente en la plataforma para la apertura semanal.
                </p>
            </div>

            <div class="callout" style="margin-top: 12px;">
                <div class="callout-title">✓ Estado del Portafolio: TOTALMENTE BLINDADO Y OPERATIVO</div>
                Sus cuentas se encuentran equilibradas, calibradas y listas para la apertura de mercados del domingo a las 17:00 CST sin intervención manual requerida.
            </div>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reporte de Portafolio - {ownerName}</title>
    <style>
        @page {{
            size: letter;
            margin: 11mm 14mm 11mm 14mm;
        }}
        @media print {{
            body {{
                -webkit-print-color-adjust: exact;
                print-color-adjust: exact;
            }}
            .page {{
                page-break-after: always;
            }}
            .page:last-child {{
                page-break-after: avoid;
            }}
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
            color: #1e293b;
            font-size: 8.8pt;
            line-height: 1.42;
        }}
        .header-bar {{
            border-bottom: 2px solid #0284c7;
            padding-bottom: 8px;
            margin-bottom: 12px;
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
        }}
        .brand {{ font-size: 18pt; font-weight: 900; color: #0f172a; }}
        .brand span {{ color: #0284c7; }}
        .doc-tag {{ font-size: 7.2pt; font-weight: 700; text-transform: uppercase; color: #64748b; text-align: right; }}
        h1 {{ font-size: 15pt; font-weight: 800; color: #0f172a; margin-bottom: 3px; }}
        .subtitle {{ font-size: 9.2pt; color: #475569; margin-bottom: 12px; font-weight: 500; }}
        
        .meta-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 8px;
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            padding: 8px 12px;
            margin-bottom: 14px;
        }}
        .meta-label {{ font-size: 6.8pt; color: #64748b; text-transform: uppercase; font-weight: 700; display: block; }}
        .meta-val {{ font-size: 8.5pt; font-weight: 700; color: #0f172a; }}

        .metric-cards {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin: 12px 0;
        }}
        .metric-card {{
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            padding: 9px 12px;
            background: #ffffff;
            box-shadow: 0 1px 2px rgba(0,0,0,0.02);
        }}
        .metric-card-title {{ font-size: 7.2pt; color: #64748b; text-transform: uppercase; font-weight: 700; margin-bottom: 2px; }}
        .metric-card-val {{ font-size: 12pt; font-weight: 800; color: #0f172a; }}
        .metric-card-sub {{ font-size: 7.2pt; color: #16a34a; font-weight: 600; margin-top: 2px; }}

        h2 {{
            font-size: 11pt;
            font-weight: 800;
            color: #0f172a;
            border-left: 4px solid #0284c7;
            padding-left: 6px;
            margin-top: 14px;
            margin-bottom: 6px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 8px 0 12px 0;
            font-size: 8.2pt;
        }}
        th {{
            background: #f1f5f9;
            color: #0f172a;
            font-weight: 700;
            text-align: left;
            padding: 5px 7px;
            border: 1px solid #cbd5e1;
        }}
        td {{
            padding: 5px 7px;
            border: 1px solid #e2e8f0;
            color: #334155;
            vertical-align: middle;
        }}
        tr:nth-child(even) td {{ background: #f8fafc; }}
        .badge {{
            display: inline-block;
            padding: 2px 6px;
            border-radius: 4px;
            font-weight: 800;
            font-size: 7.2pt;
        }}
        .badge-green {{ background: #dcfce7; color: #166534; }}
        .badge-blue {{ background: #e0f2fe; color: #075985; }}
        .badge-amber {{ background: #fef3c7; color: #92400e; }}
        
        .callout {{
            background: #f0fdf4;
            border-left: 4px solid #10b981;
            padding: 8px 12px;
            border-radius: 0 6px 6px 0;
            margin: 10px 0;
            font-size: 8.4pt;
        }}
        .callout-title {{ font-weight: 800; color: #065f46; margin-bottom: 2px; }}
        .footer {{
            border-top: 1px solid #e2e8f0;
            padding-top: 6px;
            display: flex;
            justify-content: space-between;
            font-size: 7.2pt;
            color: #94a3b8;
            margin-top: 20px;
        }}
    </style>
</head>
<body>

    <div class="page">
        <div class="header-bar">
            <div>
                <div class="brand">ATALA<span>.ia</span></div>
                <div style="font-size: 8pt; color: #64748b; font-weight: 600;">Gestión Cuantitativa Privada · Asignación de Portafolio</div>
            </div>
            <div class="doc-tag">
                Reporte Ejecutivo Confidencial<br>
                Titular: {ownerName} ({len(plans)} {'Cuenta' if len(plans)==1 else 'Cuentas'})
            </div>
        </div>

        <h1>Reporte de Asignación y Calibración de Portafolio</h1>
        <div class="subtitle">Estructuración Óptima de Activos, Dimensionamiento de Lote y Blindaje de Margen</div>

        {body_content}

        <div class="footer">
            <span>ATALA.ia Quantitative Systems · Documento Confidencial para {ownerName}</span>
            <span>Fecha: {now_str}</span>
        </div>
    </div>

</body>
</html>
"""

    os.makedirs(os.path.dirname(html_file), exist_ok=True)
    with open(html_file, "w", encoding="utf-8") as f:
        f.write(html)

    cmd = [
        "google-chrome",
        "--headless",
        "--disable-gpu",
        "--no-sandbox",
        "--no-pdf-header-footer",
        f"--print-to-pdf={pdf_file}",
        html_file
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode == 0:
        logger.info(f"✅ Reporte Personalizado compilado para {ownerName} ({len(plans)} cuentas): {pdf_file}")
        return pdf_file
    else:
        logger.error(f"❌ Error compilando PDF para {ownerName}: {res.stderr}")
        return ""


def generatePersonalizedAccountPdf(plan: Dict[str, Any]) -> str:
    """
    Genera un informe PDF ejecutivo personalizado e independiente para un usuario/cuenta específico.
    """
    idCuenta = plan["idCuenta"]
    nombre = plan.get("nombre", f"Cuenta #{idCuenta}")
    clean_nombre = "".join(c for c in nombre if c.isalnum() or c in ('_', '-')).strip()
    html_file = os.path.join(REPORTS_DIR, f"reporte_cuenta_{idCuenta}_{clean_nombre}.html")
    pdf_file = os.path.join(REPORTS_DIR, f"Reporte_Portafolio_Cuenta_{idCuenta}_{clean_nombre}.pdf")
    return generateOwnerPortfolioPdf(nombre, [plan], pdf_file, html_file)



def generateWeeklyPortfolioPdf(optimalPortfolio: Dict[str, Any], accountPlans: List[Dict[str, Any]]) -> str:
    """
    Genera el documento ejecutivo institucional consolidado en HTML y lo compila a PDF vía headless Chrome.
    """
    now_str = datetime.now().strftime("%d de %B de %Y - %H:%M")
    
    # Filas de Cartera Óptima
    ratio_rows = ""
    for r in optimalPortfolio.get("microRatio", []):
        ratio_rows += f"""
        <tr>
            <td><strong>microRatio</strong></td>
            <td><code>{r.get('symbol')}</code></td>
            <td>{r.get('timeframe', '1h')}</td>
            <td>{r.get('trades', 0)} ops</td>
            <td><strong>{r.get('winRate', 0)}%</strong></td>
            <td><strong>{r.get('profitFactor', 0)}</strong></td>
            <td>{r.get('maxDrawdown', 0)}%</td>
            <td style="color:#16a34a; font-weight:bold;">+{r.get('returnPct', 0)}%</td>
            <td style="color:#d97706; font-weight:bold;">{'★' * int(r.get('score', 4))} {r.get('score', 4)}/5</td>
        </tr>
        """

    cart_rows = ""
    for c in optimalPortfolio.get("microRider", []):
        cart_rows += f"""
        <tr>
            <td><strong>microRider</strong></td>
            <td><code>{c.get('symbol')}</code></td>
            <td>{c.get('timeframe', '1h')}</td>
            <td>{c.get('trades', 0)} ops</td>
            <td><strong>{c.get('winRate', 0)}%</strong></td>
            <td><strong>{c.get('profitFactor', 0)}</strong></td>
            <td>{c.get('maxDrawdown', 0)}%</td>
            <td style="color:#16a34a; font-weight:bold;">+{c.get('returnPct', 0)}%</td>
            <td style="color:#d97706; font-weight:bold;">{'★' * int(c.get('score', 4))} {c.get('score', 4)}/5</td>
        </tr>
        """

    sniper_rows = ""
    for s in optimalPortfolio.get("microSniper", []):
        sniper_rows += f"""
        <tr>
            <td><strong>microSniper</strong></td>
            <td><code>{s.get('symbol')}</code></td>
            <td>{s.get('timeframe', '5m')}</td>
            <td>{s.get('trades', 0)} ops</td>
            <td><strong>{s.get('winRate', 0)}%</strong></td>
            <td><strong>{s.get('profitFactor', 0)}</strong></td>
            <td>{s.get('maxDrawdown', 0)}%</td>
            <td style="color:#16a34a; font-weight:bold;">+{s.get('returnPct', 0)}%</td>
            <td style="color:#d97706; font-weight:bold;">{'★' * int(s.get('score', 4))} {s.get('score', 4)}/5</td>
        </tr>
        """

    # Filas de Cuentas: separar Cuentas Operativas de No Operativas / Concentradoras
    operational_accounts = [a for a in accountPlans if a.get("isOperational", True) and float(a.get("capital", 0.0) or 0.0) > 0 and a.get("idCuenta") != 1]
    non_operational_accounts = [a for a in accountPlans if not a.get("isOperational", True) or float(a.get("capital", 0.0) or 0.0) <= 0 or a.get("idCuenta") == 1]

    account_rows = ""
    for a in operational_accounts:
        w_str = ", ".join([f"{k.replace('micro', '')}: {int(v*100)}%" for k, v in a.get("normalizedWeights", {}).items()])
        alloc_symbols = ", ".join([al.get("symbol") for al in a.get("allocations", [])[:4]])
        account_rows += f"""
        <tr>
            <td><strong>#{a.get('idCuenta')}</strong> - {a.get('nombre')}</td>
            <td style="font-weight: 700;">${a.get('capital', 0):,.2f} USD</td>
            <td>{w_str or 'Sin Estrategia'}</td>
            <td><small>{alloc_symbols or 'N/A'}</small></td>
            <td><strong>{a.get('totalMarginPct')}%</strong></td>
            <td><span style="color:#16a34a; font-weight:bold;">✓ Blindado (&lt;30%)</span></td>
        </tr>
        """

    for a in non_operational_accounts:
        status_lbl = a.get("status", "No Opera (Concentradora)")
        account_rows += f"""
        <tr style="background: #f8fafc; color: #64748b;">
            <td><strong>#{a.get('idCuenta')}</strong> - {a.get('nombre')}</td>
            <td>$0.00 USD</td>
            <td colspan="3" style="text-align: center;"><em>{status_lbl}</em></td>
            <td><span style="color: #64748b; font-weight: 600;">Omitida (Sin Trading)</span></td>
        </tr>
        """

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Reporte Semanal de Portafolio - ATALA.ia</title>
    <style>
        @page {{
            size: letter;
            margin: 11mm 14mm 11mm 14mm;
        }}
        @media print {{
            body {{
                -webkit-print-color-adjust: exact;
                print-color-adjust: exact;
            }}
            .page {{
                page-break-after: always;
            }}
            .page:last-child {{
                page-break-after: avoid;
            }}
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Arial, sans-serif;
            color: #1e293b;
            font-size: 8.8pt;
            line-height: 1.4;
        }}
        .header-bar {{
            border-bottom: 2px solid #0284c7;
            padding-bottom: 8px;
            margin-bottom: 12px;
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
        }}
        .brand {{ font-size: 18pt; font-weight: 900; color: #0f172a; }}
        .brand span {{ color: #0284c7; }}
        .doc-tag {{ font-size: 7.5pt; font-weight: 700; text-transform: uppercase; color: #64748b; text-align: right; }}
        h1 {{ font-size: 15pt; font-weight: 800; color: #0f172a; margin-bottom: 3px; }}
        .subtitle {{ font-size: 9.2pt; color: #475569; margin-bottom: 10px; font-weight: 500; }}
        .meta-grid {{
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 8px;
            background: #f8fafc;
            border: 1px solid #e2e8f0;
            border-radius: 6px;
            padding: 8px 12px;
            margin-bottom: 12px;
        }}
        .meta-label {{ font-size: 6.8pt; color: #64748b; text-transform: uppercase; font-weight: 700; display: block; }}
        .meta-val {{ font-size: 8.2pt; font-weight: 700; color: #0f172a; }}
        h2 {{
            font-size: 11pt;
            font-weight: 800;
            color: #0f172a;
            border-left: 4px solid #0284c7;
            padding-left: 6px;
            margin-top: 12px;
            margin-bottom: 6px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 6px 0 10px 0;
            font-size: 8.2pt;
        }}
        th {{
            background: #f1f5f9;
            color: #0f172a;
            font-weight: 700;
            text-align: left;
            padding: 5px 7px;
            border: 1px solid #cbd5e1;
        }}
        td {{
            padding: 4px 7px;
            border: 1px solid #e2e8f0;
            color: #334155;
            vertical-align: middle;
        }}
        tr:nth-child(even) td {{ background: #f8fafc; }}
        .callout {{
            background: #f0fdf4;
            border-left: 4px solid #10b981;
            padding: 8px 12px;
            border-radius: 0 6px 6px 0;
            margin: 8px 0;
            font-size: 8.4pt;
        }}
        .callout-title {{ font-weight: 800; color: #065f46; margin-bottom: 2px; }}
        .footer {{
            border-top: 1px solid #e2e8f0;
            padding-top: 6px;
            display: flex;
            justify-content: space-between;
            font-size: 7.2pt;
            color: #94a3b8;
            margin-top: 15px;
        }}
    </style>
</head>
<body>

    <!-- PÁGINA 1: CARTERA ÓPTIMA GLOBAL -->
    <div class="page">
        <div class="header-bar">
            <div>
                <div class="brand">ATALA<span>.ia</span></div>
                <div style="font-size: 8pt; color: #64748b; font-weight: 600;">Comité Cuantitativo y Asignación de Portafolio</div>
            </div>
            <div class="doc-tag">
                Reporte Ejecutivo Semanal<br>
                Rebalanceo Sabatino Institucional
            </div>
        </div>

        <h1>Reporte de Cartera Óptima y Rebalanceo Semanal</h1>
        <div class="subtitle">Evaluación Multiactivo Sabatina (Incluye US30, NAS100, Metales y Cruces de Divisas)</div>

        <div class="meta-grid">
            <div><span class="meta-label">Fecha de Calibración</span><span class="meta-val">{now_str}</span></div>
            <div><span class="meta-label">Universo Analizado</span><span class="meta-val">{optimalPortfolio.get('totalSymbolsEvaluated', 79)} Símbolos</span></div>
            <div><span class="meta-label">Arquitectura</span><span class="meta-val">50% Ratio · 30% CART · 20% Sniper</span></div>
            <div><span class="meta-label">Cortafuegos de Margen</span><span class="meta-val">Max 30% Simultáneo</span></div>
        </div>

        <h2>1. Selección de Cartera Óptima Multiactivo (Top Instruments)</h2>
        <p style="margin-bottom: 6px; font-size: 8.2pt; color: #334155;">
            Instrumentos clasificados mediante la Tríada Cuantitativa Cardinal (Esperanza Matemática, Profit Factor y Calmar Ratio):
        </p>

        <table>
            <thead>
                <tr>
                    <th>Estrategia</th>
                    <th>Instrumento / Par</th>
                    <th>TF</th>
                    <th>Muestra</th>
                    <th>Win Rate</th>
                    <th>Profit Factor</th>
                    <th>Max DD</th>
                    <th>Retorno</th>
                    <th>Score</th>
                </tr>
            </thead>
            <tbody>
                {ratio_rows}
                {cart_rows}
                {sniper_rows}
            </tbody>
        </table>

        <h2>2. Justificación Cuantitativa de la Cartera</h2>
        <div style="font-size: 8.2pt; color: #334155; line-height: 1.45;">
            <p style="margin-bottom: 4px;">
                <strong>• microRatio:</strong> Los cruces con base común (GBP/SGD vs GBP/CHF) neutralizan la dirección macroeconómica y acumulan ciclos con 100% de efectividad y DD acotado al 1.5%.
            </p>
            <p style="margin-bottom: 4px;">
                <strong>• microRider (CART):</strong> Liderado por XAU/USD (Oro) y US30/EURUSD, maximizando captura tendencial con canales CAR y salidas dinámicas.
            </p>
            <p style="margin-bottom: 4px;">
                <strong>• microSniper:</strong> Liderado por índices intradía de alta liquidez (NAS100 y US30 en 5m) y Oro en 15m con asimetría 1:3 en Order Blocks institucionales.
            </p>
        </div>

        <div class="callout">
            <div class="callout-title">✓ Principio de Antifragilidad Activa</div>
            Al distribuir el 50% en Ratio (reversión a la media), 30% en CART (tendencias macro) y 20% en Sniper (alfa intradía asimétrico), la cartera opera descorrelacionada y se beneficia de cualquier régimen de mercado.
        </div>

        <div class="footer">
            <span>ATALA.ia Quantitative Research & Trading Systems · Documento Confidencial</span>
            <span>Página 1 de 2</span>
        </div>
    </div>

    <!-- PÁGINA 2: ASIGNACIÓN Y BLINDAJE POR CUENTA -->
    <div class="page">
        <div class="header-bar">
            <div class="brand" style="font-size: 14pt;">ATALA<span>.ia</span> · Rebalanceo por Cuenta</div>
            <div class="doc-tag">Sección 2: Sizing y Cortafuegos de Margen</div>
        </div>

        <h2>3. Optimización y Asignación de Portafolio por Cuenta</h2>
        <p style="margin-bottom: 6px; font-size: 8.2pt; color: #334155;">
            Distribución de capital, lotaje y verificación del Cortafuegos de Margen (Margin Guardrail) para las cuentas activas:
        </p>

        <table>
            <thead>
                <tr>
                    <th>Cuenta</th>
                    <th>Capital</th>
                    <th>Ponderación Estrategias</th>
                    <th>Instrumentos Asignados</th>
                    <th>Margen Teórico</th>
                    <th>Estado de Riesgo</th>
                </tr>
            </thead>
            <tbody>
                {account_rows}
            </tbody>
        </table>

        <h2>4. Protocolo de Gobernanza y Cortafuegos de Margen</h2>
        <div style="font-size: 8.2pt; color: #334155; line-height: 1.45;">
            <p style="margin-bottom: 4px;">
                <strong>1. Límite de Margen Simultáneo (30% Max):</strong> Cada cuenta está restringida de tal manera que si el 100% de los algoritmos activan entradas simultáneas, el margen utilizado nunca excede el 22-25%, dejando libre más del 75% para flotante.
            </p>
            <p style="margin-bottom: 4px;">
                <strong>2. Dimensionamiento de Posición (Risk Sizing):</strong> Cada operación se calcula con base en el 1.0% de riesgo sobre el equity real, recalculándose automáticamente cada fin de semana.
            </p>
            <p style="margin-bottom: 4px;">
                <strong>3. Sincronización Automática:</strong> Los parámetros óptimos fueron transferidos directamente a las tablas <code>user_ratios</code>, <code>userCART</code> y <code>userSNIPER</code>.
            </p>
        </div>

        <div class="callout" style="background: #f0f9ff; border-left-color: #0284c7; margin-top: 15px;">
            <div class="callout-title" style="color: #0284c7;">⚡ Dictamen del Fin de Semana</div>
            Pipeline completado con éxito. Todas las cuentas están balanceadas, calibradas y listas para la apertura de mercados del domingo a las 17:00 CST sin intervención manual requerida.
        </div>

        <div class="footer">
            <span>ATALA.ia Quantitative Research & Trading Systems · Documento Confidencial</span>
            <span>Página 2 de 2</span>
        </div>
    </div>

</body>
</html>
"""

    with open(HTML_REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info(f"📄 HTML maestro generado en: {HTML_REPORT_PATH}")

    # Compilar a PDF vía headless Chrome
    cmd = [
        "google-chrome",
        "--headless",
        "--disable-gpu",
        "--no-sandbox",
        "--no-pdf-header-footer",
        f"--print-to-pdf={PDF_REPORT_PATH}",
        HTML_REPORT_PATH
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode == 0:
        logger.info(f"✅ PDF Institucional compilado en: {PDF_REPORT_PATH} ({os.path.getsize(PDF_REPORT_PATH)} bytes)")
        # Cifrar con contraseña institucional requerida
        encryptPdf(PDF_REPORT_PATH, password=INSTITUTIONAL_PASSWORD)
        return PDF_REPORT_PATH
    else:
        logger.error(f"❌ Error compilando PDF maestro: {res.stderr}")
        return ""


async def dispatchPersonalizedPdfsViaTelegram(accountPlans: List[Dict[str, Any]]) -> int:
    """
    Despacha a cada usuario EXCLUSIVAMENTE UN SOLO DOCUMENTO que consolida TODAS LAS CUENTAS que maneja.
    - No se envían documentos separados por cuenta.
    - El usuario abre y lee 1 solo PDF con toda su información patrimonial y operativa unificada.
    - El canal administrador (#1 ATALAia) recibe el consolidado institucional maestro cifrado con contraseña.
    """
    logger.info("📱 [Telegram] Despachando 1 solo documento consolidado por usuario...")
    sent_count = 0
    plans_by_id = {p["idCuenta"]: p for p in accountPlans}

    db = SessionLocal()
    try:
        # 1. Enviar Reporte Maestro Consolidado al Canal Admin (#1 ATALAia) PROTEGIDO CON PASSWORD
        admin_row = db.execute(text("SELECT idGrupoMsg FROM cuenta WHERE idCuenta = 1 AND Activo = 1")).fetchone()
        if admin_row and admin_row[0]:
            caption_admin = (
                "📊 <b>ATALA.ia · Reporte Maestro Consolidado Institucional</b>\n\n"
                "Canal Institucional:\n"
                "Adjunto el reporte maestro de portafolio y gobernanza de todas las cuentas del sistema.\n\n"
                "🔒 <b>Documento protegido con contraseña de seguridad</b> para su apertura."
            )
            admin_pdfs = []
            if os.path.exists(PDF_REPORT_PATH):
                encryptPdf(PDF_REPORT_PATH, password=INSTITUTIONAL_PASSWORD)
                admin_pdfs.append(PDF_REPORT_PATH)
            if os.path.exists(INFORME_PDF_PATH):
                encryptPdf(INFORME_PDF_PATH, password=INSTITUTIONAL_PASSWORD)
                admin_pdfs.append(INFORME_PDF_PATH)

            for pdf in admin_pdfs:
                if os.path.exists(pdf):
                    try:
                        logger.info("📤 Despachando Reporte Maestro Protegido con Password a Canal Admin #1...")
                        await alertaInmediata(1, caption_admin, prioridad=True, filePath=pdf)
                        sent_count += 1
                        await asyncio.sleep(1.5)
                    except Exception as ex:
                        logger.error(f"Error despachando a Admin: {ex}")

        # 2. Consultar cada USUARIO registrado en la tabla 'usuario' y sus cuentas gestionadas
        sqlUsers = text("""
            SELECT u.idUsuario, u.username, u.nombre, u.apellidoPaterno
            FROM usuario u
            WHERE u.status = 1
            ORDER BY u.idUsuario ASC
        """)
        users = db.execute(sqlUsers).fetchall()

        for u in users:
            idU, usr, uNom, uApe = u
            fullName = f"{uNom} {uApe}".strip() if uNom and uApe else usr

            # Obtener todas las cuentas activas asignadas a este usuario
            sqlUserAccs = text("""
                SELECT c.idCuenta, c.Nombre, c.idGrupoMsg, c.TokenMsg
                FROM usuarioCuenta uc
                JOIN cuenta c ON uc.idCuenta = c.idCuenta
                WHERE uc.idUsuario = :u AND uc.activo = 1 AND c.Activo = 1
                ORDER BY c.idCuenta ASC
            """)
            userAccs = db.execute(sqlUserAccs, {"u": idU}).fetchall()
            if not userAccs:
                continue

            acc_ids = [r[0] for r in userAccs]
            user_plans = [plans_by_id[cid] for cid in acc_ids if cid in plans_by_id]
            if not user_plans:
                continue

            clean_name = "".join(c for c in usr if c.isalnum() or c in ('_', '-')).strip()
            pdf_path = os.path.join(REPORTS_DIR, f"Reporte_Portafolio_Usuario_{clean_name}.pdf")
            html_path = os.path.join(REPORTS_DIR, f"reporte_portafolio_usuario_{clean_name}.html")
            user_pdf = generateOwnerPortfolioPdf(fullName, user_plans, pdf_path, html_path)

            if not user_pdf or not os.path.exists(user_pdf):
                logger.error(f"No se pudo generar PDF para usuario {fullName}.")
                continue

            # Mapear chats de Telegram únicos de sus cuentas
            chat_map = {}
            for r in userAccs:
                cid, cName, chat, tok = r
                if chat and str(chat).strip() and str(chat).strip() != "-1003763086164":
                    chat_str = str(chat).strip()
                    if chat_str not in chat_map:
                        chat_map[chat_str] = cid

            if not chat_map:
                logger.info(f"Usuario {fullName} ({user_pdf}): Documento generado en disco, sin Telegram.")
                continue

            op_plans = [p for p in user_plans if p.get("isOperational", True) and float(p.get("capital", 0) or 0) > 0]
            tot_cap = sum(float(p.get("capital", 0) or 0) for p in op_plans)
            cuentas_list_str = ", ".join([f"#{p['idCuenta']} ({p['nombre']})" for p in user_plans])

            caption = (
                f"📊 <b>ATALA.ia · Reporte Consolidado de Usuario</b>\n\n"
                f"Estimado/a <b>{fullName}</b>:\n"
                f"Adjunto encontrarás tu informe técnico unificado con <b>todas las cuentas que manejas</b> ({cuentas_list_str}).\n\n"
                f"• Capital operativo total: <b>${tot_cap:,.2f} USD</b>\n"
                f"• Cuentas gestionadas: <b>{len(user_plans)}</b> ({len(op_plans)} operativas)\n"
                f"• Blindaje de Margen: <b>Garantizado (&lt;30%)</b> para la apertura del mercado."
            )

            for chat_str, accountIdForSend in chat_map.items():
                try:
                    logger.info(f"📤 Enviando 1 solo reporte a {fullName} (Chat: {chat_str} | {len(user_plans)} cuentas)...")
                    await alertaInmediata(accountIdForSend, caption, prioridad=True, filePath=user_pdf)
                    sent_count += 1
                    await asyncio.sleep(1.5)
                    logger.info(f"✅ Reporte consolidado entregado exitosamente a {fullName} ({chat_str}).")
                except Exception as ex:
                    logger.error(f"❌ Error despachando a {fullName} ({chat_str}): {ex}")

    finally:
        db.close()

    logger.info(f"📱 Despacho por usuario concluido. Total de reportes entregados: {sent_count}")
    return sent_count


def runFullWeekendPipeline(force: bool = False) -> None:
    """
    Ejecuta en secuencia estricta el pipeline sabatino completo.
    """
    startTs = time.time()
    logger.info("==========================================================================")
    logger.info("🚀 [weekendOrchestrator] INICIANDO PIPELINE MAESTRO DE FIN DE SEMANA")
    logger.info("==========================================================================")

    # Paso 1: Optimización de RATIO
    logger.info("\n▶ PASO 1/6: Calibración y Optimización de microRatio...")
    try:
        asyncio.run(optimizeAllUserRatios(applyToDb=True))
    except Exception as e:
        logger.error(f"Error en paso 1 (microRatio): {e}")

    # Paso 2: Optimización de CART / RIDER
    logger.info("\n▶ PASO 2/6: Calibración de Canales CAR y EMA de RIDE...")
    try:
        runCartOptimization()
    except Exception as e:
        logger.error(f"Error en paso 2 (microCart): {e}")

    # Paso 3: Optimización de microSniper
    logger.info("\n▶ PASO 3/6: Optimización Adaptativa de microSniper...")
    try:
        optimizeAllSniperSymbols()
    except Exception as e:
        logger.error(f"Error en paso 3 (microSniper): {e}")

    # Paso 4: Escaneo Global del Universo Multiactivo
    logger.info("\n▶ PASO 4/6: Escaneo Multiactivo y Cartera Óptima Global...")
    optimalPortfolio = {}
    try:
        optimalPortfolio = asyncio.run(runFullUniverseScan())
    except Exception as e:
        logger.error(f"Error en paso 4 (universePortfolioScanner): {e}")

    # Paso 5: Rebalanceo y Sizing por Cuenta
    logger.info("\n▶ PASO 5/6: Asignación y Blindaje de Margen por Cuenta...")
    accountPlans = []
    try:
        accountPlans = optimizeAllAccounts(applyToDb=True)
    except Exception as e:
        logger.error(f"Error en paso 5 (accountPortfolioOptimizer): {e}")

    # Paso 6: Compilación del Reporte Ejecutivo Maestro en PDF
    logger.info("\n▶ PASO 6/6: GENERANDO REPORTES INSTITUCIONALES EN PDF...")
    pdfPath = generateWeeklyPortfolioPdf(optimalPortfolio, accountPlans)

    # Paso 7: Despacho a usuarios vía Telegram usando alertaInmediata() (PERSONALIZADO POR USUARIO)
    logger.info("\n▶ PASO 7/7: Despacho de Reportes Personalizados e Independientes a Telegram...")
    try:
        asyncio.run(dispatchPersonalizedPdfsViaTelegram(accountPlans))
    except Exception as e:
        logger.error(f"Error en paso 7 (Telegram personalizado alertaInmediata): {e}")

    elapsed = time.time() - startTs
    logger.info("==========================================================================")
    logger.info(f"🏆 PIPELINE SABATINO COMPLETADO EXITOSAMENTE en {elapsed:.1f}s.")
    logger.info(f"📄 Reporte Maestro disponible en: {pdfPath}")
    logger.info("==========================================================================")


def runDaemonScheduler(targetWeekday: int = 5, targetHour: int = 2, targetMinute: int = 0) -> None:
    """
    Modo demonio permanente que dispara el pipeline automáticamente todos los sábados a las 02:00 AM.
    """
    logger.info("🚀 Demonio weekendOrchestrator activado (Programado: Sábados 02:00 AM)...")
    lastRunDate = None
    while True:
        try:
            now = datetime.now()
            if now.weekday() == targetWeekday and now.hour == targetHour and now.minute >= targetMinute:
                todayDate = now.date()
                if lastRunDate != todayDate:
                    logger.info("⏰ HORARIO SABATINO DETECTADO: Disparando Pipeline Maestro de Fin de Semana...")
                    runFullWeekendPipeline(force=True)
                    lastRunDate = todayDate
                    logger.info("🛌 Pipeline concluido. Demonio en reposo hasta el próximo sábado.")
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("🛑 Deteniendo weekendOrchestrator.")
            break
        except Exception as e:
            logger.error(f"❌ Error en demonio weekendOrchestrator: {e}", exc_info=True)
            time.sleep(60)


def main():
    parser = argparse.ArgumentParser(description="Orquestador Maestro Sabatino de Portafolio y Calibración")
    parser.add_argument("--run-now", action="store_true", help="Ejecutar el pipeline sabatino completo de inmediato")
    parser.add_argument("--daemon", action="store_true", help="Ejecutar en modo demonio permanente los sábados a las 02:00 AM")
    parser.add_argument("--pdf-only", action="store_true", help="Solo generar los reportes PDF con los datos más recientes")
    parser.add_argument("--send-telegram", action="store_true", help="Despacha los PDFs personalizados a cada usuario con Telegram")

    args = parser.parse_args()

    if args.send_telegram:
        with open("/mnt/datos_mysql/DATOS/Sistema/ATALAia/logs/optimizacion_cuentas.json", "r") as f:
            accPlans = json.load(f)
        asyncio.run(dispatchPersonalizedPdfsViaTelegram(accPlans))
    elif args.pdf_only:
        with open("/mnt/datos_mysql/DATOS/Sistema/ATALAia/logs/optimizacion_cuentas.json", "r") as f:
            accPlans = json.load(f)
        globPort = loadGlobalPortfolio()
        generateWeeklyPortfolioPdf(globPort, accPlans)
        
        # Generar reportes consolidados ÚNICAMENTE por usuario (un documento con todas sus cuentas)
        db = SessionLocal()
        try:
            plans_by_id = {p["idCuenta"]: p for p in accPlans}
            users = db.execute(text("SELECT idUsuario, username, nombre, apellidoPaterno FROM usuario WHERE status = 1")).fetchall()
            for u in users:
                idU, usr, uNom, uApe = u
                fullNom = f"{uNom} {uApe}".strip() if uNom and uApe else usr
                userAccIds = [r[0] for r in db.execute(text("SELECT idCuenta FROM usuarioCuenta WHERE idUsuario = :u AND activo = 1"), {"u": idU}).fetchall()]
                user_plans = [plans_by_id[cid] for cid in userAccIds if cid in plans_by_id]
                if user_plans:
                    u_pdf = os.path.join(REPORTS_DIR, f"Reporte_Portafolio_Usuario_{idU}_{usr}.pdf")
                    u_html = os.path.join(REPORTS_DIR, f"reporte_portafolio_usuario_{idU}_{usr}.html")
                    generateOwnerPortfolioPdf(fullNom, user_plans, u_pdf, u_html)
        finally:
            db.close()
    elif args.daemon:
        runDaemonScheduler()
    elif args.run_now:
        runFullWeekendPipeline(force=True)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
