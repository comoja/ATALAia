import base64
import os
import subprocess

doc_dir = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/docs/rider_summary"
pdf_output = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/RIDER_Manual_Operativo_Entradas_y_Cierres.pdf"

def img_to_base64(path):
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")

img1_b64 = img_to_base64(os.path.join(doc_dir, "grafico1_rvol_filtro.png"))
img2_b64 = img_to_base64(os.path.join(doc_dir, "grafico2_breakeven_tsl.png"))
img3_b64 = img_to_base64(os.path.join(doc_dir, "grafico3_reingreso_car.png"))

html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>Manual Operativo y Técnico: Estrategia RIDER (CART) - ATALA.ia</title>
<style>
  @page {{
    size: letter;
    margin: 18mm 16mm 18mm 16mm;
    @bottom-right {{
      content: counter(page);
    }}
  }}
  body {{
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    color: #1e293b;
    line-height: 1.5;
    font-size: 11pt;
    margin: 0;
    padding: 0;
  }}
  .header-cover {{
    border-bottom: 3px solid #2563eb;
    padding-bottom: 12px;
    margin-bottom: 20px;
  }}
  .badge-system {{
    display: inline-block;
    background: #1e3a8a;
    color: #ffffff;
    font-size: 8.5pt;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 4px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
  }}
  h1 {{
    font-size: 22pt;
    color: #0f172a;
    margin: 8px 0 4px 0;
    font-weight: 800;
  }}
  .subtitle {{
    font-size: 11.5pt;
    color: #475569;
    font-weight: 500;
    margin-bottom: 8px;
  }}
  .meta-bar {{
    display: flex;
    justify-content: space-between;
    font-size: 8.5pt;
    color: #64748b;
    border-top: 1px solid #e2e8f0;
    padding-top: 6px;
    margin-top: 6px;
  }}
  h2 {{
    font-size: 14pt;
    color: #1e3a8a;
    border-left: 4px solid #2563eb;
    padding-left: 8px;
    margin-top: 22px;
    margin-bottom: 10px;
    font-weight: 700;
  }}
  h3 {{
    font-size: 11.5pt;
    color: #0f172a;
    margin-top: 14px;
    margin-bottom: 6px;
    font-weight: 700;
  }}
  p {{
    margin: 0 0 10px 0;
    text-align: justify;
  }}
  .card {{
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 6px;
    padding: 12px 14px;
    margin-bottom: 14px;
  }}
  .card-callout {{
    border-left: 4px solid #0ea5e9;
    background: #f0f9ff;
  }}
  .card-success {{
    border-left: 4px solid #10b981;
    background: #f0fdf4;
  }}
  .card-danger {{
    border-left: 4px solid #ef4444;
    background: #fef2f2;
  }}
  .card-warning {{
    border-left: 4px solid #f59e0b;
    background: #fffbeb;
  }}
  ul, ol {{
    margin: 0 0 10px 0;
    padding-left: 20px;
  }}
  li {{
    margin-bottom: 4px;
  }}
  .figure-container {{
    text-align: center;
    margin: 16px 0;
    page-break-inside: avoid;
  }}
  .figure-container img {{
    max-width: 98%;
    height: auto;
    border: 1px solid #cbd5e1;
    border-radius: 6px;
    box-shadow: 0 2px 4px rgba(0,0,0,0.05);
  }}
  .figure-caption {{
    font-size: 8.5pt;
    color: #475569;
    margin-top: 6px;
    font-style: italic;
  }}
  table {{
    width: 100%;
    border-collapse: collapse;
    margin: 12px 0;
    font-size: 9pt;
  }}
  th, td {{
    border: 1px solid #cbd5e1;
    padding: 6px 10px;
    text-align: left;
  }}
  th {{
    background: #1e293b;
    color: #ffffff;
    font-weight: 600;
  }}
  tr:nth-child(even) {{
    background: #f8fafc;
  }}
  .page-break {{
    page-break-before: always;
  }}
  .formula {{
    background: #f1f5f9;
    border: 1px solid #cbd5e1;
    border-radius: 4px;
    padding: 8px 12px;
    font-family: "Courier New", Courier, monospace;
    font-size: 9.5pt;
    color: #0f172a;
    margin: 8px 0;
  }}
  .tag {{
    display: inline-block;
    padding: 2px 6px;
    font-size: 8pt;
    font-weight: 700;
    border-radius: 3px;
  }}
  .tag-buy {{ background: #dcfce7; color: #166534; border: 1px solid #bbf7d0; }}
  .tag-sell {{ background: #fee2e2; color: #991b1b; border: 1px solid #fecaca; }}
  .tag-be {{ background: #e0f2fe; color: #075985; border: 1px solid #bae6fd; }}
  .tag-rvol {{ background: #fef3c7; color: #92400e; border: 1px solid #fde68a; }}
</style>
</head>
<body>

<div class="header-cover">
  <span class="badge-system">ATALA.ia Algorithmic Trading Systems</span>
  <h1>MANUAL OPERATIVO Y TÉCNICO: ESTRATEGIA RIDER (CART)</h1>
  <div class="subtitle">Arquitectura Cuantitativa de Ruptura, Filtro RVOL Adaptativo, Trailing Stop Loss y BreakEven Dinámico</div>
  <div class="meta-bar">
    <span><b>Módulo:</b> microCart.py &amp; cart.xhtml</span>
    <span><b>Fecha de Emisión:</b> Noviembre 2026</span>
    <span><b>Versión:</b> 2.4 Institucional</span>
    <span><b>Estado:</b> Producción Activo</span>
  </div>
</div>

<h2>1. Resumen Ejecutivo y Filosofía Cuantitativa</h2>
<p>
  La estrategia <b>RIDER (CART - Channel Adaptive Real Trend)</b> es un sistema cuantitativo de seguimiento de tendencia de alta convicción que opera sobre <b>precios reales no normalizados</b>. A diferencia de las estrategias de reversión a la media (como RATIO), RIDER busca capturar impulsos direccionales explosivos en el momento exacto en que el precio rompe su zona de equilibrio estadístico normal y desarrolla una tendencia sostenida fuera del ruido de mercado.
</p>
<p>
  Para evitar la vulnerabilidad clásica de los sistemas de ruptura (las trampas de liquidez o <i>fakeouts</i>), RIDER incorpora una triple barrera de confirmación:
</p>
<ul>
  <li><b>Canal Adaptativo de Ruido (CAR):</b> Delimita estadísticamente el ruido natural del activo mediante bandas envolventes $\text{EMA} \pm 1.5\sigma$.</li>
  <li><b>Filtro de Volumen Relativo (RVOL Adaptativo):</b> Exige que la vela de ruptura cuente con volumen institucional $\ge 1.0\times$ relativo a la propia memoria histórica de la emisora.</li>
  <li><b>Gestión Dinámica de Cierre:</b> Salto inmediato a <b>BreakEven (+0.10x NATR)</b> al avanzar +1.0x NATR y acompañamiento por <b>Trailing Stop Loss monótono</b> delimitado al exterior del canal.</li>
</ul>

<div class="card card-callout">
  <b>Fórmula del Canal Adaptativo de Ruido (CAR):</b>
  <div class="formula">
    EMA_Central = EMA(Precios, carWindow)<br>
    CAR_Superior = EMA_Central + (carMultiplier * DesviacionEstandar(Precios, carWindow))<br>
    CAR_Inferior = EMA_Central - (carMultiplier * DesviacionEstandar(Precios, carWindow))
  </div>
  <i>Donde <b>carWindow</b> es la ventana adaptativa calibrada para la emisora (típicamente 14 a 20 periodos) y <b>carMultiplier</b> define la holgura estadística (1.5σ).</i>
</div>

<h2>2. Arquitectura de Entradas (Condiciones Institucionales)</h2>
<p>
  El microservicio <code>microCart.py</code> escanea en vivo cada 5 segundos las cotizaciones de MetaTrader 5 y MySQL. Una orden de apertura se envía únicamente cuando se satisfacen <b>todas</b> las condiciones de la dirección correspondiente:
</p>

<h3>A. Entrada en COMPRA (LARGO ▲)</h3>
<div class="card card-success">
  <ul>
    <li><span class="tag tag-buy">Precio</span> <b>Ruptura Superior:</b> El precio actual supera el canal superior: <code>Px &gt; CAR_Superior</code>.</li>
    <li><span class="tag tag-buy">Inercia</span> <b>Pendiente Positiva:</b> La vela actual muestra avance favorable: <code>(Px - Px_prev &gt; 0)</code> y cuerpo alcista <code>(Px &gt;= Open)</code>.</li>
    <li><span class="tag tag-rvol">Volumen</span> <b>Filtro RVOL Adaptativo:</b> El volumen de la barra de ruptura cumple: <code>RVOL = (Volumen / Media_Volumen[carWindow]) &gt;= 1.0</code>. Si el volumen es anémico (&lt; 1.0), la ruptura se considera falsa y se descarta.</li>
    <li><span class="tag tag-be">Salud</span> <b>Margen Libre &ge; 200%:</b> Capital suficiente para absorber la garantía sin comprometer la cuenta.</li>
    <li><span class="tag tag-be">Sesión</span> <b>Filtro Horario y Mercado Abierto:</b> Dentro de la ventana operativa configurada y mercado en sesión activa.</li>
  </ul>
</div>

<h3>B. Entrada en VENTA (CORTO ▼)</h3>
<div class="card card-danger">
  <ul>
    <li><span class="tag tag-sell">Precio</span> <b>Ruptura Inferior:</b> El precio actual perfora el canal inferior: <code>Px &lt; CAR_Inferior</code>.</li>
    <li><span class="tag tag-sell">Inercia</span> <b>Pendiente Negativa:</b> La vela actual muestra presión vendedora: <code>(Px - Px_prev &lt; 0)</code> y cuerpo bajista <code>(Px &lt;= Open)</code>.</li>
    <li><span class="tag tag-rvol">Volumen</span> <b>Filtro RVOL Adaptativo:</b> <code>RVOL &gt;= 1.0</code>. Requiere volumen institucional vendedor confirmado.</li>
    <li><span class="tag tag-sell">Dividendos</span> <b>Filtro Anti-Dividendos:</b> Prohibido abrir cortos si el activo tiene fecha ex-dividendo programada en las próximas 24 horas o durante el evento.</li>
  </ul>
</div>

<div class="figure-container">
  <img src="data:image/png;base64,{img1_b64}" alt="Filtro RVOL y Ruptura CAR">
  <div class="figure-caption"><b>Figura 1:</b> Mecánica de Entrada RIDER. En x=10 se produce una falsa ruptura con volumen anémico (RVOL=0.60x) que el sistema descarta. En x=20, la ruptura es confirmada por volumen institucional (RVOL=2.30x) y se abre la posición.</div>
</div>

<div class="page-break"></div>

<h2>3. Arquitectura de Cierres y Gestión de Riesgo</h2>
<p>
  RIDER implementa una gestión de salida de tres capas jerárquicas que garantiza la máxima protección del capital y la captura de beneficios en tendencias extendidas:
</p>

<h3>Capa 1: BreakEven Adaptativo por NATR (+1.0x NATR Trigger)</h3>
<p>
  El objetivo del BreakEven adaptativo es <b>eliminar el riesgo de pérdida en cuanto la operación demuestra inercia favorable</b>:
</p>
<ul>
  <li><b>Gatillo de Activación:</b> Se dispara en el instante exacto en que el precio extremo alcanzado supera el precio de entrada en <b>+1.0 &times; NATR</b> (Normalized Average True Range dinámico).</li>
  <li><b>Piso de Stop Loss Asegurado:</b> El Stop Loss salta de inmediato a:
    <div class="formula">
      COMPRA: StopLoss = Precio_Entrada + (0.10 * NATR)<br>
      VENTA: &nbsp;StopLoss = Precio_Entrada - (0.10 * NATR)
    </div>
  </li>
  <li><b>Ventaja Operativa:</b> El colchón de <code>+0.10 &times; NATR</code> cubre íntegramente el spread del broker y las comisiones nocturnas (swap), garantizando que el trade nunca cierre en números rojos una vez que avanzó a favor.</li>
</ul>

<h3>Capa 2: Trailing Stop Loss Dinámico (TSL Monótono)</h3>
<ul>
  <li><b>Seguimiento del Extremo:</b> El Stop Loss se re-calcula continuamente a una distancia de <code>1.0 &times; NATR</code> respecto al pico (en compras) o valle (en ventas).</li>
  <li><b>Estrictamente Monótono:</b> El Stop Loss <b>NUNCA retrocede</b>. Si el precio retrocede, el SL permanece plano en su nivel más alto alcanzado.</li>
  <li><b>Frontera Natural del CAR:</b> El Trailing SL tiene acotación institucional: nunca invade el interior del canal CAR mientras la tendencia esté vigente.</li>
</ul>

<div class="figure-container">
  <img src="data:image/png;base64,{img2_b64}" alt="BreakEven y Trailing Stop Loss">
  <div class="figure-caption"><b>Figura 2:</b> Ciclo de Gestión Dinámica. Tras la entrada (1), el precio avanza +1.0x NATR y el Stop Loss da un salto instantáneo a BreakEven +0.10x NATR (2), blindando la posición. Posteriormente el Trailing SL acompaña la subida hasta cerrar en ganancia máxima (3).</div>
</div>

<h3>Capa 3: Salida por Reingreso al CAR (Filtro Anti-Ruido al Cierre)</h3>
<ul>
  <li><b>Regla de Agotamiento de Tendencia:</b> Si el precio pierde la inercia y retorna al interior del canal CAR (<code>Px &lt;= CAR_Superior</code> en compras o <code>Px &gt;= CAR_Inferior</code> en ventas), la tendencia ha concluido y se ejecuta el cierre inmediato.</li>
  <li><b>Evaluación Estricta al Cierre de Vela (Close):</b> Las mechas intradiarias (spikes transitorios de volatilidad) <b>NO sacan la posición</b> si el cierre de la vela se mantiene en territorio exterior. Esto evita salidas prematuras causadas por ruido de microestructura.</li>
</ul>

<div class="figure-container">
  <img src="data:image/png;base64,{img3_b64}" alt="Salida por Reingreso al CAR">
  <div class="figure-caption"><b>Figura 3:</b> Salida por Reingreso al Canal. En x=6 una mecha intradiaria perfora el CAR, pero el cierre de vela (Close) se mantiene fuera, preservando la posición abierta. En x=8, el Close confirma el reingreso al canal y se ejecuta el cierre ordenado.</div>
</div>

<div class="page-break"></div>

<h2>4. Tabla Comparativa de Parámetros y Adaptabilidad por Activo</h2>
<p>
  RIDER adapta de forma autónoma su sensibilidad matemática dependiendo de la naturaleza del mercado operado:
</p>

<table>
  <thead>
    <tr>
      <th>Familia de Activo</th>
      <th>Ejemplos</th>
      <th>Tipo de Volumen</th>
      <th>Ventana RVOL</th>
      <th>Piso NATR (SL)</th>
      <th>Comportamiento Específico</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><b>Criptomonedas</b></td>
      <td>BTC/USD, ETH/USD</td>
      <td>Real (Contratos)</td>
      <td><code>carWindow</code> (14-20)</td>
      <td><b>0.50%</b> mín.</td>
      <td>Operación 24/7 sin cierre de fin de semana. Alta volatilidad con trailing amplio.</td>
    </tr>
    <tr>
      <td><b>Metales Preciosos</b></td>
      <td>XAU/USD (Oro), XAG/USD (Plata)</td>
      <td>Tick Volume MT5</td>
      <td><code>carWindow</code> (14-20)</td>
      <td><b>0.35% - 0.40%</b> mín.</td>
      <td>Cierre preventivo de fin de semana para evitar gaps dominicales agresivos.</td>
    </tr>
    <tr>
      <td><b>Divisas Mayores (Forex)</b></td>
      <td>EUR/USD, USD/CAD, USD/JPY</td>
      <td>Tick Volume MT5</td>
      <td><code>carWindow</code> (20)</td>
      <td><b>0.15%</b> mín.</td>
      <td>Filtro RVOL elimina rupturas de baja liquidez durante la sesión asiática nocturna.</td>
    </tr>
    <tr>
      <td><b>Índices Bursátiles</b></td>
      <td>NAS100, SPX500</td>
      <td>Tick Volume MT5</td>
      <td><code>carWindow</code> (20)</td>
      <td><b>0.15% - 0.25%</b> mín.</td>
      <td>Captura de tendencias macro intradiarias durante la sesión de Wall Street.</td>
    </tr>
    <tr>
      <td><b>Acciones BMV</b></td>
      <td>AMX, ALSEA, BIMBO, WALMEX</td>
      <td>Real (Títulos)</td>
      <td><code>carWindow</code> (20-30)</td>
      <td><b>2.50%</b> diario</td>
      <td>Restricción estricta de ventas en corto ante periodos de pago de dividendos.</td>
    </tr>
  </tbody>
</table>

<h2>5. Ciclo Algorítmico del Microservicio (microCart.py)</h2>
<div class="card">
  <ol>
    <li><b>Lectura de userCART:</b> Obtiene los 25 pares registrados y activos con sus ventanas adaptativas particulares.</li>
    <li><b>Descarga de Cotizaciones (MT5 Bridge &amp; MySQL):</b> Obtiene velas históricas y cotizaciones en vivo en milisegundos.</li>
    <li><b>Cálculo de Indicadores:</b>
      <ul>
        <li>EMA Central y Desviación Estándar de Precios $\to$ Canal CAR.</li>
        <li>Media móvil de volumen de ventana <code>carWindow</code> $\to$ Ratio RVOL.</li>
        <li>NATR institucional específico por símbolo.</li>
      </ul>
    </li>
    <li><b>Comprobación de Posición Existente:</b>
      <ul>
        <li><b>Si hay trade OPEN:</b> Monitorea en vivo gatillo de BreakEven (+1.0x NATR), Trailing SL dinámico y Reingreso al CAR al cierre de vela. Ejecuta webhook de cierre si se gatilla salida.</li>
        <li><b>Si NO hay trade OPEN:</b> Evalúa ruptura de CAR con pendiente favorable + confirmación de $RVOL \ge 1.0$. Si califica, valida margen ($\ge 200\%$) y despacha orden de apertura inmediata al broker.</li>
      </ul>
    </li>
    <li><b>Cooldown y Respiro Anti-Rate-Limit:</b> Pausa de 2.0s entre peticiones para garantizar estabilidad en MT5 Bridge.</li>
  </ol>
</div>

<div class="card card-success" style="margin-top: 15px; text-align: center;">
  <b>ATALA.ia Algorithmic Framework &copy; 2026. Todos los derechos reservados.</b><br>
  <span style="font-size: 8.5pt; color: #475569;">Documentación generada automáticamente conforme a las directivas de control y conciliación de trading institucional.</span>
</div>

</body>
</html>
"""

html_path = os.path.join(doc_dir, "manual_rider.html")
with open(html_path, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"HTML generado en {html_path}")

cmd = [
    "google-chrome",
    "--headless",
    "--disable-gpu",
    "--no-sandbox",
    f"--print-to-pdf={pdf_output}",
    html_path
]
res = subprocess.run(cmd, capture_output=True, text=True)
print(f"Chrome exit code: {res.returncode}")
if os.path.exists(pdf_output):
    print(f"PDF exitoso: {pdf_output} ({os.path.getsize(pdf_output)} bytes)")
else:
    print(f"Error generando PDF: {res.stderr}")
