import re

# 1. Update dashboard.xhtml
dashboard_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dashboard_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1.1 Remove 'Distribución Estadística' tab if present
idx_tab_dist = content.find('<p:tab title="Distribución Estadística">')
idx_tab_macd = content.find('<p:tab title="Indicador iMACD (Media)">')

if idx_tab_dist != -1 and idx_tab_macd != -1:
    content = content[:idx_tab_dist] + content[idx_tab_macd:]
    print("Removed Distribución Estadística tab")

# 1.2 Remove standard deviation calculations in JS
std_calc_pattern = re.compile(
    r'// -+\s*// DESVIACIÓN ESTÁNDAR DE PRECIOS NORMALIZADOS[\s\S]*?const denStdBelowSeries = rawData\.map\(\(d, i\) => \(\{ x: dates\[i\], y: stdBelowB \}\)\);',
    re.MULTILINE
)
if std_calc_pattern.search(content):
    content = std_calc_pattern.sub('// (Desviación estándar removida)', content)
    print("Removed std dev JS calculations")
else:
    print("Warning: std_calc_pattern not found directly, checking manual slice...")
    idx_std_start = content.find('// DESVIACIÓN ESTÁNDAR DE PRECIOS NORMALIZADOS')
    if idx_std_start != -1:
        idx_std_end = content.find('const crossPointsASeries = [];', idx_std_start)
        if idx_std_end != -1:
            content = content[:idx_std_start] + '// (Desviación estándar removida)\n            ' + content[idx_std_end:]
            print("Removed std dev calculations by index")

# 1.3 Remove std dev condition in cross points detection
# Old condition:
# if (!isProj && i > 0 && stdAboveA !== null && stdBelowA !== null && stdAboveB !== null && stdBelowB !== null) {
#     ...
#     const isCrossDownHigh_A = (prevP_A >= prevEma_A && currP_A < currEma_A && currP_A >= stdAboveA);
#     const isCrossUpLow_A = (prevP_A <= prevEma_A && currP_A > currEma_A && currP_A <= stdBelowA);
#     ...
#     const isCrossDownHigh_B = (prevP_B >= prevEma_B && currP_B < currEma_B && currP_B >= stdAboveB);
#     const isCrossUpLow_B = (prevP_B <= prevEma_B && currP_B > currEma_B && currP_B <= stdBelowB);

content = content.replace(
    'if (!isProj && i > 0 && stdAboveA !== null && stdBelowA !== null && stdAboveB !== null && stdBelowB !== null) {',
    'if (!isProj && i > 0) {'
)

content = content.replace(
    'const isCrossDownHigh_A = (prevP_A >= prevEma_A && currP_A < currEma_A && currP_A >= stdAboveA);',
    'const isCrossDownHigh_A = (prevP_A >= prevEma_A && currP_A < currEma_A);'
)
content = content.replace(
    'const isCrossUpLow_A = (prevP_A <= prevEma_A && currP_A > currEma_A && currP_A <= stdBelowA);',
    'const isCrossUpLow_A = (prevP_A <= prevEma_A && currP_A > currEma_A);'
)

content = content.replace(
    'const isCrossDownHigh_B = (prevP_B >= prevEma_B && currP_B < currEma_B && currP_B >= stdAboveB);',
    'const isCrossDownHigh_B = (prevP_B >= prevEma_B && currP_B < currEma_B);'
)
content = content.replace(
    'const isCrossUpLow_B = (prevP_B <= prevEma_B && currP_B > currEma_B && currP_B <= stdBelowB);',
    'const isCrossUpLow_B = (prevP_B <= prevEma_B && currP_B > currEma_B);'
)

# 1.4 Remove the 4 dataset lines in Chart.js
dataset_pattern = re.compile(
    r'\s*\{\s*label:\s*\'\+1σ\s*\'[\s\S]*?yAxisID:\s*\'y-price\'\s*\},'
    r'\s*\{\s*label:\s*\'-1σ\s*\'[\s\S]*?yAxisID:\s*\'y-price\'\s*\},'
    r'\s*\{\s*label:\s*\'\+1σ\s*\'[\s\S]*?yAxisID:\s*\'y-price\'\s*\},'
    r'\s*\{\s*label:\s*\'-1σ\s*\'[\s\S]*?yAxisID:\s*\'y-price\'\s*\},',
    re.MULTILINE
)
content = dataset_pattern.sub('', content)

# 1.5 Remove std dev from tooltip callback if present
content = re.sub(r'// 7\. Bandas de desviación \(\+1σ, -1σ\)[\s\S]*?colorBox \+ label \+ \': \' \+ parsedY\.toFixed\(4\) \+ \'</td></tr>\';\s*\}', '', content)

with open(dashboard_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("dashboard.xhtml cleaned of standard deviation!")

# 2. Clean DashboardBean.java
bean_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/java/com/atalaia/correlation/beans/DashboardBean.java'
with open(bean_path, 'r', encoding='utf-8') as f:
    bean_content = f.read()

# Disable Gaussian / Histogram processing if needed
bean_content = bean_content.replace(
    'createGaussianModel(statsNode.get("bellCurve"));',
    '// createGaussianModel(statsNode.get("bellCurve"));'
)
bean_content = bean_content.replace(
    'createHistogramModel(statsNode.get("histogram"));',
    '// createHistogramModel(statsNode.get("histogram"));'
)

with open(bean_path, 'w', encoding='utf-8') as f:
    f.write(bean_content)

print("DashboardBean.java updated!")
