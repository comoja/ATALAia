import re

dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

# 1. Add avgMeanSeries calculation right after priceMeanSeries calculation
old_calc_block = """                // Calcular media
                if (normA !== null && normB !== null) {
                    priceMeanSeries.push({ x: dates[i], y: (normA + normB) / 2 });
                } else {
                    priceMeanSeries.push({ x: dates[i], y: null });
                }
            }"""

new_calc_block = """                // Calcular media
                if (normA !== null && normB !== null) {
                    priceMeanSeries.push({ x: dates[i], y: (normA + normB) / 2 });
                } else {
                    priceMeanSeries.push({ x: dates[i], y: null });
                }
            }

            // Calcular Promedio de la Media (línea horizontal recta punteada negra)
            const validMeanValues = priceMeanSeries.map(p => p.y).filter(y => y !== null && !isNaN(y));
            let avgOfMean = null;
            if (validMeanValues.length > 0) {
                avgOfMean = validMeanValues.reduce((a, b) => a + b, 0) / validMeanValues.length;
            }
            const avgMeanSeries = rawData.map((d, i) => ({ x: dates[i], y: avgOfMean }));"""

dash = dash.replace(old_calc_block, new_calc_block)

# 2. Add dataset for Promedio Media right after Media dataset
old_dataset_block = "{ label: 'Media', data: priceMeanSeries, borderColor: '#000000', fill: false, tension: 0.4, pointRadius: 0, borderWidth: 2, yAxisID: 'y-price' },"
new_dataset_block = """{ label: 'Media', data: priceMeanSeries, borderColor: '#000000', fill: false, tension: 0.4, pointRadius: 0, borderWidth: 2, yAxisID: 'y-price' },
                        { label: 'Promedio Media' + (avgOfMean !== null ? ' [' + avgOfMean.toFixed(4) + ']' : ''), data: avgMeanSeries, borderColor: '#000000', fill: false, tension: 0, pointRadius: 0, borderWidth: 2, borderDash: [6, 4], spanGaps: true, yAxisID: 'y-price' },"""

dash = dash.replace(old_dataset_block, new_dataset_block)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Added straight dashed black line for Promedio de la Media in dashboard.xhtml!")
