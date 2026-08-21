dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_std_block = """            // -------------------------------------------------------------
            // DESVIACIÓN ESTÁNDAR DE PRECIOS NORMALIZADOS
            // (Par A - Azules / Par B - Naranjas)
            // Se calcula la Media (μ) y Desviación Estándar (σ) de cada serie normalizada
            // Banda Superior (+1σ) y Banda Inferior (-1σ)
            // -------------------------------------------------------------
            const validNormA = priceASeries.map(p => p.y).filter(y => y !== null);
            const validNormB = priceBSeries.map(p => p.y).filter(y => y !== null);

            // Par A
            let meanNormA = null, stdNormA = null, stdAboveA = null, stdBelowA = null;
            let rawStdAboveA = null, rawStdBelowA = null;
            if (validNormA.length > 0) {
                meanNormA = validNormA.reduce((a, b) => a + b, 0) / validNormA.length;
                const varianceA = validNormA.reduce((a, b) => a + Math.pow(b - meanNormA, 2), 0) / validNormA.length;
                stdNormA = Math.sqrt(varianceA);
                stdAboveA = meanNormA + stdNormA;
                stdBelowA = meanNormA - stdNormA;
                rawStdAboveA = minA + (stdAboveA * rangeA);
                rawStdBelowA = minA + (stdBelowA * rangeA);
            }

            // Par B
            let meanNormB = null, stdNormB = null, stdAboveB = null, stdBelowB = null;
            let rawStdAboveB = null, rawStdBelowB = null;
            if (validNormB.length > 0) {
                meanNormB = validNormB.reduce((a, b) => a + b, 0) / validNormB.length;
                const varianceB = validNormB.reduce((a, b) => a + Math.pow(b - meanNormB, 2), 0) / validNormB.length;
                stdNormB = Math.sqrt(varianceB);
                stdAboveB = meanNormB + stdNormB;
                stdBelowB = meanNormB - stdNormB;
                rawStdAboveB = minB + (stdAboveB * rangeB);
                rawStdBelowB = minB + (stdBelowB * rangeB);
            }"""

new_std_block = """            // -------------------------------------------------------------
            // DESVIACIÓN ESTÁNDAR DE PRECIOS NORMALIZADOS
            // (Par A - Azules / Par B - Naranjas)
            // Las bandas (+1σ y -1σ) parten ahora del Promedio de la Media (avgOfMean)
            // -------------------------------------------------------------
            const validNormA = priceASeries.map(p => p.y).filter(y => y !== null);
            const validNormB = priceBSeries.map(p => p.y).filter(y => y !== null);
            const baseCenter = (avgOfMean !== null && !isNaN(avgOfMean)) ? avgOfMean : 0.5;

            // Par A
            let meanNormA = null, stdNormA = null, stdAboveA = null, stdBelowA = null;
            let rawStdAboveA = null, rawStdBelowA = null;
            if (validNormA.length > 0) {
                meanNormA = validNormA.reduce((a, b) => a + b, 0) / validNormA.length;
                const varianceA = validNormA.reduce((a, b) => a + Math.pow(b - meanNormA, 2), 0) / validNormA.length;
                stdNormA = Math.sqrt(varianceA);
                stdAboveA = baseCenter + stdNormA;
                stdBelowA = baseCenter - stdNormA;
                rawStdAboveA = minA + (stdAboveA * rangeA);
                rawStdBelowA = minA + (stdBelowA * rangeA);
            }

            // Par B
            let meanNormB = null, stdNormB = null, stdAboveB = null, stdBelowB = null;
            let rawStdAboveB = null, rawStdBelowB = null;
            if (validNormB.length > 0) {
                meanNormB = validNormB.reduce((a, b) => a + b, 0) / validNormB.length;
                const varianceB = validNormB.reduce((a, b) => a + Math.pow(b - meanNormB, 2), 0) / validNormB.length;
                stdNormB = Math.sqrt(varianceB);
                stdAboveB = baseCenter + stdNormB;
                stdBelowB = baseCenter - stdNormB;
                rawStdAboveB = minB + (stdAboveB * rangeB);
                rawStdBelowB = minB + (stdBelowB * rangeB);
            }"""

dash = dash.replace(old_std_block, new_std_block)

with open(dash_path, 'w', encoding='utf-8') as f:
    f.write(dash)

print("Updated stdAbove and stdBelow calculations to start from avgOfMean in dashboard.xhtml!")
