import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

old_scales = """                                'y-price': {
                                    type: 'linear',
                                    position: 'left',
                                    min: -0.05,
                                    max: 1.05,
                                    grid: { color: 'rgba(226, 232, 240, 0.8)' },
                                    ticks: {
                                        color: '#334155',
                                        font: { family: 'Inter, sans-serif', size: 10.5 },
                                        callback: function(val) { return val.toFixed(2); }
                                    },
                                    title: {
                                        display: true,
                                        text: 'Precios Normalizados (0 a 1)',
                                        color: '#64748b',
                                        font: { family: 'Inter, sans-serif', size: 11.5, weight: 600 }
                                    }
                                }"""

new_scales = """                                'yA': {
                                    type: 'linear',
                                    position: 'left',
                                    min: minA - 0.05 * rangeA,
                                    max: minA + 1.05 * rangeA,
                                    grid: {
                                        color: 'rgba(226, 232, 240, 0.8)'
                                    },
                                    ticks: {
                                        color: '#007bff',
                                        font: { family: 'Inter, sans-serif', size: 10.5, weight: 800 },
                                        callback: function (value) {
                                            return value.toFixed(5);
                                        }
                                    }
                                },
                                'yB': {
                                    type: 'linear',
                                    position: 'left',
                                    min: minB - 0.05 * rangeB,
                                    max: minB + 1.05 * rangeB,
                                    grid: {
                                        drawOnChartArea: false
                                    },
                                    ticks: {
                                        color: '#ff8c00',
                                        font: { family: 'Inter, sans-serif', size: 10.5, weight: 800 },
                                        callback: function (value) {
                                            return pairB.includes('JPY') ? value.toFixed(3) : value.toFixed(5);
                                        }
                                    }
                                },
                                'y-price': {
                                    type: 'linear',
                                    position: 'right',
                                    min: -0.05,
                                    max: 1.05,
                                    display: false
                                }"""

if old_scales in content:
    content = content.replace(old_scales, new_scales)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    print("Subwindow scales patch applied successfully.")
else:
    print("Old scales not found, patch failed.")

