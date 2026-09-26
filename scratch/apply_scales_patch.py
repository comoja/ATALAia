import re

file_path = "/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml"

with open(file_path, "r", encoding="utf-8") as f:
    content = f.read()

# Find the scales section in the unified chart
# Specifically the y and y-price config

old_scales = """                            y: {
                                type: 'linear',
                                position: 'left',
                                grid: {
                                    color: 'rgba(163, 177, 198, 0.35)',
                                    borderColor: 'rgba(163, 177, 198, 0.5)'
                                },
                                ticks: {
                                    color: '#334155',
                                    font: {
                                        family: 'Plus Jakarta Sans, sans-serif',
                                        size: 11.5,
                                        weight: 600
                                    },
                                    callback: function (value) {
                                        return value.toFixed(4);
                                    }
                                }
                            },
                            'y-price': {
                                type: 'linear',
                                position: 'right',
                                min: -0.1,
                                max: 1.15,
                                grid: {
                                    drawOnChartArea: false,
                                },
                                ticks: {
                                    color: '#334155',
                                    font: {
                                        family: 'Plus Jakarta Sans, sans-serif',
                                        size: 11.5,
                                        weight: 600
                                    }
                                }
                            },"""

new_scales = """                            'yA': {
                                type: 'linear',
                                position: 'left',
                                min: minA - 0.1 * rangeA,
                                max: minA + 1.15 * rangeA,
                                grid: {
                                    color: 'rgba(163, 177, 198, 0.35)',
                                    borderColor: 'rgba(163, 177, 198, 0.5)'
                                },
                                ticks: {
                                    color: '#007bff',
                                    font: {
                                        family: 'Plus Jakarta Sans, sans-serif',
                                        size: 11.5,
                                        weight: 800
                                    },
                                    callback: function (value) {
                                        return value.toFixed(5);
                                    }
                                }
                            },
                            'yB': {
                                type: 'linear',
                                position: 'left',
                                min: minB - 0.1 * rangeB,
                                max: minB + 1.15 * rangeB,
                                grid: {
                                    drawOnChartArea: false
                                },
                                ticks: {
                                    color: '#ff8c00',
                                    font: {
                                        family: 'Plus Jakarta Sans, sans-serif',
                                        size: 11.5,
                                        weight: 800
                                    },
                                    callback: function (value) {
                                        return pairB.includes('JPY') ? value.toFixed(3) : value.toFixed(5);
                                    }
                                }
                            },
                            'y-price': {
                                type: 'linear',
                                position: 'right',
                                min: -0.1,
                                max: 1.15,
                                display: false
                            },"""

if old_scales in content:
    content = content.replace(old_scales, new_scales)
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)
    print("Patch applied successfully.")
else:
    print("Old scales not found, patch failed.")

