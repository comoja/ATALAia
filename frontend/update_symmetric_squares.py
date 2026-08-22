dash_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/frontend/src/main/resources/META-INF/resources/dashboard.xhtml'
with open(dash_path, 'r', encoding='utf-8') as f:
    dash = f.read()

old_logic = '''                        // 2. CASO NO COINCIDENTE (Cruce solo en Par A o solo en Par B) -> CUADROS (rect)
                        else if (hasSignal_A || hasSignal_B) {
                            if (hasSignal_A) {
                                valA = currP_A;
                                borderColorA = isCrossDownHigh_A ? "#ef4444" : "#22c55e";
                                colorA = isCrossDownHigh_A ? "#ef4444" : "#22c55e";
                                radiusA = 5.0;
                                styleA = "rect";
                                rotationA = 0;
                                crossTypeA = isCrossDownHigh_A ? "high_cross_down_independent" : "low_cross_up_independent";
                            }
                            if (hasSignal_B) {
                                valB = currP_B;
                                borderColorB = isCrossDownHigh_B ? "#ef4444" : "#22c55e";
                                colorB = isCrossDownHigh_B ? "#ef4444" : "#22c55e";
                                radiusB = 5.0;
                                styleB = "rect";
                                rotationB = 0;
                                crossTypeB = isCrossDownHigh_B ? "high_cross_down_independent" : "low_cross_up_independent";
                            }
                        }'''

new_logic = '''                        // 2. CASO NO COINCIDENTE (Cruce en un par con Réplica de Equidad en el Par Contrario) -> CUADROS (rect)
                        else if (hasSignal_A || hasSignal_B) {
                            if (hasSignal_A) {
                                // Par A (Señal detonante)
                                valA = currP_A;
                                borderColorA = isCrossDownHigh_A ? "#ef4444" : "#22c55e";
                                colorA = isCrossDownHigh_A ? "#ef4444" : "#22c55e";
                                radiusA = 5.5;
                                styleA = "rect";
                                rotationA = 0;
                                crossTypeA = isCrossDownHigh_A ? "high_cross_down_independent" : "low_cross_up_independent";

                                // Par B (Réplica simétrica de equidad en el par contrario)
                                valB = currP_B;
                                borderColorB = isCrossDownHigh_A ? "#22c55e" : "#ef4444"; // Acción opuesta
                                colorB = isCrossDownHigh_A ? "#22c55e" : "#ef4444";
                                radiusB = 5.5;
                                styleB = "rect";
                                rotationB = 0;
                                crossTypeB = isCrossDownHigh_A ? "low_cross_up_independent" : "high_cross_down_independent";
                            }
                            else if (hasSignal_B) {
                                // Par B (Señal detonante)
                                valB = currP_B;
                                borderColorB = isCrossDownHigh_B ? "#ef4444" : "#22c55e";
                                colorB = isCrossDownHigh_B ? "#ef4444" : "#22c55e";
                                radiusB = 5.5;
                                styleB = "rect";
                                rotationB = 0;
                                crossTypeB = isCrossDownHigh_B ? "high_cross_down_independent" : "low_cross_up_independent";

                                // Par A (Réplica simétrica de equidad en el par contrario)
                                valA = currP_A;
                                borderColorA = isCrossDownHigh_B ? "#22c55e" : "#ef4444"; // Acción opuesta
                                colorA = isCrossDownHigh_B ? "#22c55e" : "#ef4444";
                                radiusA = 5.5;
                                styleA = "rect";
                                rotationA = 0;
                                crossTypeA = isCrossDownHigh_B ? "low_cross_up_independent" : "high_cross_down_independent";
                            }
                        }'''

if old_logic in dash:
    dash = dash.replace(old_logic, new_logic)
    with open(dash_path, 'w', encoding='utf-8') as f:
        f.write(dash)
    print("Updated dashboard.xhtml with symmetric square markers on opposing pairs!")
else:
    print("Old logic not found in dashboard.xhtml!")
