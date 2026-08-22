quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_signal_eval = '''            # 2. EVALUAR ENTRADA CON 3% DE CAPITAL, symbols.margen Y MULTIPLOS DE min_lots
            sigType, direction = None, None
            if hasSignal_A and hasSignal_B:
                if isCrossUpLow_A and isCrossDownHigh_B:
                    sigType = "TRIANGULO_VERDE_COINCIDENTE"
                    direction = "LONG A / SHORT B"
                elif isCrossDownHigh_A and isCrossUpLow_B:
                    sigType = "TRIANGULO_ROJO_COINCIDENTE"
                    direction = "SHORT A / LONG B"

            elif includeBoxes and (hasSignal_A or hasSignal_B):
                if hasSignal_A:
                    if isCrossUpLow_A:
                        sigType = "CUADRO_VERDE_PAR_A"
                        direction = "LONG A / SHORT B"
                    elif isCrossDownHigh_A:
                        sigType = "CUADRO_ROJO_PAR_A"
                        direction = "SHORT A / LONG B"
                elif hasSignal_B:
                    if isCrossUpLow_B:
                        sigType = "CUADRO_VERDE_PAR_B"
                        direction = "SHORT A / LONG B"
                    elif isCrossDownHigh_B:
                        sigType = "CUADRO_ROJO_PAR_B"
                        direction = "LONG A / SHORT B"'''

name_a_clean = 'nameA = pairA if pairA else "Par A"'
name_b_clean = 'nameB = pairB if pairB else "Par B"'

new_signal_eval = '''            # 2. EVALUAR ENTRADA CON 3% DE CAPITAL, symbols.margen Y NOMBRES REALES DE PARES
            nameA = pairA if pairA else "Par A"
            nameB = pairB if pairB else "Par B"
            sigType, direction = None, None
            if hasSignal_A and hasSignal_B:
                if isCrossUpLow_A and isCrossDownHigh_B:
                    sigType = f"TRIANGULO_VERDE ({nameA} + {nameB})"
                    direction = f"LONG {nameA} / SHORT {nameB}"
                elif isCrossDownHigh_A and isCrossUpLow_B:
                    sigType = f"TRIANGULO_ROJO ({nameA} + {nameB})"
                    direction = f"SHORT {nameA} / LONG {nameB}"

            elif includeBoxes and (hasSignal_A or hasSignal_B):
                if hasSignal_A:
                    if isCrossUpLow_A:
                        sigType = f"CUADRO_VERDE {nameA}"
                        direction = f"LONG {nameA} / SHORT {nameB}"
                    elif isCrossDownHigh_A:
                        sigType = f"CUADRO_ROJO {nameA}"
                        direction = f"SHORT {nameA} / LONG {nameB}"
                elif hasSignal_B:
                    if isCrossUpLow_B:
                        sigType = f"CUADRO_VERDE {nameB}"
                        direction = f"SHORT {nameA} / LONG {nameB}"
                    elif isCrossDownHigh_B:
                        sigType = f"CUADRO_ROJO {nameB}"
                        direction = f"LONG {nameA} / SHORT {nameB}"'''

code = code.replace(target_signal_eval, new_signal_eval)

# Also check direction evaluation in close loop
target_close_dir = 'if t["direction"] == "LONG A / SHORT B":'
new_close_dir = 'if "LONG " + nameA in t["direction"] or "LONG A" in t["direction"]:'
code = code.replace(target_close_dir, new_close_dir)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py with exact symbol names for Signal Origin and Direction!")
