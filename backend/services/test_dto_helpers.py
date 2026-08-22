test_signals = [
    ("CUADRO_VERDE EUR/USD", "LONG EUR/USD / SHORT USD/MXN"),
    ("CUADRO_ROJO EUR/USD", "SHORT EUR/USD / LONG USD/MXN"),
    ("CUADRO_VERDE USD/MXN", "SHORT EUR/USD / LONG USD/MXN"),
    ("CUADRO_ROJO USD/MXN", "LONG EUR/USD / SHORT USD/MXN"),
    ("TRIANGULO_VERDE (EUR/USD + USD/MXN)", "LONG EUR/USD / SHORT USD/MXN"),
    ("TRIANGULO_ROJO (EUR/USD + USD/MXN)", "SHORT EUR/USD / LONG USD/MXN"),
    ("CUADRO_VERDE EUR/USD (EN CURSO)", "LONG EUR/USD / SHORT USD/MXN"),
]

for sig, direction in test_signals:
    # 1. Shape & Color
    is_square = "CUADRO" in sig
    is_triangle = "TRIANGULO" in sig
    is_green = "VERDE" in sig
    is_red = "ROJO" in sig
    color = "#15803d" if is_green else ("#dc2626" if is_red else "#1e293b")
    
    # Clean pair name
    clean_pair = sig.replace("CUADRO_VERDE", "").replace("CUADRO_ROJO", "").replace("TRIANGULO_VERDE", "").replace("TRIANGULO_ROJO", "").replace("(EN CURSO)", "").strip()
    clean_pair = clean_pair.strip("()")
    
    # 2. Direction colors
    # LONG -> green, SHORT -> red
    is_long_a = direction.startswith("LONG")
    pair_a_color = "#15803d" if is_long_a else "#dc2626"
    pair_b_color = "#dc2626" if is_long_a else "#15803d"
    
    print(f"Sig: '{sig}' -> Shape: {'Square' if is_square else 'Triangle'}, Color: {color}, Pair: '{clean_pair}'")
    print(f"  Direction: Pair A Color: {pair_a_color}, Pair B Color: {pair_b_color}")
