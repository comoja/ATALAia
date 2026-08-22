routes_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/api/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_live_eval = '# 6. Señal Live\n        liveSignal = quantEngine.evaluateLiveSignal(df_ratio, pairA, pairB, entryZThreshold=stdThreshold)'
replacement = '''# 6. Señal Live
        liveSignal = quantEngine.evaluateLiveSignal(df_ratio, pairA, pairB, entryZThreshold=stdThreshold)

        # 7. Backtests de Señales Gráficas (Triángulos y Cuadros con Salida en Cruce con Media)
        trianglesOnlyBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            smaPeriod=3,
            sigmaWindow=30,
            includeBoxes=False,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )
        combinedBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            smaPeriod=3,
            sigmaWindow=30,
            includeBoxes=True,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )'''

if target_live_eval in code and 'trianglesOnlyBt' not in code:
    code = code.replace(target_live_eval, replacement)
    
    # Add signalBacktest to return dictionary
    old_return_item = '"liveSignal": liveSignal,'
    new_return_item = '''"liveSignal": liveSignal,
            "signalBacktest": {
                "trianglesOnly": trianglesOnlyBt,
                "combined": combinedBt
            },'''
    code = code.replace(old_return_item, new_return_item)
    
    with open(routes_path, 'w', encoding='utf-8') as f:
        f.write(code)
    print("Updated routes.py with signalBacktest results!")
else:
    print("routes.py already updated or target not found.")
