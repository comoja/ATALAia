routes_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/api/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_calls = '''        # 7. Backtests de Señales Gráficas (Triángulos y Cuadros con Salida en Cruce con Media)
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

new_calls = '''        # 7. Backtests de Señales Gráficas (Triángulos y Cuadros con Asignación del 20% basada en symbols.min_lots)
        min_lots_a = 1000.0
        min_lots_b = 1000.0
        try:
            from backend.database.models import SessionLocal
            from sqlalchemy import text
            with SessionLocal() as db_session:
                row_a = db_session.execute(text("SELECT min_lots FROM symbols WHERE symbol = :s"), {"s": pairA}).fetchone()
                row_b = db_session.execute(text("SELECT min_lots FROM symbols WHERE symbol = :s"), {"s": pairB}).fetchone()
                if row_a and row_a[0] is not None:
                    min_lots_a = float(row_a[0])
                if row_b and row_b[0] is not None:
                    min_lots_b = float(row_b[0])
        except Exception as ex_db:
            logger.warning(f"No se pudo consultar min_lots en symbols: {ex_db}")

        trianglesOnlyBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            smaPeriod=3,
            sigmaWindow=30,
            includeBoxes=False,
            allocationPct=20.0,
            minLotsA=min_lots_a,
            minLotsB=min_lots_b,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )
        combinedBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            smaPeriod=3,
            sigmaWindow=30,
            includeBoxes=True,
            allocationPct=20.0,
            minLotsA=min_lots_a,
            minLotsB=min_lots_b,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )'''

if target_calls in code:
    code = code.replace(target_calls, new_calls)
    with open(routes_path, 'w', encoding='utf-8') as f:
        f.write(code)
    print("Updated routes.py with min_lots DB queries and 20% allocation!")
else:
    print("Target calls not found in routes.py!")
