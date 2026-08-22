routes_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/api/routes.py'
with open(routes_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_block = '''        # 7. Backtests de Señales con cuenta.Capital y Apalancamiento 1:100
        account_capital = float(capital) if capital and capital > 0 else 10000.0
        min_lots_a = 1000.0
        min_lots_b = 1000.0
        try:
            from backend.database.models import SessionLocal
            from sqlalchemy import text
            with SessionLocal() as db_session:
                if idCuenta:
                    row_c = db_session.execute(text("SELECT Capital FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).fetchone()
                    if row_c and row_c[0] is not None and float(row_c[0]) > 0:
                        account_capital = float(row_c[0])

                row_a = db_session.execute(text("SELECT min_lots FROM symbols WHERE symbol = :s"), {"s": pairA}).fetchone()
                row_b = db_session.execute(text("SELECT min_lots FROM symbols WHERE symbol = :s"), {"s": pairB}).fetchone()
                if row_a and row_a[0] is not None:
                    min_lots_a = float(row_a[0])
                if row_b and row_b[0] is not None:
                    min_lots_b = float(row_b[0])
        except Exception as ex_db:
            logger.warning(f"Error consultando cuenta/symbols en BD: {ex_db}")

        trianglesOnlyBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            smaPeriod=3,
            sigmaWindow=30,
            includeBoxes=False,
            initialCapital=account_capital,
            leverage=leverage,
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
            initialCapital=account_capital,
            leverage=leverage,
            allocationPct=20.0,
            minLotsA=min_lots_a,
            minLotsB=min_lots_b,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )'''

new_block = '''        # 7. Backtests de Señales con cuenta.Capital y symbols.margen
        account_capital = float(capital) if capital and capital > 0 else 10000.0
        min_lots_a = 1000.0
        min_lots_b = 1000.0
        margen_pct_a = 0.25
        margen_pct_b = 1.00
        try:
            from backend.database.models import SessionLocal
            from sqlalchemy import text
            with SessionLocal() as db_session:
                if idCuenta:
                    row_c = db_session.execute(text("SELECT Capital FROM cuenta WHERE idCuenta = :idc"), {"idc": idCuenta}).fetchone()
                    if row_c and row_c[0] is not None and float(row_c[0]) > 0:
                        account_capital = float(row_c[0])

                row_a = db_session.execute(text("SELECT min_lots, margen FROM symbols WHERE symbol = :s"), {"s": pairA}).fetchone()
                row_b = db_session.execute(text("SELECT min_lots, margen FROM symbols WHERE symbol = :s"), {"s": pairB}).fetchone()
                if row_a:
                    if row_a[0] is not None: min_lots_a = float(row_a[0])
                    if row_a[1] is not None: margen_pct_a = float(row_a[1])
                if row_b:
                    if row_b[0] is not None: min_lots_b = float(row_b[0])
                    if row_b[1] is not None: margen_pct_b = float(row_b[1])
        except Exception as ex_db:
            logger.warning(f"Error consultando cuenta/symbols en BD: {ex_db}")

        trianglesOnlyBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            smaPeriod=3,
            sigmaWindow=30,
            includeBoxes=False,
            initialCapital=account_capital,
            allocationPct=20.0,
            minLotsA=min_lots_a,
            minLotsB=min_lots_b,
            margenPctA=margen_pct_a,
            margenPctB=margen_pct_b,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )
        combinedBt = quantEngine.runSignalBacktest(
            df_a_tf, df_b_tf,
            smaPeriod=3,
            sigmaWindow=30,
            includeBoxes=True,
            initialCapital=account_capital,
            allocationPct=20.0,
            minLotsA=min_lots_a,
            minLotsB=min_lots_b,
            margenPctA=margen_pct_a,
            margenPctB=margen_pct_b,
            commissionBps=commissionBps,
            slippageBps=slippageBps
        )'''

code = code.replace(target_block, new_block)

with open(routes_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated routes.py with symbols.margen integration!")
