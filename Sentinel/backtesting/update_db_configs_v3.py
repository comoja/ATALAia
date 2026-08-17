import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def updateConfigsV3() -> None:
    """
    Actualiza las tablas strategyConfig y symbolNotStrategia en base a los
    hallazgos del backtesting V3 con balance compuesto.
    """
    try:
        print("\n--- Iniciando Actualizacion de Configuraciones en BD (V3 - Balance Compuesto) ---")
        connection = dbConnection.getConnection()
        if connection is None:
            print("❌ No se pudo conectar a la base de datos MySQL.")
            return

        cursor = connection.cursor()

        # ─────────────────────────────────────────────────────────────
        # 1. ACTUALIZAR strategyConfig
        # ─────────────────────────────────────────────────────────────
        print("\n▶ Actualizando strategyConfig (V3)...")

        # Estrategias habilitadas con configuracion optima V3
        # (min_rr y min_confidence ajustados por rendimiento compuesto)
        enableStrategies = [
            # (strategy,       enabled, min_rr, min_confidence)
            ("Ichimoku",         1,     1.5,    75),
            ("SpeedBot",         1,     1.5,    75),
            ("SilverBullet",     1,     1.5,    70),
            ("GenericFVG",       1,     1.5,    65),
            ("SesgoBiasHTF",     1,     1.5,    68),
            ("Patron4h",         1,     2.0,    72),   # Mayor RR requerido por confluencia 4H
            ("FVGDiario",        1,     2.0,    70),
            ("ImbalanceNY",      1,     1.5,    65),
            ("ImbalanceLDN",     1,     1.5,    65),
            ("ImbalancePMNY",    1,     1.5,    65),
            ("BreakoutNY",       1,     1.5,    60),
            ("EMA20200",         1,     1.2,    60),
        ]

        for strategy, enabled, min_rr, min_confidence in enableStrategies:
            updateQuery = """
                UPDATE strategyConfig
                SET enabled = %s, min_rr = %s, min_confidence = %s, updated_at = NOW()
                WHERE strategy = %s
            """
            cursor.execute(updateQuery, (enabled, min_rr, min_confidence, strategy))
            print(f"   ✔️  {strategy:<16} -> Enabled: {enabled} | Min RR: {min_rr} | Min Confidence: {min_confidence}")

        # Estrategias deshabilitadas permanentemente (V3 confirma bajo rendimiento compuesto)
        disableStrategies = ["SMA20_200", "Sniper"]
        for strategy in disableStrategies:
            disableQuery = """
                UPDATE strategyConfig
                SET enabled = 0, updated_at = NOW()
                WHERE strategy = %s
            """
            cursor.execute(disableQuery, (strategy,))
            print(f"   🚫  {strategy:<16} -> DESHABILITADA (balance compuesto confirma perdida sistematica)")

        # ─────────────────────────────────────────────────────────────
        # 2. ACTUALIZAR symbolNotStrategia CON EXCLUSIONES V3
        # ─────────────────────────────────────────────────────────────
        print("\n▶ Actualizando symbolNotStrategia con exclusiones V3...")

        # Exclusiones nuevas detectadas en el backtesting V3
        newExclusionsV3 = [
            # EMA20200 en pares de muy baja volatilidad (compuesto drena capital)
            ('NZD/USD', 'EMA20200', 'V3: Baja volatilidad sistematica en NZD/USD drena balance compuesto con EMA crossovers.'),
            ('USD/HKD', 'EMA20200', 'V3: Par vinculado (pegged) con rango comprimido; EMA genera senales sin momentum real.'),
            ('USD/HKD', 'GenericFVG', 'V3: USD/HKD tiene un spread y rango tan comprimido que los FVG no tienen espacio de expansion.'),
            ('USD/HKD', 'ImbalanceNY', 'V3: Par pegged; imbalances de sesion NY no tienen expansion real en este instrumento.'),
            ('USD/HKD', 'ImbalanceLDN', 'V3: Par pegged; imbalances de sesion LDN no tienen expansion real en este instrumento.'),
            ('USD/HKD', 'ImbalancePMNY', 'V3: Par pegged; imbalances PMNY sin expansion. Par ignorado por todos los bots de scalping.'),
            # GBP/CAD - alta correlacion de cruces, FVG diario pierde efectividad
            ('GBP/CAD', 'FVGDiario', 'V3: FVG diario en GBP/CAD muestra alta tasa de falsos setups por correlacion USD/CAD.'),
            # Reconfirmar exclusiones V2 en peso mexicano
            ('USD/MXN', 'ImbalanceNY', 'V3: Spread destructivo en USD/MXN confirmado en balance compuesto.'),
            ('USD/MXN', 'ImbalanceLDN', 'V3: Spread destructivo en USD/MXN confirmado en balance compuesto.'),
            ('USD/MXN', 'ImbalancePMNY', 'V3: Spread destructivo en USD/MXN confirmado en balance compuesto.'),
            ('USD/MXN', 'SilverBullet', 'V3: Excesivo spread. No apto para killzones de precision en USD/MXN.'),
            ('USD/JPY', 'SilverBullet', 'V3: Falsas rupturas intradiarias por intervenciones del BOJ confirmadas en V3.'),
            ('EUR/GBP', 'FVGDiario', 'V3: Par lateralizado con bajo ATR; FVG diario no genera expansion significativa.'),
        ]

        insertExclusionsQuery = """
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, %s, %s)
        """
        cursor.executemany(insertExclusionsQuery, newExclusionsV3)
        print(f"   ✔️  Se insertaron/actualizaron {len(newExclusionsV3)} exclusiones V3 en symbolNotStrategia.")

        connection.commit()

        # ─────────────────────────────────────────────────────────────
        # 3. MOSTRAR RESUMEN FINAL
        # ─────────────────────────────────────────────────────────────
        cursor.execute("SELECT COUNT(*) FROM symbolNotStrategia")
        totalNot = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM strategyConfig WHERE enabled = TRUE")
        totalEnabled = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM strategyConfig WHERE enabled = FALSE")
        totalDisabled = cursor.fetchone()[0]

        print("\n==========================================================")
        print("      RESUMEN FINAL DE ACTUALIZACION BD (V3)             ")
        print("==========================================================")
        print(f"✅ Estrategias HABILITADAS en strategyConfig : {totalEnabled}")
        print(f"🚫 Estrategias DESHABILITADAS              : {totalDisabled}")
        print(f"🔒 Exclusiones Simbolo-Estrategia totales  : {totalNot}")
        print("==========================================================\n")

        # Mostrar exclusiones actuales para validacion
        cursor.execute("""
            SELECT symbol, strategy, reason
            FROM symbolNotStrategia
            ORDER BY symbol, strategy
        """)
        rows = cursor.fetchall()
        print("📋 EXCLUSIONES ACTIVAS EN symbolNotStrategia:")
        currentSymbol = None
        for sym, strat, reason in rows:
            if sym != currentSymbol:
                print(f"\n  🔸 {sym}:")
                currentSymbol = sym
            print(f"     ✗ {strat:<18} | {reason[:60]}{'...' if len(reason) > 60 else ''}")

        cursor.close()
        connection.close()
        print("\n✅ Actualizacion de BD V3 completada exitosamente.")

    except Exception as e:
        print(f"❌ Error al actualizar configuraciones en la BD V3: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    updateConfigsV3()
