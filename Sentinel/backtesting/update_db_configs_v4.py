"""
ACTUALIZACION DE BD V4 - BASADA EN COMPOUNDING DE PORTAFOLIO GLOBAL
"""
import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection


def updateConfigsV4() -> None:
    """
    Actualiza strategyConfig y symbolNotStrategia con los hallazgos del
    backtesting V4 (compounding de portafolio global).
    """
    try:
        print("\n--- Actualizando BD (V4 - Compounding Portafolio Global) ---")
        conn = dbConnection.getConnection()
        if conn is None:
            print("❌ Sin conexion a MySQL.")
            return
        cur = conn.cursor()

        # ─────────────────────────────────────────────────────────────
        # 1. STRATEGYCONFIG - Ajustar parametros con base V4
        # ─────────────────────────────────────────────────────────────
        print("\n▶ Actualizando strategyConfig (V4)...")

        # En el modelo de portafolio, los bots de mayor impacto necesitan
        # configuraciones mas estrictas para proteger el capital compartido.
        enableStrategies = [
            # (strategy,       enabled, min_rr, min_confidence)
            ("SilverBullet",   1,   2.0,  78),   # Motor principal del portafolio. RR elevado.
            ("SpeedBot",       1,   2.0,  78),   # Segundo motor. Alta confianza requerida.
            ("Patron4h",       1,   2.0,  75),   # Bot transversal. FVG+MSS+EMA200 en 4H.
            ("Ichimoku",       1,   1.5,  72),   # Bot tendencial de soporte.
            ("SesgoBiasHTF",   1,   1.5,  68),   # Filtro de sesgo direccional.
            ("GenericFVG",     1,   1.5,  65),   # FVG de 15min. Buena frecuencia.
            ("FVGDiario",      1,   2.0,  70),   # FVG diario. RR alto para capital compartido.
            ("ImbalanceNY",    1,   1.5,  65),   # Sesion NY. Buen contribuidor al portafolio.
            ("ImbalanceLDN",   1,   1.5,  65),   # Sesion LDN.
            ("ImbalancePMNY",  1,   1.5,  65),   # Sesion PM-NY.
            ("BreakoutNY",     1,   1.5,  62),   # Breakout apertura NY.
            ("EMA20200",       1,   1.2,  60),   # Cruce de medias. Conservador.
        ]

        for strategy, enabled, min_rr, min_confidence in enableStrategies:
            cur.execute("""
                UPDATE strategyConfig
                SET enabled = %s, min_rr = %s, min_confidence = %s, updated_at = NOW()
                WHERE strategy = %s
            """, (enabled, min_rr, min_confidence, strategy))
            print(f"   ✔️  {strategy:<16} -> Enabled: {enabled} | Min RR: {min_rr} | Confidence: {min_confidence}")

        # Deshabilitadas permanentemente
        for strategy in ["SMA20_200", "Sniper"]:
            cur.execute("""
                UPDATE strategyConfig
                SET enabled = 0, updated_at = NOW()
                WHERE strategy = %s
            """, (strategy,))
            print(f"   🚫  {strategy:<16} -> DESHABILITADA (drena portafolio compuesto)")

        # ─────────────────────────────────────────────────────────────
        # 2. SYMBOLNOTSTRATEGIA - Exclusiones V4
        # ─────────────────────────────────────────────────────────────
        print("\n▶ Insertando exclusiones V4 en symbolNotStrategia...")

        exclusionesV4 = [
            # USD/HKD - par pegged, sin expansion en ningun bot de scalping
            ('USD/HKD', 'EMA20200',     'V4: Par pegged HKD, sin volatilidad real para EMA crossover.'),
            ('USD/HKD', 'GenericFVG',   'V4: FVG sin expansion en rango comprimido de USD/HKD.'),
            ('USD/HKD', 'ImbalanceNY',  'V4: Par pegged, imbalances NY sin follow-through real.'),
            ('USD/HKD', 'ImbalanceLDN', 'V4: Par pegged, imbalances LDN sin follow-through real.'),
            ('USD/HKD', 'ImbalancePMNY','V4: Par pegged, imbalances PM-NY sin follow-through real.'),
            ('USD/HKD', 'SpeedBot',     'V4: Baja volatilidad en HKD hace inutil el ATR breakout de SpeedBot.'),
            ('USD/HKD', 'SilverBullet', 'V4: Killzones de SilverBullet requieren liquidez; USD/HKD es insuficiente.'),
            ('USD/HKD', 'FVGDiario',    'V4: FVG diario sin expansion en par vinculado (pegged).'),
            ('USD/HKD', 'Patron4h',     'V4: Patron4h requiere impulso SMC real; USD/HKD no genera MSS validos.'),
            ('USD/HKD', 'SesgoBiasHTF', 'V4: Sin sesgo HTF real en par intervenido por HKMA.'),
            ('USD/HKD', 'BreakoutNY',   'V4: Sin breakout real en apertura NY en par pegged.'),
            ('USD/HKD', 'Ichimoku',     'V4: Ichimoku requiere tendencia; USD/HKD lateraliza permanentemente.'),
            # GBP/CAD - FVG diario con falsos setups
            ('GBP/CAD', 'FVGDiario',    'V4: FVG diario en GBP/CAD con alta tasa de rechazo por correlacion USD/CAD.'),
            # NZD/USD - baja volatilidad para EMA
            ('NZD/USD', 'EMA20200',     'V4: Baja volatilidad en NZD/USD drena portafolio compuesto con EMA tardio.'),
            # USD/MXN - exclusiones de spread reconfirmadas V4
            ('USD/MXN', 'ImbalanceNY',  'V4: Spread destructivo USD/MXN confirmado en modelo portafolio.'),
            ('USD/MXN', 'ImbalanceLDN', 'V4: Spread destructivo USD/MXN confirmado en modelo portafolio.'),
            ('USD/MXN', 'ImbalancePMNY','V4: Spread destructivo USD/MXN confirmado en modelo portafolio.'),
            ('USD/MXN', 'SilverBullet', 'V4: Spread excesivo; killzones ineficientes en USD/MXN.'),
            # USD/JPY - BOJ interference
            ('USD/JPY', 'SilverBullet', 'V4: Intervenciones del BOJ generan rupturas falsas en killzones de USD/JPY.'),
            # EUR/GBP - lateralizado
            ('EUR/GBP', 'FVGDiario',    'V4: EUR/GBP lateralizado; FVG diario sin expansion valida.'),
        ]

        cur.executemany("""
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, %s, %s)
        """, exclusionesV4)
        print(f"   ✔️  {len(exclusionesV4)} exclusiones V4 insertadas/actualizadas.")

        conn.commit()

        # ─────────────────────────────────────────────────────────────
        # 3. RESUMEN FINAL
        # ─────────────────────────────────────────────────────────────
        cur.execute("SELECT COUNT(*) FROM symbolNotStrategia")
        totalNot = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM strategyConfig WHERE enabled = 1")
        totalOn = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM strategyConfig WHERE enabled = 0")
        totalOff = cur.fetchone()[0]

        print("\n==========================================================")
        print("       RESUMEN BD ACTUALIZADA V4 (PORTAFOLIO GLOBAL)     ")
        print("==========================================================")
        print(f"✅ Estrategias HABILITADAS    : {totalOn}")
        print(f"🚫 Estrategias DESHABILITADAS : {totalOff}")
        print(f"🔒 Exclusiones totales (BD)   : {totalNot}")
        print("==========================================================\n")

        # Listado de exclusiones agrupado por simbolo
        cur.execute("""
            SELECT symbol, strategy, reason
            FROM symbolNotStrategia
            ORDER BY symbol, strategy
        """)
        rows = cur.fetchall()
        print("📋 MAPA DE EXCLUSIONES (symbolNotStrategia):")
        currentSym = None
        for sym, strat, reason in rows:
            if sym != currentSym:
                print(f"\n  🔸 {sym}:")
                currentSym = sym
            shortReason = reason[:65] + ('...' if len(reason) > 65 else '')
            print(f"     ✗ {strat:<18} | {shortReason}")

        cur.close()
        conn.close()
        print("\n✅ Actualizacion V4 completada exitosamente.")

    except Exception as e:
        print(f"❌ Error en actualizacion V4: {e}")
        import traceback
        traceback.print_exc()


if __name__ == '__main__':
    updateConfigsV4()
