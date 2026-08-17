import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def updateConfigs() -> None:
    """Corrige y actualiza las tablas strategyConfig y symbolNotStrategia en base a los resultados de corto plazo."""
    try:
        print("\n--- Iniciando Actualizacion de Configuraciones en BD (V2) ---")
        connection = dbConnection.getConnection()
        if connection is None:
            print("❌ No se pudo conectar a la base de datos MySQL.")
            return
            
        cursor = connection.cursor()
        
        # 1. ACTUALIZAR strategyConfig
        print("\n▶ Actualizando strategyConfig...")
        
        # Habilitar estrategias con alta rentabilidad demostrada (Ichimoku y SpeedBot)
        enableStrategies = [
            ("Ichimoku", 1, 1.5, 75),
            ("SpeedBot", 1, 1.5, 75)
        ]
        
        for strategy, enabled, min_rr, min_confidence in enableStrategies:
            updateQuery = """
                UPDATE strategyConfig 
                SET enabled = %s, min_rr = %s, min_confidence = %s, updated_at = NOW()
                WHERE strategy = %s
            """
            cursor.execute(updateQuery, (enabled, min_rr, min_confidence, strategy))
            print(f"   ✔️ {strategy} -> Enabled: {enabled}, Min RR: {min_rr}, Min Confidence: {min_confidence}")
            
        # Asegurar deshabilitacion de SMA20_200 y Sniper (lagging e ineficientes)
        disableStrategies = ["SMA20_200", "Sniper"]
        for strategy in disableStrategies:
            disableQuery = """
                UPDATE strategyConfig 
                SET enabled = 0, updated_at = NOW()
                WHERE strategy = %s
            """
            cursor.execute(disableQuery, (strategy,))
            print(f"   🚫 {strategy} deshabilitada globalmente (Bajo rendimiento en Backtesting)")
            
        # 2. ENRIQUECER symbolNotStrategia CON EXCLUSIONES ADICIONALES (V2)
        print("\n▶ Enriqueciendo tabla symbolNotStrategia con exclusiones V2 (Riesgo y Spread)...")
        
        newExclusions = [
            # Desactivar estrategias de scalping rápido e imbalances en USD/MXN por el spread destructivo en cuentas de bajo balance ($500)
            ('USD/MXN', 'ImbalanceNY', 'Spread destructivo en peso mexicano que devora el balance en scalp rapido.'),
            ('USD/MXN', 'ImbalanceLDN', 'Spread destructivo en peso mexicano que devora el balance en scalp rapido.'),
            ('USD/MXN', 'ImbalancePMNY', 'Spread destructivo en peso mexicano que devora el balance en scalp rapido.'),
            ('USD/MXN', 'SilverBullet', 'Excesivo spread en par exotico. No apto para killzones de precision.'),
            
            # Desactivar SilverBullet en USD/JPY por ruido excesivo en sesiones asiaticas
            ('USD/JPY', 'SilverBullet', 'Frecuentes falsos rompimientos intradiarios por intervenciones del BOJ e inestabilidad asiatica.'),
            
            # Desactivar FVGDiario en EUR/GBP por mercado sumamente comprimido y lateralizado
            ('EUR/GBP', 'FVGDiario', 'Par excesivamente lateralizado con bajo ATR diario; genera senales sin expansion.')
        ]
        
        insertExclusionsQuery = """
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, %s, %s)
        """
        cursor.executemany(insertExclusionsQuery, newExclusions)
        print(f"   ✔️ Se insertaron/actualizaron {len(newExclusions)} exclusiones estrategicas adicionales para USD/MXN, USD/JPY y EUR/GBP.")
        
        connection.commit()
        
        # Mostrar resumen final
        cursor.execute("SELECT COUNT(*) FROM symbolNotStrategia")
        totalNot = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM strategyConfig WHERE enabled = TRUE")
        totalEnabled = cursor.fetchone()[0]
        
        print("\n==========================================================")
        print("          RESUMEN DE ACTUALIZACIÓN DE BASE DE DATOS        ")
        print("==========================================================")
        print(f"📊 Estrategias HABILITADAS en strategyConfig : {totalEnabled}")
        print(f"🚫 Relaciones Símbolo-Estrategia EXCLUIDAS  : {totalNot}")
        print("==========================================================\n")
        
        cursor.close()
        connection.close()
        
    except Exception as e:
        print(f"❌ Error al actualizar configuraciones en la BD: {e}")

if __name__ == '__main__':
    updateConfigs()
