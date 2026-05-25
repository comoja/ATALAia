import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def setupSymbolNotStrategia() -> None:
    """Crea la tabla symbolNotStrategia e inserta las exclusiones recomendadas."""
    try:
        print("\n--- Iniciando Setup de Tabla symbolNotStrategia ---")
        connection = dbConnection.getConnection()
        if connection is None:
            print("❌ No se pudo conectar a la base de datos MySQL.")
            return
            
        cursor = connection.cursor()
        
        # 1. Crear la tabla en la base de datos
        createTableQuery = """
            CREATE TABLE IF NOT EXISTS symbolNotStrategia (
                symbol VARCHAR(20) NOT NULL,
                strategy VARCHAR(50) NOT NULL,
                reason VARCHAR(255),
                PRIMARY KEY (symbol, strategy)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """
        cursor.execute(createTableQuery)
        print("✅ Tabla 'symbolNotStrategia' creada o ya existente.")
        
        # 2. Definir las exclusiones recomendadas segun las auditorias cuantitativas
        exclusions = []
        
        forexPairs = [
            'AUD/USD', 'EUR/GBP', 'EUR/USD', 'GBP/CAD', 'GBP/JPY',
            'GBP/USD', 'NZD/USD', 'USD/CAD', 'USD/CHF', 'USD/JPY', 'USD/MXN'
        ]
        allStrategies = [
            'GenericFVG', 'SilverBullet', 'SpeedBot', 'Sniper', 'Patron4h',
            'BreakoutNY', 'ImbalanceNY', 'ImbalanceLDN', 'ImbalancePMNY',
            'FVGDiario', 'Ichimoku', 'EMA20200', 'SMA20_200', 'SesgoBiasHTF'
        ]
        
        # A. Desactivar SMA20_200 clasica en Forex y Oro (Ineficiente, perdidas consistentes)
        for symbol in forexPairs + ['XAU/USD']:
            exclusions.append((symbol, 'SMA20_200', 'Bajo Win Rate (38.2%) y perdidas consistentes por retraso en cruces de medias.'))
            
        # B. Desactivar EMA20200 clasica en Forex (Senales tardias y sobre-operacion)
        for symbol in forexPairs:
            exclusions.append((symbol, 'EMA20200', 'Senales tardias y sobre-operacion en mercados de rango intradiarios.'))
            
        # C. Desactivar Sniper en Oro XAU/USD (Perdidas masivas intradiarias de mas de $5,800 a 6m)
        exclusions.append(('XAU/USD', 'Sniper', 'Altas perdidas acumuladas y Win Rate inferior al 21.0% por falsas rupturas de media rapida.'))
        
        # D. Desactivar todas las estrategias en USD/HKD (Datos insuficientes y volumen nulo)
        for strategy in allStrategies:
            exclusions.append(('USD/HKD', strategy, 'Datos insuficientes y volumen nulo. Instrumento inactivo para trading algoritmico.'))
            
        # E. Desactivar SMA20_200 en Bitcoin BTC/USD (Ineficiente frente a SpeedBot)
        exclusions.append(('BTC/USD', 'SMA20_200', 'Bajo rendimiento intradiario. Se prefiere SpeedBot y SMC.'))
        
        # 3. Insertar las exclusiones utilizando REPLACE para sobreescribir y evitar duplicados
        insertQuery = """
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, %s, %s)
        """
        cursor.executemany(insertQuery, exclusions)
        connection.commit()
        print(f"✅ Se insertaron/actualizaron las exclusiones recomendadas en la tabla.")
        
        # 4. Mostrar conteo actual de registros en la tabla
        cursor.execute("SELECT COUNT(*) FROM symbolNotStrategia")
        countVal = cursor.fetchone()[0]
        print(f"📊 Total de registros actuales en 'symbolNotStrategia': {countVal}")
        
        cursor.close()
        connection.close()
        print("--- Setup Finalizado con Exito ---\n")
        
    except Exception as e:
        print(f"❌ Error durante el setup de la tabla: {e}")

if __name__ == '__main__':
    setupSymbolNotStrategia()
