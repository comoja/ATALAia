import sys
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection

def applyOptimizations() -> None:
    """Aplica de forma directa las optimizaciones validadas por el backtest en la DB."""
    try:
        print("\n=== APLICANDO OPTIMIZACIONES EN BASE DE DATOS ATALAia ===")
        connection = dbConnection.getConnection()
        if connection is None:
            print("❌ No se pudo establecer conexión con MySQL.")
            return

        cursor = connection.cursor()

        # 1. Desactivar estrategias de bajo rendimiento global
        deactivateQuery = """
            UPDATE strategyConfig 
            SET enabled = FALSE 
            WHERE strategy IN ('Patron4h', 'EMA20200', 'SpeedBot');
        """
        cursor.execute(deactivateQuery)
        connection.commit()
        print("✅ Estrategias globales desactivadas en strategyConfig ('Patron4h', 'EMA20200', 'SpeedBot').")

        # 2. Insertar los combos incompatibles en symbolNotStrategia para evitar fugas de capital
        incompatibilities = [
            ('GBP/JPY', 'Patron4h', 'Auditoria Semanal: Perdida neta -$247.65 USD'),
            ('EUR/USD', 'EMA20200', 'Auditoria Semanal: Perdida neta -$250.84 USD'),
            ('BTC/USD', 'SpeedBot', 'Auditoria Semanal: Perdida neta -$238.97 USD'),
            ('AUD/USD', 'Patron4h', 'Auditoria Semanal: Perdida neta -$210.72 USD'),
            ('AUD/USD', 'EMA20200', 'Auditoria Semanal: Perdida neta -$208.91 USD'),
            ('BTC/USD', 'SMA20_200', 'Auditoria Semanal: Perdida neta -$181.10 USD'),
            ('BTC/USD', 'Patron4h', 'Auditoria Semanal: Perdida neta -$175.35 USD'),
            ('USD/CAD', 'EMA20200', 'Auditoria Semanal: Perdida neta -$175.68 USD')
        ]

        insertQuery = """
            REPLACE INTO symbolNotStrategia (symbol, strategy, reason)
            VALUES (%s, %s, %s)
        """
        cursor.executemany(insertQuery, incompatibilities)
        connection.commit()
        print("✅ Combinaciones de bajo rendimiento añadidas/actualizadas en symbolNotStrategia.")

        # 3. Mostrar auditoria de control
        cursor.execute("SELECT strategy, enabled FROM strategyConfig")
        print("\n📊 Estado Actual de Estrategias en DB:")
        for row in cursor.fetchall():
            statusStr = "ACTIVO 🟢" if row[1] == 1 else "DESACTIVADO 🔴"
            print(f"   - {row[0]:<15}: {statusStr}")

        cursor.execute("SELECT COUNT(*) FROM symbolNotStrategia")
        countVal = cursor.fetchone()[0]
        print(f"\n📊 Total de exclusiones registradas en 'symbolNotStrategia': {countVal}")

        cursor.close()
        connection.close()
        print("\n==========================================================\n")

    except Exception as e:
        print(f"❌ Error al aplicar optimizaciones en DB: {e}")

if __name__ == '__main__':
    applyOptimizations()
