import sys
import os

# Set up paths
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection
import requests

def runTest():
    print("=== Probando el Endpoint Aplicar Sugerencias de Riesgo ===")
    
    conn = dbConnection.getConnection()
    if conn is None:
        print("❌ No se pudo conectar a la base de datos.")
        return
        
    cursor = conn.cursor(dictionary=True)
    
    # 1. Almacenar estado previo de symbolNotStrategia
    cursor.execute("SELECT * FROM symbolNotStrategia")
    prev_exclusions = cursor.fetchall()
    print(f"Exclusiones previas en 'symbolNotStrategia': {len(prev_exclusions)}")
    
    # 2. Insertar/Asegurar algunos datos de prueba en EstrategiaSymbol
    print("\nInsertando combinaciones de prueba en EstrategiaSymbol...")
    test_combos = [
        # Combo 1: Riesgo sugerido alto (debe reactivarse/eliminarse de exclusiones)
        {"symbol": "EUR/USD", "strategy": "Sniper", "riesgoSugerido": 1.5, "wins": 12, "winRate": 60.0, "totalTrades": 20, "pnlNeto": 120.0, "profitFactor": 1.8, "expectancy": 6.0, "maxDrawdown": 2.5, "fuente": "weekly", "periodoFecha": "2026-06-07"},
        # Combo 2: Riesgo sugerido bajo (debe excluirse/agregarse a exclusiones)
        {"symbol": "GBP/USD", "strategy": "GenericFVG", "riesgoSugerido": 0.4, "wins": 2, "winRate": 20.0, "totalTrades": 10, "pnlNeto": -50.0, "profitFactor": 0.5, "expectancy": -5.0, "maxDrawdown": 8.0, "fuente": "weekly", "periodoFecha": "2026-06-07"}
    ]
    
    for combo in test_combos:
        cursor.execute("""
            INSERT INTO EstrategiaSymbol 
                (symbol, strategy, totalTrades, wins, winRate, pnlNeto, profitFactor, expectancy, maxDrawdown, riesgoSugerido, fuente, periodoFecha)
            VALUES 
                (%(symbol)s, %(strategy)s, %(totalTrades)s, %(wins)s, %(winRate)s, %(pnlNeto)s, %(profitFactor)s, %(expectancy)s, %(maxDrawdown)s, %(riesgoSugerido)s, %(fuente)s, %(periodoFecha)s)
            ON DUPLICATE KEY UPDATE 
                riesgoSugerido = VALUES(riesgoSugerido),
                winRate = VALUES(winRate),
                totalTrades = VALUES(totalTrades),
                pnlNeto = VALUES(pnlNeto)
        """, combo)
    
    # Asegurar que GBP/USD - GenericFVG no esté previamente en exclusiones (para contar como nuevo excluido) o viceversa
    # Para EUR/USD - Sniper lo insertamos en symbolNotStrategia primero para ver si la reactivación lo remueve
    cursor.execute("REPLACE INTO symbolNotStrategia (symbol, strategy, reason) VALUES ('EUR/USD', 'Sniper', 'Previa de prueba')")
    cursor.execute("DELETE FROM symbolNotStrategia WHERE symbol='GBP/USD' AND strategy='GenericFVG'")
    conn.commit()
    
    print("EUR/USD - Sniper insertado temporalmente en exclusiones.")
    print("GBP/USD - GenericFVG eliminado temporalmente de exclusiones.")
    
    # 3. Invocar al endpoint REST de FastAPI
    print("\nInvocando al endpoint POST /api/v1/config/estrategia-symbol/aplicar...")
    url = "http://127.0.0.1:8000/api/v1/config/estrategia-symbol/aplicar"
    try:
        response = requests.post(url)
        print(f"Status Code: {response.status_code}")
        print(f"Response: {response.json()}")
        
        # 4. Verificar en base de datos si se aplicaron los cambios
        print("\nVerificando estado final en symbolNotStrategia...")
        cursor.execute("SELECT * FROM symbolNotStrategia WHERE symbol IN ('EUR/USD', 'GBP/USD')")
        final_exclusions = cursor.fetchall()
        
        for excl in final_exclusions:
            print(f"  - Exclusión activa en BD: Símbolo: {excl['symbol']} | Estrategia: {excl['strategy']} | Razón: {excl['reason']}")
            
        # El Sniper (EUR/USD) debe haber sido removido (riesgoSugerido=1.5 > 0.5)
        # El GenericFVG (GBP/USD) debe estar presente (riesgoSugerido=0.4 <= 0.5)
        is_sniper_excluded = any(x['symbol'] == 'EUR/USD' and x['strategy'] == 'Sniper' for x in final_exclusions)
        is_fvg_excluded = any(x['symbol'] == 'GBP/USD' and x['strategy'] == 'GenericFVG' for x in final_exclusions)
        
        if not is_sniper_excluded and is_fvg_excluded:
            print("🚀 ¡PRUEBA EXITOSA! La lógica del endpoint aplicó correctamente las exclusiones y reactivaciones.")
        else:
            print("❌ ERROR: La lógica del endpoint falló.")
            if is_sniper_excluded:
                print("  - EUR/USD Sniper sigue excluido incorrectamente.")
            if not is_fvg_excluded:
                print("  - GBP/USD GenericFVG no fue excluido.")
                
    except Exception as e:
        print(f"❌ Error al conectar o procesar respuesta de FastAPI: {e}")
        print("Asegúrate de que el servidor FastAPI está corriendo en http://127.0.0.1:8000")
        
    finally:
        # Restaurar estado original si se desea, o al menos limpiar los de prueba
        print("\nRestaurando estado original...")
        cursor.execute("DELETE FROM symbolNotStrategia WHERE symbol IN ('EUR/USD', 'GBP/USD')")
        cursor.execute("DELETE FROM EstrategiaSymbol WHERE symbol IN ('EUR/USD', 'GBP/USD') AND fuente='weekly'")
        # Re-insertar las exclusiones originales
        for prev in prev_exclusions:
            cursor.execute("REPLACE INTO symbolNotStrategia (symbol, strategy, reason) VALUES (%s, %s, %s)", (prev['symbol'], prev['strategy'], prev['reason']))
        conn.commit()
        cursor.close()
        conn.close()
        print("Restauración completada.")

if __name__ == "__main__":
    runTest()
