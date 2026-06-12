import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def update_configs():
    print("Conectando a base de datos...")
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    
    try:
        # Actualizar Sniper
        print("Actualizando umbrales de probabilidad de ML para Sniper a 0.60 / 0.40...")
        cursor.execute("""
            UPDATE strategyConfig 
            SET proba_threshold_long = 0.60, proba_threshold_short = 0.40
            WHERE strategy = 'Sniper'
        """)
        conn.commit()
        print("Umbrales de Sniper actualizados.")
        
        # Actualizar BreakoutProbability
        print("Actualizando umbrales de probabilidad de ML para BreakoutProbability a 0.60 / 0.40...")
        cursor.execute("""
            UPDATE strategyConfig 
            SET proba_threshold_long = 0.60, proba_threshold_short = 0.40
            WHERE strategy = 'BreakoutProbability'
        """)
        conn.commit()
        print("Umbrales de BreakoutProbability actualizados.")
        
        # Verificar
        cursor.execute("SELECT strategy, proba_threshold_long, proba_threshold_short FROM strategyConfig WHERE strategy IN ('Sniper', 'BreakoutProbability')")
        rows = cursor.fetchall()
        print("\n--- NUEVA CONFIGURACIÓN EN LA BD ---")
        for r in rows:
            print(f"Estrategia: {r['strategy']} | Long Thresh: {r['proba_threshold_long']} | Short Thresh: {r['proba_threshold_short']}")
            
    except Exception as e:
        print(f"Error actualizando la base de datos: {e}")
        conn.rollback()
        
    conn.close()

if __name__ == "__main__":
    update_configs()
