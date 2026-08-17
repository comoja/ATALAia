import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbConnection

def check():
    print("Conectando a base de datos...")
    conn = dbConnection.getConnection()
    cursor = conn.cursor(dictionary=True)
    
    # 1. Parámetros de configuración de las estrategias
    try:
        cursor.execute("SELECT * FROM strategyConfig WHERE strategy IN ('Sniper', 'QTrend', 'BreakoutProbability')")
        configs = cursor.fetchall()
        print("\n--- CONFIGURACIÓN DE ESTRATEGIAS ---")
        for cfg in configs:
            print(f"\nEstrategia: {cfg['strategy']}")
            for k, v in cfg.items():
                if k != 'strategy' and v is not None:
                    print(f"  {k}: {v}")
    except Exception as e:
        print(f"Error consultando strategyConfig: {e}")
        
    # 2. Resumen de trades por estrategia
    try:
        cursor.execute("""
            SELECT 
                strategy,
                status,
                COUNT(*) as count,
                SUM(pnl) as total_pnl,
                AVG(pnl) as avg_pnl,
                SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses
            FROM trades 
            WHERE strategy IN ('Sniper', 'QTrend', 'BreakoutProbability')
            GROUP BY strategy, status
        """)
        trades_summary = cursor.fetchall()
        print("\n--- RESUMEN DE TRADES ---")
        for row in trades_summary:
            print(f"Estrategia: {row['strategy']} | Status: {row['status']}")
            print(f"  Cantidad: {row['count']}")
            print(f"  PNL Total: {row['total_pnl']}")
            print(f"  PNL Promedio: {row['avg_pnl']}")
            if row['status'] == 'CLOSED':
                wins = row['wins'] or 0
                losses = row['losses'] or 0
                total = wins + losses
                win_rate = (wins / total * 100) if total > 0 else 0
                print(f"  Wins: {wins} | Losses: {losses} | Win Rate: {win_rate:.1f}%")
    except Exception as e:
        print(f"Error consultando trades: {e}")
        
    conn.close()

if __name__ == "__main__":
    check()
