import sys
import os
import pandas as pd

# Asegurar que el path del proyecto esté en el sistema
sys.path.append("/Volumes/TimeMachine/ATALAia")

from middleware.database import dbConnection

def analyze_trades():
    print("--- Cargando trades desde la Base de Datos ---")
    try:
        connection = dbConnection.getConnection()
        cursor = connection.cursor(dictionary=True)
        
        # Consultar todos los trades cerrados de lo que va del año (2026)
        query = """
            SELECT idTrade, symbol, strategy, direction, status, entryPrice, exitPrice, stopLoss, takeProfit, pnl, openTime, closeTime, idCuenta
            FROM trades
            WHERE closeTime IS NOT NULL AND YEAR(closeTime) = 2026
        """
        cursor.execute(query)
        trades = cursor.fetchall()
        
        cursor.close()
        connection.close()
        
        print(f"Total de trades cerrados en 2026 encontrados: {len(trades)}")
        
        if not trades:
            print("⚠️ No hay trades cerrados en 2026 en la base de datos.")
            return
            
        df = pd.DataFrame(trades)
        
        # Análisis por estrategia
        stats = []
        for strategy, group in df.groupby('strategy'):
            total_trades = len(group)
            winning_trades = len(group[group['pnl'] > 0])
            losing_trades = len(group[group['pnl'] <= 0])
            win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0
            
            total_gain = group[group['pnl'] > 0]['pnl'].sum()
            total_loss = abs(group[group['pnl'] <= 0]['pnl'].sum())
            profit_factor = total_gain / total_loss if total_loss > 0 else (total_gain if total_gain > 0 else 1.0)
            
            net_pnl = group['pnl'].sum()
            
            stats.append({
                'Estrategia': strategy,
                'Total Trades': total_trades,
                'Ganados': winning_trades,
                'Perdidos': losing_trades,
                'Win Rate (%)': round(win_rate, 2),
                'Profit Factor': round(profit_factor, 2),
                'PnL Neto ($)': round(net_pnl, 2)
            })
            
        df_stats = pd.DataFrame(stats)
        print("\n--- Estadísticas de Rendimiento en 2026 por Estrategia ---")
        print(df_stats.to_string(index=False))
        
        # Guardar en csv
        df_stats.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/scripts/resumen_rendimiento_2026.csv", index=False)
        print("\n✅ Resumen guardado en /Volumes/TimeMachine/ATALAia/Sentinel/scripts/resumen_rendimiento_2026.csv")
        
    except Exception as e:
        print(f"❌ Error al consultar o analizar la base de datos: {e}")

if __name__ == '__main__':
    analyze_trades()
