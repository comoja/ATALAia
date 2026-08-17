import os
import sys
from datetime import datetime

projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from middleware.database import dbConnection
from middleware.database import dbManager

def get_rate_at_time(conn, symbol, target_time):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT close FROM candles 
        WHERE symbol = %s AND timeframe = '5min' 
        ORDER BY ABS(TIMESTAMPDIFF(SECOND, timestamp, %s)) ASC 
        LIMIT 1
    """, (symbol, target_time))
    row = cursor.fetchone()
    cursor.close()
    if row and row[0]:
        return float(row[0])
    return 1.0

def main(dry_run=True):
    print(f"=== RE-CALCULANDO PNL HISTÓRICO Y CORRIGIENDO CAPITAL (DRY-RUN: {dry_run}) ===")
    
    conn = dbConnection.getConnection()
    if not conn:
        print("Error: No se pudo conectar a la base de datos.")
        return
        
    cursor = conn.cursor(dictionary=True)
    
    # 1. Obtener todos los trades cerrados
    cursor.execute("""
        SELECT idTrade, idCuenta, symbol, direction, openTime, closeTime, size, entryPrice, exitPrice, pnl, commission 
        FROM trades 
        WHERE closeTime IS NOT NULL AND status = 'CLOSED'
        ORDER BY idTrade ASC
    """)
    trades = cursor.fetchall()
    print(f"Total trades cerrados encontrados: {len(trades)}")
    
    account_adjustments = {} # idCuenta -> total PnL adjustment
    trades_to_update = [] # list of (idTrade, correct_pnl)
    
    for t in trades:
        idTrade = t['idTrade']
        idCuenta = t['idCuenta']
        symbol = t['symbol']
        direction = t['direction'].upper()
        size = float(t['size'])
        entryPrice = float(t['entryPrice'])
        exitPrice = float(t['exitPrice'])
        old_pnl = float(t['pnl']) if t['pnl'] is not None else 0.0
        commission = float(t['commission'] or 0.0)
        closeTime = t['closeTime']
        
        # Calcular PnL bruto en quote currency
        if direction == "LARGO":
            grossPnl = (exitPrice - entryPrice) * size
        else:
            grossPnl = (entryPrice - exitPrice) * size
            
        # Determinar quote currency
        symbolInfo = dbManager.getSymbol(symbol)
        quoteCurr = 'USD'
        if symbolInfo and 'quote_currency' in symbolInfo:
            quoteCurr = str(symbolInfo['quote_currency']).upper()
        elif symbolInfo and 'currencyQuote' in symbolInfo:
            quoteCurr = str(symbolInfo['currencyQuote']).upper()
            
        # Convertir a USD
        rate = 1.0
        conversion_desc = "Ninguna (USD)"
        
        if quoteCurr != 'USD' and quoteCurr != '':
            if symbol.startswith("USD/"):
                rate = exitPrice
                grossPnl = grossPnl / rate
                conversion_desc = f"Dividido por exitPrice (USD/{quoteCurr}) = {rate:.5f}"
            else:
                usd_base_symbol = f"USD/{quoteCurr}"
                quote_usd_symbol = f"{quoteCurr}/USD"
                
                rate_obtained = False
                if quoteCurr in ["GBP", "EUR", "AUD", "NZD", "BTC"]:
                    rate = get_rate_at_time(conn, quote_usd_symbol, closeTime)
                    if rate > 0:
                        grossPnl = grossPnl * rate
                        rate_obtained = True
                        conversion_desc = f"Multiplicado por rate de {quote_usd_symbol} = {rate:.5f}"
                elif quoteCurr in ["JPY", "CAD", "CHF", "MXN", "HKD"]:
                    rate = get_rate_at_time(conn, usd_base_symbol, closeTime)
                    if rate > 0:
                        grossPnl = grossPnl / rate
                        rate_obtained = True
                        conversion_desc = f"Dividido por rate de {usd_base_symbol} = {rate:.5f}"
                        
                if not rate_obtained:
                    # Fallback
                    if "JPY" in symbol:
                        rate = get_rate_at_time(conn, "USD/JPY", closeTime)
                        grossPnl = grossPnl / rate
                        conversion_desc = f"Fallback: Dividido por USD/JPY = {rate:.5f}"
                    elif "CAD" in symbol:
                        rate = get_rate_at_time(conn, "USD/CAD", closeTime)
                        grossPnl = grossPnl / rate
                        conversion_desc = f"Fallback: Dividido por USD/CAD = {rate:.5f}"
                    elif "CHF" in symbol:
                        rate = get_rate_at_time(conn, "USD/CHF", closeTime)
                        grossPnl = grossPnl / rate
                        conversion_desc = f"Fallback: Dividido por USD/CHF = {rate:.5f}"
                    elif "GBP" in symbol:
                        rate = get_rate_at_time(conn, "GBP/USD", closeTime)
                        grossPnl = grossPnl * rate
                        conversion_desc = f"Fallback: Multiplicado por GBP/USD = {rate:.5f}"
                        
        correct_pnl = round(grossPnl - commission, 2)
        diff = correct_pnl - old_pnl
        
        if abs(diff) > 0.01:
            print(f"Trade {idTrade:4d} | Cuenta {idCuenta} | {symbol:7s} | {direction:5s} | Size: {size:6.0f} | Old PnL: {old_pnl:8.2f} | New PnL: {correct_pnl:8.2f} | Diff: {diff:+8.2f} | Conv: {conversion_desc}")
            account_adjustments[idCuenta] = account_adjustments.get(idCuenta, 0.0) + diff
            trades_to_update.append((idTrade, correct_pnl))
            
    print("\n=== RESUMEN DE AJUSTES POR CUENTA ===")
    for idCuenta, total_diff in account_adjustments.items():
        # Obtener capital actual
        cursor.execute("SELECT Nombre, Capital FROM Cuenta WHERE idCuenta = %s", (idCuenta,))
        acc_info = cursor.fetchone()
        nombre = acc_info['Nombre'] if acc_info else 'Desconocido'
        old_cap = float(acc_info['Capital']) if acc_info and acc_info['Capital'] is not None else 0.0
        new_cap = old_cap + total_diff
        print(f"Cuenta {idCuenta} ({nombre:9s}) | Capital Actual: {old_cap:10.2f} | Ajuste PnL: {total_diff:+10.2f} | Nuevo Capital Proyectado: {new_cap:10.2f}")

    if not dry_run and (trades_to_update or account_adjustments):
        print("\n✏️ Aplicando cambios en la Base de Datos...")
        
        # Actualizar Trades
        for idTrade, correct_pnl in trades_to_update:
            cursor.execute("UPDATE trades SET pnl = %s WHERE idTrade = %s", (correct_pnl, idTrade))
            
        # Actualizar Cuentas
        for idCuenta, total_diff in account_adjustments.items():
            cursor.execute("UPDATE Cuenta SET Capital = Capital + %s WHERE idCuenta = %s", (total_diff, idCuenta))
            
        conn.commit()
        print("✅ Cambios guardados con éxito en la Base de Datos.")
    else:
        print("\nℹ️ No se guardaron cambios (Modo Dry-Run activo o sin diferencias).")
        
    cursor.close()
    conn.close()

if __name__ == '__main__':
    # Por defecto corre en modo dry_run
    dry_run = True
    if len(sys.argv) > 1 and sys.argv[1].lower() == 'write':
        dry_run = False
    main(dry_run=dry_run)
