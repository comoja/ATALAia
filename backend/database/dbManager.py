
import os
import sys
import mysql.connector
import pandas as pd
from datetime import datetime
# 1. Asegurar que Python vea la carpeta 'backend' como raíz
ruta_actual = os.path.dirname(os.path.abspath(__file__))
ruta_padre = os.path.join(ruta_actual, '..')
if ruta_padre not in sys.path:
    sys.path.insert(0, ruta_padre)

# 2. Importar de forma directa si están en la misma carpeta
from database import dbConnection 

import logging
logger = logging.getLogger(__name__) 

def cierraTradeEnDb(idTrade, precioCierre, fechaCierre, comentario):
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()

        sqlClose = """
            UPDATE trades 
            SET exitPrice = %s, 
                closeTime = %s
            WHERE idTrade = %s
        """
        dbCursor.execute(sqlClose, (precioCierre, fechaCierre, comentario, idTrade))
        dbConn.commit()
        
    except Exception as error:
        logger.error(f"❌ Error al cerrar trade en DB: {error}")
    finally:
        dbCursor.close()
        dbConn.close()


def verificaCierreTrade(tradeData, dfVelas):
    try:
        # 1. Convertir niveles y tiempos a formatos comparables
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        # Traemos todas las cuentas activas
        cursor.execute("SELECT *  FROM trades WHERE closeTime is null and symbol = %s", (tradeData['symbol'],))
        
        trades = cursor.fetchall()    
        for trade in trades:
            stopLoss = float(trade['stopLoss'])
            takeProfit = float(trade['takeProfit'])
            direction = trade['direction'].lower()
            idTrade = trade['idTrade']
            
            # Aseguramos que el openTime sea un objeto datetime para comparar
            openTime = tradeData['openTime']
            if isinstance(openTime, str):
                openTime = datetime.strptime(openTime, '%Y-%m-%d %H:%M:%S')

            # 2. Filtrar el DataFrame: Solo velas posteriores al openTime
            # Asumimos que la columna de fecha en dfVelas es 'datetime' y ya es tipo datetime
            dfVelas['datetime'] = pd.to_datetime(dfVelas['datetime'])
            dfPosterior = dfVelas[dfVelas['datetime'] > openTime].copy()

            if dfPosterior.empty:
                return False # No hay velas nuevas que procesar

            # 3. Iterar cronológicamente (de la más antigua a la más reciente)
            for index, row in dfPosterior.sort_values('datetime').iterrows():
                velaHigh = float(row['high'])
                velaLow = float(row['low'])
                fechaVela = row['datetime']
                precioCierre = 0
                motivoCierre = ""

                # Lógica para COMPRAS (BUY)
                if direction == 'buy':
                    if velaLow <= stopLoss:
                        precioCierre = stopLoss
                        motivoCierre = "STOP_LOSS"
                    elif velaHigh >= takeProfit:
                        precioCierre = takeProfit
                        motivoCierre = "TAKE_PROFIT"

                # Lógica para VENTAS (SELL)
                elif direction == 'sell':
                    if velaHigh >= stopLoss:
                        precioCierre = stopLoss
                        motivoCierre = "STOP_LOSS"
                    elif velaLow <= takeProfit:
                        precioCierre = takeProfit
                        motivoCierre = "TAKE_PROFIT"

                # 4. Si se activó un cierre, ejecutar en DB y salir del loop
                if precioCierre > 0:
                    print(f"🎯 Trade {idTrade} cerrado por {motivoCierre} en {fechaVela}")
                    cierraTradeEnDb(idTrade, precioCierre, fechaVela, motivoCierre)
                    return True 
        conn.close()        
    except Exception as error:
        logger.error(f"❌ Error en verificaCierreTrade: {error}")
        return False


def logTrade(symbol, regime, pf, sharpe):
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO trades (symbol, regime, pf, sharpe)
        VALUES (%s, %s, %s, %s)
    """, (symbol, regime, pf, sharpe))

    conn.commit()
    conn.close()

def getAccount(id=None):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        # Traemos todas las cuentas activas
        if id:
            cursor.execute("SELECT * FROM CUENTA WHERE idCuenta = %s ", (id,))
        else:
            cursor.execute("SELECT * FROM CUENTA WHERE Activo=1")
        
        cuentas = cursor.fetchall()  # <--- CAMBIO CLAVE: fetchall() devuelve una LISTA
        
        conn.close()
        if cuentas:
            #logger.info(f"Se cargaron {len(cuentas)} cuentas activas.")
            return cuentas
        return [] # Retorna lista vacía si no hay nada
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return []
    
def getSymbols():
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        # Traemos todas las cuentas activas
        cursor.execute("SELECT * FROM SentinelSymbol WHERE Activo=1")
        
        symbols = cursor.fetchall()  # <--- CAMBIO CLAVE: fetchall() devuelve una LISTA
        
        conn.close()
        if symbols:
            #logger.info(f"Se cargaron {len(symbols)} símbolos activos.")
            return symbols
        return [] # Retorna lista vacía si no hay nada
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return []
    
def buscaTrade(tradeData):
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)

        # Añadimos la condición de que la fecha de apertura sea HOY
        # y que el trade no tenga fecha de cierre (closeTime IS NULL)
        sqlCheck = """
            SELECT idTrade FROM trades 
            WHERE idCuenta = %s 
                AND symbol = %s 
                AND direction = %s 
                AND closeTime IS NULL 
                AND DATE(openTime) = CURDATE()
            LIMIT 1
        """
        
        paramsCheck = (
            tradeData['idCuenta'], 
            tradeData['symbol'], 
            tradeData['direction']
        )
        
        dbCursor.execute(sqlCheck, paramsCheck)
        tradeExistente = dbCursor.fetchone()

        if tradeExistente:
            # Si existe y es de hoy, actualizamos (por ejemplo, el precio actual o SL/TP)
            actualizarTrade(tradeExistente['idTrade'], tradeData)
            logger.info(f"🔄 Trade {tradeExistente['idTrade']} actualizado para {tradeData['symbol']}")
        else:
            insertarTrade(tradeData)
            logger.info(f"🆕 Nuevo trade insertado para {tradeData['symbol']}")

    except Exception as error:
        logger.error(f"❌ Error en buscaTrade: {error}")
    finally:
        if 'dbCursor' in locals(): 
            dbCursor.close()

def actualizarTrade( idTrade, data):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()
        
        sqlUpdate = """
            UPDATE trades 
            SET closeTime = %s, exitPrice = %s, pnl = %s, slippage = %s, commission = %s, openTime = %s
            WHERE idTrade = %s
        """
        valores = (
            data['closeTime'], data['exitPrice'], data['pnl'], 
            data.get('slippage', 0), data.get('commission', 0), data['openTime'], idTrade
        )
        
        cursor.execute(sqlUpdate, valores)
        conn.commit()
        logger.info(f"✅ Trade {idTrade} actualizado.")

    except Exception as e:
        logger.error(f"❌ Error al actualizarTrade {idTrade}: {e}")
        if 'conn' in locals(): conn.rollback()

def insertarTrade( data):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()

        sqlInsert = """
            INSERT INTO trades (idCuenta, symbol, direction, openTime, size, entryPrice, stopLoss, takeProfit,intervalo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)
        """
        valores = (
            data['idCuenta'], data['symbol'], data['direction'], 
            data['openTime'], data['size'], data['entryPrice'], 
            data.get('stopLoss'), data.get('takeProfit'),data['intervalo']
        )

        cursor.execute(sqlInsert, valores)
        conn.commit()
        logger.info(f"🚀 Nuevo trade insertado: {data['symbol']}")

    except Exception as e:
        logger.error(f"❌ Error al insertarTrade: {e}")
        if 'conn' in locals(): conn.rollback()
